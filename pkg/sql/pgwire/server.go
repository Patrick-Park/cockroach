// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Ben Darnell

package pgwire

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

const (
	// ErrSSLRequired is returned when a client attempts to connect to a
	// secure server in cleartext.
	ErrSSLRequired = "cleartext connections are not permitted"

	// ErrDraining is returned when a client attempts to connect to a server
	// which is not accepting client connections.
	ErrDraining = "server is not accepting clients"
)

// Fully-qualified names for metrics.
var (
	MetaConns    = metric.Metadata{Name: "sql.conns"}
	MetaBytesIn  = metric.Metadata{Name: "sql.bytesin"}
	MetaBytesOut = metric.Metadata{Name: "sql.bytesout"}
)

const (
	version30     = 196608
	versionCancel = 80877102
	versionSSL    = 80877103
)

const (
	// drainMaxWait is the amount of time a draining server gives to sessions
	// with ongoing transactions to finish work before cancellation.
	drainMaxWait = 10 * time.Second
	// cancelMaxWait is the amount of time a draining server gives to sessions
	// to react to cancellation and return before a forceful shutdown.
	cancelMaxWait = 1 * time.Second
)

// baseSQLMemoryBudget is the amount of memory pre-allocated in each connection.
var baseSQLMemoryBudget = envutil.EnvOrDefaultInt64("COCKROACH_BASE_SQL_MEMORY_BUDGET",
	int64(2.1*float64(mon.DefaultPoolAllocationSize)))

// connReservationBatchSize determines for how many connections memory
// is pre-reserved at once.
var connReservationBatchSize = 5

var (
	sslSupported   = []byte{'S'}
	sslUnsupported = []byte{'N'}
)

type cancelChan struct {
	secret int32
	done   chan struct{}
	cancel context.CancelFunc
}

// cancelChanMap keeps track of channels that are closed after the associated
// cancellation function has been called and the cancellation has taken
// place. The key is the process ID field of the BackendKeyData message.
type cancelChanMap map[int32]cancelChan

// Server implements the server side of the PostgreSQL wire protocol.
type Server struct {
	AmbientCtx log.AmbientContext
	cfg        *base.Config
	executor   *sql.Executor

	metrics ServerMetrics

	mu struct {
		syncutil.Mutex
		// connCancelMap entries represent connections started when the server
		// was not draining. Each value is a function that can be called to
		// cancel the associated connection. The corresponding key is a channel
		// that is closed when the connection is done.
		connCancelMap cancelChanMap
		draining      bool
	}

	sqlMemoryPool mon.MemoryMonitor
	connMonitor   mon.MemoryMonitor
}

// ServerMetrics is the set of metrics for the pgwire server.
type ServerMetrics struct {
	BytesInCount   *metric.Counter
	BytesOutCount  *metric.Counter
	Conns          *metric.Counter
	ConnMemMetrics sql.MemoryMetrics
	SQLMemMetrics  sql.MemoryMetrics

	internalMemMetrics *sql.MemoryMetrics
}

func makeServerMetrics(
	internalMemMetrics *sql.MemoryMetrics, histogramWindow time.Duration,
) ServerMetrics {
	return ServerMetrics{
		Conns:              metric.NewCounter(MetaConns),
		BytesInCount:       metric.NewCounter(MetaBytesIn),
		BytesOutCount:      metric.NewCounter(MetaBytesOut),
		ConnMemMetrics:     sql.MakeMemMetrics("conns", histogramWindow),
		SQLMemMetrics:      sql.MakeMemMetrics("client", histogramWindow),
		internalMemMetrics: internalMemMetrics,
	}
}

// noteworthySQLMemoryUsageBytes is the minimum size tracked by the
// client SQL pool before the pool start explicitly logging overall
// usage growth in the log.
var noteworthySQLMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_SQL_MEMORY_USAGE", 100*1024*1024)

// noteworthyConnMemoryUsageBytes is the minimum size tracked by the
// connection monitor before the monitor start explicitly logging overall
// usage growth in the log.
var noteworthyConnMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_CONN_MEMORY_USAGE", 2*1024*1024)

// MakeServer creates a Server.
func MakeServer(
	ambientCtx log.AmbientContext,
	cfg *base.Config,
	executor *sql.Executor,
	internalMemMetrics *sql.MemoryMetrics,
	maxSQLMem int64,
	histogramWindow time.Duration,
) *Server {
	server := &Server{
		AmbientCtx: ambientCtx,
		cfg:        cfg,
		executor:   executor,
		metrics:    makeServerMetrics(internalMemMetrics, histogramWindow),
	}
	server.sqlMemoryPool = mon.MakeMonitor("sql",
		server.metrics.SQLMemMetrics.CurBytesCount,
		server.metrics.SQLMemMetrics.MaxBytesHist,
		0, noteworthySQLMemoryUsageBytes)
	server.sqlMemoryPool.Start(context.Background(), nil, mon.MakeStandaloneBudget(maxSQLMem))

	server.connMonitor = mon.MakeMonitor("conn",
		server.metrics.ConnMemMetrics.CurBytesCount,
		server.metrics.ConnMemMetrics.MaxBytesHist,
		int64(connReservationBatchSize)*baseSQLMemoryBudget, noteworthyConnMemoryUsageBytes)
	server.connMonitor.Start(context.Background(), &server.sqlMemoryPool, mon.BoundAccount{})

	server.mu.Lock()
	server.mu.connCancelMap = make(cancelChanMap)
	server.mu.Unlock()

	return server
}

// Match returns true if rd appears to be a Postgres connection.
func Match(rd io.Reader) bool {
	var buf readBuffer
	_, err := buf.readUntypedMsg(rd)
	if err != nil {
		return false
	}
	version, err := buf.getUint32()
	if err != nil {
		return false
	}
	return version == version30 || version == versionSSL || version == versionCancel
}

// IsDraining returns true if the server is not currently accepting
// connections.
func (s *Server) IsDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.draining
}

// Metrics returns the metrics struct.
func (s *Server) Metrics() *ServerMetrics {
	return &s.metrics
}

// SetDraining (when called with 'true') prevents new connections from being
// served and waits a reasonable amount of time for open connections to
// terminate before canceling them.
// An error will be returned when connections that have been cancelled have not
// responded to this cancellation and closed themselves in time. The server
// will remain in draining state, though open connections may continue to
// exist.
// When called with 'false', switches back to the normal mode of operation in
// which connections are accepted.
// The RFC on drain modes has more information regarding the specifics of
// what will happen to connections in different states:
// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/drain_modes.md
func (s *Server) SetDraining(drain bool) error {
	return s.setDrainingImpl(drain, drainMaxWait, cancelMaxWait)
}

func (s *Server) setDrainingImpl(
	drain bool, drainWait time.Duration, cancelWait time.Duration,
) error {
	// This anonymous function returns a copy of s.mu.connCancelMap if there are
	// any active connections to cancel. We will only attempt to cancel
	// connections that were active at the moment the draining switch happened.
	// It is enough to do this because:
	// 1) If no new connections are added to the original map all connections
	// will be cancelled.
	// 2) If new connections are added to the original map, it follows that they
	// were added when s.mu.draining = false, thus not requiring cancellation.
	// These connections are not our responsibility and will be handled when the
	// server starts draining again.
	connCancelMap := func() cancelChanMap {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.mu.draining == drain {
			return nil
		}
		s.mu.draining = drain
		if !drain {
			return nil
		}

		connCancelMap := make(cancelChanMap)
		for key, cancel := range s.mu.connCancelMap {
			connCancelMap[key] = cancel
		}
		return connCancelMap
	}()
	if len(connCancelMap) == 0 {
		return nil
	}

	// Spin off a goroutine that waits for all connections to signal that they
	// are done and reports it on allConnsDone. The main goroutine signals this
	// goroutine to stop work through quitWaitingForConns.
	allConnsDone := make(chan struct{})
	quitWaitingForConns := make(chan struct{})
	defer close(quitWaitingForConns)
	go func() {
		defer close(allConnsDone)
		for _, cancel := range connCancelMap {
			select {
			case <-cancel.done:
			case <-quitWaitingForConns:
				return
			}
		}
	}()

	// Wait for all connections to finish up to drainWait.
	select {
	case <-time.After(drainWait):
	case <-allConnsDone:
	}

	// Cancel the contexts of all sessions if the server is still in draining
	// mode.
	if stop := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.mu.draining {
			return true
		}
		for _, cancel := range connCancelMap {
			// There is a possibility that different calls to SetDraining have
			// overlapping connCancelMaps, but context.CancelFunc calls are
			// idempotent.
			cancel.cancel()
		}
		return false
	}(); stop {
		return nil
	}

	select {
	case <-time.After(cancelWait):
		return errors.Errorf("some sessions did not respond to cancellation within %s", cancelWait)
	case <-allConnsDone:
	}
	return nil
}

// ServeConn serves a single connection, driving the handshake process
// and delegating to the appropriate connection type.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	var key, secret int32

	s.mu.Lock()
	draining := s.mu.draining
	if !draining {
		var err error
		key, secret, err = s.generateBackendKeyData()
		if err != nil {
			return err
		}

		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		done := make(chan struct{})
		s.mu.connCancelMap[key] = cancelChan{
			secret: secret,
			done:   done,
			cancel: cancel,
		}
		defer func() {
			cancel()
			close(done)
			s.mu.Lock()
			delete(s.mu.connCancelMap, key)
			s.mu.Unlock()
		}()
	}
	s.mu.Unlock()

	// If the Server is draining, we will use the connection only to send an
	// error, so we don't count it in the stats. This makes sense since
	// DrainClient() waits for that number to drop to zero,
	// so we don't want it to oscillate unnecessarily.
	if !draining {
		s.metrics.Conns.Inc(1)
		defer s.metrics.Conns.Dec(1)
	}

	var buf readBuffer
	n, err := buf.readUntypedMsg(conn)
	if err != nil {
		return err
	}
	s.metrics.BytesInCount.Inc(int64(n))
	version, err := buf.getUint32()
	if err != nil {
		return err
	}
	errSSLRequired := false
	if version == versionSSL {
		if len(buf.msg) > 0 {
			return errors.Errorf("unexpected data after SSLRequest: %q", buf.msg)
		}

		if s.cfg.Insecure {
			if _, err := conn.Write(sslUnsupported); err != nil {
				return err
			}
		} else {
			if _, err := conn.Write(sslSupported); err != nil {
				return err
			}
			tlsConfig, err := s.cfg.GetServerTLSConfig()
			if err != nil {
				return err
			}
			conn = tls.Server(conn, tlsConfig)
		}

		n, err := buf.readUntypedMsg(conn)
		if err != nil {
			return err
		}
		s.metrics.BytesInCount.Inc(int64(n))
		version, err = buf.getUint32()
		if err != nil {
			return err
		}
	} else if !s.cfg.Insecure {
		errSSLRequired = true
	}

	if version == version30 {
		// We make a connection before anything. If there is an error
		// parsing the connection arguments, the connection will only be
		// used to send a report of that error.
		v3conn := makeV3Conn(conn, &s.metrics, &s.sqlMemoryPool, s.executor)
		defer v3conn.finish(ctx)

		if v3conn.sessionArgs, err = parseOptions(buf.msg); err != nil {
			return v3conn.sendInternalError(err.Error())
		}

		if errSSLRequired {
			return v3conn.sendInternalError(ErrSSLRequired)
		}
		if draining {
			return v3conn.sendError(newAdminShutdownErr(errors.New(ErrDraining)))
		}

		v3conn.sessionArgs.User = parser.Name(v3conn.sessionArgs.User).Normalize()
		if err := v3conn.handleAuthentication(ctx, s.cfg.Insecure); err != nil {
			return v3conn.sendInternalError(err.Error())
		}

		// Reserve some memory for this connection using the server's
		// monitor. This reduces pressure on the shared pool because the
		// server monitor allocates in chunks from the shared pool and
		// these chunks should be larger than baseSQLMemoryBudget.
		//
		// We only reserve memory to the connection monitor after
		// authentication has completed successfully, so as to prevent a DoS
		// attack: many open-but-unauthenticated connections that exhaust
		// the memory available to connections already open.
		acc := s.connMonitor.MakeBoundAccount()
		if err := acc.Grow(ctx, baseSQLMemoryBudget); err != nil {
			return errors.Errorf("unable to pre-allocate %d bytes for this connection: %v",
				baseSQLMemoryBudget, err)
		}

		err := v3conn.serve(ctx, s.IsDraining, acc, key, secret)
		// If the error that closed the connection is related to an
		// administrative shutdown, relay that information to the client.
		if code, ok := pgerror.PGCode(err); ok && code == pgerror.CodeAdminShutdownError {
			return v3conn.sendError(err)
		}
		return err
	}

	if version == versionCancel {
		key, err := buf.getUint32()
		if err != nil {
			return err
		}
		secret, err := buf.getUint32()
		if err != nil {
			return err
		}
		s.mu.Lock()
		cancel, ok := s.mu.connCancelMap[int32(key)]
		s.mu.Unlock()
		if ok && cancel.secret == int32(secret) {
			cancel.cancel()
		}
		return nil
	}

	return errors.Errorf("unknown protocol version %d", version)
}

// generateBackendKeyData generates 2 random int32s, both guaranteed to be
// non-zero. The first (the key) is guaranteed to be unique among the server's
// active connection's keys. Should be called with s.mu already locked.
func (s *Server) generateBackendKeyData() (int32, int32, error) {
	const tryLimit = 100
	var key, secret int32
	tries := 0
	for key == 0 && tries < tryLimit {
		tries++
		err := binary.Read(rand.Reader, binary.BigEndian, &key)
		if err != nil {
			return 0, 0, err
		}
		// guarantee key is unique
		if _, ok := s.mu.connCancelMap[key]; ok {
			key = 0
		}
	}
	for secret == 0 && tries < tryLimit {
		tries++
		err := binary.Read(rand.Reader, binary.BigEndian, &secret)
		if err != nil {
			return 0, 0, err
		}
	}
	if key == 0 {
		return 0, 0, errors.New("could not generate backend key data")
	}
	return key, secret, nil
}
