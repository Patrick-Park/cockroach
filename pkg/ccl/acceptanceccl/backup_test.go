// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package acceptanceccl

// The benchmarks in this file are remote tests that use Terrafarm to manage
// and run tests against dedicated test clusters. See allocator_test.go for
// instructions on how to set this up to run locally. Also note that you likely
// want to increase `-benchtime` to something more like 5m (the default is 1s).

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance"
	"github.com/cockroachdb/cockroach/pkg/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const longWaitTime = 2 * time.Minute

const (
	archivedStoreURL    = "gs://cockroach-test/allocatortest"
	urlStore3s          = archivedStoreURL + "/3nodes-10g-262ranges"
	bulkArchiveStoreURL = "gs://cockroach-test/bulkops/10nodes-2t-50000ranges"
)

type benchmarkTest struct {
	b testing.TB
	// nodes is the number of nodes this cluster will have.
	nodes int
	// storeURL is the Google Cloud Storage URL from which the test will
	// download stores. Nothing is downloaded if storeURL is empty.
	storeURL string
	// prefix is the prefix that will be prepended to all resources created by
	// Terraform.
	prefix string
	// cockroachDiskSizeGB is the size, in gigabytes, of the disks allocated
	// for CockroachDB nodes. Leaving this as 0 accepts the default in the
	// Terraform configs. This must be in GB, because Terraform only accepts
	// disk size for GCE in GB.
	cockroachDiskSizeGB int
	// copyStore is true if the archive store is many *.sst and other store
	// files instead of a single .tgz.
	copyStore bool

	f *terrafarm.Farmer
}

func (bt *benchmarkTest) Start(ctx context.Context) {
	bt.f = acceptance.MakeFarmer(bt.b, bt.prefix, acceptance.GetStopper())

	if bt.cockroachDiskSizeGB != 0 {
		bt.f.AddVars["cockroach_disk_size"] = strconv.Itoa(bt.cockroachDiskSizeGB)
	}

	log.Infof(ctx, "creating cluster with %d node(s)", bt.nodes)
	if err := bt.f.Resize(bt.nodes); err != nil {
		bt.b.Fatal(err)
	}
	acceptance.CheckGossip(ctx, bt.b, bt.f, longWaitTime, acceptance.HasPeers(bt.nodes))
	bt.f.Assert(ctx, bt.b)
	log.Info(ctx, "initial cluster is up")

	if bt.storeURL == "" {
		log.Info(ctx, "no stores are necessary")
		return
	}

	// We must stop the cluster because `nodectl` pokes at the data directory.
	log.Info(ctx, "stopping cluster")
	for i := 0; i < bt.f.NumNodes(); i++ {
		if err := bt.f.Kill(ctx, i); err != nil {
			bt.b.Fatalf("error stopping node %d: %s", i, err)
		}
	}

	log.Info(ctx, "downloading archived stores from Google Cloud Storage in parallel")
	errors := make(chan error, bt.f.NumNodes())
	for i := 0; i < bt.f.NumNodes(); i++ {
		go func(nodeNum int) {
			if !bt.copyStore {
				errors <- bt.f.Exec(nodeNum, "./nodectl download "+bt.storeURL)
			} else {
				cmd := fmt.Sprintf(`gsutil -m cp -r "%s/node%d/*" "%s"`, bt.storeURL, nodeNum, "/mnt/data0")
				log.Infof(ctx, "exec on node %d: %s", nodeNum, cmd)
				errors <- bt.f.Exec(nodeNum, cmd)
			}
		}(i)
	}
	for i := 0; i < bt.f.NumNodes(); i++ {
		if err := <-errors; err != nil {
			bt.b.Fatalf("error downloading store %d: %s", i, err)
		}
	}

	log.Info(ctx, "restarting cluster with archived store(s)")
	for i := 0; i < bt.f.NumNodes(); i++ {
		if err := bt.f.Restart(ctx, i); err != nil {
			bt.b.Fatalf("error restarting node %d: %s", i, err)
		}
	}
	acceptance.CheckGossip(ctx, bt.b, bt.f, longWaitTime, acceptance.HasPeers(bt.nodes))
	bt.f.Assert(ctx, bt.b)
}

func (bt *benchmarkTest) Close(ctx context.Context) {
	if r := recover(); r != nil {
		bt.b.Errorf("recovered from panic to destroy cluster: %v", r)
	}
	if bt.f != nil {
		log.Infof(ctx, "shutting down cluster")
		bt.f.MustDestroy(bt.b)
	}
}

func BenchmarkBackupBig(b *testing.B) {
	bt := benchmarkTest{
		b:                   b,
		nodes:               3,
		storeURL:            urlStore3s,
		cockroachDiskSizeGB: 250,
		prefix:              "backup",
	}

	ctx := context.Background()
	defer bt.Close(ctx)
	bt.Start(ctx)

	db, err := gosql.Open("postgres", bt.f.PGUrl(ctx, 0))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// (mis-)Use a sub benchmark to avoid running the setup code more than once.
	b.Run("", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			if err := os.RemoveAll("/mnt/data0/BenchmarkBackupBig"); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			log.Infof(ctx, "starting backup")
			row := db.QueryRow(`BACKUP DATABASE foo TO '/mnt/data0/BenchmarkBackupBig'`)
			var unused string
			var dataSize int64
			if err := row.Scan(&unused, &unused, &unused, &dataSize); err != nil {
				bt.b.Fatal(err)
			}
			b.SetBytes(dataSize)
			log.Infof(ctx, "backed up %s", humanizeutil.IBytes(dataSize))
		}
	})
}

func BenchmarkBackup2TB(b *testing.B) {
	bt := benchmarkTest{
		b:                   b,
		nodes:               10,
		storeURL:            bulkArchiveStoreURL,
		copyStore:           true,
		cockroachDiskSizeGB: 250,
		prefix:              "backup2tb",
	}

	ctx := context.Background()
	bt.Start(ctx)
	defer bt.Close(ctx)

	db, err := gosql.Open("postgres", bt.f.PGUrl(ctx, 0))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// (mis-)Use a sub benchmark to avoid running the setup code more than once.
	b.Run("", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			backupBaseURI := fmt.Sprintf("gs://%s/BenchmarkBackup2TB/%s-%d",
				acceptanceBucket,
				timeutil.Now().Format(time.RFC3339Nano),
				b.N,
			)

			log.Infof(ctx, "starting backup")
			row := db.QueryRow(fmt.Sprintf(`BACKUP DATABASE foo TO '%s'`, backupBaseURI))
			var unused string
			var dataSize int64
			if err := row.Scan(&unused, &unused, &unused, &dataSize); err != nil {
				bt.b.Fatal(err)
			}
			b.SetBytes(dataSize)
			log.Infof(ctx, "backed up %s", humanizeutil.IBytes(dataSize))
		}
	})
}

const (
	backupRestoreRowPayloadSize = 100

	// TODO(mjibson): attempt to unify these with the identical ones in sqlccl.
	bankCreateDatabase = `CREATE DATABASE bench`
	bankCreateTable    = `CREATE TABLE bench.bank (
		id INT PRIMARY KEY,
		balance INT,
		payload STRING,
		FAMILY (id, balance, payload)
	)`
	bankInsert = `INSERT INTO bench.bank VALUES (%d, %d, '%s')`
)

// restoreBucket is the name of the gs bucket to save the generated files
// used during restoration. This bucket should be configured with a TTL on
// files so it doesn't grow forever. Create a lifecycle.json file with:
//
// {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 5}}]}
//
// Then run: $ gsutil lifecycle set lifecycle.json gs://cockroach-acceptance
const acceptanceBucket = "cockroach-acceptance"

func BenchmarkRestoreBig(b *testing.B) {
	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()

	container := os.Getenv("AZURE_CONTAINER")
	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	if container == "" || accountName == "" || accountKey == "" {
		b.Fatal("env variables AZURE_CONTAINER, AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY must be set")
	}

	bt := benchmarkTest{
		b:                   b,
		nodes:               3,
		cockroachDiskSizeGB: 250,
		prefix:              "restore",
	}

	defer bt.Close(ctx)
	bt.Start(ctx)

	sqlDB, err := gosql.Open("postgres", bt.f.PGUrl(ctx, 0))
	if err != nil {
		b.Fatal(err)
	}
	defer sqlDB.Close()

	r := sqlutils.MakeSQLRunner(b, sqlDB)

	r.Exec(bankCreateDatabase)

	// (mis-)Use a sub benchmark to avoid running the setup code more than once.
	b.Run("", func(b *testing.B) {
		log.Info(ctx, b.N)
		restoreBaseURI := fmt.Sprintf("azure://%s/BenchmarkRestoreBig/%s-%d?%s=%s&%s=%s",
			container,
			timeutil.Now().Format(time.RFC3339Nano),
			b.N,
			storageccl.AzureAccountNameParam,
			accountName,
			storageccl.AzureAccountKeyParam,
			accountKey,
		)
		log.Info(ctx, restoreBaseURI)

		var buf bytes.Buffer
		buf.WriteString(bankCreateTable)
		buf.WriteString(";\n")
		for i := 0; i < b.N; i++ {
			payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)
			fmt.Fprintf(&buf, bankInsert, i, 0, payload)
			buf.WriteString(";\n")
		}

		ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
		desc, err := sqlccl.Load(ctx, sqlDB, &buf, "bench", restoreBaseURI, ts, 0)
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		log.Infof(ctx, "starting restore")
		r.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, restoreBaseURI))
		b.SetBytes(desc.DataSize / int64(b.N))
		log.Infof(ctx, "restored %s", humanizeutil.IBytes(desc.DataSize))
		b.StopTimer()
	})
}
