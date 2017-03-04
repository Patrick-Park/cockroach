#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

# Check that \? prints the help text.
send "\\?\r"
eexpect "You are using"
eexpect "More documentation"
eexpect root@

stop_server $argv
