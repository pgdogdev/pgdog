#!/bin/bash
#
# pgBench test run.
#
PGPASSWORD=pgdog pgbench -P 1 -h 127.0.0.1 -p 6432 -U pgdog pgdog -c 50 -t 100000 -S
