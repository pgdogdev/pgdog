#!/bin/bash
#
# pgBench test run.
#
pgbench -P 1 -h 127.0.0.1 -p 6432 -U pgdog pgdog -c 50 -t 1000 -S
