#!/bin/bash
export PGPASSWORD=pgdog
psql -h 127.0.0.1 -p 6432 -U pgdog failover
