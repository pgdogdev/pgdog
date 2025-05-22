#!/bin/bash
export PGPORT=${1:-5433}
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres postgres
