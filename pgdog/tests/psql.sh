#!/bin/bash
PGPASSWORD=pgdog psql -h 127.0.0.1 -p 6432 -U ${1:-pgdog} ${2:-pgdog}
