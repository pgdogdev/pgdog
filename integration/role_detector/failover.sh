#!/bin/bash
docker stop $(docker ps | grep 45000 | awk '{print $1}')
PGPASSWORD=postgres psql -h 127.0.0.1 -p 45001 -U postgres postgres -c 'SELECT pg_promote()'
