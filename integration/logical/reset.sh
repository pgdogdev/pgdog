#!/bin/bash
psql -p 5433 pgdog -c 'TRUNCATE TABLE user_profiles; TRUNCATE TABLE users;'
psql -p 5434 pgdog -c 'TRUNCATE TABLE user_profiles; TRUNCATE TABLE users;'
