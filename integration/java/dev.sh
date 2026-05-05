#!/bin/bash
set -e

if [[ ! -f postgres.jar ]]; then
    curl -L https://jdbc.postgresql.org/download/postgresql-42.7.5.jar > postgres.jar
fi

export CLASSPATH="$PWD/out:$PWD/postgres.jar"

mkdir -p out
javac -d out pgdog.java
java -ea Pgdog
