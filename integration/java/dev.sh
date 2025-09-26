#!/bin/bash
set -e

if [[ ! -f postgres.jar ]]; then
    curl -L https://jdbc.postgresql.org/download/postgresql-42.7.5.jar > postgres.jar
fi

CLASS_PATH="$PWD:$PWD/postgres.jar"

javac pgdog.java
java -cp ${CLASS_PATH} -ea Pgdog
