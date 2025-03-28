#!/bin/bash
set -ex
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

pushd ${SCRIPT_DIR}

if [[ ! -f postgres.jar ]]; then
    curl -L https://jdbc.postgresql.org/download/postgresql-42.7.5.jar > postgres.jar
fi

CLASS_PATH="$PWD:$PWD/postgres.jar"

javac pgdog.java
java -cp ${CLASS_PATH} Pgdog
popd
