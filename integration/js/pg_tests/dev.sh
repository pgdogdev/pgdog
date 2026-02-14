#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd ${SCRIPT_DIR}

npm install

# Generate Prisma client
DATABASE_URL="postgresql://pgdog:pgdog@127.0.0.1:6432/pgdog" npx prisma generate

timeout 60 npm test

popd
