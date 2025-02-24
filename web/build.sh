#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

rm -rf /tmp/pgdog-web
mkdir -p /tmp/pgdog-web/docs

pushd "$SCRIPT_DIR/../docs"
source venv/bin/activate
mkdocs build
cp -R site/* /tmp/pgdog-web/docs/
popd

pushd "$SCRIPT_DIR"
cp * /tmp/pgdog-web/

pushd /tmp/pgdog-web

zip -r pgdog.zip -x blog/**/*.md .
mv pgdog.zip "$SCRIPT_DIR"
popd
popd
