#!/bin/bash
#
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -e
cd ${SCRIPT_DIR}/../
cargo doc
cd target/doc
aws s3 sync pgdog_plugin s3://pgdog-docsrs/pgdog_plugin
aws s3 sync pgdog_macros s3://pgdog-docsrs/pgdog_macros
aws s3 sync pgdog s3://pgdog-docsrs/pgdog
aws s3 sync pg_query  s3://pgdog-docsrs/pg_query
aws s3 cp ${SCRIPT_DIR}/docs_redirect.html s3://pgdog-docsrs/index.html
