#!/bin/bash
#
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -e
cd ${SCRIPT_DIR}/../
cargo doc
cd target/doc
aws s3 sync pgdog_plugin s3://pgdog-docsrs/pgdog_plugin
aws s3 sync pgdog s3://pgdog-docsrs/pgdog
aws s3 sync pgdog_plugin_build s3://pgdog-docsrs/pgdog_plugin_build
aws s3 cp ${SCRIPT_DIR}/docs_redirect.html s3://pgdog-docsrs/index.html
