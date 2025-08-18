#!/bin/bash
#
set -e
cargo doc
cd target/doc
aws s3 sync pgdog_plugin s3://pgdog-docsrs/pgdog_plugin
aws s3 sync pgdog s3://pgdog-docsrs/pgdog
aws s3 sync pgdog_plugin_build s3://pgdog-docsrs/pgdog_plugin_build
