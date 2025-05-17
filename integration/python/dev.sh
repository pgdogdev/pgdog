#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd ${SCRIPT_DIR}

virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

pytest -x

popd
