#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

rm -rf /tmp/pgdog-web
mkdir -p /tmp/pgdog-web/docs

pushd "$SCRIPT_DIR/../docs"
source venv/bin/activate
mkdocs build
rsync -av --progress site/ /tmp/pgdog-web/docs/
popd

pushd "$SCRIPT_DIR"
rsync -av --progress ${PWD}/ /tmp/pgdog-web/ \
--exclude venv \
--exclude *.md \
--exclude *.toml \
--exclude *.sh \
--exclude *.scss \
--exclude *.py \
--exclude pgdog.zip \
--exclude *.DS_Store \
--exclude requirements.txt \
--exclude blog/head.html \
--exclude blog/footer.html \

pushd /tmp/pgdog-web

zip -r pgdog.zip .

mv pgdog.zip "$SCRIPT_DIR"
popd
popd
