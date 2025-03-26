#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd ${SCRIPT_DIR}
if [[ ! -f data.parquet ]]; then
    curl -L https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings/resolve/main/data/train-00000-of-00004-1a1932c9ca1c7152.parquet?download=true > data.parquet
fi
popd

for shard in {0..15}; do
    createdb "shard_${shard}"
    psql -c "grant all on database shard_${shard} to pgdog"
    psql -c "grant all on schema public to pgdog" "shard_${shard}"
    psql -c "ALTER DATABASE shard_${shard} REFRESH COLLATION VERSION"
done
