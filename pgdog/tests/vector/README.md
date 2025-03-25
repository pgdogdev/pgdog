# Sharding pgvector

This demo uses [Cohere/wikipedia](https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings/blob/main/data/train-00000-of-00004-1a1932c9ca1c7152.parquet) dataset. Embeddings are in 768 dimensions (float32).

## Setup

Install [pgvector](https://github.com/pgvector/pgvector) into your shards. Make sure to run:

```postgresql
CREATE EXTENSION vector;
```

Download the parket file by running `setup.sh`:

```bash
bash setup.sh
```

Setup venv by running `run.sh`:

```bash
bash run.sh
```

## Calculate k-means

Calculate k-means centroids:

```bash
python read_parquet.py --file data.parquet --kmeans
```

Add `--plot` for a visualization. Centroids will be written to `centroids.json`. Copy this file into working directory for PgDog and add it to a sharded table:

```toml
[[sharded_tables]]
database = "pgdog_sharded"
name = "embeddings"
data_type = "vector"
column = "embedding"
centroids_path = "centroids.json"
```

## Ingest data

Ingest (and shard) data:

```bash
python read_parquet.py --file data.parquet
```

## Query

```bash
export PGPASSWORD=pgdog
psql -f select.sql -U pgdog -h 127.0.0.1 -p 6432 -d pgdog
```
