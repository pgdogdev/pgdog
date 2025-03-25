import pyarrow.parquet as pq
import sys
import psycopg
import click
import pandas as pd
from sklearn.cluster import KMeans
import numpy as np

@click.command()
@click.option("--file", help="Parquet file with data")
@click.option("--kmeans/--ingest", default=False, help="Calculate centroids")
def read(file, kmeans):
    if kmeans:
        X = []
        emb = pd.read_parquet(file, columns=["emb"]).iloc[:,0]
        for col in emb:
            l = col.tolist()
            X.append(l)
        kmeans = KMeans(n_clusters=16, random_state=0, n_init="auto").fit(X)
        print(kmeans.cluster_centers_.tolist())
    else:
        conn = psycopg.connect("host=127.0.0.1 port=6432 user=pgdog password=pgdog dbname=pgdog_sharded")
        cur = conn.cursor()
        file = pq.ParquetFile(file)
        for batch in file.iter_batches(batch_size=100):
            with cur.copy("COPY embeddings (id, title, body, embedding) FROM STDIN") as copy:
                for record in batch.to_pylist():
                    copy.write_row([record["id"], record["title"], record["text"], str(record["emb"])])
            print("COPYed 100 records")
            conn.commit()

if __name__ == "__main__":
    read()
