import pyarrow.parquet as pq
import sys
import psycopg
import click
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import numpy as np
import json
import matplotlib.pyplot as plt
import matplotlib.figure as figure

@click.command()
@click.option("--file", help="Parquet file with data")
@click.option("--kmeans/--ingest", default=False, help="Calculate centroids")
@click.option("--plot/--no-plot", default=False, help="Plot with centroids")
def read(file, kmeans, plot):
    if kmeans:
        X = []
        emb = pd.read_parquet(file, columns=["emb"]).iloc[:,0]
        for col in emb:
            l = col.tolist()
            X.append(l)

        kmeans = KMeans(n_clusters=16, random_state=0, n_init="auto").fit(X)
        centroids = kmeans.cluster_centers_.tolist()
        with open("centroids.json", "w") as f:
            json.dump(centroids, f)
            print("Centroids written to centroids.json")

        if plot:
            plt.figure(figsize=(800, 600))
            plt.axis("off")
            reduced = PCA(n_components=2).fit(X).transform(X)
            x = [v[0] for v in reduced]
            y = [v[1] for v in reduced]
            plt.scatter(x, y, linestyle="None", marker=".", color='g')
            reduced = PCA(n_components=2).fit(centroids).transform(centroids)
            x = [v[0] for v in reduced]
            y = [v[1] for v in reduced]
            plt.scatter(x, y, linestyle="None", marker="x", color='r', s=120)

            plt.show()

    else:
        conn = psycopg.connect("host=127.0.0.1 port=6432 user=pgdog password=pgdog dbname=pgdog_sharded")
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS embeddings (id BIGINT, title TEXT, body TEXT, embedding vector(768))")
        conn.commit()
        file = pq.ParquetFile(file)
        count = 0
        for batch in file.iter_batches(batch_size=100):
            with cur.copy("COPY embeddings (id, title, body, embedding) FROM STDIN") as copy:
                for record in batch.to_pylist():
                    copy.write_row([record["id"], record["title"], record["text"], str(record["emb"])])
                    count += 1
            print(f"Ingested {count} records")
            conn.commit()

if __name__ == "__main__":
    read()
