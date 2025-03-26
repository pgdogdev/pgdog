import psycopg
import json

with open("test_embeddings.json") as f:
    embeddings = json.load(f)

if __name__ == "__main__":
    conn = psycopg.connect("user=pgdog password=pgdog dbname=pgdog_sharded host=127.0.0.1 port=6432") # change dbname to pgdog to get ground truth
    cur = conn.cursor()

    results = []
    for embedding in embeddings:
        vec = str(embedding[0])
        cur.execute("SELECT embedding FROM embeddings WHERE embedding <-> %s < 0.1 ORDER BY embedding <-> %s LIMIT 5", (vec,vec,))
        neighbors = cur.fetchall()
        results.append(len(neighbors))
        conn.commit()

    hits = 0
    misses = 0
    for h in results:
        if h > 0:
            hits += 1
        else:
            misses += 1
    print(f"hits: {hits}, misses: {misses}")
