import psycopg

if __name__ == "__main__":
    conn = psycopg.connect("user=pgdog password=pgdog dbname=pgdog_sharded host=127.0.0.1 port=6432")
    cur = conn.cursor()

    cur.execute("SELECT embedding FROM embeddings LIMIT 100")
    embeddings = cur.fetchall()
    hits = []
    for embedding in embeddings:
        vec = str(embedding[0])
        cur.execute("SELECT embedding FROM embeddings WHERE embedding <-> %s < 0.1 ORDER BY embedding <-> %s LIMIT 5", (vec,vec,))
        neighbors = cur.fetchall()
        hits.append(len(neighbors))
    print(hits)
