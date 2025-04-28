import psycopg
import csv
import sqlite3

def data():
    conn = psycopg.connect("host=127.0.0.1 port=6432 user=admin password=pgdog dbname=admin")
    cur = conn.cursor()
    conn.autocommit = True
    cur.execute("SHOW QUERY_CACHE")
    return cur.fetchall()

def fetch_data():
    conn = sqlite3.connect("query_cache.sqlite3")
    conn.execute("""CREATE TABLE IF NOT EXISTS query_cache (
        query TEXT,
        hits INTEGER,
        direct INTEGER,
        multi INTEGER
    )""")
    conn.execute("DELETE FROM query_cache");
    rows = data()
    for query in rows:
        query = list(query)
        for i in range(1, 4):
            query[i] = int(query[i])
        cur = conn.execute("SELECT COUNT(*) FROM query_cache WHERE query = ?", [query[0]])
        exists = cur.fetchone()
        if exists[0] == 1:
            conn.execute(
                "UPDATE query_cache SET hits = hits + ?, direct = direct + ?, multi = multi + ? WHERE query = ?",
                [query[1], query[2], query[3], query[0]]
            )
        else:
            conn.execute("INSERT INTO query_cache VALUES (?, ?, ?, ?)", query)
    conn.commit()


def to_csv():
    with open("query_cache.csv", "w") as f:
        writer = csv.writer(f)
        for row in data():
            writer.writerow(row)

if __name__ == "__main__":
    fetch_data()
