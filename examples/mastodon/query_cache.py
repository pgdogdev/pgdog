import psycopg
import csv

def data():
    conn = psycopg.connect("host=127.0.0.1 port=6432 user=admin password=pgdog dbname=admin")
    cur = conn.cursor()
    conn.autocommit = True
    cur.execute("SHOW QUERY_CACHE")
    return cur.fetchall()

def to_csv():
    with open("query_cache.csv", "w") as f:
        writer = csv.writer(f)
        for row in data():
            writer.writerow(row)

if __name__ == "__main__":
    to_csv()
