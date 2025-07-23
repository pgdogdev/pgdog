#
# Download <https://www.kaggle.com/datasets/lokeshparab/gutenberg-books-and-metadata-2025?resource=download>
#
# Untar it and pass the path of the "archive" folder to this script
# as the first argument and the number of books you want to ingest as the second argument.
#
# e.g.: python3 gutenberg.py /Users/lev/Downloads/archive 80000
#
# Dataset is 16GB in Postgres.
#
import csv
import psycopg
import sys
from os.path import join, exists
from tqdm import tqdm
from datetime import datetime

def schema(cursor: psycopg.Cursor):
    cursor.execute("""CREATE TABLE IF NOT EXISTS pgdog.books (
        id BIGINT PRIMARY KEY,
        title VARCHAR,
        content VARCHAR
    )""")

def ingest(path: str, conn: psycopg.Cursor, limit: int):
    with conn.copy("COPY pgdog.books (id, title, content) FROM STDIN") as copy:
        with open(join(path, "gutenberg_metadata.csv")) as f:
            reader = csv.reader(f)

            # Headers
            next(reader)
            progress = tqdm(total=limit)
            bytes = 0
            start = datetime.now()

            for row in reader:
                id = int(row[0])
                title = row[3]

                if id >= limit:
                    break

                book_path = join(path, "books", str(id))

                if not exists(book_path):
                    continue

                with open(book_path) as book:
                    book = book.read()

                copy.write_row([id, title, book])

                bytes += len(str(id)) + len(title) + len(book)
                elapsed = start - datetime.now()
                bytes_per_sec = bytes / elapsed.seconds
                progress.update(1)
                progress.set_postfix(speed=f'{bytes_per_sec / 1024} KB/s')

if __name__ == "__main__":
    path = sys.argv[1]
    limit = int(sys.argv[2])

    conn = psycopg.connect("postgres://pgdog:pgdog@127.0.0.1:5432/pgdog")
    conn.autocommit = True

    schema(conn.cursor())
    conn.cursor().execute("TRUNCATE TABLE pgdog.books")
    ingest(path, conn.cursor(), limit)
