import psycopg

def admin():
    conn = psycopg.connect("dbname=admin user=admin password=pgdog host=127.0.0.1 port=6432")
    conn.autocommit = True
    return conn

def no_out_of_sync():
    conn = admin()
    cur = conn.cursor()
    cur.execute("SHOW POOLS;")
    pools = cur.fetchall()
    for pool in pools:
        print(pools)
        assert pool[-1] == 0
