import psycopg
from psycopg import ClientCursor
from logging import getLogger, DEBUG

logger = getLogger(__name__)
logger.setLevel(DEBUG)

tables = """SELECT table_name FROM information_schema.tables WHERE table_schema='public'"""

foreign_key = """
SELECT
    r.table_name
FROM information_schema.constraint_column_usage       u
INNER JOIN information_schema.referential_constraints fk
           ON u.constraint_catalog = fk.unique_constraint_catalog
               AND u.constraint_schema = fk.unique_constraint_schema
               AND u.constraint_name = fk.unique_constraint_name
INNER JOIN information_schema.key_column_usage        r
           ON r.constraint_catalog = fk.constraint_catalog
               AND r.constraint_schema = fk.constraint_schema
               AND r.constraint_name = fk.constraint_name
WHERE
    u.column_name::TEXT = %s::TEXT AND
    u.table_catalog = 'mastodon_development' AND
    u.table_schema = 'public' AND
    u.table_name::TEXT = %s::TEXT
"""

table_columns = """SELECT column_name
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name  = %s
   AND column_name LIKE '%%id'
     ;"""

def get_fks(table, cur, checked):
    if table in checked:
        return []

    cur.execute(table_columns, [table])
    columns = cur.fetchall()

    foreign_keys = []

    for column in columns:
        column = column[0]
        cur.execute(foreign_key, [column, table])
        fks = cur.fetchall()
        foreign_keys = foreign_keys + fks
    checked.add(table)

    if not foreign_keys:
        return foreign_keys

    result = foreign_keys

    for fk in foreign_keys:
        result += get_fks(fk[0], cur, checked)

    return result

if __name__ == "__main__":
    conn = psycopg.connect("dbname=mastodon_development", cursor_factory=ClientCursor)
    cur = conn.cursor()

    cur.execute(tables)
    tables = cur.fetchall()
    foreign_keys = []

    for table in tables:
        checked = set()
        table = table[0]
        cur.execute(foreign_key, ["id", table])
        fks = get_fks(table, cur, checked)
        foreign_keys.append((table, len(fks)))
    sorted_keys = sorted(foreign_keys, key=lambda table: table[1])
    print(sorted_keys[-1])
