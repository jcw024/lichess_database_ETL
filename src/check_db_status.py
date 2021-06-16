from CONFIG import DB_NAME, DB_USER
import psycopg2

if __name__ == "__main__":
    connect_string = "dbname=" + DB_NAME + " user=" + DB_USER
    conn = psycopg2.connect(connect_string)
    cur = conn.cursor()
    cur.execute("select pg_size_pretty(pg_relation_size('games'));")
    print(f"games table size: {cur.fetchone()[0]}")
    """
    cur.execute("select * from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'games';")
    column_data = cur.fetchall()
    columns = []
    for c in column_data:
        columns.append(c[3])
    cur.execute("select * from games limit 5;")
    print("top 5 rows:")
    print(columns)
    for row in cur.fetchall():
        print(row)
    """
    cur.execute("SELECT COUNT(*) FROM games;")
    print(f"total rows: {cur.fetchall()}")
