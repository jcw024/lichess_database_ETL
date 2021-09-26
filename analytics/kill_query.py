import psycopg2

if __name__ == "__main__":
    #NOTE: haven't got this to work on psycopg2, but the query can be run on postgres to kill long queries
    DB_NAME = "lichess_games_db"
    DB_USER = "joe"
    connect_string = "dbname=" + DB_NAME + " user=" + DB_USER
    conn = psycopg2.connect(connect_string)

    queries = []
    filenames = []
    q0 = "select pg_cancel_backend(select pid FROM pg_stat_activity where state='active');"
    cur = conn.cursor()
    cur.execute(q0)
    conn.commit()
