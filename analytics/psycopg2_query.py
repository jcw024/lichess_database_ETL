import psycopg2
import csv

def select_query(sql_list, conn, filenames=None):
    """takes in a query or list of queries, a psycopg2 connection, and a list of filenames 
    and writes the output to csv files"""
    if isinstance(sql_list, str):
        sql_list = (sql_list,)
    if filenames is None:
        filenames = []
        for i in range(0,len(sql_list)):
            filenames.append("results_" + str(i) + ".csv")
    elif len(sql_list) != len(filenames):
        print("length of filenames != number of sql queries")
        quit()

    cur = conn.cursor()
    for (sql, filename) in zip(sql_list, filenames):
        print("RUNNING: \n" + sql)
        cur.execute(sql)
        colnames = [desc[0] for desc in cur.description]
        with open(filename, 'w') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(colnames)
            for r in cur:
                csv_out.writerow(r)


if __name__ == "__main__":
    DB_NAME = "lichess_games_db"
    DB_USER = "joe"
    connect_string = "dbname=" + DB_NAME + " user=" + DB_USER
    conn = psycopg2.connect(connect_string)

    queries = []
    filenames = []
    queries.append(
    """
    SELECT * FROM GAMES limit 1000;
    """
    )
    queries.append(
    """
    SELECT * FROM user_ids limit 10;
    """
    )
    select_query(queries, conn)

    #conn.commit()
