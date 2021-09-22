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
    #baseline query ~30 minutes to run, 7.5M disk page fetches
    q0 = "EXPLAIN SELECT COUNT(*) FROM Games;"

    q1 = """
    (SELECT * FROM GAMES g JOIN user_ids u ON g.white = u.id
    WHERE g.whiteratingdiff < 30
    limit 100)
    UNION ALL
    (SELECT * FROM GAMES g JOIN user_ids u ON g.black = u.id
    WHERE g.blackratingdiff < 30
    limit 100);
    """
    q2 = """
    EXPLAIN
    SELECT EXTRACT(DOW FROM date_time) as Day, 
    EXTRACT(HOUR FROM date_time) as Hour, 
    EXTRACT(MINUTE FROM date_time) as Minute,
    COUNT(date_time) as number_of_games
    FROM games 
    GROUP BY 
    EXTRACT(DOW FROM date_time), 
    EXTRACT(HOUR FROM date_time), 
    EXTRACT(MINUTE FROM date_time)
    ORDER BY Day, Hour, Minute;
    """
    q3 = """
    SELECT event, white, black, whiteratingdiff, blackratingdiff, whiteelo, blackelo, date_time, site
    FROM GAMES WHERE white = 101000 OR black = 101000
    ORDER BY event, date_time
    """
    q4 = """
    SELECT sub1.white, sub1.white_game_count + sub2.black_game_count as total_games
    FROM
    (SELECT White, COUNT(*) as white_game_count
    FROM games g1
    GROUP BY White) as sub1
    JOIN
    (SELECT Black, COUNT(*) as black_game_count
    FROM games g2
    GROUP BY Black) as sub2
    ON sub1.white = sub2.black
    ORDER BY total_games DESC;
    """
    q5 = """
    SELECT
    CASE WHEN LEAST(Whiteelo, Blackelo) < 600 THEN 'elo < 600'
    WHEN LEAST(Whiteelo, Blackelo) >= 600 AND LEAST(Whiteelo, Blackelo) < 800 THEN '600 <= elo < 800'
    WHEN LEAST(Whiteelo, Blackelo) >= 800 AND LEAST(Whiteelo, Blackelo) < 1000 Then '800 <= elo < 1000'
    WHEN LEAST(Whiteelo, Blackelo) >= 1000 AND LEAST(Whiteelo, Blackelo) < 1200 Then '1000 <= elo < 1200'
    WHEN LEAST(Whiteelo, Blackelo) >= 1200 AND LEAST(Whiteelo, Blackelo) < 1400 Then '1200 <= elo < 1400'
    WHEN LEAST(Whiteelo, Blackelo) >= 1400 AND LEAST(Whiteelo, Blackelo) < 1600 Then '1400 <= elo < 1600'
    WHEN LEAST(Whiteelo, Blackelo) >= 1600 AND LEAST(Whiteelo, Blackelo) < 1800 Then '1600 <= elo < 1800'
    WHEN LEAST(Whiteelo, Blackelo) >= 1800 AND LEAST(Whiteelo, Blackelo) < 2000 Then '1800 <= elo < 2000'
    WHEN LEAST(Whiteelo, Blackelo) >= 2000 AND LEAST(Whiteelo, Blackelo) < 2200 Then '2000 <= elo < 2200'
    WHEN LEAST(Whiteelo, Blackelo) >= 2200 AND LEAST(Whiteelo, Blackelo) < 2400 Then '2200 <= elo < 2400'
    WHEN LEAST(Whiteelo, Blackelo) >= 2400 AND LEAST(Whiteelo, Blackelo) < 2600 Then '2400 <= elo < 2600'
    WHEN LEAST(Whiteelo, Blackelo) >= 2600 AND LEAST(Whiteelo, Blackelo) < 2800 Then '2600 <= elo < 2800'
    WHEN LEAST(Whiteelo, Blackelo) >= 2800 Then '2800 <= elo' END as elo_bracket,
    COUNT(*) as total_games
    FROM games
    GROUP by 1;
    """

    q6 = """
    SELECT sub1.white, sub1.event, sub1.white_game_count + sub2.black_game_count as total_games
    FROM
    (SELECT White, event, COUNT(*) as white_game_count
    FROM games g1
    GROUP BY White, event) as sub1
    JOIN
    (SELECT Black, event, COUNT(*) as black_game_count
    FROM games g2
    GROUP BY Black, event) as sub2
    ON sub1.white = sub2.black
    AND sub1.event = sub2.event
    ORDER BY total_games DESC;
    """

    q7 = """
    SELECT event, count(*) 
    FROM games
    GROUP BY event;
    """

    q8 = """
    EXPLAIN
    SELECT
    CASE WHEN LEAST(Whiteelo, Blackelo) < 600 THEN 'elo < 600'
    WHEN LEAST(Whiteelo, Blackelo) >= 600 AND LEAST(Whiteelo, Blackelo) < 800 THEN '600 <= elo < 800'
    WHEN LEAST(Whiteelo, Blackelo) >= 800 AND LEAST(Whiteelo, Blackelo) < 1000 Then '800 <= elo < 1000'
    WHEN LEAST(Whiteelo, Blackelo) >= 1000 AND LEAST(Whiteelo, Blackelo) < 1200 Then '1000 <= elo < 1200'
    WHEN LEAST(Whiteelo, Blackelo) >= 1200 AND LEAST(Whiteelo, Blackelo) < 1400 Then '1200 <= elo < 1400'
    WHEN LEAST(Whiteelo, Blackelo) >= 1400 AND LEAST(Whiteelo, Blackelo) < 1600 Then '1400 <= elo < 1600'
    WHEN LEAST(Whiteelo, Blackelo) >= 1600 AND LEAST(Whiteelo, Blackelo) < 1800 Then '1600 <= elo < 1800'
    WHEN LEAST(Whiteelo, Blackelo) >= 1800 AND LEAST(Whiteelo, Blackelo) < 2000 Then '1800 <= elo < 2000'
    WHEN LEAST(Whiteelo, Blackelo) >= 2000 AND LEAST(Whiteelo, Blackelo) < 2200 Then '2000 <= elo < 2200'
    WHEN LEAST(Whiteelo, Blackelo) >= 2200 AND LEAST(Whiteelo, Blackelo) < 2400 Then '2200 <= elo < 2400'
    WHEN LEAST(Whiteelo, Blackelo) >= 2400 AND LEAST(Whiteelo, Blackelo) < 2600 Then '2400 <= elo < 2600'
    WHEN LEAST(Whiteelo, Blackelo) >= 2600 AND LEAST(Whiteelo, Blackelo) < 2800 Then '2600 <= elo < 2800'
    WHEN LEAST(Whiteelo, Blackelo) >= 2800 Then '2800 <= elo' END as elo_bracket,
    SUM(CASE WHEN analyzed THEN 1 ELSE 0 END) as analyzed_games,
    COUNT(*) as total_games
    FROM games
    GROUP by 1;
    """

    q9 = """
    EXPLAIN
    SELECT g1.White as player, g1.whiteelo as elo, g1.event, g1.date_time
    FROM games g1
    UNION ALL
    SELECT g1.Black as player, g1.blackelo as elo, g1.event, g1.date_time
    FROM games g1;
    """
    #select min and max ratings per player with start and end dates of activity (black games)
    q10 = """
    SELECT player, event, min(elo), max(elo), min(date_time), max(date_time) FROM
    (SELECT white as player, whiteelo as elo, event, date_time
    FROM games
    WHERE ABS(whiteratingdiff) < 30
    UNION ALL
    SELECT black as player, blackelo as elo, event, date_time
    FROM games
    WHERE ABS(blackratingdiff) < 30) as s
    GROUP BY player, event;
    """
    q11 = """
    SELECT g.Black as player, MIN(g.blackelo), g.date_time::date, 
            AGE(g.date_time::date, Min(sub.start_date)) as days_since_start
    FROM games g
    JOIN (SELECT g2.black, min(g2.date_time::date) as start_date
            FROM games g2
            WHERE g2.event = 'B'
            AND ABS(g2.blackratingdiff) < 30
            GROUP BY g2.black) sub
    on g.black = sub.black
    WHERE g.event = 'B'
    AND ABS(g.blackratingdiff) < 30
    GROUP BY g.Black, g.date_time::date
    """

    queries = [q11]
    filenames = ["q11.csv"]
    select_query(queries, conn, filenames=filenames)

    #conn.commit()
