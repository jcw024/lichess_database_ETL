from psycopg2.errors import InFailedSqlTransaction
import psycopg2
import psycopg2.extras
import io

def initialize_tables(conn):
    """creates a games and user_ids table in postgresql if they do not already exist"""
    #setup database tables. database name and user is configured in CONFIG.py
    cur = conn.cursor()
    #sort columns alphabetically for the copy_from method used to insert rows later
    games_columns = sorted(["Event VARCHAR(1) NOT NULL", "Site VARCHAR(8) PRIMARY KEY", 
            "White INT NOT NULL", "Black INT NOT NULL", 
            "Result VARCHAR(1) NOT NULL", "WhiteElo SMALLINT NOT NULL", 
            "BlackElo SMALLINT NOT NULL", "WhiteRatingDiff SMALLINT NOT NULL", 
            "BlackRatingDiff SMALLINT NOT NULL", 
            "ECO VARCHAR(3) NOT NULL", "TimeControl SMALLINT NOT NULL", 
            "Termination VARCHAR(1) NOT NULL", "BlackTitle VARCHAR(3)", 
            "WhiteTitle VARCHAR(3)", "Analyzed BOOLEAN NOT NULL", 
            "Date_time timestamp NOT NULL"])
    cur.execute(
            "CREATE TABLE IF NOT EXISTS games (" + ', '.join(games_columns) + ");"
            )
    cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_ids (
            ID INT NOT NULL PRIMARY KEY, username varchar(30) NOT NULL)
            """
            )
    conn.commit()
    #create lookup tables for event, result, ECO?, termination
    return [col.split(" ")[0] for col in games_columns]

def copy_data(conn, batch, table, retry=False):
    """takes a list of games and a database connection and inserts into database"""
    try:
        cur = conn.cursor()
        csv_object = io.StringIO()
        for item in batch:
            csv_object.write('|'.join(map(csv_format, item.values())) + '\n')
        csv_object.seek(0)
        try:
            cur.copy_from(csv_object, table, sep='|')
        except psycopg2.errors.UniqueViolation:
            copy_conflict(csv_object, conn, table)
        conn.commit()
    except InFailedSqlTransaction:  #if sql transaction fail, do a rollback and retry
        if not retry:
            conn.rollback()
            copy_data(conn, batch, table, retry=True)
        else: 
            raise InFailedSqlTransaction

def copy_conflict(csv_object, conn, table):
    csv_object.seek(0)
    conn.rollback()
    cur = conn.cursor()
    cur.execute("CREATE temporary table __copy as (select * from " + table + " limit 0);")
    cur.copy_from(csv_object, "__copy", sep='|')
    cur.execute("INSERT INTO " + table + " SELECT * FROM __copy ON CONFLICT DO NOTHING")
    cur.execute("DROP TABLE __copy")
    return

def csv_format(val):
    """used in copy_data function to format data for the csv_object to be used in the copy_from method.
    Takes a val of any type and returns the val converted to a str. 
    None is converted to '\\N', indicating a null value"""
    if val is None:
        return r'\N'
    return str(val)

def write_row(columns, values, conn, table):
    """takes in a list of columns and a list of values and writes the data to 
    a postgresql database table using the provided psycopg2 connection"""
    if len(columns) == 0: return False
    col_text = ', '.join(columns)
    row_text = "%s" + ", %s"*(len(values)-1)
    command = "INSERT INTO " + table + " (" + col_text + ") VALUES (" + row_text + ") ON CONFLICT DO NOTHING"
    try:
        cur = conn.cursor()
        cur.execute(command, list(values))
    except psycopg2.errors.NotNullViolation:
        pass
    conn.commit()
    return True

def dump_dict(data_dict, conn):
    id_list = [{"id":i[1],"username":i[0]} for i in data_dict.items()]  #formatting to fit expected input of copy_data function
    copy_data(conn, id_list, "user_ids")
    conn.commit()
    return

def load_id_dict(conn):
    """reads data from the 'user_ids' table and returns the data in a dict
    of username: ID"""
    cur = conn.cursor()
    cur.execute("SELECT * FROM user_ids;")
    conn.commit()
    user_IDs = cur.fetchall()
    id_dict = {}
    for (ID, username) in user_IDs:
        id_dict[username] = ID
    return id_dict


