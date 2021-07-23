from kafka import KafkaConsumer
from CONFIG import DB_NAME, DB_USER, BATCH_SIZE #enter these values in CONFIG.public.py, then change CONFIG to CONFIG.public
from datetime import datetime                   #or rename CONFIG.public.py to CONFIG.py
from tqdm import tqdm
from collections import OrderedDict
from psycopg2.errors import InFailedSqlTransaction
import re
import psycopg2
import psycopg2.extras
import io

def initialize_tables(conn):
    """creates a games and user_IDs table in postgresql if they do not already exist"""
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
            CREATE TABLE IF NOT EXISTS user_IDs (
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
    copy_data(conn, id_list, "user_IDs")
    conn.commit()
    return

def load_id_dict(conn):
    """reads data from the 'user_IDs' table and returns the data in a dict
    of username: ID"""
    cur = conn.cursor()
    cur.execute("SELECT * FROM user_IDs;")
    conn.commit()
    user_IDs = cur.fetchall()
    id_dict = {}
    for (ID, username) in user_IDs:
        id_dict[username] = ID
    return id_dict

def assign_user_ID(username, id_dict, new_id_dict):
    """takes a username and gets the ID or assigns a new one if not already in id_dict
    returns the ID and id_dict (with the new ID added if a new one was added)
    if a new id was added, it will be added to new_id_dict"""
    if username in id_dict:
        return id_dict[username], id_dict, new_id_dict
    elif len(id_dict) == 0:
        ID = 1
    else:
        ID = max(id_dict.values()) + 1
    id_dict[username] = ID
    new_id_dict[username] = ID
    return ID, id_dict, new_id_dict

def format_data(key, val):
    """takes in lichess game key and value and formats the data prior to writing it to the database"""
    if key == "Event":
        if "bullet" in val.lower():
            val = 'b'
        elif "blitz" in val.lower():
            val = 'B'
        elif "standard" in val.lower() or "rapid" in val.lower():
            val = 'R'
        elif "classical" in val.lower():
            val = 'c'
        elif "correspondence" in val.lower():
            val = 'C'
        else:
            val = '?'
    elif key == "UTCDate":
        val = datetime.strptime(val, '%Y.%m.%d').date()
    elif key == "UTCTime":
        val = datetime.strptime(val, '%H:%M:%S').time()
    elif key == "Site":
        val = re.search("org/(.*)", val).group(1)
    elif key in ("WhiteRatingDiff", "BlackRatingDiff", "WhiteElo", "BlackElo"):
        if "?" in val:  #if any player is "anonymous" or has provisional rating, 
            val = None  #elo data will be NULL. this will trigger the game to be thrown out
        else:
            val = int(val)
    elif key == "Termination":
        if val == "Normal": val = 'N'
        elif val == "Time forfeit": val = 'F'
        elif val == "Abandoned": val = 'A'
        else: val = '?'     #usually means cheater detected
    elif key == "TimeControl":
        val = format_time_control(val)
    elif key == "Result":
        if val == "1/2-1/2":
            val = 'D'
        elif val == "1-0":
            val = 'W'
        elif val == "0-1":
            val = 'B'
        else:
            val = '?'
    return (key, val)

def merge_datetime(game):
    """takes in a game dict and merges the date and time with datetime.combine()"""
    try:
        game['Date_time'] = datetime.combine(game['UTCDate'], game['UTCTime'])
        del game['UTCDate']
        del game['UTCTime']
    except KeyError:
        if 'UTCDate' in game:
            del game['UTCDate']
        if 'UTCTime' in game:
            del game['UTCTime']
    return game

def format_time_control(time_control):
    """takes in a time_control string (i.e. '300+5') and converts to int by 
    multiplying the increment by 40 moves (how lichess categorizes game time control type)"""
    try:
        time_control = time_control.split("+")
        return int(time_control[0]) + int(time_control[1])*40   
    except ValueError:
        return 0

def format_game(game):
    """takes game and adds an 'analyzed' key/value, fills in player titles if not already existing, and formats dates"""
    #game moves are not stored to save disk space, just track if the game has been analyzed or not
    try:
        if any([game[i] is None for i in ["BlackElo", "WhiteElo"]]):
            return {}   #check if black or white are None, throw game out if yes
        if any([game[i] is None for i in ["WhiteRatingDiff", "BlackRatingDiff", "WhiteElo", "BlackElo"]]):
            return {}   #throw out the game if any player is "anonymous" with no rating
    except KeyError:
        return {}
    if "WhiteTitle" not in game:
        game["WhiteTitle"] = None
    if "BlackTitle" not in game:
        game["BlackTitle"] = None
    game = merge_datetime(game)
    return OrderedDict(sorted(game.items()))

if __name__ == "__main__":
    connect_string = "dbname=" + DB_NAME + " user=" + DB_USER
    conn = psycopg2.connect(connect_string)
    try:    #if any exception, write the id_dict to "user_IDs" database table to record new user_IDs before raising error
        games_columns = initialize_tables(conn)         #create necessary tables in postgresql if they don't already exist
        id_dict = load_id_dict(conn)    #load dict to assign user IDs to usernames
        new_id_dict = {}

        #setup consumer
        consumer_configs = {
                'bootstrap_servers':'localhost:9092', 
                'group_id':'main_group', 
                'auto_offset_reset':'earliest', 
                'max_partition_fetch_bytes':1048576*100,
                'enable_auto_commit':True   #whether or not to continue where consumer left off or start over
            }

        consumer = KafkaConsumer('ChessGamesTopic', **consumer_configs)
        print("starting consumer...")

        #consumer will read data until it's read a full game's data, then add the game data to batch
        batch = []  #database writes are done in batches to minimize server roundtrips
        game = OrderedDict()
        for line in tqdm(consumer):
            line = line.value.decode('utf-8')
            if line == '\n' or line[0] == ' ': continue
            try:
                key = re.search("\[(.*?) ",line).group(1)
                val = re.search(" \"(.*?)\"\]", line).group(1)
                if key in ("Date", "Round", "Opening"): continue    #skip irrelevant data (adjust if you prefer) 
                if key not in games_columns + ["UTCDate", "UTCTime"]: continue   #if some unforseen data type not in table, skip it
                if key in ("White", "Black"):
                    (val, id_dict, new_id_dict) = assign_user_ID(val, id_dict, new_id_dict)   #converts username to user ID and updates id_dict
                key, val = format_data(key, val)
                game[key] = val
            except AttributeError:
                pass

            #checks if the line is describing the moves of a game (the line starts with "1"). 
            #If so, all the data for the game has been read and we can format the game data
            if line[0] == '1':
                if 'eval' in line:
                    game["Analyzed"] = True
                else:
                    game["Analyzed"] = False 
                game = format_game(game)
                if game:
                    batch.append(game)
                game = OrderedDict()   #reset game dict variable for the next set of game data
                if len(batch) >= BATCH_SIZE:
                    copy_data(conn, batch, "games")
                    dump_dict(new_id_dict, conn)
                    batch = []
                    new_id_dict = {}
    except (Exception, KeyboardInterrupt) as e:
        #on consumer shutdown, write remaining games data and id_dict values to database
        print(f"{e} exception raised, writing id_dict to database")
        dump_dict(id_dict, conn)
        copy_data(conn, batch, "games")
        raise e


