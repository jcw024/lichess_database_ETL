from kafka import KafkaConsumer
from CONFIG import DB_NAME, DB_USER
from datetime import datetime
from tqdm import tqdm
import re
import psycopg2

def write_row(game, conn):
    """takes in a list of a single game's data and writes the data to 
    a postgresql database"""
    if len(game) == 0: return game
    col_text = ', '.join(game.keys())
    row_text = "%s" + ", %s"*(len(game.values())-1)
    command = "INSERT INTO games (" + col_text + ") VALUES (" + row_text + ") ON CONFLICT DO NOTHING"
    #print(command, game.values())
    try:
        cur = conn.cursor()
        cur.execute(command, tuple(game.values()))
    except psycopg2.errors.NotNullViolation:
        pass
    conn.commit()
    return game 

def format_data(key, val):
    """takes in lichess game key and value and formats the data prior to writing it to the database"""
    if key == "Event":
        if "bullet" in key.lower():
            val = 'b'
        elif "blitz" in key.lower():
            val = 'B'
        elif "rapid" in key.lower():
            val = 'R'
        elif "classical" in key.lower():
            val = 'c'
        elif "correspondence" in key.lower():
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
            val = None  #elo data will be NULL. write_row will throw this game out later
        else:
            val = int(val)
    elif key == "Termination":
        if val == "Normal": val = 'N'
        elif val == "Time forfeit": val = 'F'
        elif val == "Abandoned": val = 'A'
        else: val = '?'
    elif key == "TimeControl":
        val = format_time_control(val)
    elif key == "Result":
        if val == "1/2-1/2":
            val = 'D'
        elif val == "1-0":
            val = 'W'
        elif val = "0-1":
            val = 'B'
        else:
            val = '?'
    return (key, val)

def merge_datetime(game):
    """takes in a game dict and merges the date and time with datetime.combine()"""
    game['timestamp'] = datetime.combine(game['UTCDate'], game['UTCTime'])
    del game['UTCDate']
    del game['UTCTime']
    return game

def format_time_control(time_control):
    """takes in a time_control string (i.e. '300+5') and converts to int by 
    multiplying the increment by 40 moves (how lichess categorizes game time control type)"""
    try:
        time_control = time_control.split("+")
        return int(time_control[0]) + int(time_control[1])*40   
    except TypeError:
        return None

if __name__ == "__main__":
    #setup database tables. database name and user is configured in CONFIG.py
    connect_string = "dbname=" + DB_NAME + " user=" + DB_USER
    conn = psycopg2.connect(connect_string)
    cur = conn.cursor()
    cur.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
            Event VARCHAR(1) NOT NULL, Site VARCHAR(8) PRIMARY KEY, 
            White VARCHAR(30) NOT NULL, Black VARCHAR(30) NOT NULL, 
            Result VARCHAR(1) NOT NULL, date_time timestamp NOT NULL, 
            WhiteElo SMALLINT NOT NULL, BlackElo SMALLINT NOT NULL, 
            WhiteRatingDiff SMALLINT NOT NULL, BlackRatingDiff SMALLINT NOT NULL, 
            ECO VARCHAR(3) NOT NULL, TimeControl SMALLINT NOT NULL, 
            Termination VARCHAR(1) NOT NULL, BlackTitle VARCHAR(3), 
            WhiteTitle VARCHAR(3), Analyzed BOOLEAN NOT NULL);
            """
            )
    conn.commit()
    #create lookup tables for event, result, ECO?, termination

    #setup consumer
    consumer_configs = {
            'bootstrap_servers':'localhost:9092', 
            'group_id':'main_group', 
            'auto_offset_reset':'earliest', 
            'enable_auto_commit':True #switch to True when actually loading data, False for testing consumer.py
        }

    consumer = KafkaConsumer('ChessGamesTopic', **consumer_configs)
    print("starting consumer...")

    #consumer will read data until it's read a full game's data, then write to database
    game = {}
    for line in tqdm(consumer):
        line = line.value.decode('utf-8')
        if line == '\n' or line[0] == ' ': continue
        try:
            key = re.search("\[(.*?) ",line).group(1)
            val = re.search(" \"(.*?)\"\]", line).group(1)
            if key in ("Date", "Round", "Opening"): continue    #skip irrelevant data (adjust if you prefer) 
            key, val = format_data(key, val)
            game[key] = val
        except AttributeError:
            pass

        #checks if the line is describing the moves of a game. If so, all the data for the game has been read
        if line[0] == '1':
            #game moves are not stored to save disk space, just track if the game has been analyzed or not
            if 'eval' in line:
                game["analyzed"] = True
            else:
                game["analyzed"] = False 
            game = merge_datetime(game)
            write_row(game, conn)
            game = {}   #reset game dict variable for the next set of game data


