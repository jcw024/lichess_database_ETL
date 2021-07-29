from kafka import KafkaConsumer
from data_process_util import *
from database_util import *
from CONFIG import DB_NAME, DB_USER, BATCH_SIZE #enter these values in CONFIG.public.py, then change CONFIG to CONFIG.public
from datetime import datetime                   #or rename CONFIG.public.py to CONFIG.py
from tqdm import tqdm
from collections import OrderedDict
from psycopg2.errors import InFailedSqlTransaction
import re
import psycopg2
import psycopg2.extras
import io

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
                    print(batch)
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


