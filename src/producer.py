from kafka import KafkaProducer
from bz2 import BZ2File as bzopen
import time
import json
import os
import glob

SRC_PATH = os.getcwd()
BZ2_DATA = glob.glob(SRC_PATH+"/../data/lichess*")

def read_lines(bzip_file):
    with bzopen(bzip_file,"r") as bzfin:
        game_data = []
        for i, line in enumerate(bzfin):
            yield line
            continue
            line_decode = line.decode('utf-8')
            if line_decode is not None:
                game_data.append(line) 
            if line_decode[0] == '1':
                yield ''.join(game_data)
                game_data = []

def write_row():
    print("writing row")
    return

if __name__ == "__main__":

    consumer_msgs = []
    #producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x:json.dumps(x).encode("utf-8"))
    lines = read_lines('../data/lichess_db_standard_rated_2013-01.pgn.bz2')
    for bzip_file in BZ2_DATA:
        #games = get_games('bzip_file')
        for line in lines:
            print(line)
            #producer.send('lichess_games_topic', line)
            #print(f'sent: {line}')
            line = line.decode('utf-8')
            consumer_msgs.append(line)
            if line[0] == '1':
                if 'eval' in line:
                    consumer_msgs.append('[analyzed "True"]')
                else:
                    consumer_msgs.append('[analyzed "False"]')
                write_row()
                consumer_msgs = []
            time.sleep(1)
    #producer.flush()


