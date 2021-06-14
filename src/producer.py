from kafka import KafkaProducer
from bz2 import BZ2File as bzopen
from collections import defaultdict
from tqdm import tqdm
import time
import os
import glob

SRC_PATH = os.getcwd()
BZ2_DATA = glob.glob(SRC_PATH+"/../data/lichess*")

def read_lines(bzip_file):
    with bzopen(bzip_file,"r") as bzfin:
        game_data = []
        for i, line in enumerate(bzfin):
            yield line

if __name__ == "__main__":

    consumer_msgs = []
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    lines = read_lines('../data/lichess_db_standard_rated_2013-01.pgn.bz2')
    #for bzip_file in BZ2_DATA:
        #lines = read_lines('bzip_file')
    for line in tqdm(lines):
        if len(line) <= 1: continue
        producer.send('ChessGamesTopic', line)
        #print(f'sent: {line}')
        #time.sleep(0.2)
    #producer.flush()


