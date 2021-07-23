from kafka import KafkaProducer
from bz2 import BZ2File as bzopen
from collections import defaultdict
from tqdm import tqdm
import time
import os
import glob

SRC_PATH = os.getcwd()  #assumes cwd is lichess_games/src
BZ2_DATA = glob.glob(SRC_PATH+"/../data/lichess*")

def read_lines(bzip_file):
    """takes a bzip file path and returns a generator that yields each line in the file"""
    with bzopen(bzip_file,"r") as bzfin:
        game_data = []
        for i, line in enumerate(bzfin):
            yield line

def start_producer(url):
    """assumes a lichess bzip file specified by the url has been downloaded in ../data and creates a kafka producer
    to send data to the kafka broker"""
    data_path = SRC_PATH+"/../data/"
    filename = url.split('/')[-1]
    filepath = data_path + filename
    producer = KafkaProducer(bootstrap_servers='localhost:9092', linger_ms=1000*10000, batch_size=16384*50)
    lines = read_lines(filepath)
    for line in tqdm(lines):
        if len(line) <= 1: continue
        producer.send('ChessGamesTopic', line)

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='localhost:9092', linger_ms=1000*10000, batch_size=16384*50)
    lines = read_lines('../data/lichess_db_standard_rated_2013-01.pgn.bz2')
    #for bzip_file in BZ2_DATA:
    #    lines = read_lines('bzip_file')
    for line in tqdm(lines):
        if len(line) <= 1: continue
        producer.send('ChessGamesTopic', line)
