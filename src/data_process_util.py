from datetime import datetime
from collections import OrderedDict
from bz2 import BZ2File as bzopen
import re

def read_lines(bzip_file):
    """takes a bzip file path and returns a generator that yields each line in the file"""
    with bzopen(bzip_file,"r") as bzfin:
        game_data = []
        for i, line in enumerate(bzfin):
            yield line

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


