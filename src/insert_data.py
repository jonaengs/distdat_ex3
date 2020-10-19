import os
import itertools
from functools import partial
from pathlib import Path
from pprint import pprint

from DbConnector import DbConnector


db = DbConnector().db
USER, ACTIVITY, TRACKPOINT = collection_names = "User", "Activity", "TrackPoint"

__activity_id = 0
__trackpoint_id = 0
user_ids = range(182)
labeled_users = tuple(map(int, Path("dataset/labeled_ids.txt").read_text().split("\n")[:-1])) # closes file automatically
all_tp_files = (
    os.path.join(f"dataset/Data/{uid:03}/Trajectory", fn)
    for uid in user_ids
    for fn in os.listdir(f"dataset/Data/{uid:03}/Trajectory"))
valid_tp_files = filter(lambda fp: len(open(fp).readlines()) <= 2506, all_tp_files)

def next_activity_id():
    global __activity_id
    __activity_id += 1
    return __activity_id

def next_tp_id():
    global __trackpoint_id
    __trackpoint_id += 1
    return __trackpoint_id

"""
    LAGER DICTIONARY AV ALLE BRUKERE
"""
def get_users():
    return (
        {"_id": f"{i:03}", "has_labels": i in labeled_users}
        for i in user_ids
    )

"""
    LAGER DICTIONARY AV ALLE AKTIVITETER SOM HAR LABELS
"""
def parse_label(line):
    start_dt, end_dt, transport = line.strip().split("\t")
    return {"start_date_time": start_dt, "end_date_time": end_dt, "transportation_mode": transport}

def get_labeled_activities():
    for uid in labeled_users:
        for line in open(f"dataset/Data/{uid:03}/labels.txt").readlines()[1:]:
            activity = parse_label(line) | {"user_id": f"{uid:03}", "_id": next_activity_id()}
            a_start_dt = activity["start_date_time"]
            # finn fil som tilsvarer a_start_dt
            # lag trackpoints av alle tp i fila
            # map(
            #       lambda tp: tp | {"_id": next_tp_id(), "activity_id": activity["_id"]}, 
            #       map(parse_tp, lines)
            #    )

    return (
        parse_label(line) | {"user_id": f"{uid:03}", "_id": next_activity_id()}
        for uid in labeled_users
        for line in open(f"dataset/Data/{uid:03}/labels.txt").readlines()[1:]
    )

"""
    LAGER DICTIONARY AV ALLE TRACKPOINTS
"""
def activity_from_tps(tp1, tp2):
    start_dt, end_dt = parse_tp(tp1)["date_time"], parse_tp(tp2)["date_time"]
    return {"_id": next_activity_id(), "start_date_time": start_dt, "end_date_time": end_dt}

def parse_tp(line):
    split = line.strip().split(",")
    return dict(zip(
        ("lat", "lon", "altitude", "date_time"),
        (split[0], split[1], split[3], " ".join(split[5:]))
    ))

def get_tps_and_acts():
    for uid in user_ids:
        user_tp_path = f"dataset/Data/{uid:03}/Trajectory"
        for tp_fp in map(partial(os.path.join, user_tp_path), os.listdir(user_tp_path)):
           lines = open(tp_fp).readlines()[6:] # skip header
           if len(lines) <= 2500:
               activity = activity_from_tps(lines[0], lines[-1]) | {"user_id": f"{uid:03}", "_id": next_activity_id()}
               file_trackpoints = map(
                   lambda tp: tp | {"_id": next_tp_id(), "activity_id": activity["_id"]}, 
                   map(parse_tp, lines)
                )
               yield activity, file_trackpoints

"""
    UTILITY
"""
def print_documents(collection_name, n=10):
    any(map(pprint, db[collection_name].find({})[:n]))
    print("\n" + "-"*50 + "\n")

"""
    INTERAGER MED DATABASE
"""
from jonatan_queries import query1, query5, query9
def do_queries():
    for q in (query1, query5, query9):
        q(db)

def reset_collections():
    for cname in collection_names:
        if collection := db[cname]:
            collection.drop()
        db.create_collection(cname)

def insert():
    reset_collections()
    
    db[USER].insert_many(get_users())
    db[ACTIVITY].insert_many(get_labeled_activities())
    for activity, trackpoints in itertools.islice(get_tps_and_acts(), 10): # henter kun 10 første
        db[ACTIVITY].insert_one(activity)
        db[TRACKPOINT].insert_many(trackpoints)

    any(map(print_documents, collection_names))

    do_queries()


if __name__ == '__main__':
    print(list(get_users()))