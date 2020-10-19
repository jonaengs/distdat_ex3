import os
import itertools
from pathlib import Path
from pprint import pprint

from DbConnector import DbConnector


db = DbConnector().db
USER, ACTIVITY, TRACKPOINT = collection_names = "User", "Activity", "TrackPoint"

__activity_id = 0
__trackpoint_id = 0
labeled_users = tuple(map(int, Path("dataset/labeled_ids.txt").read_text().split("\n")[:-1])) # closes file automatically
all_tp_files = (
    os.path.join(f"dataset/Data/{uid:03}/Trajectory", fn)
    for uid in range(182)
    for fn in os.listdir(f"dataset/Data/{uid:03}/Trajectory"))
valid_tp_files = list(filter(lambda fp: len(open(fp).readlines()) <= 2506, all_tp_files))

def next_activity_id():
    global __activity_id
    __activity_id += 1
    return __activity_id

def next_tp_id():
    global __trackpoint_id
    __trackpoint_id += 1
    return __trackpoint_id

"""
    LAGER DICTIONARY FOR ALLE BRUKERE
"""
def get_users():
    return (
        {"_id": f"{i:03}", "has_labels": i in labeled_users}
        for i in range(182)
    )

"""
    LAGER DICTIONARY AV ALLE AKTIVITETER SOM HAR LABELS
"""
def parse_label(line):
    start_dt, end_dt, transport = line.strip().split("\t")
    return {"start_date_time": start_dt, "end_date_time": end_dt, "transportation_mode": transport}

def get_labeled_activities():
    return (
        parse_label(line) | {"user_id": f"{uid:03}", "_id": next_activity_id()}
        for uid in labeled_users
        for line in open(f"dataset/Data/{uid:03}/labels.txt").readlines()[1:]
    )

"""
    LAGER DICTIONARY AV ALLE TRACKPOINTS
"""
def parse_tp(line):
    split = line.strip().split(",")
    return dict(zip(
        ("lat", "lon", "altitude", "date_datetime"),
        (split[0], split[1], split[3], " ".join(split[5:]))
    ))

def get_all_trackpoints(n=10):
    return (
        parse_tp(line) | {"_id": next_tp_id(), "activity_id": None}
        for fp in itertools.islice(valid_tp_files, n or 1234567890)
        for line in open(fp).readlines()[6:]
    )

def print_documents(collection_name, n=10):
    any(map(pprint, db[collection_name].find({})[:n]))
    print("\n" + "-"*50 + "\n")

from jonatan_queries import query1, query5, query9
def do_queries():
    for q in (query1, query5, query9):
        q(db)

def insert():
    for cname in collection_names:
        if collection := db[cname]:
            collection.drop()
        collection = db.create_collection(cname)
    
    db[USER].insert_many(get_users())
    db[ACTIVITY].insert_many(get_labeled_activities())
    db[TRACPOINT].insert_many(get_all_trackpoints())

    do_queries()


if __name__ == '__main__':
    insert()