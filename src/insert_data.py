from pathlib import Path
from pprint import pprint

from DbConnector import DbConnector


db = DbConnector().db
USER, ACTIVITY, TRACKPOINT = collection_names = "User", "Activity", "TrackPoint"
__activity_id = 0
labeled_users = list(map(int, Path("dataset/labeled_ids.txt").read_text().split("\n")[:-1])) # closes file automatically
valid_files = 

def get_users():
    return [
        {"_id": f"{i:03}", "has_labels": i in labeled_users}
        for i in range(182)
    ]

def next_activity_id():
    global __activity_id
    __activity_id += 1
    return __activity_id

def parse_label(line):
    start_dt, end_dt, transport = line.strip().split("\t")
    return {"start_date_time": start_dt, "end_date_time": end_dt, "transportation_mode": transport}

def get_labeled_activities():
    return [
        parse_label(line) | {"user_id": f"{uid:03}", "_id": next_activity_id()}
        for uid in labeled_users
        for line in open(f"dataset/Data/{uid:03}/labels.txt").readlines()[1:]
    ]


def print_documents(collection_name, n=10):
    any(map(pprint, db[collection_name].find({})[:n]))
    print("\n" + "-"*50 + "\n")

def insert():
    for cname, get_data in zip(collection_names, (get_users, get_labeled_activities)):
        if collection := db[cname]:
            collection.drop()
        collection = db.create_collection(cname)
        collection.insert_many(get_data())
        print_documents(cname)
        

if __name__ == '__main__':
    insert()