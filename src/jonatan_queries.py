from collections import Counter
from itertools import groupby
from datetime import datetime, timedelta

from DbConnector import DbConnector


def query1(db):
    plurals_and_names = (('users', 'User'), ('activities', 'Activity'), ('trackpoints', 'TrackPoint'))
    for c_plural, c_name in plurals_and_names:
        documents = db[c_name].find({}).collection
        print(f"{c_plural} has {documents.count_documents({})} entries")


def query5(db):
    activities_with_transport = db["Activity"].find({
        "transportation_mode": {"$exists": True, "$ne": None}
    })
    transports = (a["transportation_mode"] for a in activities_with_transport)
    print(Counter(transports))


def query9(db):  # antar trackpoint peker til aktivitet som peker til bruker
    activity_user_map = {
        doc["_id"]: doc["user_id"] for doc in
        db.Activity.find({}, {"_id": 1, "user_id": 1})
    }
    grouped_trackpoints = db.TrackPoint.aggregate([{"$group": {
            "_id": "$activity_id", 
            "datetimes": {"$push": "$date_time"}}
        }], allowDiskUse=True)
    
    users = set()
    for doc in grouped_trackpoints:
        aid, tp_dts = doc["_id"], doc["datetimes"]
        if activity_user_map[aid] in users:  # skip 
            continue
        for prev_dt, dt in zip(tp_dts[:-1], tp_dts[1:]):
            if (dt - prev_dt > timedelta(minutes=5)):
                users.add(activity_user_map[aid])
                break

    print(*sorted(users), sep=", ")



if __name__ == '__main__':
    db = DbConnector().db
    query1(db)
    query5(db)
    query9(db)

