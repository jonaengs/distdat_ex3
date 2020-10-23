from collections import Counter, defaultdict
from datetime import timedelta
from pprint import pprint

from DbConnector import DbConnector


def query1():
    for c_name in ("User", "Activity", "TrackPoint"):
        num_docs = db[c_name].count_documents({})
        print(f"{c_name} has {num_docs} entries")


def query5():
    activities_with_transport = db["Activity"].find({
        "transportation_mode": {"$exists": True, "$ne": None}
    }, {"transportation_mode": 1})
    transports = (a["transportation_mode"] for a in activities_with_transport)
    print("Count of registered transportation modes:")
    print(Counter(transports))


def query9():
    activity_user_map = {
        doc["_id"]: doc["user_id"] for doc in
        db.Activity.find({}, {"_id": 1, "user_id": 1})
    }
    grouped_trackpoints = db.TrackPoint.aggregate([{"$group": {
            "_id": "$activity_id", 
            "datetimes": {"$push": "$date_time"}}
        }], allowDiskUse=True)
    
    num_invalid = defaultdict(int)
    for doc in grouped_trackpoints:
        aid, tp_dts = doc["_id"], doc["datetimes"]
        for prev_dt, dt in zip(tp_dts[:-1], tp_dts[1:]):
            if (dt - prev_dt > timedelta(minutes=5)):
                num_invalid[activity_user_map[aid]] += 1
    print("Number of invalid activities per user:")
    print(dict(num_invalid))


if __name__ == '__main__':
    db = DbConnector().db
    query1()
    query5()
    query9()

