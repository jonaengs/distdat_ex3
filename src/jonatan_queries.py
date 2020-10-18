from collections import Counter
from itertools import groupby
import datetime

def query1(db):
    # plurals_and_names = (('activities', 'Activity'), ('users', 'User'), ('trackpoints', 'TrackPoint'))
    plurals_and_names = (('persons', 'Person'), )
    for c_plural, c_name in plurals_and_names:
        documents = db[c_name].find({}).collection
        print(f"{c_plural} has {documents.count_documents({})} entries")

def query5(db):
    activities_with_transport = db["Activity"].find({
        "transportation_mode": {"$exists": True, "$ne": None}
    })
    transports = map(lambda a: a["transportation_mode"], activities_with_transport)
    print(Counter(transports))

def query9(db):
    # antar trackpoint peker til aktivitet som peker til bruker
    parse_dt = lambda s: datetime.strptime(s, r"%Y-%m-%d %H:%M:%S")
    all_trackpoints = db["TrackPoints"].find({})
    users = set()
    for activity_tps in groupby(all_trackpoints, key=lambda tp: tp["activity_id"]):
        for prev_tp, tp in zip(activity_tps[:-1], activity_tps[1:]):
            if parse_dt(tp) - parse_dt(prev_tp) > datetime.timedelta(minutes=5):
                activity_cursor = db["Activity"].find({"_id": tp["activity_id"]})
                user_id = activity_cursor[0]["user_id"]
                users.add(user_id)
                break
    print(users)





