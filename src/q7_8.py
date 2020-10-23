from geopy.distance import geodesic
import re
from DbConnector import DbConnector


# Find the total distance (in km) walked in 2008, by user with id=112.
def query7(db): 
    activities = {
        a["_id"]: (a["start_date_time"], a["end_date_time"])
        for a in
        db["Activity2"].find({"user_id": "112", "transportation_mode": "walk"})
    }
    activity_ids = list(activities.keys())
    all_trackPoints = db["TrackPoint2"].aggregate([{
        "$group": {
            "_id": "$activity_id",
            "lat": {"$push": "$lat"},
            "lon": {"$push": "$lon"}
        }}], allowDiskUse=True)
    all_trackPoints = {tp["_id"]: zip(tp["lat"], tp["lon"]) for tp in all_trackPoints if tp["_id"] in activity_ids}
    distance = geodesic((0,0), (0,0))
    for aid, date_times in activities.items():
        start_dt, end_dt = date_times
        if start_dt.year == end_dt.year == 2008:
            lat_lon_pairs = list(all_trackPoints[aid])
            for prev_lls, lls in zip(lat_lon_pairs[:-1], lat_lon_pairs[1:]):
                distance += geodesic(prev_lls, lls)
    return distance

#Find the top 20 users who gained the most altitude meters
def query8(db):
    trackPoints = db["TrackPoint"].find({}, {"_id": 1, "activity_id": 1, "altitude": 1})
    activities = db["Activity"].find({}, {"user_id": 1, "_id": 1})
    user_tp_map = {
        a["user_id"]: [tp for tp in trackPoints if tp["activity_id"] == a["_id"]]
        for a in activities
    }
    stats = {}
    for userId in range(0, 182):
        altitude = 0
        for index in range(1, len(trackPoints)):
            cords1 = trackPoints[index-1].alt
            cords2 = trackPoints[index].alt
            altitude += abs(cords1 - cords2)
        for user2 in range(len(users)):
            if altitude > counts[user2]:
                users.insert(index, userId)
                counts.insert(index, altitude)
                break
    return users[:20], altitude[:20]

if __name__ == "__main__":
    db = DbConnector().db
    print(query7(db))
    # print(query8(db))
