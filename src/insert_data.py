from pathlib import Path

from DbConnector import DbConnector


db = DbConnector().db
USER, ACTIVITY, TRACKPOINT = collection_names = "User", "Activity", "TrackPoint"
activity_id = 0
labeled_users = list(map(int, Path("../dataset/labeled_ids.txt").read_text().split("\n")[:-1])) # closes file automatically

def get_users():
    users = {
        i: {"_id": f"{i:03}", "has_labels": i in labeled_users}
        for i in range(182)
    }
    return users.values()

def next_activity_id():
    global activity_id
    activity_id += 1
    return activity_id

def parse_label(line):
    start_dt, end_dt, transport = line.strip().split("\t")
    return {"start_date_time": start_dt, "end_date_time": end_dt, "transportation_mode": transport}

def get_labeled_activities():
    activities = []
    for uid in labeled_users:
        fp = f"../dataset/Data/{uid:03}/labels.txt"
        with open(fp) as f:
            for line in f.readlines()[1:]:
                activity = {"user_id": f"{uid:03}", "id": next_activity_id()}
                activities.append(activity | parse_label(line))
    return activities

def insert():
    for cname in collection_names:
        if collection := db[cname]:
            collection.drop()
        db.create_collection(cname)

    users = db[USER]
    users.insert_many(get_users())

    activities = db[ACTIVITY].insert_many(get_labeled_activities())

if __name__ == '__main__':
    insert()