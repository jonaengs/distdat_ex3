from pprint import pprint
from collections import Counter
from itertools import groupby
from datetime import datetime, timedelta
from geopy.distance import geodesic
from DbConnector import DbConnector

def print_query(query_number, result):
  print('Query %s:\n' % query_number)
  for doc in result:
    pprint(doc)
  print('\n')

def query1(db):
    print('Query 1\n')
    plurals_and_names = (('users', 'User'), ('activities', 'Activity'), ('trackpoints', 'TrackPoint'))
    for c_plural, c_name in plurals_and_names:
        documents = db[c_name].find({}).collection
        print(f"{c_plural} has {documents.count_documents({})} entries")
    print('\n')

def query2(db):
    # Find the average number of activities per user

    collection = db['Activity']
    result = collection.aggregate(
      [{
        '$group':
            {
              '_id': '$user_id',
              'activity_count': {'$sum': 1}
            }
      },
      {
        '$group':
            {
              '_id': 'null',
              'average_activities_per_user': {'$avg': '$activity_count'}
            }
      },
      {
        '$project':
          {
            '_id': 0,
            'average_activities_per_user': 1
          }
      }]
    )
    print_query(2, result)

def query6a(db): # a) Find the year with the most activities.

    collection = db['Activity']
    result = collection.aggregate(
      [
        {
          '$group':
              {
                '_id': {'$year': '$start_date_time'},
                'num_of_activities': {'$sum': 1}
              }
        },
        {
          '$sort': {'num_of_activities': -1}
        },
        {
          '$limit': 1
        }
      ]
    )
    print_query('6a', result)

def query3(db):
    # Find the top 20 users with the highest number of activities

    collection = db['Activity']
    result = collection.aggregate(
        [
            {
                '$group':
                {
                    '_id': '$user_id',
                    'sum': {'$sum': 1}
                }
            },
            {
                '$sort':
                {
                    'sum': -1
                }
            },
            {
                '$project':
                {
                    '_id': 0,
                    '_user': '$_id',
                    'number_of_activities': '$sum'
                }
            },
            {
                '$limit': 20
            }
        ]
    )
    print_query(3, result)


def query4(db):
    # Find all users who have taken a taxi.

    collection = db['Activity']
    result = collection.aggregate(
        [
            {
                '$match':
                {
                    'transportation_mode': "taxi"
                }
            },
            {
                '$group':
                {
                    '_id': "$user_id"
                }
            },
            {
                '$sort':
                {
                    '_id': 1
                }
            },
            {
                '$project':
                {
                    '_id': 0,
                    'taxi_user': '$_id',
                }
            }
        ]
    )
    print_query(4, result)

def query5(db):
    print('Query 5\n')
    activities_with_transport = db["Activity"].find({
        "transportation_mode": {"$exists": True, "$ne": None}
    })
    transports = (a["transportation_mode"] for a in activities_with_transport)
    print(Counter(transports))
    print('\n')

def query6b(db):
    # b) Is this also the year with most recorded hours?

    collection = db['Activity']
    result = collection.aggregate(
      [
        {
          '$group':
              {
                '_id': {'$year': '$start_date_time'},
                'recorded_hours': {'$sum': { "$subtract": ["$end_date_time", "$start_date_time" ]}}
              }
        },
        {
          '$sort': {'recorded_hours': -1}
        },
        {
          '$limit': 1
        }
      ]
    )
    print_query('6b', result)

def query7(db):
    print('Query 7\n')
    activities = {
        a["_id"]: (a["start_date_time"], a["end_date_time"])
        for a in
        db["Activity"].find({"user_id": "112", "transportation_mode": "walk"})
    }
    activity_ids = list(activities.keys())
    all_trackPoints = db["TrackPoint"].aggregate([{
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
    
    print('Distance:', distance, '\n')

# Find the top 20 users who gained the most altitude meters
# def query8(db):
#     trackPoints = db["TrackPoint"].find({}, {"_id": 1, "activity_id": 1, "altitude": 1})
#     activities = db["Activity"].find({}, {"user_id": 1, "_id": 1})
#     user_tp_map = {
#         a["user_id"]: [tp for tp in trackPoints if tp["activity_id"] == a["_id"]]
#         for a in activities
#     }
#     stats = {}
#     for userId in range(0, 182):
#         altitude = 0
#         for index in range(1, len(trackPoints)):
#             cords1 = trackPoints[index-1].alt
#             cords2 = trackPoints[index].alt
#             altitude += abs(cords1 - cords2)
#         for user2 in range(len(users)):
#             if altitude > counts[user2]:
#                 users.insert(index, userId)
#                 counts.insert(index, altitude)
#                 break
#     return users[:20], altitude[:20]

def query9(db):
    print('Query 9\n')
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
    print('\n')

def query10(db):
    # Find the users who have tracked an activity in the Forbidden City of Beijing.

    # To perform this task we decided to split it into two queries
    # First retrieve every track point that was inside the Forbidden City
    # Then iterate though all track point's references to activity and find the activities and it's user_id
    # We used a set to collect the user IDs to avoid duplicates

    collection = db['TrackPoint']

    track_points_in_forbidden_city = collection.find(
        {
            'lat': {'$gt': 39.915, '$lt': 39.917},
            'lon': {'$gt': 116.396, '$lt': 116.398}
        }
    )

    users_in_forbidden_city = set()
    collection = db['Activity']
    for track_point in track_points_in_forbidden_city:
        activity_document = collection.find({'_id': track_point['activity_id']})
        for activity in activity_document:
            users_in_forbidden_city.add(activity['user_id'])
    
    user_list = list(users_in_forbidden_city)
    user_list.sort()
    print_query('10', user_list)

def query11(db):
    # Find all users who have registered transportation_mode and their most used transportation_mode.

    collection = db['Activity']
    result = collection.aggregate(
        [
            {
                '$match':
                {
                    'transportation_mode':
                    {
                        '$exists': 1
                    }
                }
            },
            {
                '$group':
                {
                    '_id':
                    {
                        'user_id': "$user_id",
                        'transportation_mode': "$transportation_mode"
                    },
                    'most_used':
                    {
                        '$max': "$transportation_mode"
                    },
                    'count':
                    {
                        '$sum': 1
                    }
                }
            },
            {
                '$sort':
                {
                    "_id.user_id": 1,
                    'count': -1
                }
            },
            {
                '$group':
                {
                    '_id': "$_id.user_id",
                    'most_used':
                    {
                        '$push':
                        {
                            'mode': "$most_used"
                        }
                    }
                }
            },
            {
                '$project':
                {
                    '_id': 0,
                    '_user': "$_id",
                    'most_used_transportation_mode':
                    {
                        '$slice':
                        ["$most_used", 1]
                    }
                }
            },
            {
                '$sort':
                {
                    '_user': 1
                }
            }
        ]
    )
    print_query(11, result)

def main():
    try:
        connection = DbConnector()
        db = connection.db
        # query2(db)
        # query3(db)
        # query4(db)
        # query5(db)
        # query6a(db)
        # query6b(db)
        query7(db)
        # query9(db)
        # query10(db)
        # query11(db)
    except Exception as e:
        print('ERROR: Failed to use database:', e)
    finally:
        if connection:
            connection.close_connection()

if __name__ == '__main__':
    main()