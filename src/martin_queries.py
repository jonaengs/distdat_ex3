from pprint import pprint 
from DbConnector import DbConnector

def print_query(query_number, result):
  print('Query %s:\n' % query_number)
  for doc in result:
    pprint(doc)
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
            users_in_forbidden_city.add(int(activity['user_id']))

    print_query('10', users_in_forbidden_city)

def main():
    try:
        connection = DbConnector()
        db = connection.db
        query2(db)
        query6a(db)
        query6b(db)
        query10(db)
    except Exception as e:
        print('ERROR: Failed to use database:', e)
    finally:
        if connection:
            connection.close_connection()

if __name__ == '__main__':
    main()