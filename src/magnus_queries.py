from pprint import pprint
from DbConnector import DbConnector


def print_query(query_number, result):
    print('Query %s:\n' % query_number)
    for doc in result:
        pprint(doc)
    print('\n')


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
    print_query(5, result)


def main():
    try:
        connection = DbConnector()
        db = connection.db
        query3(db)
        query4(db)
        query5(db)
    except Exception as e:
        print('ERROR: Failed to use database:', e)
    finally:
        if connection:
            connection.close_connection()


if __name__ == '__main__':
    main()
