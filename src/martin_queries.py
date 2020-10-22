from pprint import pprint 
from martin_connector import DbConnector

def print_query(query_number, result):
  print("Query %s:\n" % query_number)
  for doc in result:
    pprint(doc)
  print("\n")

def query2(db):
    # Find the average number of activities per user

    collection = db["martin_test"]
    #result = collection.find({})
    result = collection.aggregate(
      [{
        "$group":
            {
              "_id": "$qty",
              "qty_count": {"$sum": 1}
            }
      },
      {
        "$group":
            {
              "_id": "null",
              "average_count_per_qty": {"$avg": "$user_count"}
            }
      }]
    )
    print_query(2, result)

def query6(db):
    # a) Find the year with the most activities.
    # b) Is this also the year with most recorded hours?

    collection = db["Person"]
    result = collection.aggregate(
      [
        {
          "$project":
              {
                "_id": "$_id",
                "Average_courses": {"$cond": {"if": {"$isArray": "$courses"}, "then": {"$size": "$courses"}, "else": "NA"}}
              }
        },
        {
          "$sort": {"_id": -1}
        },
        {
          "$limit": 3
        }
      ]
    )
    print_query("6a", result)

def main():
    try:
        connection = DbConnector()
        db = connection.db
        query2(db)
        #query6(db)
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if connection:
            connection.close_connection()

if __name__ == '__main__':
    main()