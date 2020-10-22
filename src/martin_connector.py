from pymongo import MongoClient, version


class DbConnector:
    def __init__(self,
                 DATABASE='martin_test',
                 HOST="tdt4225-02.idi.ntnu.no",
                 USER="martin",
                 PASSWORD="martin"):
        uri = "mongodb://%s:%s@%s/%s" % (USER, PASSWORD, HOST, DATABASE)
        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
