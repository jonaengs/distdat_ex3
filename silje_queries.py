from geopy.distance import geodesic
import re
from DbConnector import DbConnector


db = DbConnector().db

# Find the top 20 users with the highest number of activities
def query3(db):
    users = []
    counts = []
    for userId in range(182):
        count = return_documents("Activity").count_documents({"userId": userId})
        # gå igjennom alle values hver gang en instans settes inn
        for index in range(len(userId)):
            if count > counts[index]:
                users.insert(index, userId)
                counts.insert(index, count)
                break
    return users[:20], counts[:20]

# find all users who have taken a taxi   
def query4(db):
    users = []
    for userId in range(182):
        taxi_count = return_documents("Activity").count_documents({"userId": userId, "transportation_mode": "taxi"})
        if taxi_count > 0:
            users.append(userId)
    return users

# Find the total distance (in km) walked in 2008, by user with id=112.
def query7(db): 
    activities = return_documents("Activity").find({"userId": 112, "transportation_mode": "walk"}).toArray()
    distance = 0
    for activity in activities:
        if regExMatch("2018", activity.start_date_time, activity.end_date_time):
            trackPoints = return_documents("TrackPoints").find({"activityId": activity.id}).toArray()
            for index in range(len(1,trackPoints)):
                cords1 = (trackPoints[index-1].lat, trackPoints[index-1].lon)
                cords2 = (trackPoints[index].lat, trackPoints[index].lon)
                distance += geodesic(cords1, cords2).km
    return distance

#Find the top 20 users who gained the most altitude meters
def query8(db):
    users = []
    counts = []
    for userId in range(0, 182):
        altitude = 0
        trackPoints = return_documents("TrackPoints").find({"userId": userId}).toArray()
        # gå igjennom alle values hver gang en instans settes inn
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


# Find all users who have registered transportation_mode and their most used transportation_mode
def query11(db):
    transport_modes = query_5()[0]
    users = []
    most_used_tran_modes = []
    for userId in range(len(0,182)):
        highestCount = 0
        mostUsedTranMode = ""
        for transport_mode in transport_modes:
            count = return_documents("Activity").count_documents({"transportation_mode": transport_mode, "userId": userId})  
            if count > highestCount and transport_mode != None: # None, null???
                highestCount = count
                mostUsedTranMode = transport_mode
        if highestCount > 0:
            users.append(userId)
            most_used_tran_modes.append(mostUsedTranMode)
    return users, most_used_tran_modes

def regExMatch(regExp, testStr1, testStr2):
    match1 = re.search(regExp, testStr1)
    match2 = re.search(regExp, testStr2)
    if match1 != None or match2 != None:
        return True
    else:
        return False

def return_documents(collection_name):
    collection = db[collection_name]
    documents = collection.find({})
    return documents.toArray()

def do_query():
    for q in (query3, query7, query11):
        q(db)

if __name__ == "__main__":
    do_query()
