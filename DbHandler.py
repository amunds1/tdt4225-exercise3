import os
import bson
from pprint import pprint
import time
from utils.DbConnector import DbConnector
from utils.utils import convertToCorrectDateFormat, split_activity
from datetime import datetime
from dateutil import parser

class DatabaseHandler:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

        self.users = []
        self.activities = []
        self.trackpoints = []

    def create_collection(self, collection_name):
        collection = self.db.create_collection(collection_name)
        # print('Created collection: ', collection)

    def drop_and_create_collections(self):
        # Drop collections and remove data
        # self.drop_collection("trackpoints")
        # self.drop_collection("activities")
        # self.drop_collection("users")

        # Create collections
        self.create_collection("trackpoints")
        self.create_collection("activities")
        self.create_collection("users")

    def fetch_documents_from_collection(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_collection(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_collections(self):
        collections = self.client['exercise3'].list_collection_names()
        print(collections)

    """
    INSERT DATA METHODS
    """
    def index_users(self):
        # Using insert_many() 0.0286 sec
        # Using insert_one() 0.1003 sec

        startTime = time.time()

        # Generate dictionary of users where entries are on the form {"000": {"id_": 000, "has_labels": False}}
        users = dict((str(userID).zfill(3), {"user_id": str(userID).zfill(3), "has_labels": False, "activities": []})
                     for userID in range(0, 183))

        # Iterate trough labeled_ids.txt and set "has_labels" to True, if the an user with a match ID exists there
        with open("../dataset/dataset/labeled_ids.txt") as file:
            for userID in file:
                users[str(userID[:-1])].update({"has_labels": True})

        self.users = users.values()

        print("Indexed 183 users in {time} seconds\n".format(time=(round(time.time() - startTime, 4))))

    def insert_trackpoint(self, filePath, fileName, userID, labelsFilePath="null"):
        with open(filePath) as trackpointsFile:
            # Stores all trajectories in a .plt file
            trackpointsInFile = []

            # Generate an unique id for each activity
            activityID = bson.objectid.ObjectId()

            collection = self.db["users"]
            user = collection.find_one({'user_id': f"{userID}"})

            # Iterate through each trackpoint
            for trackpoint in trackpointsFile.readlines()[6:]:
                trackpoint = trackpoint.split(",")

                # Convert to datetime object
                dateTimeParsed = parser.parse(trackpoint[5] + " " + trackpoint[6].replace('\n', ''))

                # Long lat
                trackpointsInFile.append({
                    "_id": bson.objectid.ObjectId(),
                    "location": {
                        "type": "Point",
                        "coordinates": [float(trackpoint[1]), float(trackpoint[0])]
                    },
                    "activity_id": activityID,
                    "user_id": user["_id"],
                    "altitude": float(trackpoint[3]),
                    "date_days": "",
                    "date_time": dateTimeParsed,
                })

            activityFormatted = {
                "_id": activityID,
                "user_id": userID,
                "transportation_mode": None,
                "start_date_time": trackpointsInFile[0]['date_time'],
                "end_date_time": trackpointsInFile[-1]['date_time'],
                "trackpoints": [trackpoint['_id'] for trackpoint in trackpointsInFile]
            }

            # If user has a label.txt file, match entries against .plt file
            if labelsFilePath != "null":
                with open(labelsFilePath) as activitiesFile:

                    # Loop trough each activity in activities file
                    for activity in activitiesFile.readlines()[1:]:
                        # Split activity into array to easily retrieve certain information
                        activity = split_activity(activity)

                        activityStartTimeFormatted = parser.parse(convertToCorrectDateFormat(activity[0])
                                                                  + " " + activity[1])
                        activityEndTimeFormatted = parser.parse(convertToCorrectDateFormat(activity[2])
                                                                + " " + activity[3])

                        if activityStartTimeFormatted == trackpointsInFile[0]["date_time"] \
                                and activityEndTimeFormatted == trackpointsInFile[-1]["date_time"]:
                            print("Match")
                            activityFormatted["start_date_time"] = activityStartTimeFormatted
                            activityFormatted["end_date_time"] = activityEndTimeFormatted
                            activityFormatted["transportation_mode"] = activity[4]

                        # Once a match is found, skip iterating trough the rest of the file
                        break

            self.activities.append(activityFormatted)

            collection = self.db["users"]
            collection.update_one({'user_id': f"{userID}"}, {'$push': {'activities': activityID}})

            self.trackpoints.extend(trackpointsInFile)

    def insert_data(self):
        """
        Add everything then insert: 21 mins
        Insert after iterating trough a .plt file: 4.5 min
        Insert after iterating trough a .plt file with datetime parsing: 16.583

        """

        startTime = time.time()

        self.db["users"].create_index("user_id")

        # Populate users collection
        self.index_users()
        self.db["users"].insert_many(self.users)

        # 100 ("../dataset/dataset/Data/100")
        # 097 has matching activities
        for (root, dirs, files) in os.walk("../dataset/dataset"):
            if "Trajectory" in root:
                # Retrieve user id from root
                userID = root.split("/")[4]

                print(f"Now on user: {userID}")

                # Iterate trough each .plt file belonging to a user
                for trajectoryFile in files:

                    fileName = trajectoryFile.split(".")[0]

                    # Skip .plt files with more than 2500 trackpoints
                    if sum(1 for _ in open(f"{root}/{trajectoryFile}")) >= 2506:
                        continue

                    if os.path.isfile(f"../dataset/dataset/Data/{userID}/labels.txt"):

                        self.insert_trackpoint(filePath=f"{root}/{trajectoryFile}",
                                               fileName=fileName,
                                               userID=userID,
                                               labelsFilePath=f"../dataset/dataset/Data/{userID}/labels.txt")
                    else:
                        self.insert_trackpoint(filePath=f"{root}/{trajectoryFile}", fileName=fileName, userID=userID)

                    self.db["activities"].insert_many(self.activities)
                    self.db["trackpoints"].insert_many(self.trackpoints)

                    self.activities = []
                    self.trackpoints = []

        print("Inserted data in {time} seconds".format(time=(round(time.time() - startTime, 4))))


def main():
    handler = DatabaseHandler()
    handler.drop_and_create_collections()
    handler.insert_data()


if __name__ == '__main__':
    main()
