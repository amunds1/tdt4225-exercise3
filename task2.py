from datetime import timezone, datetime

from haversine import haversine
from tabulate import tabulate

from DbHandler import DatabaseHandler
from utils.DbConnector import DbConnector


class Question:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    # Complete
    def one(self):
        for collection in ["users", "activities", "trackpoints"]:
            print("{collection} has {count} documents".format(
                collection=collection,
                count=self.db[collection].count_documents(filter={})))

    # Complete
    def two(self):
        for result in self.db["activities"].aggregate([
            {
                '$group': {
                    '_id': '$user_id',
                    'num_activities': {
                        '$count': {}
                    }
                }
            }, {
                '$group': {
                    '_id': None,
                    'avgActivities': {
                        '$avg': '$num_activities'
                    },
                    'minActivities': {
                        '$min': '$num_activities'
                    },
                    'maxActivities': {
                        '$max': '$num_activities'
                    }
                }
            }
        ]):
            print(f"Average: {result['avgActivities']}")
            print(f"Min: {result['minActivities']}")
            print(f"Max: {result['maxActivities']}")

    # Complete
    def three(self):
        print("List of top 10 user with the most activities")
        for user in self.db["users"].aggregate([
            {
                '$project': {
                    '_id': '$user_id',
                    'activities_count': {
                        '$size': {
                            '$ifNull': [
                                '$activities', []
                            ]
                        }
                    }
                }
            }, {
                '$sort': {
                    'activities_count': -1
                }
            }, {
                '$limit': 10
            }
        ]):
            print(f"User: {user['_id']} Activities: {user['activities_count']}")

    # Complete
    def four(self):
        for result in self.db["activities"].aggregate([
            {
                '$project': {
                    '_id': '$user_id',
                    'duration': {
                        '$dateDiff': {
                            'startDate': '$start_date_time',
                            'endDate': '$end_date_time',
                            'unit': 'day'
                        }
                    }
                }
            }, {
                '$match': {
                    'duration': {
                        '$gte': 1
                    }
                }
            }, {
                '$group': {
                    '_id': '$_id'
                }
            }, {
                '$count': 'users'
            }
        ]):
            print(f"Number of users that have started an activity one day and ended it the next: {result['users']}")

    # Complete
    def five(self):
        for result in self.db["activities"].aggregate([
            {
                '$group': {
                    '_id': {
                        'user_id': '$user_id',
                        'transportation_mode': '$transportation_mode',
                        'start_date_time': '$start_date_time',
                        'end_date_time': '$end_date_time',
                        'trackpoints': '$trackpoints'
                    },
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$match': {
                    'count': {
                        '$gte': 2
                    }
                }
            }
        ], allowDiskUse=True):
            print(result)

    # Complete
    def six(self):
        for result in self.db["trackpoints"].aggregate([
            {
                '$geoNear': {
                    'near': {
                        'type': 'Point',
                        'coordinates': [
                            116.33031, 39.97548
                        ]
                    },
                    'distanceField': 'dist.calculated',
                    'maxDistance': 100,
                    'includeLocs': 'dist.location',
                    'spherical': True
                }
            }, {
                '$match': {
                    '$expr': {
                        '$eq': [
                            '2008-08-24 15:38:00', {
                                '$dateToString': {
                                    'date': '$date_time',
                                    'format': '%Y-%m-%d %H:%M:%S'
                                }
                            }
                        ]
                    }
                }
            }, {
                '$match': {
                    'date_time': {
                        '$gte': datetime(2008, 8, 24, 14, 38, 0, tzinfo=timezone.utc),
                        '$lt': datetime(2010, 5, 1, 16, 38, 0, tzinfo=timezone.utc)
                    }
                }
            }, {
                '$lookup': {
                    'from': 'users',
                    'localField': 'user_id',
                    'foreignField': '_id',
                    'as': 'user'
                }
            }, {
                '$unwind': {
                    'path': '$user'
                }
            }
        ]):
            print(f"User close in time and space: {result['user']['user_id']}")

    # Complete
    def seven(self):
        users = ['135', '132', '104', '103', '168', '157', '150', '159', '166', '161', '102', '105', '133', '134',
                 '160', '158',
                 '167', '151', '169', '156', '024', '023', '015', '012', '079', '046', '041', '048', '077', '083',
                 '084', '070',
                 '013', '014', '022', '025', '071', '085', '049', '082', '076', '040', '078', '047', '065', '091',
                 '096', '062',
                 '054', '053', '098', '038', '007', '000', '009', '036', '031', '052', '099', '055', '063', '097',
                 '090', '064',
                 '030', '008', '037', '001', '039', '006', '174', '180', '173', '145', '142', '129', '116', '111',
                 '118', '127',
                 '120', '143', '144', '172', '181', '175', '121', '119', '126', '110', '128', '117', '153', '154',
                 '162', '165',
                 '131', '136', '109', '100', '107', '138', '164', '163', '155', '152', '106', '139', '101', '137',
                 '108', '130',
                 '089', '042', '045', '087', '073', '074', '080', '020', '027', '018', '011', '016', '029', '081',
                 '075', '072',
                 '086', '044', '088', '043', '017', '028', '010', '026', '019', '021', '003', '004', '032', '035',
                 '095', '061',
                 '066', '092', '059', '050', '057', '068', '034', '033', '005', '002', '056', '069', '051', '093',
                 '067', '058',
                 '060', '094', '112', '115', '123', '124', '170', '177', '148', '141', '146', '179', '125', '122',
                 '114', '113',
                 '147', '178', '140', '176', '149', '171']

        for user in self.db["activities"].aggregate([
            {
                '$match': {
                    'transportation_mode': 'taxi'
                }
            }
        ]):
            users.remove(user['user_id'])

        print(users)

    # Complete
    def eight(self):
        for transportation in self.db["activities"].aggregate([
            {
                '$match': {
                    'transportation_mode': {
                        '$ne': None
                    }
                }
            }, {
                '$group': {
                    '_id': '$transportation_mode',
                    'distinct_users': {
                        '$count': {}
                    }
                }
            }
        ]):
            print(f"{transportation['_id']} {transportation['distinct_users']} distinct users")

    # Complete
    def nine(self):
        # Task a
        for result in self.db["activities"].aggregate([
            {
                '$group': {
                    '_id': {
                        'year': {
                            '$year': '$start_date_time'
                        },
                        'month': {
                            '$month': '$start_date_time'
                        }
                    },
                    'total_cost_month': {
                        '$count': {}
                    }
                }
            }, {
                '$sort': {
                    'total_cost_month': -1
                }
            }, {
                '$limit': 1
            }
        ]):
            print(f"{result['_id']['year']}-{result['_id']['month']} had the most activities")

        # Task b
        for result in self.db["activities"].aggregate([
            {
                '$project': {
                    'user_id': '$user_id',
                    'month': {
                        '$month': '$start_date_time'
                    },
                    'year': {
                        '$year': '$start_date_time'
                    },
                    'duration': {
                        '$dateDiff': {
                            'startDate': '$start_date_time',
                            'endDate': '$end_date_time',
                            'unit': 'minute'
                        }
                    }
                }
            }, {
                '$match': {
                    'month': 11,
                    'year': 2008
                }
            }, {
                '$group': {
                    '_id': '$user_id',
                    'activities': {
                        '$count': {}
                    },
                    'total_duration': {
                        '$sum': '$duration'
                    }
                }
            }, {
                '$sort': {
                    'activities': -1
                }
            }, {
                '$limit': 2
            }
        ]):
            print(
                f"User {result['_id']} has a total of {result['activities']} with a total duration of "
                f"{result['total_duration'] / 60} hours recorded in november of 2008")

    # Complete
    def ten(self):
        result = self.db["activities"].aggregate([
            {
                '$match': {
                    'transportation_mode': 'walk'
                }
            }, {
                '$lookup': {
                    'from': 'users',
                    'localField': 'user_id',
                    'foreignField': 'user_id',
                    'as': 'user'
                }
            }, {
                '$unwind': {
                    'path': '$user'
                }
            }, {
                '$match': {
                    'user_id': '112'
                }
            }, {
                '$lookup': {
                    'from': 'trackpoints',
                    'localField': 'trackpoints',
                    'foreignField': '_id',
                    'as': 'trackpoints_embedded'
                }
            }
        ])

        distance = 0
        oldPos = (0, 0)

        for user in result:
            for trackpoint in user["trackpoints_embedded"]:
                newPos = (trackpoint["location"]["coordinates"][1], trackpoint["location"]["coordinates"][0])
                distance += haversine(oldPos, newPos)

                oldPos = newPos

        print(round(distance, 2), "kilometres")

    # Complete
    def eleven(self):
        usersWithAltitudeGained = {}

        for userIDQuery in range(0, 183):
            formattedUserID = str(userIDQuery).zfill(3)
            print(f"Now on user {formattedUserID}")

            queryResult = self.db["activities"].aggregate([
                {
                    '$lookup': {
                        'from': 'users',
                        'localField': 'user_id',
                        'foreignField': 'user_id',
                        'as': 'user'
                    }
                }, {
                    '$unwind': {
                        'path': '$user'
                    }
                }, {
                    '$match': {
                        'user.user_id': f'{formattedUserID}'
                    }
                }, {
                    '$lookup': {
                        'from': 'trackpoints',
                        'localField': 'trackpoints',
                        'foreignField': '_id',
                        'as': 'trackpoints_embedded'
                    }
                }
            ])

            for activity in queryResult:
                userID = activity['user_id']

                altitudeGained = 0
                oldAlt = -999

                for trackpoint in activity["trackpoints_embedded"]:
                    newAlt = trackpoint["altitude"]

                    # On first iteration set oldAlt to newAlt to get correct initial alt difference
                    if oldAlt == -999:
                        oldAlt = newAlt

                    if newAlt > oldAlt or newAlt != -777:
                        altitudeGained += newAlt - oldAlt

                        usersWithAltitudeGained[userID] = altitudeGained

                    oldAlt = newAlt

        sortedResult = sorted(usersWithAltitudeGained.items(), key=lambda x: x[1], reverse=True)
        print(tabulate(sortedResult[0:20], headers=('User ID', 'Altitude gained (m)')))

    # Complete
    def twelve(self):
        invalidActivities = {}

        for userIDQuery in range(0, 183):
            print(f"Now on user {userIDQuery}")

            queryResult = self.db["activities"].aggregate([
                {
                    '$lookup': {
                        'from': 'users',
                        'localField': 'user_id',
                        'foreignField': 'user_id',
                        'as': 'user'
                    }
                }, {
                    '$unwind': {
                        'path': '$user'
                    }
                }, {
                    '$match': {
                        'user.user_id': f'{str(userIDQuery).zfill(3)}'
                    }
                }, {
                    '$lookup': {
                        'from': 'trackpoints',
                        'localField': 'trackpoints',
                        'foreignField': '_id',
                        'as': 'trackpoints_embedded'
                    }
                }
            ])

            for activity in queryResult:
                oldTrackpoint = None

                activityIsInvalid = True

                for trackpoint in activity["trackpoints_embedded"]:
                    # Skip comparison trackpoint is the first one in the array
                    if oldTrackpoint is None:
                        oldTrackpoint = trackpoint
                        continue

                    # Calculate time delta in minutes between current trackpoint and old trackpoint
                    timeDifference = (trackpoint["date_time"] - oldTrackpoint["date_time"]).total_seconds() / 60

                    # If time difference >= 5 min continue iterating
                    if timeDifference >= 5:
                        activityIsInvalid = True
                    else:
                        # If time difference is < 5 min move on to the next activity
                        activityIsInvalid = False
                        break

                    # Set old trackpoint to current trackpoint
                    oldTrackpoint = trackpoint

                if activityIsInvalid:
                    if activity['user_id'] not in invalidActivities:
                        invalidActivities[activity['user_id']] = 1
                    else:
                        invalidActivities[activity['user_id']] += 1

        for i in invalidActivities:
            print(f"{i} has {invalidActivities[i]} invalid activities")


def main():
    question = Question()

    # question.one()
    # question.two()
    # question.three()
    # question.four()
    # question.five()
    # question.six()
    question.seven()
    # question.eight()
    # question.nine()
    # question.ten()
    # question.eleven()
    # question.twelve()


if __name__ == '__main__':
    main()
