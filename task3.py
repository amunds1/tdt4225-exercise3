from datetime import timezone, datetime

from haversine import haversine

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

    def seven(self):
        users = []
        for user in self.db["users"].aggregate([
            {
                '$match': {
                    'transportation_mode': {
                        '$eq': 'taxi'
                    }
                }
            }, {
                '$group': {
                    '_id': '$user_id'
                }
            }
        ]):
            users.append(user['_id'])

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

    def ten(self):
        # Find the total distance (in km) walked in 2008, by user with id=112.
        result = self.db["users"].aggregate([
            {
                '$match': {
                    'user_id': '112'
                }
            }, {
                '$lookup': {
                    'from': 'trackpoints',
                    'localField': '_id',
                    'foreignField': 'user_id',
                    'as': 'trackpoints_embedded'
                }
            }
        ])

        for user in result:
            distance = 0
            oldPos = (0, 0)

            for trackpoint in user["trackpoints_embedded"]:
                newPos = (trackpoint["location"]["coordinates"][1], trackpoint["location"]["coordinates"][0])
                distance += haversine(oldPos, newPos)

                oldPos = newPos

        print(round(distance, 2), "kilometres")


def main():
    question = Question()

    # question.one()
    # question.two()
    # question.three()
    # question.four()
    # question.six()
    # question.seven()
    # question.eight()
    # question.nine()
    question.ten()


if __name__ == '__main__':
    main()
