def convertToCorrectDateFormat(date):
    return date.replace("/", "-")


def split_activity(activity):
    # Replace tabulates with whitespace
    activity = activity.replace("\t", " ")
    # Replace newline with empty string
    activity = activity.replace("\n", "")
    # Replace / with - for correct date format
    activity = activity.replace("/", "-")

    activity = activity.split(" ")

    return activity
