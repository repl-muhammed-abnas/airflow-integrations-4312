from datetime import datetime

def get_todays_date():
    date = datetime.now()
    return {
        'date': date.strftime("%m/%d/%Y"),
        'description': 'Effective on ' + date.strftime("%m/%d/%Y"),
        'day': date.day,
        'month': date.month,
        'year': date.year
    }

def get_date_object(datestring):
    date = datetime.strptime(datestring, "%m/%d/%Y")
    return {
        'day': date.day,
        'month': date.month,
        'year': date.year
    }

def get_date_string_from_object(dateobj):
    return str(dateobj['month']) + '/' + str(dateobj['day']) + '/' + str(dateobj['year'])
