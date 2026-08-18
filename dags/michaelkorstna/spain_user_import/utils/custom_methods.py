from datetime import datetime

def get_date_string(dateobj):
    return str(dateobj['day']) + "/" + str(dateobj['month']) + "/" + str(dateobj['year'])

def get_date_object(datestring):
    dateobj = datetime.strptime(datestring, "%d/%m/%Y")
    return {
        'day': dateobj.day,
        'month': dateobj.month,
        'year': dateobj.year
    }
