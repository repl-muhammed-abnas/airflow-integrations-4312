from datetime import datetime

def get_date_object(datestring):
    dateobj = datetime.strptime(datestring, "%d/%m/%Y")
    return {
        'day': dateobj.day,
        'month': dateobj.month,
        'year': dateobj.year
    }
