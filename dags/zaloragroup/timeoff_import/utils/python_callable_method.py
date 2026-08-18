from datetime import datetime, timezone
import os
import rail

def get_required_timeoffuri(timeofftype):
    all_timeoff_types = rail.result('get_all_time_off_types')
    timeoffuri = ''
    for timeoff in all_timeoff_types:
        if timeoff.get('displayText') == timeofftype:
            timeoffuri = timeoff.get('uri')
    return timeoffuri

def get_current_date():
    return  datetime.now(timezone.utc).strftime("%m_%d_%Y_T%H_%M_%S")

def to_download():
    file_path = rail.result('new_file_sensor')
    file_name = os.path.split(file_path)[1]
    if file_name not in ('Processing','Logs','Archive'):
        return True
    return False
