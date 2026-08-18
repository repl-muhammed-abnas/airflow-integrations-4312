from datetime import datetime
from os.path import getsize
from rail import existing_artifact, result


def do_has_file_content():
    with existing_artifact(result('download_file')) as artifact:
        return getsize(artifact.local_filename) > 0


def get_replicon_date(date_str, fmt='%Y/%m/%d'):
    datetime_obj = datetime.strptime(date_str, fmt)
    return {
        'year': datetime_obj.year,
        'month': datetime_obj.month,
        'day': datetime_obj.day
    }


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }
