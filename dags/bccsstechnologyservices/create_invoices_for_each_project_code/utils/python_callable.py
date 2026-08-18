# pylint: disable=line-too-long
from datetime import datetime, timedelta
import rail
from rail.lib.artifact import existing_artifact
null = None


def get_todays_date_data():
    time_now = datetime.now()
    return {'year': time_now.strftime("%Y"),
            'month': time_now.strftime("%-m"),
            'day': time_now.strftime("%e")
            }


def get_payment_date_data():
    time_now = datetime.now()
    due_date = time_now + timedelta(days=15)
    return {'year': due_date.strftime("%Y"),
            'month': due_date.strftime("%-m"),
            'day': due_date.strftime("%e")
            }


def get_start_date_data(dag_run):
    date = dag_run.conf['start_date']
    if date:
        start_date = datetime.strptime(date, "%m/%d/%Y")
        return {'year': start_date.strftime("%Y"),
                'month': start_date.strftime("%-m"),
                'day': start_date.strftime("%e")
                }
    return null


def get_end_date_data(dag_run):
    date = dag_run.conf['end_date']
    if date:
        end_date = datetime.strptime(date, "%m/%d/%Y")
        return {'year': end_date.strftime("%Y"),
                'month': end_date.strftime("%-m"),
                'day': end_date.strftime("%e")
                }
    return null


def get_email_file_data():
    with existing_artifact(rail.result('download_email_file'), mode='r') as artifact:
        data = artifact.file.read()
        return data
