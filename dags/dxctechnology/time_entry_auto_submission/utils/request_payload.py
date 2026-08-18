from datetime import datetime
import rail


def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%d %B %Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_time_entry_revision_data(dag_run):
    return {
        "users": [
            {
                "uri": dag_run.conf['Useruri'],
            }
        ],
        "dateRange": {
            "startDate": get_replicon_date(dag_run.conf['Timesheetstartdate']),
            "endDate": get_replicon_date(dag_run.conf['Timesheetenddate']),
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
        }
    }


def get_submit_batch_data():
    revision_uris = []
    for i in rail.result("entry_per_user")['entries']:
        revision_uris.append(i['revisionuri'])

    return {
        "timeEntryRevisionGroupUris": revision_uris,
        # pylint: disable=line-too-long
        "comments": "PLEASE NOTE: Time Entry(s) from the previous week and earlier that you did NOT SUBMIT have been AUTOMATICALLY SUBMITTED.  Please review any Timesheets that are not SUBMITTED (i.e. Not Entered, Not Submitted or Submission Failed) and use the “Submit Timesheet” button for them to be submitted ASAP."
    }
