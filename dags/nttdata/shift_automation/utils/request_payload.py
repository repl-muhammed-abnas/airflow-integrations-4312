null = None

def get_shift_schedule_summary_payload(dag_run):
    return {
        "userSearch": {
          "includeShiftAssignmentsWithNoUser": "false",
          "specificUserUris": [
            dag_run.conf["useruri"]
          ]
        },
        "shiftSearch": null,
        "objectExtensionFieldSearches": [],
        "dateRange": {
          "startDate": {
            "year": dag_run.conf["startdateyear"],
            "month": dag_run.conf["startdatemonth"],
            "day": dag_run.conf["startdateday"]
          },
          "endDate": {
            "year": dag_run.conf["enddateyear"],
            "month": dag_run.conf["enddatemonth"],
            "day": dag_run.conf["enddateday"]
          },
          "relativeDateRangeUri": null,
          "relativeDateRangeAsOfDate": null
        }
    }
