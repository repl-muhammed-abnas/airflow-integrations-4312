import rail
import pendulum
from seaspanshipyards.auto_timesheet_generation import config


def get_report_generate_batch_payload():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")["uri"],
                "filterValues": "",
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_generate_timesheets_payload():
    next_date = (pendulum.now(config.time_zone)).add(days=1)
    return {
        "userUris": list(map(lambda item: item['useruri'], rail.get_current_context()['dag_run'].conf['user_data'])),
        "date": {
            "year": next_date.year,
            "month": next_date.month,
            "day": next_date.day
        },
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
    }
