import rail
from odessa.resource_allocation_removal_on_holidays.utils.python_callable import get_start_date
from odessa.resource_allocation_removal_on_holidays.utils.python_callable import get_end_date

null = None


def get_holidays_in_data_range_payload():
    return {
        "holidayCalendarUri": rail.result('for_each_item_in_querylist_do')['holidaycalendaruri'],
        "dateRange": {
            "startDate": {
                "year": get_start_date()['year'],
                "month": get_start_date()['month'],
                "day": get_start_date()['day']
            },
            "endDate": {
                "year": get_end_date()['year'],
                "month": get_end_date()['month'],
                "day": get_end_date()['day']
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_report_payload(dag_run):
    return {
        "resourceUri": dag_run.conf['resourceUri'],
        "dateRange": {
            "startDate": {
                "year": dag_run.conf['holidayyear'],
                "month": dag_run.conf['holidaymonth'],
                "day": dag_run.conf['holidayday']
            },
            "endDate": {
                "year": dag_run.conf['holidayyear'],
                "month": dag_run.conf['holidaymonth'],
                "day": dag_run.conf['holidayday']
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_project_resource_allocation_payload(dag_run):
    return {
        "projectUri": rail.result('for_each_project_allocation')['project']['uri'],
        "resourceUri": dag_run.conf['resourceUri'],
        "dateRange": {
            "startDate": {
                "year": dag_run.conf['holidayyear'],
                "month": dag_run.conf['holidaymonth'],
                "day": dag_run.conf['holidayday']
            },
            "endDate": {
                "year": dag_run.conf['holidayyear'],
                "month": dag_run.conf['holidaymonth'],
                "day": dag_run.conf['holidayday']
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        },
        "allocationTime": {
            "hoursPerDay": null,
            "percentageOfWorkDay": null,
            "totalDuration": {
                "hours": "0",
                "minutes": "0",
                "seconds": "0",
                "milliseconds": "0",
                "microseconds": "0"
            }
        },
        "workdayUris": [],
        "resourceAllocationOptionUris": []
    }
