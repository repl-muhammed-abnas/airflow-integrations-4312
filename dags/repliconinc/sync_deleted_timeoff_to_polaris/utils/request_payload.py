from datetime import datetime as dt, timedelta
from pendulum import now
import rail
from repliconinc.sync_deleted_timeoff_to_polaris import config
from repliconinc.sync_deleted_timeoff_to_polaris import instances

null = None

def get_run_report_payload(instance):
    get_specific_report_details = rail.result('get_specific_report_details')

    def get_specific_filter_uri(filter_name):
        return rail.find_first_by_attr_and_get_attr(
            get_specific_report_details["filterConfiguration"]["enabledFilters"], 'displayText', filter_name, 'uri')
    day_offset= 0 if instance in ["trial"] else 1
    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="ModifiedOnUtcDateRangeFilter"),
                        "value": None
                    },
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="ModifiedOnUtcDateRangeFilter"),
                        "value": str((now() - timedelta(days=day_offset)).strftime('%m/%d/%Y'))
                    },
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="ModifiedOnUtcDateRangeFilter"),
                        "value": str((now() - timedelta(days=day_offset)).strftime('%m/%d/%Y'))
                    },
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_run_report_payload1():
    get_specific_report_details = rail.result('get_specific_report_details1')

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }   


def _get_start_date_in_format_from_conf(dag_run):
    """
    Return a dict with keys 'year', 'month', 'day'.
    Prefer `dag_run.conf['start_date_in_format']` if provided,
    otherwise try to derive it from `timeoffdate` / `start_date` strings
    using a set of common date formats. Falls back to dateutil.parser if available.
    """
    s = dag_run.conf.get("start_date_in_format")
    if isinstance(s, dict) and all(k in s for k in ("year", "month", "day")):
        return s

    date_str = dag_run.conf.get("currentstartdate")
    if not date_str:
        raise KeyError("start_date_in_format not present and no 'timeoffdate'/'start_date' found in dag_run.conf")

    date_str = str(date_str).strip()

    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y%m%d",
        "%m-%d-%Y",
        "%b %d, %Y",   # Jan 7, 2026
        "%B %d, %Y",   # January 7, 2026
        "%b %d %Y",    # Jan 7 2026
        "%B %d %Y",    # January 7 2026
    )

    for fmt in formats:
        try:
            parsed = dt.strptime(date_str, fmt)
            return {"year": parsed.strftime("%Y"), "month": parsed.strftime("%m"), "day": parsed.strftime("%d")}
        except Exception:
            continue

    # try ISO parser as a last attempt
    try:
        parsed = dt.fromisoformat(date_str)
        return {"year": parsed.strftime("%Y"), "month": parsed.strftime("%m"), "day": parsed.strftime("%d")}
    except Exception:
        pass

    # try dateutil if available for more flexible parsing
    try:
        from dateutil.parser import parse as _dateutil_parse
        parsed = _dateutil_parse(date_str)
        return {"year": parsed.strftime("%Y"), "month": parsed.strftime("%m"), "day": parsed.strftime("%d")}
    except Exception:
        pass

    raise ValueError(f"Could not parse date from '{date_str}' to derive start_date_in_format")

    
def get_time_off_details_for_user_and_date_range_payload(dag_run):
    sd = _get_start_date_in_format_from_conf(dag_run)
    return {
        "userUri": dag_run.conf.get("useruri"),
        "dateRange": {
          "startDate": {
            "year": sd["year"],
            "month": sd["month"],
            "day": sd["day"]
          },
          "endDate": {
            "year": sd["year"],
            "month": sd["month"],
            "day": sd["day"]
          },
          "relativeDateRangeUri": null,
          "relativeDateRangeAsOfDate": null
        }
    }

def delete_timeoff_payload_1():
    return {
        "timeOffUri": rail.result("get_time_off_details_for_user_and_date_range_1")[0].get("uri")
    }
