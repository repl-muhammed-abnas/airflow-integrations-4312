from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import pendulum
import rail
from crl.payroll_export_uk.mapper.payroll_calendar_mapper import UK_PAYROLL_CALENDER_MAPPER

null = None

def get_compose_item_payroll_uk_data_row(item):
    return [
        item["RECTY"],
        item["CLIID"],
        item["INTCA"],
        item["ORDNO"],
        item["IOPER"],
        item["INFTY"],
        item["SUBTY"],
        item['BEGDA'],
        item['ENDDA'],
        item["OBJPS"],
        item["SPRPS"],
        item["SEQNR"],
        item["EXTRA"],
        item["LGART"],
        item["STDAZ"],
        item["BEGUZ"],
        item["ENDUZ"],
        item["BETRG"],
        item["WAERS"],
        item["ANZHL"].replace(",", ""),
        item["ZEINH"],
        item["VTKEN"],
        item["BWGRL"],
        item["AUFKZ"],
        item["ENDOF"],
        item["UFLD1"],
        item["UFLD2"],
        item["UFLD3"],
        item["KEYPR"],
        item["TRFGR"],
        item["TRFST"],
        item["PRAKN"],
        item["PRAKZ"],
        item["OTYPE"],
        item["PLANS"],
        item["VERSL"],
        item["EXBEL"],
        item["WTART"],
        item["TDLANGU"],
        item["TDSUBLA"],
        item["TDTYPE"]
    ]

def get_create_payrun_download_batch_payload():
    payrunuri = rail.result('get_payrun_batch_result')['payRunUri']
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [payrunuri],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        },
        "fileFormatScriptUri": rail.result("get_adp_payroll_script")
    }

def get_export_start_date(end_date):
    return end_date + relativedelta(months=-6)

def get_export_end_date(time_zone,dag_run):
    current_date = pendulum.now(time_zone).strftime("%d-%m-%Y")
    if dag_run.conf.get("pay_period_end_date"):
        return dt.strptime(dag_run.conf.get("pay_period_end_date"), "%d-%m-%Y").date()
    return dt.strptime(rail.find_first_by_attr_and_get_attr(UK_PAYROLL_CALENDER_MAPPER,
                                                            "payroll_processing_date", current_date, "pay_period_end_date"), "%d-%m-%Y").date()

def get_create_payrun_batch_payload(time_zone, dag_run):
    end_date = get_export_end_date(time_zone, dag_run)
    start_date = get_export_start_date(end_date)
    return  {
  "columnUris": [],
  "filterExpression": {
    "leftExpression": {
      "leftExpression": {
        "leftExpression": {
          "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
          },
          "operatorUri": "urn:replicon:filter-operator:in",
          "rightExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": {
              "uri": null,
              "uris": [],
              "bool": null,
              "date": null,
              "money": null,
              "number": null,
              "text": null,
              "time": null,
              "calendarDayDurationValue": null,
              "workdayDurationValue": null,
              "dateRange": {
                "startDate": {
                  "year": start_date.year,
                  "month": start_date.month,
                  "day": start_date.day
                },
                "endDate": {
                  "year": end_date.year,
                  "month": end_date.month,
                  "day": end_date.day
                },
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              },
              "dateTimeUtc": null,
              "dateTimeUtcRange": null,
              "numberRange": null
            },
            "filterDefinitionUri": null
          },
          "value": null,
          "filterDefinitionUri": null
        },
        "operatorUri": "urn:replicon:filter-operator:and",
        "rightExpression": {
          "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run-status"
          },
          "operatorUri": "urn:replicon:filter-operator:in",
          "rightExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": {
              "uri": null,
              "uris": [
                "urn:replicon:payable-time-pay-run-status:none"
              ],
              "bool": null,
              "date": null,
              "money": null,
              "number": null,
              "text": null,
              "time": null,
              "calendarDayDurationValue": null,
              "workdayDurationValue": null,
              "dateRange": null,
              "dateTimeUtc": null,
              "dateTimeUtcRange": null,
              "numberRange": null
            },
            "filterDefinitionUri": null
          },
          "value": null,
          "filterDefinitionUri": null
        },
        "value": null,
        "filterDefinitionUri": null
      },
      "operatorUri": "urn:replicon:filter-operator:and",
      "rightExpression": {
        "leftExpression": {
          "leftExpression": null,
          "operatorUri": null,
          "rightExpression": null,
          "value": null,
          "filterDefinitionUri": "urn:replicon:pay-run-filter:payable-time-approval-status"
        },
        "operatorUri": "urn:replicon:filter-operator:in",
        "rightExpression": {
          "leftExpression": null,
          "operatorUri": null,
          "rightExpression": null,
          "value": {
            "uri": null,
            "uris": [
              "urn:replicon:payable-time-approval-status:approved"
            ],
            "bool": null,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null,
            "dateTimeUtc": null,
            "dateTimeUtcRange": null,
            "numberRange": null
          },
          "filterDefinitionUri": null
        },
        "value": null,
        "filterDefinitionUri": null
      },
      "value": null,
      "filterDefinitionUri": null
    },
    "operatorUri": "urn:replicon:filter-operator:and",
    "rightExpression": {
      "leftExpression": {
        "leftExpression": {
          "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:pay-run-filter:location"
          },
          "operatorUri": "urn:replicon:filter-operator:in",
          "rightExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": {
              "uri": null,
              "uris": rail.result("get_location_child_hierarchy_data"),
              "bool": null,
              "date": null,
              "money": null,
              "number": null,
              "text": null,
              "time": null,
              "calendarDayDurationValue": null,
              "workdayDurationValue": null,
              "dateRange": null,
              "dateTimeUtc": null,
              "dateTimeUtcRange": null,
              "numberRange": null
            },
            "filterDefinitionUri": null
          },
          "value": null,
          "filterDefinitionUri": null
        },
        "operatorUri": "urn:replicon:filter-operator:and",
        "rightExpression": {
          "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:pay-run-filter:user"
          },
          "operatorUri": "urn:replicon:filter-operator:in",
          "rightExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": {
              "uri": null,
              "uris": rail.result("create_object_uris"),
              "bool": null,
              "date": null,
              "money": null,
              "number": null,
              "text": null,
              "time": null,
              "calendarDayDurationValue": null,
              "workdayDurationValue": null,
              "dateRange": null,
              "dateTimeUtc": null,
              "dateTimeUtcRange": null,
              "numberRange": null
            },
            "filterDefinitionUri": null
          },
          "value": null,
          "filterDefinitionUri": null
        },
        "value": null,
        "filterDefinitionUri": null
      },
      "operatorUri": "urn:replicon:filter-operator:and",
      "rightExpression": {
        "leftExpression": {
          "leftExpression": null,
          "operatorUri": null,
          "rightExpression": null,
          "value": null,
          "filterDefinitionUri": "urn:replicon:pay-run-filter:employee-type-group"
        },
        "operatorUri": "urn:replicon:filter-operator:not-in",
        "rightExpression": {
          "leftExpression": null,
          "operatorUri": null,
          "rightExpression": null,
          "value": {
            "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_type'),
                                                 "displaytext", "Contingent Worker", "uri"),
            "uris": [],
            "bool": null,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null,
            "dateTimeUtc": null,
            "dateTimeUtcRange": null,
            "numberRange": null
          },
          "filterDefinitionUri": null
        },
        "value": null,
        "filterDefinitionUri": null
      },
      "value": null,
      "filterDefinitionUri": null
    },
    "value": null,
    "filterDefinitionUri": null
  }
}



def get_user_data():
    return {
        "page": "1",
        "pagesize": "10000000",
        "columnUris": [
            "urn:replicon:user-list-column:user"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "true"
                }
            }
        }
    }

def get_create_object_set(dag_run):
    return {
        "userUris": list(dag_run.conf['uri'])
    }

def get_payload():
    return {
        "target": {
            "uri": rail.result('get_payrun_batch_result')['payRunUri']
        }
    }
def get_compose_item_timeoff_uk_data_row(item):
    return [
        "P2001",
        item["CLIID"],
        item["INTCA"],
        item["ORDNO"],
        item["IOPER"],
        "2001",
        item["SUBTY"],
        item["BEGDA"],
        item["ENDDA"],
        item["OBJPS"],
        item["SPRPS"],
        item["SEQNR"],
        item["EXTRA"],
        item["LGART"],
        "",
        "",
        item["ANZHL"].replace(",", ""),
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        ""
    ]


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def is_upload_data_to_sftp_failed_timeoff():
    if get_task_state('upload_timeoff_encrypted_file_to_sftp') == 'failed':
        return True
    return False

def is_upload_log_to_sftp_failed_timeoff():
    if get_task_state('upload_timeoff_log_to_sftp') == 'failed':
        return True
    return False


def is_upload_data_to_sftp_failed_payroll():
    if get_task_state('upload_payroll_encrypted_file_to_sftp') == 'failed':
        return True
    return False

def is_upload_log_to_sftp_failed_payroll():
    if get_task_state('upload_payroll_log_to_sftp') == 'failed':
        return True
    return False