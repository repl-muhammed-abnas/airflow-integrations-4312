from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
null = None

def get_time_download_payload():
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [rail.result("get_export_batch_results")["timeDataExportUri"]],
                },
            },
        },
        "fileFormatScriptUri": rail.result("get_all_time_download_scripts")
    }

def get_export_batch_results_payload():
    return {
        "timeDataExportBatchUri": rail.result('create_export')
    }


def get_create_export_payload():
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": "urn:replicon:approval-status:approved",
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
                        "dateTimeUtcRange": null
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "urn:replicon:time-data-item-time-data-export-status:none",
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
                            "dateTimeUtcRange": null
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
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
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
                                    "year": (datetime.now().replace(day=1)-timedelta(days=32)).replace(day=1).strftime("%Y"),
                                    "month": (datetime.now().replace(day=1)-timedelta(days=32)).replace(day=1).strftime("%m"),
                                    "day": (datetime.now().replace(day=1)-timedelta(days=32)).replace(day=1).strftime("%d")
                                },
                                "endDate": {
                                    "year": (datetime.now().replace(day=1)-timedelta(days=1)).strftime("%Y"),
                                    "month": (datetime.now().replace(day=1)-timedelta(days=1)).strftime("%m"),
                                    "day": (datetime.now().replace(day=1)-timedelta(days=1)).strftime("%d")
                                },
                                "relativeDateRangeUri": null,
                                "relativeDateRangeAsOfDate": null
                            },
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
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


def get_cancel_timeoff_export_payload():
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        }
    }


def get_mark_as_completed_payload():
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        }
    }


def get_download_url_payload():
    return {
        "timeDataDownloadBatchUri": rail.result('create_download_batch')
    }


def get_child_conf():
    return {
        "workspaceid": Variable.get("elevatebio_workspaceid"),
        "modelid": Variable.get("elevatebio_modelid"),
        "tokenvalue": json.loads(rail.result("authentication"))["tokenInfo"]["tokenValue"],
        "time_data": rail.result("create_timeexport_collection")
    }

def get_revert_draft_payload():
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
        }
    }

def get_time_export_details_payload():
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        }
    }
