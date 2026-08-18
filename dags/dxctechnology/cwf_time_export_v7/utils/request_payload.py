from datetime import datetime, timezone
import rail
from dxctechnology.cwf_time_export_v7.utils import python_callable_method


def get_today_utc_date():
    return datetime.now(timezone.utc)


def get_all_past_time_export_data_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:time-data-export-list-column:time-data-export",
            "urn:replicon:time-data-export-list-column:status",
            "urn:replicon:time-data-export-list-column:creation-date"
        ],
        "sort": [
            {
                "columnUri": "urn:replicon:time-data-export-list-column:creation-date",
                "isAscending": "false"
            }
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-list-filter:cancelled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "bool": "false"
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "REG-CWF"
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-list-filter:creation-date-range"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "dateRange": {
                                "startDate": {
                                    "year": rail.result("get_cwf_data")['ackdateyear'],
                                    "month": rail.result("get_cwf_data")['ackdatemonth'],
                                    "day": rail.result("get_cwf_data")['ackdateday']
                                },
                                "endDate": None
                            }
                        }
                    }
                }
            }
        }
    }


def get_twb_list(data):
    if not data:
        return []
    return list(map(lambda item: {
        'name': item['cells'][0]['textValue'],
        'uri': item['cells'][0]['uri'],
        'createdatetime': item['cells'][2]['textValue']
    }, data['rows']))


def output_payload(data):
    return list(map(lambda x: {
        "identifier": x['Identifier'],
        "creation_time": x['createdatetime']
    }, data))

def get_psa_time_export_conf(export_name, data):
    return {
        'downloadurl': None,
        'fileformaturi': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_time_download_scripts'), 'displayText', 'CWF PSA Time Export', 'uri'),
        'timeexporturi': rail.result('get_timedataexport_batchresults')['timeDataExportUri'],
        'twbname': export_name,
        'last_twb_name': rail.result("completed_exports_list")['Timeexport'],
        'last_twb_uri': rail.result("completed_exports_list")['uri'],
        'payload_identifier_replicon_uniqueid': export_name + '|PSA',
        'oef_name': 'PSA_Payload_Processed',
        'last_twb_unique_indentifier': rail.result("completed_exports_list")['Timeexport'] + '|PSA',
        'twb_list': get_twb_list(data)
    }

def get_gsap_time_export_conf(export_name, data):
    return {
        'downloadurl': None,
        'fileformaturi': rail.result('log_message_fileformat_uri'),
        'timeexporturi': rail.result('get_timedataexport_batchresults')['timeDataExportUri'],
        'twbname': export_name,
        'last_twb_name': rail.result("completed_exports_list")['Timeexport'],
        'last_twb_uri': rail.result("completed_exports_list")['uri'],
        'payload_identifier_replicon_uniqueid': export_name + '|GS',
        'oef_name': 'GSAP_Payload_Processed',
        'last_twb_unique_indentifier': rail.result("completed_exports_list")['Timeexport'] + '|GS',
        'twb_list': get_twb_list(data)
    }

def get_c1_time_export_conf(export_name, data):
    return {
        'downloadurl': None,
        'fileformaturi': rail.result('log_message_fileformat_uri'),
        'timeexporturi': rail.result('get_timedataexport_batchresults')['timeDataExportUri'],
        'twbname': export_name,
        'last_twb_name': rail.result("completed_exports_list")['Timeexport'],
        'last_twb_uri': rail.result("completed_exports_list")['uri'],
        'payload_identifier_replicon_uniqueid': export_name + '|C1',
        'oef_name': 'C1_Payload_Processed',
        'last_twb_unique_indentifier': rail.result("completed_exports_list")['Timeexport'] + '|C1',
        'twb_list': get_twb_list(data)
    }


def get_compass_time_export_conf(export_name, data, config):
    return {
        'downloadurl': None,
        'fileformaturi': rail.result('log_message_fileformat_uri'),
        'timeexporturi': rail.result('get_timedataexport_batchresults')['timeDataExportUri'],
        'twbname': export_name,
        'last_twb_name': rail.result("completed_exports_list")['Timeexport'],
        'last_twb_uri': rail.result("completed_exports_list")['uri'],
        'twb_list': get_twb_list(data),
        'payload_identifier_replicon_uniqueid_pn1': export_name + ("|PN1" if config.instance == "production" else '|NT1'),
        'payload_identifier_replicon_uniqueid_pj1': export_name + ("|PJ1" if config.instance == "production" else '|NT3'),
        'payload_identifier_replicon_uniqueid_p01': export_name + ("|P01" if config.instance == "production" else '|NT2'),
        'oef_name_pn1': 'Compass_PN1/NT1_Payload_Processed',
        'oef_name_p01': 'Compass_P01/NT2_Payload_Processed',
        'oef_name_pj1': 'Compass_PJ1/NT3_Payload_Processed',
        # pylint: disable=line-too-long
        'last_twb_unique_indentifier_pn1': rail.result("completed_exports_list")['Timeexport'] + ("|PN1" if config.instance == "production" else '|NT1'),
        'last_twb_unique_indentifier_p01': rail.result("completed_exports_list")['Timeexport'] + ("|P01" if config.instance == "production" else '|NT2'),
        'last_twb_unique_indentifier_pj1': rail.result("completed_exports_list")['Timeexport'] + ("|PJ1" if config.instance == "production" else '|NT3'),
        'Pn1_sentoef': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field_bindings"), 'displayText', 'COMPASS_PN1_sent', 'uri'),
        'Pj1_sentoef': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field_bindings"), 'displayText', 'COMPASS_PJ1_sent', 'uri'),
        'P01_sentoef': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field_bindings"), 'displayText', 'COMPASS_P01_sent', 'uri')
    }


def get_emea_parameters(dag_run, config):
    return {
        'config': config,
        'region': 'EMEA',
        'task_type': 'emea',
        'output_filename': 'P01_ReplicontoCOMPASS',
        'shortid': dag_run.conf['payload_identifier_replicon_uniqueid_p01'],
        'internal_oef_uri': dag_run.conf['P01_sentoef'],
        'compass_oef_name': dag_run.conf['oef_name_p01'],
        'internal_oef_name': 'COMPASS_P01_sent'
    }


def get_update_oef_param(internal_oef_name):
    if internal_oef_name == 'COMPASS_PN1_sent':
        uri = '{{ dag_run.conf.Pn1_sentoef }}'
    if internal_oef_name == 'COMPASS_PJ1_sent':
        uri = '{{ dag_run.conf.Pj1_sentoef }}'
    if internal_oef_name == 'COMPASS_P01_sent':
        uri = '{{ dag_run.conf.P01_sentoef }}'
    return {
        'objectUri': '{{ dag_run.conf.timeexporturi }}',
        'value': {
            'definition': {
                'uri': uri
            },
            'textValue': 'Yes'
        }
    }


def check_ack_date_and_name():
    name = python_callable_method.get_dag_run_conf()['name']
    twbname = python_callable_method.get_dag_run_conf()['twbname']
    createdatetime = python_callable_method.get_dag_run_conf()[
        'createdatetime']
    date_format = datetime.strptime(createdatetime, '%d %B %Y %H:%M:%S %p')
    current_date = datetime.now()
    if name != twbname:
        if date_format < current_date:
            return True
        return False
    return False


def get_ack_conf(item):
    return {
        "name": item['name'],
        "uri": item['uri'],
        "createdatetime": item['createdatetime'],
        "erp": "compass",

    }

def get_acknowlegement_payload(dag_run):
    return {
        "objectUri": dag_run.conf['timeexporturi'],
        "value": {
            "definition": {
            "uri": rail.result("get_all_oefs_for_the_exports")[0]['uri'],
            },
            "textValue": "Yes",
        }
    }

def get_psa_cost_centers():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": "urn:replicon:cost-center-list-column:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [],
                    "bool": True,
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": None,
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None,
                    "dateTimeUtcRange": None,
                    "numberRange": None
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }

def get_psa_orgs():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
             "urn:replicon:department-group-list-column:department-group",
             "urn:replicon:department-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [],
                    "bool": True,
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": None,
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None,
                    "dateTimeUtcRange": None,
                    "numberRange": None
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }
