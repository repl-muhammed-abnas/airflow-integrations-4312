import json
from datetime import datetime, timedelta
from pytz import timezone
from airflow.models import Variable
import math
import itertools
import rail
# pylint:disable = too-many-statements, line-too-long
# pylint: disable=cell-var-from-loop
# pylint: disable=unsubscriptable-object
# pylint:disable = too-many-statements
null = None


_VALID_SUPERVISOR_SOURCES = {
    'SUPERVISOR',
    'MANAGER',
    'SUPERVISOR_WITH_MANAGER_FALLBACK',
    'MANAGER_WITH_SUPERVISOR_FALLBACK',
}

_VALID_LOGINNAME_SOURCES = {
    'EMPL_ID',
    'EMAIL_ID',
}


def check_for_value(value):
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def resolve_supervisor(data, source='SUPERVISOR', employee_id=None):
    spvsr = check_for_value(data.get('SPVSR_EMPL_ID'))
    mgr = check_for_value(data.get('MGR_EMPL_ID'))

    def _evaluate_source(value):
        if value is None:
            return (None, None)
        if employee_id and value == check_for_value(employee_id):
            return (None, 'self_supervisor')
        return (value, None)

    if source == 'SUPERVISOR':
        return _evaluate_source(spvsr)
    if source == 'MANAGER':
        return _evaluate_source(mgr)
    if source == 'SUPERVISOR_WITH_MANAGER_FALLBACK':
        # Fall back to MGR only when SPVSR is genuinely blank.
        return _evaluate_source(mgr) if spvsr is None else _evaluate_source(spvsr)
    if source == 'MANAGER_WITH_SUPERVISOR_FALLBACK':
        return _evaluate_source(spvsr) if mgr is None else _evaluate_source(mgr)
    raise ValueError(
        f"[supervisor_source_field] Invalid value '{source}'. "
    )


def create_dag(config):
    _supervisor_source = getattr(config, 'supervisor_source_field', 'SUPERVISOR')
    if _supervisor_source not in _VALID_SUPERVISOR_SOURCES:
        raise ValueError(
            f"[supervisor_source_field] Invalid value '{_supervisor_source}' in instance '{config.instance}'. "
        )

    _loginname_source = getattr(config, 'loginname_source_field', 'EMPL_ID')
    if _loginname_source not in _VALID_LOGINNAME_SOURCES:
        raise ValueError(
            f"[loginname_source_field] Invalid value '{_loginname_source}' in instance '{config.instance}'. "
        )

    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_user_sync_main_{config.instance}',
        description=f'deltek_costpoint_user_sync_{config.instance}',
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_last_run_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_run_date',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def do_get_last_run_date():
            tz = timezone(config.time_zone) if hasattr(config, 'time_zone') and config.time_zone else timezone('UTC')
            current_time = datetime.now(tz) - timedelta(seconds=2)
            lookup_timestamp_value = Variable.get(
                config.last_run_date_var_name, default_var=None)
            last_run_date = (datetime.fromisoformat(
                lookup_timestamp_value) if lookup_timestamp_value else current_time).isoformat()
            # Variable.set(config.last_run_date_var_name,
            #              current_time.isoformat())
            rail.set_result(current_time.isoformat(), 'current_time')
            return last_run_date

        get_last_run_date = rail.PythonOperator(
            task_id='get_last_run_date',
            python_callable=do_get_last_run_date
        )

        update_last_run_date = rail.PythonOperator(
            task_id='update_last_run_date',
            python_callable=lambda: Variable.set(config.last_run_date_var_name,
                                                 rail.result('get_last_run_date', 'current_time'))
        )

        can_load_data_in_chunks = rail.IfOperator(
            task_id='can_load_data_in_chunks',
            test=lambda: Variable.get(
                    config.get_data_in_chunk_var_name, default_var='false').lower() == 'true',
            yes_task='get_modified_users_in_chunks',
            no_task='get_modified_users'
        )

        def get_filters():
            return [
                {
                    "name": "LDMEINFO_EMPL_LAST_MODIFIED",
                    "relation": "gt=",
                    "value": get_time()
                }
            ]

        def get_user_filter_items():
            items = []
            last_item = []
            a_to_z_chars = list(map(chr, range(ord('A'), ord('Z')+1)))
            for item in a_to_z_chars:
                items.append([
                    {
                        "name": "FIRST_NAME",
                        "relation": "like%",
                        "value": item
                    }
                ] + get_filters())
                last_item.append({
                    "name": "FIRST_NAME",
                    "relation": "not like%",
                    "value": item
                })
            last_item = last_item + get_filters()
            items.append(last_item)
            return items

        get_modified_users_in_chunks = rail.DeltekCostPointServiceCallForEachItemOperator(
            task_id='get_modified_users_in_chunks',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            items=get_user_filter_items,
            data=lambda item: {
                "filter": {
                    "id": "polaris_exp_user_details",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMEINFO_EMPL",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": item
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            flatten=True
        )

        def get_time():
            time_zone = timezone(config.time_zone)
            datetime_in_timezone = datetime.fromisoformat(
                rail.result('get_last_run_date')).astimezone(time_zone)
            return (datetime_in_timezone).replace(tzinfo=None).isoformat()

        get_modified_users = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_users',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_user_details",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMEINFO_EMPL",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "LDMEINFO_EMPL_LAST_MODIFIED",
                                                "relation": "gt=",
                                                "value": get_time()
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }
        )

        def is_costpoint_user_present():
            cost_point_user_obj = rail.result('get_modified_users')\
                if rail.result('get_modified_users') else None
            if cost_point_user_obj:
                for companyData in cost_point_user_obj:
                    if companyData['document']['rows'] and\
                            len(companyData['document']['rows']) > 0:
                        return True
            return False

        if_costpoint_user_present = rail.IfOperator(
            task_id='if_costpoint_user_present',
            test=is_costpoint_user_present,
            yes_task="supervisor_processing_log",
            no_task="delete_this_dagrun",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        supervisor_processing_log = rail.CreateLogOperator(
            task_id='supervisor_processing_log',
        )

        def filter_timeoff_types(response):
            return list(map(lambda row:
                            {
                                "name": row["cells"][0]["textValue"],
                                "code": row["cells"][1].get('textValue'),
                                "uri": row["cells"][0]["uri"],
                                "is_enabled": row['cells'][2]['textValue']
                            }, response.json()["d"]["rows"]))

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffTypeListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:time-off-type-list-column:name",
                    "urn:replicon:time-off-type-list-column:description",
                    "urn:replicon:time-off-type-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=filter_timeoff_types
        )

        def get_value(data, index, pluck_key):
            return data['cells'][index].get(pluck_key)

        def filter_group_data(res):
            return list(
                map(lambda item:
                    {
                        'name': get_value(item, 0, 'textValue'),
                        'uri': get_value(item, 0, 'uri'),
                        'code': get_value(item, 1, 'textValue'),
                    }, res['rows'])
            )

        get_all_departments = rail.RepliconServiceOperator(
            task_id='get_all_departments',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=filter_group_data
        )

        get_all_divisions = rail.RepliconServiceOperator(
            task_id="get_all_divisions",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
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
                }
            },
            data_handler=filter_group_data
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
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
                }
            },
            data_handler=filter_group_data
        )

        get_all_employeetypes = rail.RepliconServiceOperator(
            task_id="get_all_employeetypes",
            endpoint="services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:employee-type-group",
                    "urn:replicon:employee-type-group-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
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
                }
            },
            data_handler=filter_group_data
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id="get_all_cost_centers",
            endpoint="services/CostCenterListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:cost-center-list-column:cost-center",
                        "urn:replicon:cost-center-list-column:code"
                    ]
            },
            data_handler=filter_group_data
        )

        get_all_service_centers = rail.RepliconServiceOperator(
            task_id="get_all_service_centers",
            endpoint="services/ServiceCenterListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:service-center-list-column:service-center",
                        "urn:replicon:service-center-list-column:code"
                    ]
            },
            data_handler=filter_group_data
        )

        def filter_schedule_data(res):
            return list(
                map(lambda item:
                    {
                        'name': get_value(item, 0, 'textValue'),
                        'code': get_value(item, 1, 'textValue'),
                        'uri': get_value(item, 2, 'uri'),
                    }, res['rows'])
            )

        get_all_schedules = rail.RepliconServiceOperator(
            task_id="get_all_schedules",
            endpoint="services/OfficeScheduleListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:office-schedule-list-column:name",
                    "urn:replicon:office-schedule-list-column:description",
                    "urn:replicon:office-schedule-list-column:office-schedule"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=filter_schedule_data
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda oefs: {
                'generallabourcategories': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.oef_generallabourcategories, 'uri'),
                'paytype': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.oef_paytype, 'uri'),
                'oeftaxableentity': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.oef_oeftaxableentity, 'uri'),
                'oefemployeeclass': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.oef_oefemployeeclass, 'uri'),
                'oefflsaexempt': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.oef_oefflsaexempt, 'uri'),
                'projectlaborcategory': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.oef_projectlaborcategory, 'uri'),
                'company': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.oef_company, 'uri'),
                'idtype': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.oef_dtype, 'uri')
            },
        )

        def get_time_off_uri_to_assigned(leave_child_res):
            timeoff_uris = []
            for leave in leave_child_res:
                tiemoff_name = leave["row"]['data']['LV_CD']
                timeoff_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_alltimeoff_types'), 'code', tiemoff_name, 'uri', '')
                if timeoff_uri:
                    timeoff_uris.append(timeoff_uri)
            if config.costpoint_timeoff_type_name:
                costpoint_timeoff_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_alltimeoff_types'), 'name', config.costpoint_timeoff_type_name, 'uri', '')
                if costpoint_timeoff_uri:
                    timeoff_uris.append(costpoint_timeoff_uri)
            return timeoff_uris

        def timesheet_period_list_input(response):
            rows = response.json()['d']['rows']
            return list(map(lambda row: {
                "code": row['cells'][1].get('textValue'),
                "uri": row['cells'][2].get('uri')
            }, rows)) if rows else []

        get_all_timesheet_periods = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_periods',
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000000",
                "columnUris":  [
                    "urn:replicon:timesheet-period-list-column:name",
                    "urn:replicon:timesheet-period-list-column:description",
                    "urn:replicon:timesheet-period-list-column:timesheet-period"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:timesheet-period-list-filter:enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
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
                }
            },
            response_filter=timesheet_period_list_input
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        def get_formatted_data(response):
            tag_info = list(map(lambda row: {
                "name": row['cells'][0]['textValue'],
                "code": row['cells'][1].get('textValue'),
                "uri": row['cells'][3]['uri'],
                "is_enabled": row['cells'][4]['textValue']
            }, response['rows']))
            return tag_info if tag_info else []

        get_oef_tags_for_glc = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_glc",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').generallabourcategories }}",
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
                }
            },
            data_handler=get_formatted_data
        )

        get_oef_tags_for_paytype = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_paytype",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').paytype }}",
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
                }
            },
            data_handler=get_formatted_data
        )

        get_oef_tags_for_taxableentity = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_taxableentity",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').oeftaxableentity }}",
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
                }
            },
            data_handler=get_formatted_data
        )

        get_oef_tags_for_employeeclas = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_employeeclas",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').oefemployeeclass }}",
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
                }
            },
            data_handler=get_formatted_data
        )

        get_oef_tags_for_flsaexempt = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_flsaexempt",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').oefflsaexempt }}",
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
                }
            },
            data_handler=get_formatted_data
        )

        get_oef_tags_for_plc = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_plc",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').projectlaborcategory }}",
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
                }
            },
            data_handler=get_formatted_data
        )

        get_oef_tags_for_idtype = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_idtype",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').idtype }}",
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
                }
            },
            data_handler=get_formatted_data
        )

        def project_role_list_input(response):
            rows = response.json()['d']['rows']
            return list(map(lambda row: {
                "name": row['cells'][0].get('textValue'),
                "code": row['cells'][1].get('textValue'),
                "uri": row['cells'][2].get('uri')
            }, rows)) if rows else []

        get_all_roles = rail.RepliconServiceOperator(
            task_id='get_all_roles',
            endpoint="/services/ProjectRoleListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000000",
                "columnUris":  [
                    "urn:replicon:project-role-list-column:name",
                    "urn:replicon:project-role-list-column:description",
                    "urn:replicon:project-role-list-column:project-role"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=project_role_list_input
        )

        def get_salary_info_history(emplabinfo_childs, employee_id):
            emp_history = []
            index = 0
            if emplabinfo_childs:
                for emp_salary_info in emplabinfo_childs:
                    index = index + 1
                    generalLabercategory = (emp_salary_info['row']['data'].get(
                        'LDM_EMPLLABINFO_CHILD_GENL_LAB_CAT_CD') or emp_salary_info['row']['data'].get(
                        'GENL_LAB_CAT_CD')) if emp_salary_info else None
                    employeetype = emp_salary_info['row']['data'].get(
                        'S_EMPL_TYPE_CD') if emp_salary_info else None
                    location = (emp_salary_info['row']['data'].get(
                        'LDM_EMPLLABINFO_CHILD_LAB_LOC_CD') or emp_salary_info['row']['data'].get(
                        'LAB_LOC_CD')) if emp_salary_info else None
                    division = (emp_salary_info['row']['data'].get(
                        'LDM_EMPLLABINFO_CHILD_ORG_ID') or emp_salary_info['row']['data'].get(
                        'ORG_ID')) if emp_salary_info else None
                    plc = (emp_salary_info['row']['data'].get(
                        'LDM_EMPLLABINFO_CHILD_BILL_LAB_CAT_CD') or emp_salary_info['row']['data'].get(
                        'BILL_LAB_CAT_CD')) if emp_salary_info else None
                    work_schedule_code = emp_salary_info['row']['data'].get(
                        'TC_WORK_SCHED_CD') if emp_salary_info else None
                    sup_id, _ = (
                        resolve_supervisor(emp_salary_info['row']['data'],
                                           source=_supervisor_source,
                                           employee_id=employee_id)
                        if emp_salary_info else (None, None)
                    )
                    emp_history.append({
                        "index": index + 1,
                        "effectivedate": (emp_salary_info['row']['data'].get('LDM_EMPLLABINFO_CHILD_EFFECT_DT') or
                                          emp_salary_info['row']['data'].get('EFFECT_DT')) if emp_salary_info else None,
                        "generalLabercategory": generalLabercategory,
                        "employeetype": employeetype,
                        "location": location,
                        "division": division,
                        "employeclassId": emp_salary_info['row']['data'].get('EMPL_CLASS_CD') if emp_salary_info else None,
                        "flsaexempt": emp_salary_info['row']['data'].get('EXMPT_FL') if emp_salary_info else None,
                        "hiredateFl": emp_salary_info['row']['data'].get('HIRE_DT_FL') if emp_salary_info else None,
                        "hourlyamount": emp_salary_info['row']['data'].get('HRLY_AMT') if emp_salary_info else None,
                        "plc": plc,
                        "plc_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_roles'), 'code', plc, 'uri')
                        if plc else None,
                        "workschedule": rail.find_first_by_attr_and_get_attr(rail.result('get_all_schedules'), 'name', work_schedule_code, 'uri')
                        if work_schedule_code else None,
                        "ratetype": emp_salary_info['row']['data'].get('S_HRLY_SAL_CD') if emp_salary_info else None,
                        "homeorganization": (emp_salary_info['row']['data'].get('LDM_EMPLLABINFO_CHILD_ORG_ID') or
                                           emp_salary_info['row']['data'].get('ORG_ID')) if emp_salary_info else None,
                        "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_departments'), 'code', generalLabercategory, 'uri')
                        if generalLabercategory else None,
                        "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_employeetypes'), 'code', employeetype, 'uri')
                        if employeetype else None,
                        "locationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_locations'), 'code', location, 'uri') if location else None,
                        "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions'), 'code', division, 'uri') if division else None,
                        "supervisor": sup_id,
                    })
            return emp_history

        def get_user_obj_from_cospoint():
            cost_point_user_obj = rail.result('get_modified_users')
            cost_point_user = []
            emplabinfo_child = []
            for companyUsers in cost_point_user_obj:
                company = companyUsers['_company']
                if companyUsers['document']['rows'] and\
                        len(companyUsers['document']['rows']) > 0:
                    for cost_point_usr in companyUsers['document']['rows']:
                        employee_sub_trees = cost_point_usr['row'].get(
                            'children', [])
                        employment_history_info = list(filter(
                            lambda item: item['row']['rsId'] == "LDMEINFO_EMPLOYMENT_HIST", employee_sub_trees))
                        emplabinfo_child_info = list(filter(
                            lambda item: item['row']['rsId'] == "LDM_EMPLLABINFO_CHILD", employee_sub_trees))
                        emplabinfo_child = emplabinfo_child_info[0] if emplabinfo_child_info else None
                        leave_child = list(filter(
                            lambda item: item['row']['rsId'] == "LDM_EMPLLVACCRL_CHLD", employee_sub_trees))
                        employment_history = employment_history_info[0] if employment_history_info else None

                        generallabourcategoriestaguriinfo = list(filter(
                            lambda x: x['code'] and x['code'] == cost_point_usr['row']['data'].get('GENL_LAB_CAT_CD'), rail.result("get_oef_tags_for_glc")))
                        paytypetaguriinfo = list(filter(lambda x: x['code'] and x['code'] == cost_point_usr['row']['data'].get(
                            'PAY_TYPE'), rail.result("get_oef_tags_for_paytype"))) if cost_point_usr else None
                        oeftaxableentitytaguriinfo = list(filter(lambda x: x['code'] and x['code'] == cost_point_usr['row']['data'].get(
                            'TAXBLE_ENTITY_ID'), rail.result("get_oef_tags_for_taxableentity"))) if cost_point_usr else None
                        oefemployeeclasstaguriinfo = list(filter(lambda x: x['code'] and x['code'] == emplabinfo_child['row']['data'].get(
                            'EMPL_CLASS_CD'), rail.result('get_oef_tags_for_employeeclas'))) if emplabinfo_child else None
                        oefflsaexempttaguriinfo = list(filter(lambda x: x['code'] and x['code'] == emplabinfo_child['row']['data'].get(
                            'EXMPT_FL'), rail.result('get_oef_tags_for_flsaexempt'))) if emplabinfo_child else None
                        projectlaborcategorytaguriinfo = list(filter(lambda x: x['code'] and x['code'] == (emplabinfo_child['row']['data'].get(
                            'LDM_EMPLLABINFO_CHILD_BILL_LAB_CAT_CD') or emplabinfo_child['row']['data'].get(
                            'BILL_LAB_CAT_CD')), rail.result('get_oef_tags_for_plc'))) if emplabinfo_child else None
                        idtype = "Employee" if cost_point_usr['row']['data'].get(
                            'CONTRACTOR_FL') == 'N' else "Contractor Employee"
                        idtypetaginfo = list(filter(lambda x: x['name'] and x['name'] == idtype, rail.result(
                            'get_oef_tags_for_idtype'))) if emplabinfo_child else None
                        holidyainfo = list(filter(lambda x: x['displayText'] and x['displayText'] == emplabinfo_child['row']['data'].get(
                            'TC_WORK_SCHED_CD'), rail.result('get_all_holiday_calendars'))) if emplabinfo_child else None

                        sup_id, sup_invalid_reason = (
                            resolve_supervisor(
                                emplabinfo_child['row']['data'],
                                source=_supervisor_source,
                                employee_id=cost_point_usr['row']['data'].get('EMPL_ID'),
                            )
                            if emplabinfo_child else (None, None)
                        )

                        cost_point_user.append({
                            "loginname": cost_point_usr['row']['data'].get(_loginname_source),
                            "employeestatus": cost_point_usr['row']['data'].get('S_EMPL_STATUS_CD'),
                            "firstname": cost_point_usr['row']['data'].get('FIRST_NAME'),
                            "employeeterminationdate": cost_point_usr['row']['data'].get('TERM_DT'),
                            "lastname": cost_point_usr['row']['data'].get('LAST_NAME'),
                            "emailaddress": cost_point_usr['row']['data'].get('EMAIL_ID'),
                            "employeeId": cost_point_usr['row']['data'].get('EMPL_ID'),
                            "location": (emplabinfo_child['row']['data'].get('LDM_EMPLLABINFO_CHILD_LAB_LOC_CD') or
                                       emplabinfo_child['row']['data'].get('LAB_LOC_CD')) if emplabinfo_child else None,
                            "division": (emplabinfo_child['row']['data'].get('LDM_EMPLLABINFO_CHILD_ORG_ID') or
                                       emplabinfo_child['row']['data'].get('ORG_ID')) if emplabinfo_child else None,
                            "costcenter": cost_point_usr['row']['data'].get('ACCT_ID'),
                            "servicecenter": cost_point_usr['row']['data'].get('COUNTRY_CD'),
                            "departmentgroup": cost_point_usr['row']['data'].get('GENL_LAB_CAT_CD'),
                            "displayname": cost_point_usr['row']['data'].get('LAST_FIRST_NAME'),
                            "generalLabercategory": cost_point_usr['row']['data'].get('GENL_LAB_CAT_CD'),
                            "paytype": cost_point_usr['row']['data'].get('PAY_TYPE'),
                            "payrollserviceid": "",
                            "taxableentity": cost_point_usr['row']['data'].get('TAXBLE_ENTITY_ID'),
                            "userenddate": employment_history['row']['data'].get('LDMEINFO_EMPLOYMENT_HIST_TERM_DT') if employment_history else None,
                            "timesheettemplate": cost_point_usr['row']['data'].get('TIME_ENTRY_TYPE'),
                            "timesheetperiod": cost_point_usr['row']['data'].get('TS_PD_CD'),
                            "timesheetperioduri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timesheet_periods'),
                                                                                       'code', cost_point_usr['row']['data'].get('TS_PD_CD'), 'uri', null),
                            "workcomppId": cost_point_usr['row']['data'].get('WORK_COMP_CD'),
                            "supervisor": sup_id,
                            "supervisor_invalid_reason": sup_invalid_reason,
                            "employeetype": emplabinfo_child['row']['data'].get('S_EMPL_TYPE_CD') if emplabinfo_child else None,
                            "employeclassId": emplabinfo_child['row']['data'].get('EMPL_CLASS_CD') if emplabinfo_child else None,
                            "flsaexempt": emplabinfo_child['row']['data'].get('EXMPT_FL') if emplabinfo_child else None,
                            "hiredateFl": emplabinfo_child['row']['data'].get('HIRE_DT_FL') if emplabinfo_child else None,
                            "hourlyamount": emplabinfo_child['row']['data'].get('HRLY_AMT') if emplabinfo_child else None,
                            "plc": (emplabinfo_child['row']['data'].get('LDM_EMPLLABINFO_CHILD_BILL_LAB_CAT_CD') or
                                  emplabinfo_child['row']['data'].get('BILL_LAB_CAT_CD')) if emplabinfo_child else None,
                            "effectivedate": (emplabinfo_child['row']['data'].get('LDM_EMPLLABINFO_CHILD_EFFECT_DT') or
                                              emplabinfo_child['row']['data'].get('EFFECT_DT')) if emplabinfo_child else None,
                            "workschedule": emplabinfo_child['row']['data'].get('TC_WORK_SCHED_CD') if emplabinfo_child else None,
                            "ratetype": emplabinfo_child['row']['data'].get('S_HRLY_SAL_CD') if emplabinfo_child else None,
                            "laborlocation": cost_point_usr['row']['data'].get('LAB_LOC_CD'),
                            "homeorganization": cost_point_usr['row']['data'].get('CHG_ORG_ID'),
                            "hiredate": cost_point_usr['row']['data'].get('ORIG_HIRE_DT'),
                            "terminationdate": employment_history['row']['data'].get('LDMEINFO_EMPLOYMENT_HIST_TERM_DT') if employment_history else None,
                            "cityname": cost_point_usr['row']['data'].get('CITY_NAME'),
                            "contractor":  cost_point_usr['row']['data'].get('CONTRACTOR_FL'),
                            "state": cost_point_usr['row']['data'].get('MAIL_STATE_DC'),
                            "timeoffassigned": ','.join(list(map(lambda data: data["row"]['data'].get('LV_CD'), leave_child))),
                            "timeoffassigneduris": ','.join(get_time_off_uri_to_assigned(leave_child)),
                            "generallabourcategoriesuri": rail.result('get_user_oefs')['generallabourcategories'],
                            "generallabourcategoriestaguri": generallabourcategoriestaguriinfo[0]['uri'] if generallabourcategoriestaguriinfo else None,
                            "paytypeuri": rail.result('get_user_oefs')['paytype'],
                            "paytypetaguri": paytypetaguriinfo[0]['uri'] if paytypetaguriinfo else None,
                            "oeftaxableentityuri": rail.result('get_user_oefs')['oeftaxableentity'],
                            "oeftaxableentitytaguri": oeftaxableentitytaguriinfo[0]['uri'] if oeftaxableentitytaguriinfo else None,
                            "oefemployeeclassuri": rail.result('get_user_oefs')['oefemployeeclass'],
                            "oefemployeeclasstaguri": oefemployeeclasstaguriinfo[0]['uri'] if oefemployeeclasstaguriinfo else None,
                            "oefflsaexempturi": rail.result('get_user_oefs')['oefflsaexempt'],
                            "oefflsaexempttaguri": oefflsaexempttaguriinfo[0]['uri'] if oefflsaexempttaguriinfo else None,
                            "projectlaborcategoryuri": rail.result('get_user_oefs')['projectlaborcategory'],
                            "companyCode": company,
                            "companyOefUri": rail.result('get_user_oefs')['company'],
                            "projectlaborcategorytaguri": projectlaborcategorytaguriinfo[0]['uri'] if projectlaborcategorytaguriinfo else None,
                            "employeehistory": get_salary_info_history(emplabinfo_child_info, cost_point_usr['row']['data'].get('EMPL_ID')),
                            "servicecenteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_service_centers'),
                                                                                     'code', cost_point_usr['row']['data'].get('COUNTRY_CD'), 'uri')
                            if cost_point_usr['row']['data'].get('COUNTRY_CD') else None,
                            "costcenteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_cost_centers'),
                                                                                  'code', cost_point_usr['row']['data'].get('ACCT_ID'), 'uri')
                            if cost_point_usr['row']['data'].get('ACCT_ID') else None,
                            "contractfluri": rail.result('get_user_oefs')['idtype'],
                            "contractfltaguri": idtypetaginfo[0]['uri'] if idtypetaginfo else None,
                            "holidaycalanderuri": holidyainfo[0]['uri'] if holidyainfo else None,
                        })

            # User Sync Criteria: keep only employees the instance's criteria
            # allow (matched against the mapper's mapping_criteria). Guarded so
            # instances without a matches_filter_criteria (the default) sync
            # everyone, exactly as before.
            matches_filter_criteria = getattr(
                config, 'matches_filter_criteria', None)
            if matches_filter_criteria:
                cost_point_user = [
                    user for user in cost_point_user
                    if matches_filter_criteria(user)]

            return cost_point_user

        cost_point_user = rail.PythonOperator(
            task_id='cost_point_user',
            python_callable=get_user_obj_from_cospoint
        )

        def getChunks(arrayof_obj):
            chunk_size = 50
            chunks = [arrayof_obj[i:i + chunk_size]
                      for i in range(0, len(arrayof_obj), chunk_size)]
            return chunks

        def joinFilter(leftExpression, rightExpression, operatorUri):
            return {
                "leftExpression": leftExpression,
                "operatorUri": operatorUri,
                "rightExpression": rightExpression
            }

        def getFilterExpression(employeeId):
            return {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": employeeId
                    }
                }
            }

        def combineLeaves(leaves):
            if leaves:
                if len(leaves) == 0:
                    return None
                if len(leaves) == 1:
                    return leaves[0]
                if len(leaves) == 2:
                    return joinFilter(leaves[0], leaves[1], "urn:replicon:filter-operator:or")
                if len(leaves) > 2:
                    midpoint = math.ceil(len(leaves) / 2)
                    return joinFilter(combineLeaves(leaves[:midpoint]), combineLeaves(leaves[midpoint:]), "urn:replicon:filter-operator:or")

        def get_filter_super_user_request(modified_user, columnUris):
            leaves = []
            for loginname in modified_user:
                filterExpression = getFilterExpression(loginname)
                leaves.append(filterExpression)

            finalFilterExpression = combineLeaves(leaves)
            return {
                "page": 1,
                "pagesize": 10000,
                "columnUris": columnUris,
                "sort": [],
                "filterExpression": finalFilterExpression
            }

        def get_userlist_request():
            userlist_request = []
            cp_modified_users = [user_info['employeeId']
                                 for user_info in rail.result('cost_point_user')]
            cp_modified_users_chunk = getChunks(cp_modified_users)
            for loginname_list in cp_modified_users_chunk:
                chunk_request = get_filter_super_user_request(loginname_list, [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ])
                userlist_request.append(chunk_request)
            return userlist_request

        def get_user_data_from_list(response):
            user_data = []
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            cp_modified_users = [{'loginname': user_info['loginname'], 'employeeId': user_info['employeeId']}
                                 for user_info in rail.result('cost_point_user')]
            for item in cp_modified_users:
                # Match on employee id (cells[2]); the Costpoint login name source
                # is configurable and frequently differs from the Replicon login
                # name, so matching on login name can miss existing users.
                matched_rows = [
                    x for x in flatten_rows
                    if x['cells'][2].get('textValue') == item['employeeId']
                ]
                if not matched_rows:
                    continue
                # A single Costpoint employee id can map to multiple Replicon
                # users (e.g. a disabled duplicate). Prefer the enabled user,
                # falling back to the first match.
                chosen = next(
                    (x for x in matched_rows
                     if x['cells'][3].get('boolValue') is True
                     or x['cells'][3].get('textValue') == 'True'),
                    matched_rows[0])
                user_data.append({
                    'uri': chosen['cells'][0]['uri'],
                    'status': chosen['cells'][3]['textValue'] if 'textValue' in chosen['cells'][3] else None,
                    'loginname': item['loginname'],
                    'employeeId': item['employeeId']
                })
            return user_data if user_data else ''

        search_modified_cp_users_from_rep = rail.RepliconServiceCallForEachItemOperator(
            task_id='search_modified_cp_users_from_rep',
            endpoint='/services/UserListService1.svc/GetData',
            items=lambda: get_userlist_request(),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True,
            data=lambda item: item,
            all_result_data_handler=get_user_data_from_list
        )

        def is_role_missing():
            modified_data = rail.result('cost_point_user')
            role_present = False
            for cp_user in modified_data:
                plcdetails = list(
                    filter(lambda x: x['plc_uri'] is None, cp_user.get('employeehistory')))
                if len(plcdetails):
                    role_present = True
                    break
            return role_present

        if_role_missing = rail.IfOperator(
            task_id='if_role_missing',
            test=is_role_missing,
            yes_task="get_last_run_date_for_roles",
            no_task="process_user",
        )

        def do_get_last_run_date_for_roles():
            current_time = datetime.utcnow()
            lookup_timestamp_value = Variable.get(
                config.last_run_date_inital_sync_var_name, default_var=None)
            last_run_date = (datetime.fromisoformat(
                lookup_timestamp_value) if lookup_timestamp_value else current_time).isoformat()
            Variable.set(config.last_run_date_inital_sync_var_name,
                         current_time.isoformat())
            return last_run_date

        get_last_run_date_for_roles = rail.PythonOperator(
            task_id='get_last_run_date_for_roles',
            python_callable=do_get_last_run_date_for_roles
        )

        def get_time_for_roles():
            time_zone = timezone(config.time_zone)
            datetime_in_timezone = datetime.fromisoformat(
                rail.result('get_last_run_date')).astimezone(time_zone)
            return (datetime_in_timezone).replace(tzinfo=None).isoformat()

        trigger_dag_run_add_role = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_add_role',
            retries=0,
            items=[1],
            trigger_dag_id=f'deltek_costpoint_roles_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "last_modified": get_time_for_roles()
            }
        )

        wait_for_completion_trigger_dag_run_add_role = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_add_role',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_add_role") }}'
        )

        def project_role_list_input(response):
            rows = response.json()['d']['rows']
            return list(map(lambda row: {
                "name": row['cells'][0].get('textValue'),
                "code": row['cells'][1].get('textValue'),
                "uri": row['cells'][2].get('uri')
            }, rows)) if rows else []

        get_updated_roles = rail.RepliconServiceOperator(
            task_id='get_updated_roles',
            endpoint="/services/ProjectRoleListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000000",
                "columnUris":  [
                    "urn:replicon:project-role-list-column:name",
                    "urn:replicon:project-role-list-column:description",
                    "urn:replicon:project-role-list-column:project-role"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=project_role_list_input
        )

        def update_new_plcs():
            modified_data = rail.result('cost_point_user')

            for employee_data in modified_data:
                for emp_history in employee_data['employeehistory']:
                    if emp_history['plc_uri']:
                        emp_history['plc_uri'] = rail.find_first_by_attr_and_get_attr(
                            rail.result('get_all_roles'), 'code', emp_history['plc'], 'uri')

            return modified_data

        process_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_user',
            retries=0,
            items=update_new_plcs,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_process_each_user_dag_{config.instance}',
            conf=lambda item: {
                **item,
                **{
                    "supervisor_processing_log": rail.result('supervisor_processing_log'),
                    "all_permissions": rail.result('get_all_permissionsets'),
                    "modified_users": rail.result('search_modified_cp_users_from_rep')
                }
            }
        )

        wait_for_completion_trigger_process_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_user") }}'
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_supervisor_entries():
            supervisor_details = []
            supervisor_log_informations = get_data_from_document(
                rail.result('supervisor_processing_log'))
            for supervisor_info in supervisor_log_informations:
                if supervisor_info['properties']:
                    supervisor_details.append({
                        "loginname": supervisor_info['properties'].get('loginname'),
                        "employeeid": supervisor_info['properties'].get('employeeid'),
                        "useruri": supervisor_info['properties'].get('useruri'),
                        "supervisorassignment": supervisor_info['properties'].get('supervisorassignment'),
                        "supervisorpermissionuri": supervisor_info['properties'].get('supervisorpermissionuri'),
                        "action": supervisor_info['properties'].get('action'),
                        "status": supervisor_info['properties'].get('status'),
                    })
            return supervisor_details

        trigger_dag_run_deltek_costpoint_supervisor_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_deltek_costpoint_supervisor_assignment',
            retries=0,
            items=get_supervisor_entries,
            trigger_dag_id=f'deltek_costpoint_supervisor_assignment_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item
            }
        )

        wait_for_completion_trigger_dag_run_deltek_costpoint_supervisor_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_deltek_costpoint_supervisor_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_deltek_costpoint_supervisor_assignment") }}'
        )

        initialize_master_log = rail.WriteLogOperator(
            task_id='initialize_master_log',
            items=[],
            message='master log initialized',
        )

        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ get_master_log() | load_all_records | to_json }}"
        )

        def get_errror_logs():
            final_logs = json.loads(rail.result('load_master_log'))
            error_logs = rail.find_first_by_attr_and_get_attr(
                final_logs, 'properties.status', 'Error', 'properties.employeeid')
            return error_logs

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=get_errror_logs
        )

        has_error_logs = rail.IfOperator(
            task_id='has_error_logs',
            test=lambda: bool(rail.result('get_logged_errors')),
            yes_task='create_csv_lines',
            no_task='catch_error'
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('load_master_log')}}",
            header=[
                    'parentjobid',
                    'employeeid',
                    'action',
                    'status',
                    'reason',
                    'job id'],
            row=[
                "{{ dag_run_ecid() }}",
                "{{ item.properties.employeeid }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.get('reason','') }}",
                "{{ item.ecid }}",
            ]
        )

        log_filename = rail.PythonOperator(
            task_id='log_filename',
            python_callable=lambda:  rail.render_template(
                "Log_{{ dag_run_ecid() }}_user_sync.csv")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines')}}",
            output_file_name='{{ result("log_filename") }}',
            expires_in_seconds=7*24*60*60,
        )

        send_mail_error = rail.EmailOperator(
            task_id='send_mail_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint User sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Deltek Costpoint User sync is completed with failures based on the file - '{{ result('log_filename') }}'. Please find the  link below to download the logs.
            <br /> <br /> <a href="{{ result('generate_download_link') }}">Download log file</a><br /> <br /><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: 'Error:' +
            rail.render_template("{{get_error_message()}}")
        )

        send_configuration_error = rail.EmailOperator(
            task_id='send_configuration_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint User sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Deltek Costpoint User sync is completed with error as the basic settings was misconfigured<br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_last_run_date >> can_load_data_in_chunks
        can_load_data_in_chunks >> rail.Label(
            'Yes') >> get_modified_users_in_chunks >> update_last_run_date >> if_costpoint_user_present
        can_load_data_in_chunks >> rail.Label('No') >> get_modified_users >> \
            update_last_run_date >> if_costpoint_user_present
        if_costpoint_user_present >> rail.Label(
            'No') >> delete_this_dagrun
        if_costpoint_user_present >> rail.Label('Yes') >> supervisor_processing_log >> get_alltimeoff_types >> \
            get_all_departments >> get_all_divisions >> get_all_locations >> \
            get_all_employeetypes >> get_all_cost_centers >> get_all_service_centers >> \
            get_all_schedules >> get_all_holiday_calendars >> get_user_oefs >> get_all_timesheet_periods >> get_all_permissionsets >> \
            get_oef_tags_for_glc >> get_oef_tags_for_paytype >> get_oef_tags_for_taxableentity >> \
            get_oef_tags_for_employeeclas >> get_oef_tags_for_flsaexempt >> get_oef_tags_for_plc >> \
            get_oef_tags_for_idtype >> get_all_roles >> cost_point_user >> search_modified_cp_users_from_rep >> if_role_missing
        if_role_missing >> rail.Label('No') >> process_user
        if_role_missing >> rail.Label('Yes') >> get_last_run_date_for_roles >> trigger_dag_run_add_role >>  \
            wait_for_completion_trigger_dag_run_add_role >> get_updated_roles >> process_user
        process_user >> wait_for_completion_trigger_process_user >> \
            trigger_dag_run_deltek_costpoint_supervisor_assignment >> \
            wait_for_completion_trigger_dag_run_deltek_costpoint_supervisor_assignment >> \
            initialize_master_log >> load_master_log >> get_logged_errors >> has_error_logs
        has_error_logs >> rail.Label('Yes') >> create_csv_lines >> log_filename >> generate_download_link >> \
            send_mail_error >> catch_error
        has_error_logs >> rail.Label(
            'no') >> catch_error >> send_configuration_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
