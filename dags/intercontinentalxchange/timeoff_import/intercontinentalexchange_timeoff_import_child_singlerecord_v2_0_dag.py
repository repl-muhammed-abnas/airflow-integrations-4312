
from datetime import timedelta, datetime
import itertools
import uuid
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalxchange_timeoff_import_intercontinentalexchange_timeoff_import_child_singlerecord_v2_0_{config.instance}',
        description=f'IntercontinentalExchange_timeoff_import_child_singlerecord_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_user_startdate_present_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_user_startdate_present_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_user_startdate_present_3 = rail.IfOperator(
            task_id='if_request_user_startdate_present_3',
            test='''{{ dag_run.conf.user_startdate | is_truthy }}''',
            yes_task="if_leave_start_date_to_date_greater_than_dataworkato_service0dcd1af2requestleave_end_dateto_date_4",
            no_task="intercontinentalexchange_timeoff_import_logs_add_entry_56",
        )

        def start_endate_comparison(dag_run):
            start_date = datetime.strptime(
                dag_run.conf['leave_start_date'], '%Y%m%d')
            end_date = datetime.strptime(
                dag_run.conf['leave_end_date'], '%Y%m%d')
            return start_date > end_date

        if_leave_start_date_to_date_greater_than_dataworkato_service0dcd1af2requestleave_end_dateto_date_4 = rail.IfOperator(
            task_id='if_leave_start_date_to_date_greater_than_dataworkato_service0dcd1af2requestleave_end_dateto_date_4',
            test=start_endate_comparison,
            yes_task="intercontinentalexchange_timeoff_import_logs_add_entry_5",
            no_task="if_leave_start_date_to_date_equals_to_true_7",
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_5 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_5',
            message="Since leave end date is prior to leave start date",
            severity="Skipped",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Skipped",
                "description": "Since leave end date is prior to leave start date",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        def user_start_date_comparision(dag_run):
            user_start_date = datetime.strptime(
                dag_run.conf['user_startdate'], '%b %d, %Y')
            leave_start_date = datetime.strptime(
                dag_run.conf['leave_start_date'], '%Y%m%d')
            return user_start_date <= leave_start_date

        if_leave_start_date_to_date_equals_to_true_7 = rail.IfOperator(
            task_id='if_leave_start_date_to_date_equals_to_true_7',
            test=user_start_date_comparision,
            yes_task="get_datasearchtimeoffdatathroughentryidtextsearch_8",
            no_task="intercontinentalexchange_timeoff_import_logs_add_entry_54",
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def get_timeoff_uris(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            return [x['cells'][0]['uri'] for x in flatten_rows] if flatten_rows else []

        get_datasearchtimeoffdatathroughentryidtextsearch_8 = rail.RepliconServicePageOperator(
            task_id='get_datasearchtimeoffdatathroughentryidtextsearch_8',
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:time-off-list-column:time-off",
                    dag_run.conf['OefColumndefinitionUri']
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": dag_run.conf['OefFilterDefinitionUri']
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
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
                            "text": dag_run.conf['entry_id'],
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
            page_handler=page_handler,
            all_result_data_handler=get_timeoff_uris
        )

        if_d_rows_greater_than_0_9 = rail.IfOperator(
            task_id='if_d_rows_greater_than_0_9',
            test="{{result('get_datasearchtimeoffdatathroughentryidtextsearch_8') | length > 0}}",
            yes_task="_adhoc_http_action_11",
            no_task="if_status_downcase_equals_to_approved_13",
        )

        _adhoc_http_action_11 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_11',
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=lambda: {
                "timeOffUris": rail.result('get_datasearchtimeoffdatathroughentryidtextsearch_8')
            }
        )

        batch_entry, batch_exit = rail.batch_execution(
            group_id='execute_batch_management',
            creation_task_id='_adhoc_http_action_11'
        )

        if_status_downcase_equals_to_approved_13 = rail.IfOperator(
            task_id='if_status_downcase_equals_to_approved_13',
            test='''{{ dag_run.conf.status | lower =='approved' or dag_run.conf.status | lower =='submitted' }}''',
            yes_task="if_request_daydiff_equals_to_0_14",
            no_task="if_status_downcase_equals_to_withdrawn_48",
        )

        if_request_daydiff_equals_to_0_14 = rail.IfOperator(
            task_id='if_request_daydiff_equals_to_0_14',
            test='''{{ dag_run.conf.daydiff == 0 }}''',
            yes_task="if_timeoff_type_upcase_equals_to_regular_15",
            no_task="if_timeoff_type_upcase_equals_to_regular_22",
        )

        if_timeoff_type_upcase_equals_to_regular_15 = rail.IfOperator(
            task_id='if_timeoff_type_upcase_equals_to_regular_15',
            test='''{{ dag_run.conf.timeoff_type | lower == 'regular' }}''',
            yes_task="create_time_off_draft_16",
            no_task="if_timeoff_type_upcase_equals_to_extended_18",
        )

        create_time_off_draft_16 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_16",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_16 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_16",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_16')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['regulartimeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['day_hours']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_16 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_16",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_16') }}"
            }
        )

        put_timeoff_entry_id_oef_value_16 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_16",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_16').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_16 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_16",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_16')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_17 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_17',
            message="time-off added successfully",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_timeoff_type_upcase_equals_to_extended_18 = rail.IfOperator(
            task_id='if_timeoff_type_upcase_equals_to_extended_18',
            test='''{{ dag_run.conf.timeoff_type | lower=='extended' }}''',
            yes_task="create_time_off_draft_19",
            no_task="if_status_downcase_equals_to_withdrawn_48",
        )

        create_time_off_draft_19 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_19",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_19 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_19",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_19')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['extended_timeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['day_hours']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_19 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_19",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_19') }}"
            }
        )

        put_timeoff_entry_id_oef_value_19 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_19",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_19').uri}}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_19 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_19",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_19')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_20 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_20',
            message="time-off added successfully",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_timeoff_type_upcase_equals_to_regular_22 = rail.IfOperator(
            task_id='if_timeoff_type_upcase_equals_to_regular_22',
            test='''{{ dag_run.conf.timeoff_type | lower =='regular' }}''',
            yes_task="if_request_start_date_duration_present_23",
            no_task="if_timeoff_type_upcase_equals_to_extended_35",
        )

        if_request_start_date_duration_present_23 = rail.IfOperator(
            task_id='if_request_start_date_duration_present_23',
            test='''{{ dag_run.conf.start_date_duration | is_truthy  and dag_run.conf.end_date_duration | is_truthy }}''',
            yes_task="create_time_off_draft_24",
            no_task="if_request_start_date_duration_blank_26",
        )

        create_time_off_draft_24 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_24",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_24 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_24",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_24')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['regulartimeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['start_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['end_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_24 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_24",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_24') }}"
            }
        )

        put_timeoff_entry_id_oef_value_24 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_24",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_24').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_24 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_24",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_24')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_25 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_25',
            message="time-off added successfully",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_start_date_duration_blank_26 = rail.IfOperator(
            task_id='if_request_start_date_duration_blank_26',
            test='''{{ dag_run.conf.start_date_duration | is_falsy  and dag_run.conf.end_date_duration | is_falsy }}''',
            yes_task="create_time_off_draft_27",
            no_task="if_request_start_date_duration_present_29",
        )

        create_time_off_draft_27 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_27",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_27 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_27",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_27')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['regulartimeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_27 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_27",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_27') }}"
            }
        )

        put_timeoff_entry_id_oef_value_27 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_27",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_27').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_27 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_27",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_27')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_28 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_28',
            message="time-off added successfully",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_start_date_duration_present_29 = rail.IfOperator(
            task_id='if_request_start_date_duration_present_29',
            test='''{{ dag_run.conf.start_date_duration | is_truthy  and dag_run.conf.end_date_duration | is_falsy }}''',
            yes_task="create_time_off_draft_30",
            no_task="if_request_start_date_duration_blank_32",
        )

        create_time_off_draft_30 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_30",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_30 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_30",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_30')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['regulartimeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['start_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_30 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_30",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_30') }}"
            }
        )

        put_timeoff_entry_id_oef_value_30 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_30",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_30').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_30 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_30",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_30')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_31 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_31',
            message="time-off added successfully",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_start_date_duration_blank_32 = rail.IfOperator(
            task_id='if_request_start_date_duration_blank_32',
            test='''{{ dag_run.conf.start_date_duration | is_falsy  and dag_run.conf.end_date_duration | is_truthy }}''',
            yes_task="create_time_off_draft_33",
            no_task="if_timeoff_type_upcase_equals_to_extended_35",
        )

        create_time_off_draft_33 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_33",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_33 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_33",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_33')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['regulartimeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['end_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_33 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_33",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_33') }}"
            }
        )

        put_timeoff_entry_id_oef_value_33 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_33",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_33').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_33 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_33",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_33')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_34 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_34',
            message="time-off added successfully",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_timeoff_type_upcase_equals_to_extended_35 = rail.IfOperator(
            task_id='if_timeoff_type_upcase_equals_to_extended_35',
            test='''{{ dag_run.conf.timeoff_type | lower == 'extended' }}''',
            yes_task="if_request_start_date_duration_present_36",
            no_task="if_status_downcase_equals_to_withdrawn_48",
        )

        if_request_start_date_duration_present_36 = rail.IfOperator(
            task_id='if_request_start_date_duration_present_36',
            test='''{{ dag_run.conf.start_date_duration | is_truthy  and dag_run.conf.end_date_duration | is_truthy }}''',
            yes_task="create_time_off_draft_37",
            no_task="if_request_start_date_duration_blank_39",
        )

        create_time_off_draft_37 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_37",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_37 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_37",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_37')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['extended_timeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['start_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['end_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_37 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_37",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_37') }}"
            }
        )

        put_timeoff_entry_id_oef_value_37 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_37",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_37').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_37 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_37",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_37')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_38 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_38',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_start_date_duration_blank_39 = rail.IfOperator(
            task_id='if_request_start_date_duration_blank_39',
            test='''{{ dag_run.conf.start_date_duration | is_falsy  and dag_run.conf.end_date_duration | is_falsy }}''',
            yes_task="create_time_off_draft_40",
            no_task="if_request_start_date_duration_blank_42",
        )

        create_time_off_draft_40 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_40",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_40 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_40",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_40')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['extended_timeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_40 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_40",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_40') }}"
            }
        )

        put_timeoff_entry_id_oef_value_40 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_40",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_40').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_40 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_40",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_40')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_41 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_41',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_start_date_duration_blank_42 = rail.IfOperator(
            task_id='if_request_start_date_duration_blank_42',
            test='''{{ dag_run.conf.start_date_duration | is_falsy  and dag_run.conf.end_date_duration | is_truthy }}''',
            yes_task="create_time_off_draft_43",
            no_task="if_request_start_date_duration_present_45",
        )

        create_time_off_draft_43 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_43",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_43 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_43",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_43')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['extended_timeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['end_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_43 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_43",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_43') }}"
            }
        )

        put_timeoff_entry_id_oef_value_43 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_43",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_43').uri}}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_43 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_43",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_43')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_44 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_44',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_start_date_duration_present_45 = rail.IfOperator(
            task_id='if_request_start_date_duration_present_45',
            test='''{{ dag_run.conf.start_date_duration | is_truthy  and dag_run.conf.end_date_duration | is_falsy }}''',
            yes_task="create_time_off_draft_46",
            no_task="if_status_downcase_equals_to_withdrawn_48",
        )

        create_time_off_draft_46 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_46",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_timeoff_entry_46 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_46",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_46')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['extended_timeoff_typeuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(dag_run.conf['start_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(dag_run.conf['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_46 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_46",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_46') }}"
            }
        )

        put_timeoff_entry_id_oef_value_46 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_46",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_46').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ dag_run.conf.entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_46 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_46",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_46')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_47 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_47',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_status_downcase_equals_to_withdrawn_48 = rail.IfOperator(
            task_id='if_status_downcase_equals_to_withdrawn_48',
            test='''{{ dag_run.conf.status | lower =='withdrawn' }}''',
            yes_task="if_d_rows_greater_than_0_49",
            no_task="intercontinentalexchange_timeoff_import_logs_add_entry_59",
        )

        if_d_rows_greater_than_0_49 = rail.IfOperator(
            task_id='if_d_rows_greater_than_0_49',
            test='''{{ result('get_datasearchtimeoffdatathroughentryidtextsearch_8') | length > 0 }}''',
            yes_task="intercontinentalexchange_timeoff_import_logs_add_entry_50",
            no_task="intercontinentalexchange_timeoff_import_logs_add_entry_52",
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_50 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_50',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off entry removed successfully as the status is withdrwan ",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_52 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_52',
            message="time-off entry skipped as status is withdrawn",
            severity="Success",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Success",
                "description": "time-off entry skipped as status is withdrawn and no previous time-off entry is available for the entry_id given",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_54 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_54',
            message="time-off entry skipped as leave start date is prior to users start date",
            severity="Skipped",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Skipped",
                "description": "time-off entry skipped as leave start date is prior to users start date",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_56 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_56',
            message="time-off entry skipped as user start date isn't available in replicon",
            severity="Skipped",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Skipped",
                "description": "time-off entry skipped as user start date isn't available in replicon",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        def get_error_details():
            error_message = rail.render_template('{{ get_error_message() }}')
            if error_message and "Timesheets cannot be created more than 2 month(s) in the future".lower() in error_message.lower():
                return "Timesheets cannot be created more than 2 month(s) in the future, hence time off cannot be added"
            return error_message

        intercontinentalexchange_timeoff_import_logs_add_entry_59 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_59',
            message="Timesheets cannot be created more than 2 month(s) in the future",
            severity="Skipped",
            trigger_rule='one_failed',
            properties=lambda dag_run: {
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "entry_id": "{{ dag_run.conf.entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.leave_end_date }}",
                "employee_name": "{{ dag_run.conf.name }}",
                "approval_status": "{{ dag_run.conf.status }}",
                "status": "Error",
                "description": get_error_details(),
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> if_request_user_startdate_present_3
        if_request_user_startdate_present_3 >> rail.Label(
            'No') >> intercontinentalexchange_timeoff_import_logs_add_entry_56 >> log_to_sumo
        if_request_user_startdate_present_3 >> rail.Label(
            'Yes') >> if_leave_start_date_to_date_greater_than_dataworkato_service0dcd1af2requestleave_end_dateto_date_4
        if_leave_start_date_to_date_greater_than_dataworkato_service0dcd1af2requestleave_end_dateto_date_4 >> rail.Label(
            'Yes') >> intercontinentalexchange_timeoff_import_logs_add_entry_5 >> log_to_sumo
        if_leave_start_date_to_date_greater_than_dataworkato_service0dcd1af2requestleave_end_dateto_date_4 >> rail.Label(
            'No') >> if_leave_start_date_to_date_equals_to_true_7
        if_leave_start_date_to_date_equals_to_true_7 >> rail.Label(
            'Yes') >> get_datasearchtimeoffdatathroughentryidtextsearch_8 >> if_d_rows_greater_than_0_9
        if_leave_start_date_to_date_equals_to_true_7 >> rail.Label(
            'No') >> intercontinentalexchange_timeoff_import_logs_add_entry_54 >> log_to_sumo
        if_d_rows_greater_than_0_9 >> rail.Label(
            'Yes') >> _adhoc_http_action_11 >> batch_entry
        batch_exit >> if_status_downcase_equals_to_approved_13
        if_d_rows_greater_than_0_9 >> rail.Label(
            'No') >> if_status_downcase_equals_to_approved_13
        if_status_downcase_equals_to_approved_13 >> rail.Label(
            'Yes') >> if_request_daydiff_equals_to_0_14
        if_request_daydiff_equals_to_0_14 >> rail.Label(
            'No') >> if_timeoff_type_upcase_equals_to_regular_22
        if_request_daydiff_equals_to_0_14 >> rail.Label(
            'Yes') >> if_timeoff_type_upcase_equals_to_regular_15
        if_timeoff_type_upcase_equals_to_regular_15 >> rail.Label(
            'Yes') >> create_time_off_draft_16 >> put_timeoff_entry_16 >> publish_time_off_draft_16 >> put_timeoff_entry_id_oef_value_16 >> \
            submit_time_off_entry_16 >> intercontinentalexchange_timeoff_import_logs_add_entry_17 >> if_timeoff_type_upcase_equals_to_extended_18
        if_timeoff_type_upcase_equals_to_regular_15 >> rail.Label(
            'No') >> if_timeoff_type_upcase_equals_to_extended_18
        if_timeoff_type_upcase_equals_to_extended_18 >> rail.Label(
            'Yes') >> create_time_off_draft_19 >> put_timeoff_entry_19 >> publish_time_off_draft_19 >> put_timeoff_entry_id_oef_value_19 >> \
            submit_time_off_entry_19 >> intercontinentalexchange_timeoff_import_logs_add_entry_20 >> if_status_downcase_equals_to_withdrawn_48
        if_timeoff_type_upcase_equals_to_extended_18 >> rail.Label(
            'No') >> if_status_downcase_equals_to_withdrawn_48
        if_timeoff_type_upcase_equals_to_regular_22 >> rail.Label(
            'No') >> if_timeoff_type_upcase_equals_to_extended_35
        if_timeoff_type_upcase_equals_to_regular_22 >> rail.Label(
            'Yes') >> if_request_start_date_duration_present_23
        if_request_start_date_duration_present_23 >> rail.Label(
            'Yes') >> create_time_off_draft_24 >> put_timeoff_entry_24 >> publish_time_off_draft_24 >> \
            put_timeoff_entry_id_oef_value_24 >> submit_time_off_entry_24 >> intercontinentalexchange_timeoff_import_logs_add_entry_25 >> \
            if_request_start_date_duration_blank_26
        if_request_start_date_duration_present_23 >> rail.Label(
            'No') >> if_request_start_date_duration_blank_26
        if_request_start_date_duration_blank_26 >> rail.Label(
            'Yes') >> create_time_off_draft_27 >> put_timeoff_entry_27 >> publish_time_off_draft_27 >> \
            put_timeoff_entry_id_oef_value_27 >> submit_time_off_entry_27 >> intercontinentalexchange_timeoff_import_logs_add_entry_28 >> \
            if_request_start_date_duration_present_29
        if_request_start_date_duration_blank_26 >> rail.Label(
            'No') >> if_request_start_date_duration_present_29
        if_request_start_date_duration_present_29 >> rail.Label(
            'Yes') >> create_time_off_draft_30 >> put_timeoff_entry_30 >> publish_time_off_draft_30 >> \
            put_timeoff_entry_id_oef_value_30 >> submit_time_off_entry_30 >> intercontinentalexchange_timeoff_import_logs_add_entry_31 >> \
            if_request_start_date_duration_blank_32
        if_request_start_date_duration_present_29 >> rail.Label(
            'No') >> if_request_start_date_duration_blank_32
        if_request_start_date_duration_blank_32 >> rail.Label(
            'Yes') >> create_time_off_draft_33 >> put_timeoff_entry_33 >> publish_time_off_draft_33 >> put_timeoff_entry_id_oef_value_33 >> \
            submit_time_off_entry_33 >> intercontinentalexchange_timeoff_import_logs_add_entry_34 >> if_timeoff_type_upcase_equals_to_extended_35
        if_request_start_date_duration_blank_32 >> rail.Label(
            'No') >> if_timeoff_type_upcase_equals_to_extended_35
        if_timeoff_type_upcase_equals_to_extended_35 >> rail.Label(
            'No') >> if_status_downcase_equals_to_withdrawn_48
        if_timeoff_type_upcase_equals_to_extended_35 >> rail.Label(
            'Yes') >> if_request_start_date_duration_present_36
        if_request_start_date_duration_present_36 >> rail.Label(
            'Yes') >> create_time_off_draft_37 >> put_timeoff_entry_37 >> publish_time_off_draft_37 >> \
            put_timeoff_entry_id_oef_value_37 >> submit_time_off_entry_37 >> intercontinentalexchange_timeoff_import_logs_add_entry_38 >> \
            if_request_start_date_duration_blank_39
        if_request_start_date_duration_present_36 >> rail.Label(
            'No') >> if_request_start_date_duration_blank_39
        if_request_start_date_duration_blank_39 >> rail.Label(
            'Yes') >> create_time_off_draft_40 >> put_timeoff_entry_40 >> publish_time_off_draft_40 >> put_timeoff_entry_id_oef_value_40 >> \
            submit_time_off_entry_40 >> intercontinentalexchange_timeoff_import_logs_add_entry_41 >> if_request_start_date_duration_blank_42
        if_request_start_date_duration_blank_39 >> rail.Label(
            'No') >> if_request_start_date_duration_blank_42
        if_request_start_date_duration_blank_42 >> rail.Label(
            'Yes') >> create_time_off_draft_43 >> put_timeoff_entry_43 >> publish_time_off_draft_43 >> \
            put_timeoff_entry_id_oef_value_43 >> submit_time_off_entry_43 >> intercontinentalexchange_timeoff_import_logs_add_entry_44 >> \
            if_request_start_date_duration_present_45
        if_request_start_date_duration_blank_42 >> rail.Label(
            'No') >> if_request_start_date_duration_present_45
        if_request_start_date_duration_present_45 >> rail.Label(
            'Yes') >> create_time_off_draft_46 >> put_timeoff_entry_46 >> publish_time_off_draft_46 >> \
            put_timeoff_entry_id_oef_value_46 >> submit_time_off_entry_46 >> intercontinentalexchange_timeoff_import_logs_add_entry_47 >> \
            if_status_downcase_equals_to_withdrawn_48
        if_request_start_date_duration_present_45 >> rail.Label(
            'No') >> if_status_downcase_equals_to_withdrawn_48
        if_timeoff_type_upcase_equals_to_extended_35 >> rail.Label(
            'No') >> if_status_downcase_equals_to_withdrawn_48
        if_status_downcase_equals_to_approved_13 >> rail.Label(
            'No') >> if_status_downcase_equals_to_withdrawn_48
        if_status_downcase_equals_to_withdrawn_48 >> rail.Label(
            'Yes') >> if_d_rows_greater_than_0_49
        if_d_rows_greater_than_0_49 >> rail.Label(
            'Yes') >> intercontinentalexchange_timeoff_import_logs_add_entry_50 >> intercontinentalexchange_timeoff_import_logs_add_entry_59 >> log_to_sumo
        if_d_rows_greater_than_0_49 >> rail.Label(
            'No') >> intercontinentalexchange_timeoff_import_logs_add_entry_52 >> intercontinentalexchange_timeoff_import_logs_add_entry_59 >> log_to_sumo
        if_status_downcase_equals_to_withdrawn_48 >> rail.Label(
            'No') >> intercontinentalexchange_timeoff_import_logs_add_entry_59 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
