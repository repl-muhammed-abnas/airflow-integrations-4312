
from datetime import timedelta, datetime
import uuid
import itertools
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalxchange_timeoff_import_intercontinentalexchange_timeoff_import_child_mutliplerecords_v2_0_{config.instance}',
        description=f'IntercontinentalExchange_timeoff_import_child_Mutliplerecords_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
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
            yes_task="get_datasearchtimeoffdatathroughentryidtextsearch_4",
            no_task="intercontinentalexchange_timeoff_import_logs_add_entry_63",
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

        get_datasearchtimeoffdatathroughentryidtextsearch_4 = rail.RepliconServicePageOperator(
            task_id='get_datasearchtimeoffdatathroughentryidtextsearch_4',
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
                            "text": dag_run.conf['timeoffs'][0]['entry_id'],
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

        if_d_rows_greater_than_0_5 = rail.IfOperator(
            task_id='if_d_rows_greater_than_0_5',
            test='''{{ result('get_datasearchtimeoffdatathroughentryidtextsearch_4') | length > 0 }}''',
            yes_task="_adhoc_http_action_7",
            no_task="foreach_request_9",
        )

        _adhoc_http_action_7 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_7',
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=lambda: {
                "timeOffUris": rail.result("get_datasearchtimeoffdatathroughentryidtextsearch_4")
            }
        )

        batch_entry, batch_exit = rail.batch_execution(
            group_id='execute_batch_management',
            creation_task_id='_adhoc_http_action_7'
        )

        foreach_request_9 = rail.ForEachOperator(
            task_id='foreach_request_9',
            items="{{ dag_run.conf.timeoffs | to_json }}",
            start_task='if_leave_start_date_to_date_greater_than_dataforeachforeach_request_9leave_end_dateto_date_10',
            end_task='foreach_request_9_end'
        )

        def leave_start_endate_comparison():
            start_date = datetime.strptime(
                rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d')
            end_date = datetime.strptime(
                rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d')
            return start_date > end_date

        def user_start_date_comparision(dag_run):
            user_start_date = datetime.strptime(
                dag_run.conf['user_startdate'], '%b %d, %Y')
            leave_start_date = datetime.strptime(
                rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d')
            return user_start_date <= leave_start_date

        if_leave_start_date_to_date_greater_than_dataforeachforeach_request_9leave_end_dateto_date_10 = rail.IfOperator(
            task_id='if_leave_start_date_to_date_greater_than_dataforeachforeach_request_9leave_end_dateto_date_10',
            test=leave_start_endate_comparison,
            yes_task="intercontinentalexchange_timeoff_import_logs_add_entry_11",
            no_task="if_leave_start_date_to_date_equals_to_true_13",
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_11 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_11',
            message="Time-off skipped since leave end date is prior to leave start date",
            severity="Skipped",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Skipped",
                "description": "Time-off skipped since leave end date is prior to leave start date",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_leave_start_date_to_date_equals_to_true_13 = rail.IfOperator(
            task_id='if_leave_start_date_to_date_equals_to_true_13',
            test=user_start_date_comparision,
            yes_task="if_status_downcase_equals_to_approved_15",
            no_task="intercontinentalexchange_timeoff_import_logs_add_entry_61",
        )

        if_status_downcase_equals_to_approved_15 = rail.IfOperator(
            task_id='if_status_downcase_equals_to_approved_15',
            test='''{{ result('foreach_request_9').status | lower == 'approved' or result('foreach_request_9').status | lower =='submitted' }}''',
            yes_task="if_foreach_request_9_daydiff_equals_to_0_16",
            no_task="if_status_downcase_equals_to_withdrawn_50",
        )

        if_foreach_request_9_daydiff_equals_to_0_16 = rail.IfOperator(
            task_id='if_foreach_request_9_daydiff_equals_to_0_16',
            test='''{{ result('foreach_request_9').daydiff == 0 }}''',
            yes_task="if_timeoff_type_upcase_equals_to_regular_17",
            no_task="if_timeoff_type_upcase_equals_to_regular_24",
        )

        if_timeoff_type_upcase_equals_to_regular_17 = rail.IfOperator(
            task_id='if_timeoff_type_upcase_equals_to_regular_17',
            test='''{{ result('foreach_request_9').timeoff_type | lower == 'regular' }}''',
            yes_task="create_time_off_draft_18",
            no_task="if_timeoff_type_upcase_equals_to_extended_20",
        )

        create_time_off_draft_18 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_18",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_18 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_18",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_18')
                    },
                    "owner": {
                        "uri": rail.result('foreach_request_9')['useruri'],
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
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['day_hours']) * 3600),
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

        publish_time_off_draft_18 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_18",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_18') }}"
            }
        )

        put_timeoff_entry_id_oef_value_18 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_18",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_18').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_18 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_18",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_18')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_19 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_19',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_timeoff_type_upcase_equals_to_extended_20 = rail.IfOperator(
            task_id='if_timeoff_type_upcase_equals_to_extended_20',
            test='''{{ result('foreach_request_9').timeoff_type | lower =='extended' }}''',
            yes_task="create_time_off_draft_21",
            no_task="if_status_downcase_equals_to_withdrawn_50",
        )

        create_time_off_draft_21 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_21",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_21 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_21",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_21')
                    },
                    "owner": {
                        "uri": rail.result('foreach_request_9')['useruri'],
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
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['day_hours']) * 3600),
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

        publish_time_off_draft_21 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_21",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_21') }}"
            }
        )

        put_timeoff_entry_id_oef_value_21 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_16",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_21').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_21 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_21",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_21')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_22 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_22',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_timeoff_type_upcase_equals_to_regular_24 = rail.IfOperator(
            task_id='if_timeoff_type_upcase_equals_to_regular_24',
            test='''{{ result('foreach_request_9').timeoff_type | lower == 'regular' }}''',
            yes_task="if_foreach_request_9_start_date_duration_present_25",
            no_task="if_timeoff_type_upcase_equals_to_extended_37",
        )

        if_foreach_request_9_start_date_duration_present_25 = rail.IfOperator(
            task_id='if_foreach_request_9_start_date_duration_present_25',
            test='''{{ result('foreach_request_9').start_date_duration | is_truthy  and result('foreach_request_9').end_date_duration | is_truthy }}''',
            yes_task="create_time_off_draft_26",
            no_task="if_foreach_request_9_start_date_duration_present_28",
        )

        create_time_off_draft_26 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_26",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_26 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_26",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_26')
                    },
                    "owner": {
                        "uri": rail.result('foreach_request_9')['useruri'],
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
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['start_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['end_date_duration']) * 3600),
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

        publish_time_off_draft_26 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_26",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_26') }}"
            }
        )

        put_timeoff_entry_id_oef_value_26 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_26",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_26').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_26 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_26",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_26')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_27 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_27',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_foreach_request_9_start_date_duration_present_28 = rail.IfOperator(
            task_id='if_foreach_request_9_start_date_duration_present_28',
            test='''{{ result('foreach_request_9').start_date_duration | is_truthy  and result('foreach_request_9').end_date_duration | is_falsy }}''',
            yes_task="create_time_off_draft_29",
            no_task="if_foreach_request_9_start_date_duration_blank_31",
        )

        create_time_off_draft_29 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_29",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_29 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_29",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_29')
                    },
                    "owner": {
                        "uri": rail.result('foreach_request_9')['useruri'],
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
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['start_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').day
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

        publish_time_off_draft_29 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_29",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_29') }}"
            }
        )

        put_timeoff_entry_id_oef_value_29 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_29",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_29').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_29 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_29",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_29')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_30 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_30',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_foreach_request_9_start_date_duration_blank_31 = rail.IfOperator(
            task_id='if_foreach_request_9_start_date_duration_blank_31',
            test='''{{ result('foreach_request_9').start_date_duration | is_falsy  and result('foreach_request_9').end_date_duration | is_truthy }}''',
            yes_task="create_time_off_draft_32",
            no_task="if_foreach_request_9_start_date_duration_blank_34",
        )

        create_time_off_draft_32 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_32",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_32 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_32",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_32')
                    },
                    "owner": {
                        "uri": rail.result('foreach_request_9')['useruri'],
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
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['end_date_duration']) * 3600),
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

        publish_time_off_draft_32 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_32",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_32') }}"
            }
        )

        put_timeoff_entry_id_oef_value_32 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_32",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_32').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_32 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_32",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_32')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_33 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_33',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_foreach_request_9_start_date_duration_blank_34 = rail.IfOperator(
            task_id='if_foreach_request_9_start_date_duration_blank_34',
            test='''{{ result('foreach_request_9').start_date_duration | is_falsy  and result('foreach_request_9').end_date_duration | is_falsy }}''',
            yes_task="create_time_off_draft_35",
            no_task="if_timeoff_type_upcase_equals_to_extended_37",
        )

        create_time_off_draft_35 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_35",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_35 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_35",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_35')
                    },
                    "owner": {
                        "uri": rail.result('foreach_request_9')['useruri'],
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
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').day
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

        publish_time_off_draft_35 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_35",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_35') }}"
            }
        )

        put_timeoff_entry_id_oef_value_35 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_35",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_35').uri}}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_35 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_35",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_35')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_36 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_36',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_timeoff_type_upcase_equals_to_extended_37 = rail.IfOperator(
            task_id='if_timeoff_type_upcase_equals_to_extended_37',
            test='''{{ result('foreach_request_9').timeoff_type | lower =='extended' }}''',
            yes_task="if_foreach_request_9_start_date_duration_present_38",
            no_task="if_status_downcase_equals_to_withdrawn_50",
        )

        if_foreach_request_9_start_date_duration_present_38 = rail.IfOperator(
            task_id='if_foreach_request_9_start_date_duration_present_38',
            test='''{{ result('foreach_request_9').start_date_duration | is_truthy  and result('foreach_request_9').end_date_duration | is_truthy }}''',
            yes_task="create_time_off_draft_39",
            no_task="if_foreach_request_9_start_date_duration_present_41",
        )

        create_time_off_draft_39 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_39",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_39 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_39",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_39')
                    },
                    "owner": {
                        "uri": rail.result('foreach_request_9')['useruri'],
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
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['start_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['end_date_duration']) * 3600),
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

        publish_time_off_draft_39 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_39",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_39') }}"
            }
        )

        put_timeoff_entry_id_oef_value_39 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_39",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_39').uri}}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_39 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_39",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_39')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_40 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_40',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_foreach_request_9_start_date_duration_present_41 = rail.IfOperator(
            task_id='if_foreach_request_9_start_date_duration_present_41',
            test='''{{ result('foreach_request_9').start_date_duration | is_truthy  and result('foreach_request_9').end_date_duration | is_falsy }}''',
            yes_task="create_time_off_draft_42",
            no_task="if_foreach_request_9_start_date_duration_blank_44",
        )

        create_time_off_draft_42 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_42",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_42 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_42",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_time_off_draft_42')
                    },
                    "owner": {
                        "uri": rail.result('foreach_request_9')['useruri'],
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
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['start_date_duration']) * 3600),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').year,
                                "month": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').month,
                                "day": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": int(float(rail.result('foreach_request_9')['end_date_duration']) * 3600),
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

        publish_time_off_draft_42 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_42",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_42') }}"
            }
        )

        put_timeoff_entry_id_oef_value_42 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_42",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_42').uri }}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_42 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_42",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_42')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_43 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_43',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_foreach_request_9_start_date_duration_blank_44 = rail.IfOperator(
            task_id='if_foreach_request_9_start_date_duration_blank_44',
            test='''{{ result('foreach_request_9').start_date_duration | is_falsy  and result('foreach_request_9').end_date_duration | is_truthy }}''',
            yes_task="create_time_off_draft_45",
            no_task="if_foreach_request_9_start_date_duration_blank_47",
        )

        create_time_off_draft_45 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_45",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_45 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_45",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "timeOff": {
                        "target": {
                            "uri": rail.result('create_time_off_draft_45')
                        },
                        "owner": {
                            "uri": rail.result('foreach_request_9')['useruri'],
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
                                    "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                    "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                    "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                                },
                                "timeOfDay": null,
                                "relativeDuration": null,
                                "specificDuration": null
                            },
                            "timeOffEnd": {
                                "date": {
                                    "year": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').year,
                                    "month": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').month,
                                    "day": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').day
                                },
                                "timeOfDay": null,
                                "relativeDuration": null,
                                "specificDuration": {
                                    "hours": "0",
                                    "minutes": "0",
                                    "seconds": int(float(rail.result('foreach_request_9')['end_date_duration']) * 3600),
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
            }
        )

        publish_time_off_draft_45 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_45",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_45') }}"
            }
        )

        put_timeoff_entry_id_oef_value_45 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_45",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_45').uri}}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_45 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_45",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_45')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_46 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_46',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_foreach_request_9_start_date_duration_blank_47 = rail.IfOperator(
            task_id='if_foreach_request_9_start_date_duration_blank_47',
            test='''{{ result('foreach_request_9').start_date_duration | is_falsy  and result('foreach_request_9').end_date_duration | is_falsy }}''',
            yes_task="create_time_off_draft_48",
            no_task="if_status_downcase_equals_to_withdrawn_50",
        )

        create_time_off_draft_48 = rail.RepliconServiceOperator(
            task_id="create_time_off_draft_48",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('foreach_request_9').useruri }}"
            }
        )

        put_timeoff_entry_48 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_48",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "timeOff": {
                        "target": {
                            "uri": rail.result('create_time_off_draft_48')
                        },
                        "owner": {
                            "uri": rail.result('foreach_request_9')['useruri'],
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
                                    "year": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').year,
                                    "month": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').month,
                                    "day": datetime.strptime(rail.result('foreach_request_9')['leave_start_date'], '%Y%m%d').day
                                },
                                "timeOfDay": null,
                                "relativeDuration": null,
                                "specificDuration": null
                            },
                            "timeOffEnd": {
                                "date": {
                                    "year": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').year,
                                    "month": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').month,
                                    "day": datetime.strptime(rail.result('foreach_request_9')['leave_end_date'], '%Y%m%d').day
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
            }
        )

        publish_time_off_draft_48 = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft_48",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft_48') }}"
            }
        )

        put_timeoff_entry_id_oef_value_48 = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value_48",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_48').uri}}",
                "extensionFieldValues": [
                    {
                        "definition": {
                            "uri": "{{ dag_run.conf.timeentryidoef_uri }}",
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": "{{ result('foreach_request_9').entry_id }}",
                        "fileValue": null,
                        "jsonValue": null
                    }
                ]
            }
        )

        submit_time_off_entry_48 = rail.RepliconServiceOperator(
            task_id="submit_time_off_entry_48",
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_48')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_49 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_49',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off added successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_status_downcase_equals_to_withdrawn_50 = rail.IfOperator(
            task_id='if_status_downcase_equals_to_withdrawn_50',
            test='''{{ result('foreach_request_9').status | lower =='withdrawn' }}''',
            yes_task="if_d_rows_greater_than_0_51",
            no_task="foreach_request_9_end",
        )

        if_d_rows_greater_than_0_51 = rail.IfOperator(
            task_id='if_d_rows_greater_than_0_51',
            test='''{{ result('get_datasearchtimeoffdatathroughentryidtextsearch_4') | length > 0 }}''',
            yes_task="intercontinentalexchange_timeoff_import_logs_add_entry_52",
            no_task="intercontinentalexchange_timeoff_import_logs_add_entry_54",
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_52 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_52',
            message="na",
            severity="Success",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Success",
                "description": "time-off entry removed successfully as the status is withdrwan ",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_54 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_54',
            message="na",
            severity="Skipped",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Skipped",
                "description": "time-off entry skipped as status is withdrawn and no previous time-off entry is available for the entry_id given",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_61 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_61',
            message="na",
            severity="Skipped",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ result('foreach_request_9').entry_id }}",
                "leave_start_dt": "{{ result('foreach_request_9').leave_start_date }}",
                "leave_end_dt": "{{ result('foreach_request_9').leave_end_date }}",
                "employee_name": "{{ result('foreach_request_9').name }}",
                "approval_status": "{{ result('foreach_request_9').status }}",
                "status": "Skipped",
                "description": "time-off entry skipped as leave start date is prior to users start date",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        foreach_request_9_end = rail.EmptyOperator(
            task_id='foreach_request_9_end',
        )

        intercontinentalexchange_timeoff_import_logs_add_entry_63 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_63',
            message="na",
            severity="Skipped",
            properties={
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ dag_run.conf.timeoffs[0].entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.timeoffs[0].leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.timeoffs[0].leave_end_date }}",
                "employee_name": "{{ dag_run.conf.timeoffs[0].name }}",
                "approval_status": "{{ dag_run.conf.timeoffs[0].status }}",
                "status": "Skipped",
                "description": "time-off entry skipped as user start isn't available in replicon",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        def get_error_details():
            error_message = rail.render_template('{{ get_error_message() }}')
            if error_message and "Timesheets cannot be created more than 2 month(s) in the future".lower() in error_message.lower():
                return "Timesheets cannot be created more than 2 month(s) in the future, hence time off cannot be added"
            return error_message

        intercontinentalexchange_timeoff_import_logs_add_entry_65 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_entry_65',
            message="{{ get_error_message() }}",
            trigger_rule='one_failed',
            severity="Error",
            properties=lambda dag_run: {
                "employee_id": "{{ result('foreach_request_9').employee_id }}",
                "entry_id": "{{ dag_run.conf.timeoffs[0].entry_id }}",
                "leave_start_dt": "{{ dag_run.conf.timeoffs[0].leave_start_date }}",
                "leave_end_dt": "{{ dag_run.conf.timeoffs[0].leave_end_date }}",
                "employee_name": "{{ dag_run.conf.timeoffs[0].name }}",
                "approval_status": "{{ dag_run.conf.timeoffs[0].status }}",
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
        if_request_user_startdate_present_3 >> rail.Label('No') >> \
            intercontinentalexchange_timeoff_import_logs_add_entry_63 >> log_to_sumo
        if_request_user_startdate_present_3 >> rail.Label(
            'Yes') >> get_datasearchtimeoffdatathroughentryidtextsearch_4 >> if_d_rows_greater_than_0_5
        if_d_rows_greater_than_0_5 >> rail.Label(
            'Yes') >> _adhoc_http_action_7 >> batch_entry
        batch_exit >> foreach_request_9
        if_d_rows_greater_than_0_5 >> rail.Label(
            'No') >> foreach_request_9 >> if_leave_start_date_to_date_greater_than_dataforeachforeach_request_9leave_end_dateto_date_10
        if_leave_start_date_to_date_greater_than_dataforeachforeach_request_9leave_end_dateto_date_10 >> rail.Label(
            'Yes') >> intercontinentalexchange_timeoff_import_logs_add_entry_11 >> log_to_sumo
        if_leave_start_date_to_date_greater_than_dataforeachforeach_request_9leave_end_dateto_date_10 >> rail.Label(
            'No') >> if_leave_start_date_to_date_equals_to_true_13
        if_leave_start_date_to_date_equals_to_true_13 >> rail.Label('No') >> \
            intercontinentalexchange_timeoff_import_logs_add_entry_61 >> foreach_request_9_end
        if_leave_start_date_to_date_equals_to_true_13 >> rail.Label(
            'Yes') >> if_status_downcase_equals_to_approved_15
        if_status_downcase_equals_to_approved_15 >> rail.Label(
            'Yes') >> if_foreach_request_9_daydiff_equals_to_0_16
        if_foreach_request_9_daydiff_equals_to_0_16 >> rail.Label(
            'No') >> if_timeoff_type_upcase_equals_to_regular_24
        if_foreach_request_9_daydiff_equals_to_0_16 >> rail.Label(
            'Yes') >> if_timeoff_type_upcase_equals_to_regular_17
        if_timeoff_type_upcase_equals_to_regular_17 >> rail.Label(
            'Yes') >> create_time_off_draft_18 >> put_timeoff_entry_18 >> publish_time_off_draft_18 >> \
            put_timeoff_entry_id_oef_value_18 >> submit_time_off_entry_18 >> intercontinentalexchange_timeoff_import_logs_add_entry_19 >> if_timeoff_type_upcase_equals_to_extended_20
        if_timeoff_type_upcase_equals_to_regular_17 >> rail.Label(
            'No') >> if_timeoff_type_upcase_equals_to_extended_20
        if_timeoff_type_upcase_equals_to_extended_20 >> rail.Label(
            'Yes') >> create_time_off_draft_21 >> put_timeoff_entry_21 >> publish_time_off_draft_21 >> \
            put_timeoff_entry_id_oef_value_21 >> submit_time_off_entry_21 >> intercontinentalexchange_timeoff_import_logs_add_entry_22 >> if_status_downcase_equals_to_withdrawn_50
        if_timeoff_type_upcase_equals_to_extended_20 >> rail.Label(
            'No') >> if_status_downcase_equals_to_withdrawn_50
        if_timeoff_type_upcase_equals_to_regular_24 >> rail.Label(
            'Yes') >> if_foreach_request_9_start_date_duration_present_25
        if_foreach_request_9_start_date_duration_present_25 >> rail.Label(
            'Yes') >> create_time_off_draft_26 >> put_timeoff_entry_26 >> publish_time_off_draft_26 >> put_timeoff_entry_id_oef_value_26 >> \
            submit_time_off_entry_26 >> intercontinentalexchange_timeoff_import_logs_add_entry_27 >> if_foreach_request_9_start_date_duration_present_28
        if_foreach_request_9_start_date_duration_present_25 >> rail.Label(
            'No') >> if_foreach_request_9_start_date_duration_present_28
        if_foreach_request_9_start_date_duration_present_28 >> rail.Label(
            'Yes') >> create_time_off_draft_29 >> put_timeoff_entry_29 >> publish_time_off_draft_29 >> \
            put_timeoff_entry_id_oef_value_29 >> submit_time_off_entry_29 >> intercontinentalexchange_timeoff_import_logs_add_entry_30 >> if_foreach_request_9_start_date_duration_blank_31
        if_foreach_request_9_start_date_duration_present_28 >> rail.Label(
            'No') >> if_foreach_request_9_start_date_duration_blank_31
        if_foreach_request_9_start_date_duration_blank_31 >> rail.Label(
            'Yes') >> create_time_off_draft_32 >> put_timeoff_entry_32 >> publish_time_off_draft_32 >> \
            put_timeoff_entry_id_oef_value_32 >> submit_time_off_entry_32 >> \
            intercontinentalexchange_timeoff_import_logs_add_entry_33 >> if_foreach_request_9_start_date_duration_blank_34
        if_foreach_request_9_start_date_duration_blank_31 >> rail.Label(
            'No') >> if_foreach_request_9_start_date_duration_blank_34
        if_foreach_request_9_start_date_duration_blank_34 >> rail.Label(
            'Yes') >> create_time_off_draft_35 >> put_timeoff_entry_35 >> publish_time_off_draft_35 >> put_timeoff_entry_id_oef_value_35 >> \
            submit_time_off_entry_35 >> intercontinentalexchange_timeoff_import_logs_add_entry_36 >> if_timeoff_type_upcase_equals_to_extended_37
        if_foreach_request_9_start_date_duration_blank_34 >> rail.Label(
            'No') >> if_timeoff_type_upcase_equals_to_extended_37
        if_timeoff_type_upcase_equals_to_regular_24 >> rail.Label(
            'No') >> if_timeoff_type_upcase_equals_to_extended_37
        if_timeoff_type_upcase_equals_to_extended_37 >> rail.Label(
            'Yes') >> if_foreach_request_9_start_date_duration_present_38
        if_foreach_request_9_start_date_duration_present_38 >> rail.Label(
            'Yes') >> create_time_off_draft_39 >> put_timeoff_entry_39 >> publish_time_off_draft_39 >> \
            put_timeoff_entry_id_oef_value_39 >> submit_time_off_entry_39 >> \
            intercontinentalexchange_timeoff_import_logs_add_entry_40 >> if_foreach_request_9_start_date_duration_present_41
        if_foreach_request_9_start_date_duration_present_38 >> rail.Label(
            'No') >> if_foreach_request_9_start_date_duration_present_41
        if_foreach_request_9_start_date_duration_present_41 >> rail.Label(
            'Yes') >> create_time_off_draft_42 >> put_timeoff_entry_42 >> publish_time_off_draft_42 >> \
            put_timeoff_entry_id_oef_value_42 >> submit_time_off_entry_42 >> \
            intercontinentalexchange_timeoff_import_logs_add_entry_43 >> if_foreach_request_9_start_date_duration_blank_44
        if_foreach_request_9_start_date_duration_present_41 >> rail.Label(
            'No') >> if_foreach_request_9_start_date_duration_blank_44
        if_foreach_request_9_start_date_duration_blank_44 >> rail.Label(
            'Yes') >> create_time_off_draft_45 >> put_timeoff_entry_45 >> publish_time_off_draft_45 >> \
            put_timeoff_entry_id_oef_value_45 >> submit_time_off_entry_45 >> \
            intercontinentalexchange_timeoff_import_logs_add_entry_46 >> if_foreach_request_9_start_date_duration_blank_47
        if_foreach_request_9_start_date_duration_blank_44 >> rail.Label(
            'No') >> if_foreach_request_9_start_date_duration_blank_47
        if_foreach_request_9_start_date_duration_blank_47 >> rail.Label(
            'Yes') >> create_time_off_draft_48 >> put_timeoff_entry_48 >> publish_time_off_draft_48 >> \
            put_timeoff_entry_id_oef_value_48 >> submit_time_off_entry_48 >> \
            intercontinentalexchange_timeoff_import_logs_add_entry_49 >> if_status_downcase_equals_to_withdrawn_50
        if_foreach_request_9_start_date_duration_blank_47 >> rail.Label(
            'No') >> if_status_downcase_equals_to_withdrawn_50
        if_timeoff_type_upcase_equals_to_extended_37 >> rail.Label(
            'No') >> if_status_downcase_equals_to_withdrawn_50
        if_status_downcase_equals_to_approved_15 >> rail.Label(
            'No') >> if_status_downcase_equals_to_withdrawn_50
        if_status_downcase_equals_to_withdrawn_50 >> rail.Label(
            'Yes') >> if_d_rows_greater_than_0_51
        if_d_rows_greater_than_0_51 >> rail.Label(
            'Yes') >> intercontinentalexchange_timeoff_import_logs_add_entry_52 >> foreach_request_9_end
        if_d_rows_greater_than_0_51 >> rail.Label(
            'No') >> intercontinentalexchange_timeoff_import_logs_add_entry_54 >> foreach_request_9_end
        if_status_downcase_equals_to_withdrawn_50 >> rail.Label(
            'No') >> foreach_request_9_end
        foreach_request_9 >> foreach_request_9_end >> intercontinentalexchange_timeoff_import_logs_add_entry_65 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
