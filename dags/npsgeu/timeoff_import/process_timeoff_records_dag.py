
from datetime import timedelta, datetime
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_records_dagid,
        description=f'NPSGEU - process_timeoff_records V2.0 child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        def get_replicon_date(date_str):
            if not date_str:
                return None
            # date format in 2006040
            try:
                date = datetime.strptime(date_str, '%m/%d/%Y')
                return {
                    'year': date.year,
                    'month': date.month,
                    'day': date.day
                }
            except:  # pylint: disable=bare-except
                return None

        def get_specific_duration(amount):
            float_amt = float(amount)
            hours, decimal_part = divmod(float_amt, 1)
            minutes, seconds = divmod(decimal_part * 60, 1)
            seconds *= 60
            return {
                "hours": str(int(hours)),
                "minutes": str(int(minutes)),
                "seconds": str(int(seconds)),
                "milliseconds": "0",
                "microseconds": "0"
            }

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_timeoff_import_child_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_timeoff_import_child_logs',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_timeoff_import_child_logs = rail.CreateLogOperator(
            task_id='create_timeoff_import_child_logs'
        )

        create_timeoff_import_timesheetstatus_logs = rail.CreateLogOperator(
            task_id='create_timeoff_import_timesheetstatus_logs'
        )

        if_request_timeoffuri_blank_3 = rail.IfOperator(
            task_id='if_request_timeoffuri_blank_3',
            test=lambda dag_run: dag_run.conf['timeoffuri'] is null,
            yes_task="npsgeu_timeoffimport_logs_add_entry_4",
            no_task="if_request_timeoffaction_not_contains_update_6",
        )

        npsgeu_timeoffimport_logs_add_entry_4 = rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_4',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "hours": "{{ dag_run.conf.amount }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "status": "Ignored",
                "details": "Timeoff type is not available in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        if_request_timeoffaction_not_contains_update_6 = rail.IfOperator(
            task_id='if_request_timeoffaction_not_contains_update_6',
            test=lambda dag_run: dag_run.conf['timeoffaction'] != 'Update' and dag_run.conf['timeoffaction'] != 'Add',
            yes_task="npsgeu_timeoffimport_logs_add_entry_7",
            no_task="get_time_off_type_assignments_for_user_9",
        )

        npsgeu_timeoffimport_logs_add_entry_7 = rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_7',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "hours": "{{ dag_run.conf.amount }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "status": "Ignored",
                "details": "Incorrect \"Status\" value",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        get_time_off_type_assignments_for_user_9 = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user_9',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data= lambda dag_run: {
                "userUri": dag_run.conf['useruri']
            }
        )


        log_checkif_timeofftypeisassignedtouser_10 = rail.PythonOperator(
            task_id='log_checkif_timeofftypeisassignedtouser_10',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_time_off_type_assignments_for_user_9'), 'name', dag_run.conf['timeofftype'], 'uri')
        )

        if_log_checkif_timeofftypeisassignedtouser_10_blank_11 = rail.IfOperator(
            task_id='if_log_checkif_timeofftypeisassignedtouser_10_blank_11',
            test='''{{ result('log_checkif_timeofftypeisassignedtouser_10') | is_falsy }}''',
            yes_task="npsgeu_timeoffimport_logs_add_entry_11",
            no_task="get_timesheet_for_date2_14",
        )

        npsgeu_timeoffimport_logs_add_entry_11 = rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_11',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.amount }}",
                "status": "Ignored",
                "details": "The timeoff type '{{ dag_run.conf.timeofftype }}' is not assigned or is in Disbaled status for the user.",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        get_timesheet_for_date2_14 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_14',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "date": get_replicon_date(dag_run.conf['startdate']),
                "timesheetGetOptionUri": null
            }
        )

        if_timesheet_uri_present_15 = rail.IfOperator(
            task_id='if_timesheet_uri_present_15',
            test=lambda: rail.result('get_timesheet_for_date2_14') and rail.result(
                'get_timesheet_for_date2_14')['timesheet']['uri'] is not null,
            yes_task="get_timesheet_details_16",
            no_task="get_time_off_type_details_20",
        )

        get_timesheet_details_16 = rail.RepliconServiceOperator(
            task_id='get_timesheet_details_16',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2_14').timesheet.uri }}"
            }
        )


        if_d_statusuri_ends_with_approved_17 = rail.IfOperator(
            task_id='if_d_statusuri_ends_with_approved_17',
            test=lambda: bool(rail.result('get_timesheet_details_16')['statusUri'].rsplit(
                ':', 1)[-1] == 'approved' or rail.result('get_timesheet_details_16')['statusUri'].rsplit(':', 1)[-1] == 'waiting'),
            yes_task="reopen_18",
            no_task="get_time_off_type_details_20",
        )

        reopen_18 = rail.RepliconServiceOperator(
            task_id='reopen_18',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2_14').timesheet.uri }}",
                "unitOfWorkId": "Reopen_"+str(uuid.uuid4()),
                "comments": "Reopened by Replicon Integration"
            }
        )

        npsgeu_timeofftimeport_timesheetstatus_add_entry_19 = rail.WriteLogOperator(
            task_id='npsgeu_timeofftimeport_timesheetstatus_add_entry_19',
            log="{{ result('create_timeoff_import_timesheetstatus_logs') }}",
            message="na",
            severity="Info",
            properties={
                "timesheeturi": "{{ result('get_timesheet_for_date2_14').timesheet.uri }}",
                "status": "{{ result('get_timesheet_details_16').statusUri }}"
            }
        )

        get_time_off_type_details_20=rail.RepliconServiceOperator(
            task_id='get_time_off_type_details_20',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeDetails",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                }
        )

        if_request_timeoffaction_contains_add_21 = rail.IfOperator(
            task_id='if_request_timeoffaction_contains_add_21',
            test=lambda dag_run: dag_run.conf['timeoffaction'] == 'Add',
            yes_task="create_new_time_off_draft_22",
            no_task="if_request_timeoffaction_contains_update_47",
        )

        create_new_time_off_draft_22 = rail.RepliconServiceOperator(
            task_id='create_new_time_off_draft_22',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_d_measurementunituri_contains_days_23=rail.IfOperator(
            task_id='if_d_measurementunituri_contains_days_23',
            test='''{{ result('get_time_off_type_details_20').measurementUnitUri | matches('days') }}''',
            yes_task="if_request_amount_less_than_8_24",
            no_task="if_request_amount_less_than_8_38",
        )

        if_request_amount_less_than_8_24 = rail.IfOperator(
            task_id='if_request_amount_less_than_8_24',
            test=lambda dag_run: bool(float(dag_run.conf['amount']) < 8),
            yes_task="if_request_amount_greater_than_4_25",
            no_task="put_time_off2_fullbooking_32",
        )

        if_request_amount_greater_than_4_25=rail.IfOperator(
            task_id='if_request_amount_greater_than_4_25',
            test=lambda dag_run: bool(float(dag_run.conf['amount']) > 4  and float(dag_run.conf['amount']) < 8),
            yes_task="put_time_off2_threequarterdaybooking_26",
            no_task="if_request_amount_greater_than_2_27",
        )

        put_time_off2_threequarterdaybooking_26=rail.RepliconServiceOperator(
            task_id='put_time_off2_threequarterdaybooking_26',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data= lambda dag_run: {
                "timeOff": {
                    "target": {
                    "uri": rail.result('create_new_time_off_draft_22')
                    },
                    "owner": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": None,
                    "parameterCorrelationId": None
                    },
                    "timeOffType": {
                    "uri": rail.result('log_checkif_timeofftypeisassignedtouser_10'),
                    "name": None
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                    "timeOffStart": {
                        "date":  get_replicon_date(dag_run.conf['startdate']),
                        "timeOfDay": {
                        "hour": "0",
                        "minute": "0",
                        "second": "0"
                        },
                        "relativeDuration": "urn:replicon:time-off-relative-duration:three-quarter-day",
                        "specificDuration": None
                    },
                    "timeOffEnd": None
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
                }
                )

        if_request_amount_greater_than_2_27=rail.IfOperator(
            task_id='if_request_amount_greater_than_2_27',
            test=lambda dag_run: bool(float(dag_run.conf['amount']) > 2  and float(dag_run.conf['amount']) < 5) ,
            yes_task="put_time_off2_halfdaybooking_28",
            no_task="if_request_amount_less_than_3_29",
        )

        put_time_off2_halfdaybooking_28=rail.RepliconServiceOperator(
            task_id='put_time_off2_halfdaybooking_28',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run:{
                "timeOff": {
                    "target": {
                    "uri":  rail.result('create_new_time_off_draft_22')
                    },
                    "owner": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                    },
                    "timeOffType": {
                    "uri": rail.result('log_checkif_timeofftypeisassignedtouser_10'),
                    "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                    "timeOffStart": {
                        "date": get_replicon_date(dag_run.conf['startdate']),
                        "timeOfDay": {
                        "hour": "0",
                        "minute": "0",
                        "second": "0"
                        },
                        "relativeDuration": "urn:replicon:time-off-relative-duration:half-day",
                        "specificDuration": null
                    },
                    "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
                }
            )
        if_request_amount_less_than_3_29=rail.IfOperator(
            task_id='if_request_amount_less_than_3_29',
            test=lambda dag_run: bool(float(dag_run.conf['amount']) < 3),
            yes_task="put_time_off2_quarterdaybooking_30",
            no_task="publish_time_off_draft_33",
        )

        put_time_off2_quarterdaybooking_30=rail.RepliconServiceOperator(
            task_id='put_time_off2_quarterdaybooking_30',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
            "timeOff": {
                "target": {
                "uri": rail.result('create_new_time_off_draft_22')
                },
                "owner": {
                "uri": dag_run.conf['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
                },
                "timeOffType": {
                "uri": rail.result('log_checkif_timeofftypeisassignedtouser_10'),
                "name": null
                },
                "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_replicon_date(dag_run.conf['startdate']),
                    "timeOfDay": {
                    "hour": "0",
                    "minute": "0",
                    "second": "0"
                    },
                    "relativeDuration": "urn:replicon:time-off-relative-duration:quarter-day",
                    "specificDuration": null
                },
                "timeOffEnd": null
                },
                "userExplicitEntries": [],
                "comments": "Added by Replicon Integration",
                "customFieldValues": []
            }
            }
        )

        put_time_off2_fullbooking_32=rail.RepliconServiceOperator(
            task_id='put_time_off2_fullbooking_32',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                    "uri": rail.result('create_new_time_off_draft_22')
                    },
                    "owner": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                    },
                    "timeOffType": {
                    "uri": dag_run.conf['timeoffuri'],
                    "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                    "timeOffStart": {
                        "date": get_replicon_date(dag_run.conf['startdate']),
                        "timeOfDay": null,
                        "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                        "specificDuration": null
                    },
                    "timeOffEnd": null
                    },
                    "userExplicitEntries": [ ],
                    "comments": "Added by Replicon Integration",
                "customFieldValues": []
                }
                }
            )


        publish_time_off_draft_33=rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_33',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_new_time_off_draft_22') }}"
            }
        )


        force_approve_34=rail.RepliconServiceOperator(
            task_id='force_approve_34',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
            "timeOffUri": "{{ result('publish_time_off_draft_33').uri }}",
            "unitOfWorkId": str(uuid.uuid4()),
            "comments": "Approved by Replicon Integration"
            }
        )


        npsgeu_timeoffimport_logs_add_entry_35=rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_35',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.amount }}",
                "status": "success",
                "details":"{{'Full day timeoff booking addeed in Replicon' if result('force_approve_34')| is_truthy else 'Timeoff booking added in Replicon' }}",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        if_request_amount_less_than_8_38=rail.IfOperator(
            task_id='if_request_amount_less_than_8_38',
            test=lambda dag_run: bool(float(dag_run.conf['amount']) < 8),
            yes_task="decimal_convert_39",
            no_task="put_time_off2_fullbooking_42",
        )


        decimal_convert_39 = rail.EmptyOperator(
            task_id='decimal_convert_39',
        )



        put_time_off2_partialbooking_40 = rail.RepliconServiceOperator(
            task_id='put_time_off2_partialbooking_40',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_new_time_off_draft_22')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['timeoffuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": get_replicon_date(dag_run.conf['startdate']),
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": get_specific_duration(dag_run.conf['amount'])
                        },
                        "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        put_time_off2_fullbooking_42 = rail.RepliconServiceOperator(
            task_id='put_time_off2_fullbooking_42',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_new_time_off_draft_22')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['timeoffuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": get_replicon_date(dag_run.conf['startdate']),
                            "timeOfDay": null,
                            "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                            "specificDuration": null
                        },
                        "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_43 = rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_43',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_new_time_off_draft_22') }}"
            }
        )

        force_approve_44 = rail.RepliconServiceOperator(
            task_id='force_approve_44',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_43').uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Replicon Integration"
            }
        )

        npsgeu_timeoffimport_logs_add_entry_45 = rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_45',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "timeofftype": dag_run.conf['timeofftype'],
                "startdate": dag_run.conf['startdate'],
                "hours": dag_run.conf['amount'],
                "status": "Success",
                "details": "Full day timeoff booking addeed in Replicon" if rail.result('force_approve_44') is not null else "Timeoff booking added in Replicon",
                "timeoffaction": dag_run.conf['timeoffaction']
            }
        )

        if_request_timeoffaction_contains_update_47 = rail.IfOperator(
            task_id='if_request_timeoffaction_contains_update_47',
            test=lambda dag_run: dag_run.conf['timeoffaction'] == 'Update',
            yes_task="get_data_timeoffbookings_48",
            no_task="finish",
        )

        def get_timeoff_booking_list(response):
            data = response.json()['d']

            return list(map(lambda row: {
                "hours": row['cells'][2]['textValue'],
                "timeoffbookinguri": row['cells'][0]['uri'],
                "timeofftype": row['cells'][3]['uri'],
                "timeoffapprovalstatus": row['cells'][1]['textValue'],
                "timeoffname": row['cells'][3]['textValue']
            },  data['rows']))

        get_data_timeoffbookings_48 = rail.RepliconServiceOperator(
            task_id='get_data_timeoffbookings_48',
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:time-off-list-column:time-off",
                    "urn:replicon:time-off-list-column:approval-status",
                    "urn:replicon:time-off-list-column:total-duration",
                    "urn:replicon:time-off-list-column:time-off-type"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": dag_run.conf['useruri'],
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
                            "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
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
                                    "startDate": get_replicon_date(dag_run.conf['startdate']),
                                    "endDate": get_replicon_date(dag_run.conf['startdate']),
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
                }
            },
            response_filter=get_timeoff_booking_list
        )

        if_request_amount_less_than_0_50 = rail.IfOperator(
            task_id='if_request_amount_less_than_0_50',
            test=lambda dag_run: float(dag_run.conf['amount']) < 0,
            yes_task="invoke_custom_ruby_code_51",
            no_task="invoke_custom_ruby_code_71",
        )
        invoke_custom_ruby_code_51 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_51',
            python_callable=lambda dag_run: {
                "timeoffbookinguri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'timeoffbookinguri'),
                "timeofftype": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'timeofftype'),
                "timeoffapprovalstatus": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'timeoffapprovalstatus'),
                "hours": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'hours'),
                "timeoffname": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'timeoffname')
            }
        )
        if_output_timeoffname_present_52 = rail.IfOperator(
            task_id='if_output_timeoffname_present_52',
            test='''{{ result('invoke_custom_ruby_code_51').timeoffname | is_truthy }}''',
            yes_task="if_hours_to_f_less_than_dataworkato_service8989a8b5requestamountto_fabs_53",
            no_task="npsgeu_timeoffimport_logs_add_entry_68",
        )

        if_hours_to_f_less_than_dataworkato_service8989a8b5requestamountto_fabs_53 = rail.IfOperator(
            task_id='if_hours_to_f_less_than_dataworkato_service8989a8b5requestamountto_fabs_53',
            test=lambda dag_run: float(rail.result('invoke_custom_ruby_code_51')[
                'hours']) <= abs(float(dag_run.conf['amount'])),
            yes_task="delete_time_off_54",
            no_task="log_getthedifferencehoursin_seconds_58",
        )

        delete_time_off_54 = rail.RepliconServiceOperator(
            task_id='delete_time_off_54',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('invoke_custom_ruby_code_51').timeoffbookinguri }}"
            }
        )

        npsgeu_timeoffimport_logs_add_entry_55 = rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_55',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.amount }}",
                "status": "Success",
                "details": "Timeoff booking deleted in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        log_getthedifferencehoursin_seconds_58 = rail.PythonOperator(
            task_id='log_getthedifferencehoursin_seconds_58',
            python_callable=lambda dag_run: int((abs(float(rail.result('get_data_timeoffbookings_48')[
                                                0]['hours'])) - abs(float(dag_run.conf['amount']))) * 3600)
        )

        if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_59 = rail.IfOperator(
            task_id='if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_59',
            test='''{{ result('invoke_custom_ruby_code_51').timeoffapprovalstatus != 'Not Submitted' }}''',
            yes_task="reopen_60",
            no_task="create_edit_time_off_draft_61",
        )

        reopen_60 = rail.RepliconServiceOperator(
            task_id='reopen_60',
            endpoint="/services/TimeOffApprovalService1.svc/Reopen",
            data={
                "timeOffUri": "{{ result('invoke_custom_ruby_code_51').timeoffbookinguri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Reopened by Integration"
            }
        )

        create_edit_time_off_draft_61 = rail.RepliconServiceOperator(
            task_id='create_edit_time_off_draft_61',
            endpoint="/services/TimeOffService1.svc/CreateEditTimeOffDraft",
            data={
                "timeOffUri": "{{ result('invoke_custom_ruby_code_51').timeoffbookinguri }}"
            }
        )

        put_time_off2_62 = rail.RepliconServiceOperator(
            task_id='put_time_off2_62',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_edit_time_off_draft_61')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['timeoffuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": get_replicon_date(dag_run.conf['startdate']),
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": rail.result('log_getthedifferencehoursin_seconds_58'),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Updated by Replicon Integration",
                    "customFieldValues": []
                }
            }
        )

        publish_time_off_draft_63 = rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_63',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_edit_time_off_draft_61') }}"
            }
        )

        force_approve_64 = rail.RepliconServiceOperator(
            task_id='force_approve_64',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_63').uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Replicon Integration"
            }
        )

        npsgeu_timeoffimport_logs_add_entry_65 = rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_65',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.amount }}",
                "status": "Success",
                "details": "Timeoff booking updated in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        npsgeu_timeoffimport_logs_add_entry_68 = rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_68',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.amount }}",
                "status": "Ignored",
                "details": "Timeoff booking is not available in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        invoke_custom_ruby_code_71 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_71',
            python_callable=lambda dag_run: {
                "timeoffbookinguri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'timeoffbookinguri'),
                "timeofftype": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'timeofftype'),
                "timeoffapprovalstatus": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'timeoffapprovalstatus'),
                "hours": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'hours'),
                "timeoffname": rail.find_first_by_attr_and_get_attr(rail.result('get_data_timeoffbookings_48'), 'timeoffname', dag_run.conf['timeofftype'], 'timeoffname')
            }
        )

        if_output_timeoffname_present_72 = rail.IfOperator(
            task_id='if_output_timeoffname_present_72',
            test='''{{ result('invoke_custom_ruby_code_71').timeoffname | is_truthy }}''',
            yes_task="decimal_convert_73",
            no_task="npsgeu_timeoffimport_logs_add_entry_94",
        )

        decimal_convert_73 = rail.EmptyOperator(
            task_id='decimal_convert_73',
        )

        if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_74 = rail.IfOperator(
            task_id='if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_74',
            test='''{{ result('invoke_custom_ruby_code_71').timeoffapprovalstatus != 'Not Submitted' }}''',
            yes_task="reopen_75",
            no_task="create_edit_time_off_draft_76",
        )

        reopen_75 = rail.RepliconServiceOperator(
            task_id='reopen_75',
            endpoint="/services/TimeOffApprovalService1.svc/Reopen",
            data={
                "timeOffUri": "{{ result('invoke_custom_ruby_code_71').timeoffbookinguri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Reopened by Integration"
            }
        )

        create_edit_time_off_draft_76 = rail.RepliconServiceOperator(
            task_id='create_edit_time_off_draft_76',
            endpoint="/services/TimeOffService1.svc/CreateEditTimeOffDraft",
            data={
                "timeOffUri": "{{ result('invoke_custom_ruby_code_71').timeoffbookinguri }}"
            }
        )

        if_d_measurementunituri_contains_days_77=rail.IfOperator(
            task_id='if_d_measurementunituri_contains_days_77',
            test='''{{ result('get_time_off_type_details_20').measurementUnitUri | matches('days') }}''',
            yes_task="if_request_amount_less_than_8_78",
            no_task="put_time_off2_t_obookinginhours_88",
        )

        if_request_amount_less_than_8_78 = rail.IfOperator(
            task_id='if_request_amount_less_than_8_78',
            test=lambda dag_run: bool(float(dag_run.conf['amount']) < 8),
            yes_task="if_request_amount_greater_than_4_79",
            no_task="put_time_off2_fullbooking_86",
        )

        if_request_amount_greater_than_4_79=rail.IfOperator(
            task_id='if_request_amount_greater_than_4_79',
            test=lambda dag_run: bool(float(dag_run.conf['amount']) > 4  and float(dag_run.conf['amount']) < 8),
            yes_task="put_time_off2_threequarterdaybooking_80",
            no_task="if_request_amount_less_than_5_81",
        )

        put_time_off2_threequarterdaybooking_80=rail.RepliconServiceOperator(
            task_id='put_time_off2_threequarterdaybooking_80',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data= lambda dag_run: {
                "timeOff": {
                    "target": {
                    "uri": rail.result('create_edit_time_off_draft_76')
                    },
                    "owner": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": None,
                    "parameterCorrelationId": None
                    },
                    "timeOffType": {
                    "uri": dag_run.conf['timeoffuri'],
                    "name": None
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                    "timeOffStart": {
                        "date":  get_replicon_date(dag_run.conf['startdate']),
                        "timeOfDay": {
                        "hour": "0",
                        "minute": "0",
                        "second": "0"
                        },
                        "relativeDuration": "urn:replicon:time-off-relative-duration:three-quarter-day",
                        "specificDuration": None
                    },
                    "timeOffEnd": None
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
                }
                )

        if_request_amount_less_than_5_81=rail.IfOperator(
            task_id='if_request_amount_less_than_5_81',
            test=lambda dag_run: bool(float(dag_run.conf['amount']) > 2  and float(dag_run.conf['amount']) < 5) ,
            yes_task="put_time_off2_halfdaybooking_82",
            no_task="if_request_amount_less_than_3_83",
        )

        put_time_off2_halfdaybooking_82=rail.RepliconServiceOperator(
            task_id='put_time_off2_halfdaybooking_82',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run:{
                "timeOff": {
                    "target": {
                    "uri":  rail.result('create_edit_time_off_draft_76')
                    },
                    "owner": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                    },
                    "timeOffType": {
                    "uri": dag_run.conf['timeoffuri'],
                    "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                    "timeOffStart": {
                        "date": get_replicon_date(dag_run.conf['startdate']),
                        "timeOfDay": {
                        "hour": "0",
                        "minute": "0",
                        "second": "0"
                        },
                        "relativeDuration": "urn:replicon:time-off-relative-duration:half-day",
                        "specificDuration": null
                    },
                    "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
                }
            )
        if_request_amount_less_than_3_83=rail.IfOperator(
            task_id='if_request_amount_less_than_3_83',
            test=lambda dag_run: bool(float(dag_run.conf['amount']) < 3),
            yes_task="put_time_off2_quarterdaybooking_84",
            no_task="publish_time_off_draft_89",
        )

        put_time_off2_quarterdaybooking_84=rail.RepliconServiceOperator(
            task_id='put_time_off2_quarterdaybooking_84',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
            "timeOff": {
                "target": {
                "uri": rail.result('create_edit_time_off_draft_76')
                },
                "owner": {
                "uri": dag_run.conf['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
                },
                "timeOffType": {
                "uri": dag_run.conf['timeoffuri'],
                "name": null
                },
                "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_replicon_date(dag_run.conf['startdate']),
                    "timeOfDay": {
                    "hour": "0",
                    "minute": "0",
                    "second": "0"
                    },
                    "relativeDuration": "urn:replicon:time-off-relative-duration:quarter-day",
                    "specificDuration": null
                },
                "timeOffEnd": null
                },
                "userExplicitEntries": [],
                "comments": "Added by Replicon Integration",
                "customFieldValues": []
            }
            }
        )

        put_time_off2_fullbooking_86=rail.RepliconServiceOperator(
            task_id='put_time_off2_fullbooking_86',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                    "uri": rail.result('create_edit_time_off_draft_76')
                    },
                    "owner": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                    },
                    "timeOffType": {
                    "uri": dag_run.conf['timeoffuri'],
                    "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                    "timeOffStart": {
                        "date": get_replicon_date(dag_run.conf['startdate']),
                        "timeOfDay": null,
                        "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                        "specificDuration": null
                    },
                    "timeOffEnd": null
                    },
                    "userExplicitEntries": [ ],
                    "comments": "Added by Replicon Integration",
                "customFieldValues": []
                }
                }
            )

        put_time_off2_t_obookinginhours_88=rail.RepliconServiceOperator(
            task_id='put_time_off2_t_obookinginhours_88',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                    "uri": rail.result('create_edit_time_off_draft_76')
                    },
                    "owner": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                    },
                    "timeOffType": {
                    "uri": dag_run.conf['timeoffuri'],
                    "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                    "timeOffStart": {
                        "date": get_replicon_date(dag_run.conf['startdate']),
                        "timeOfDay": null,
                        "relativeDuration": null,
                        "specificDuration": get_specific_duration(dag_run.conf['amount'])
                    },
                    "timeOffEnd": null
                    },
                    "userExplicitEntries": [ ],
                    "comments": "Updated by Replicon Integration",
                "customFieldValues": []
                }
                }
            )

        publish_time_off_draft_89 = rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_89',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_edit_time_off_draft_76') }}"
            }
        )

        force_approve_90 = rail.RepliconServiceOperator(
            task_id='force_approve_90',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_89').uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Replicon Integration"
            }
        )

        npsgeu_timeoffimport_logs_add_entry_91 = rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_91',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.amount }}",
                "status": "Success",
                "details": "Timeoff booking updated in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        npsgeu_timeoffimport_logs_add_entry_94= rail.WriteLogOperator(
            task_id='npsgeu_timeoffimport_logs_add_entry_94',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.amount }}",
                "status": "Ignored",
                "details": "Timeoff booking is not available in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_timeoff_import_child_logs') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "hours": "{{ dag_run.conf.amount }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}",
                "status": "Error",
                "details":'{{ get_error_message() }}'
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_timeoff_import_child_logs
        create_timeoff_import_child_logs >> create_timeoff_import_timesheetstatus_logs >> if_request_timeoffuri_blank_3
        if_request_timeoffuri_blank_3 >> rail.Label(
            'Yes') >> npsgeu_timeoffimport_logs_add_entry_4 >> finish
        if_request_timeoffuri_blank_3 >> rail.Label(
            'No') >> if_request_timeoffaction_not_contains_update_6
        if_request_timeoffaction_not_contains_update_6 >> rail.Label(
            'Yes') >> npsgeu_timeoffimport_logs_add_entry_7 >> finish
        if_request_timeoffaction_not_contains_update_6 >> rail.Label(
            'No') >> get_time_off_type_assignments_for_user_9 >> log_checkif_timeofftypeisassignedtouser_10 >> if_log_checkif_timeofftypeisassignedtouser_10_blank_11
        if_log_checkif_timeofftypeisassignedtouser_10_blank_11 >> rail.Label(
            'Yes') >> npsgeu_timeoffimport_logs_add_entry_11 >> finish
        if_log_checkif_timeofftypeisassignedtouser_10_blank_11 >> rail.Label(
            'No') >> get_timesheet_for_date2_14 >> if_timesheet_uri_present_15
        if_timesheet_uri_present_15 >> rail.Label(
            'Yes') >> get_timesheet_details_16 >> if_d_statusuri_ends_with_approved_17
        if_d_statusuri_ends_with_approved_17 >> rail.Label(
            'Yes') >> reopen_18 >> npsgeu_timeofftimeport_timesheetstatus_add_entry_19 >> get_time_off_type_details_20
        if_d_statusuri_ends_with_approved_17 >> rail.Label(
            'No') >> get_time_off_type_details_20
        if_timesheet_uri_present_15 >> rail.Label(
            'No') >> get_time_off_type_details_20
        get_time_off_type_details_20 >> if_request_timeoffaction_contains_add_21
        if_request_timeoffaction_contains_add_21 >> rail.Label(
            'Yes') >> create_new_time_off_draft_22 >> if_d_measurementunituri_contains_days_23

        if_d_measurementunituri_contains_days_23 >> rail.Label(
            'Yes') >> if_request_amount_less_than_8_24
        if_d_measurementunituri_contains_days_23 >> rail.Label(
            'No') >> if_request_amount_less_than_8_38

        if_request_amount_less_than_8_24 >> rail.Label(
            'Yes') >> if_request_amount_greater_than_4_25
        if_request_amount_greater_than_4_25 >> rail.Label(
            'Yes') >> put_time_off2_threequarterdaybooking_26 >> publish_time_off_draft_33

        if_request_amount_greater_than_2_27 >> rail.Label(
            'Yes') >> put_time_off2_halfdaybooking_28 >> publish_time_off_draft_33
        if_request_amount_greater_than_2_27 >> rail.Label(
            'No') >> if_request_amount_less_than_3_29

        if_request_amount_less_than_3_29 >> rail.Label(
            'Yes') >> put_time_off2_quarterdaybooking_30 >> publish_time_off_draft_33
        if_request_amount_less_than_3_29 >> rail.Label(
            'No') >> publish_time_off_draft_33

        if_request_amount_greater_than_4_25 >> rail.Label(
            'No') >> if_request_amount_greater_than_2_27

        if_request_amount_less_than_8_24 >> rail.Label(
            'No') >> put_time_off2_fullbooking_32 >> publish_time_off_draft_33 >> force_approve_34 >> npsgeu_timeoffimport_logs_add_entry_35 >> finish

        if_request_amount_less_than_8_38 >> rail.Label(
            'Yes') >> decimal_convert_39 >> put_time_off2_partialbooking_40 >> publish_time_off_draft_43 >> force_approve_44 >> npsgeu_timeoffimport_logs_add_entry_45 >> finish
        if_request_amount_less_than_8_38 >> rail.Label(
            'No') >> put_time_off2_fullbooking_42 >> publish_time_off_draft_43 >> force_approve_44 >> npsgeu_timeoffimport_logs_add_entry_45 >> finish
        if_request_timeoffaction_contains_add_21 >> rail.Label(
            'No') >> if_request_timeoffaction_contains_update_47
        if_request_timeoffaction_contains_update_47 >> rail.Label(
            'Yes') >> get_data_timeoffbookings_48 >> if_request_amount_less_than_0_50
        if_request_amount_less_than_0_50 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_51 >> if_output_timeoffname_present_52
        if_output_timeoffname_present_52 >> rail.Label(
            'Yes') >> if_hours_to_f_less_than_dataworkato_service8989a8b5requestamountto_fabs_53
        if_output_timeoffname_present_52 >> rail.Label(
            'No') >> npsgeu_timeoffimport_logs_add_entry_68 >> finish
        if_hours_to_f_less_than_dataworkato_service8989a8b5requestamountto_fabs_53 >> rail.Label(
            'Yes') >> delete_time_off_54 >> npsgeu_timeoffimport_logs_add_entry_55 >> finish
        if_hours_to_f_less_than_dataworkato_service8989a8b5requestamountto_fabs_53 >> rail.Label(
            'No') >> log_getthedifferencehoursin_seconds_58 >> if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_59
        if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_59 >> rail.Label(
            'Yes') >> reopen_60 >> create_edit_time_off_draft_61
        if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_59 >> rail.Label(
            'No') >> create_edit_time_off_draft_61 >> put_time_off2_62 >> publish_time_off_draft_63 >> force_approve_64 >> npsgeu_timeoffimport_logs_add_entry_65 >> finish
        if_request_amount_less_than_0_50 >> rail.Label(
            'No') >> invoke_custom_ruby_code_71 >> if_output_timeoffname_present_72
        if_output_timeoffname_present_72 >> rail.Label(
            'Yes') >> decimal_convert_73 >> if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_74
        if_output_timeoffname_present_72 >> rail.Label(
            'No') >> npsgeu_timeoffimport_logs_add_entry_94 >> finish
        if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_74 >> rail.Label(
            'Yes') >> reopen_75 >> create_edit_time_off_draft_76
        if_output_timeoffapprovalstatus_not_equals_to_notsubmitted_74 >> rail.Label(
            'No') >> create_edit_time_off_draft_76 >> if_d_measurementunituri_contains_days_77

        if_d_measurementunituri_contains_days_77 >> rail.Label(
            'Yes') >> if_request_amount_less_than_8_78

        if_request_amount_less_than_8_78 >> rail.Label(
            'Yes') >> if_request_amount_greater_than_4_79

        if_request_amount_greater_than_4_79 >> rail.Label(
            'Yes') >> put_time_off2_threequarterdaybooking_80 >> publish_time_off_draft_89

        if_request_amount_less_than_5_81 >> rail.Label(
            'Yes') >> put_time_off2_halfdaybooking_82 >> publish_time_off_draft_89

        if_request_amount_less_than_3_83 >> rail.Label(
            'Yes') >> put_time_off2_quarterdaybooking_84 >> publish_time_off_draft_89
        if_request_amount_less_than_3_83 >> rail.Label(
            'No') >> publish_time_off_draft_89

        if_request_amount_less_than_5_81 >> rail.Label(
            'No') >> if_request_amount_less_than_3_83

        if_request_amount_greater_than_4_79 >> rail.Label(
            'No') >> if_request_amount_less_than_5_81

        if_request_amount_less_than_8_78 >> rail.Label(
            'No') >> put_time_off2_fullbooking_86 >> publish_time_off_draft_89 >> force_approve_90 >> npsgeu_timeoffimport_logs_add_entry_91 >> finish

        if_d_measurementunituri_contains_days_77 >> rail.Label(
            'No') >> put_time_off2_t_obookinginhours_88

        put_time_off2_t_obookinginhours_88 >> publish_time_off_draft_89 >> force_approve_90 >> npsgeu_timeoffimport_logs_add_entry_91 >> finish


        if_request_timeoffaction_contains_update_47 >> rail.Label(
            'No') >> finish


        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
