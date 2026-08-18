
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'assuranceagency_timeoffimport_process_bookings_child_{config.instance}',
        description=f'Assuranceagency timeoffimport - process bookings - Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_useruri_blank_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_useruri_blank_2',
            end_task='catch_and_log_entry',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_useruri_blank_2=rail.IfOperator(
            task_id='if_request_useruri_blank_2',
            test='''{{ dag_run.conf.useruri | is_falsy }}''',
            yes_task="assuranceagency_timeoffimport_logs_add_entry_3",
            no_task="if_request_timeoffuri_blank_5",
        )

        assuranceagency_timeoffimport_logs_add_entry_3=rail.WriteLogOperator(
            task_id='assuranceagency_timeoffimport_logs_add_entry_3',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="ignored",
            properties={
              "jobid": "{{dag_run.conf.callerjobid}}",
              "childjobid": "{{ dag_run_ecid() }}",
              "username": "{{ dag_run.conf.username }}",
              "employeeid": "{{ dag_run.conf.employeeid }}",
              "timeofftype": "{{ dag_run.conf.timeofftype }}",
              "startdate": "{{ dag_run.conf.startdate }}",
              "hours": "{{ dag_run.conf.startdayhours }}",
              "status": "ignored",
              "details": "User is not available in Replicon"
            }
        )

        if_request_timeoffuri_blank_5=rail.IfOperator(
            task_id='if_request_timeoffuri_blank_5',
            test='''{{ dag_run.conf.timeoffuri | is_falsy }}''',
            yes_task="assuranceagency_timeoffimport_logs_add_entry_6",
            no_task="if_request_startdaytype_not_contains_partialday_8",
        )

        assuranceagency_timeoffimport_logs_add_entry_6=rail.WriteLogOperator(
            task_id='assuranceagency_timeoffimport_logs_add_entry_6',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="ignored",
            properties={
              "jobid": "{{dag_run.conf.callerjobid}}",
              "childjobid": "{{ dag_run_ecid() }}",
              "username": "{{ dag_run.conf.username }}",
              "employeeid": "{{ dag_run.conf.employeeid }}",
              "timeofftype": "{{ dag_run.conf.timeofftype }}",
              "startdate": "{{ dag_run.conf.startdate }}",
              "hours": "{{ dag_run.conf.startdayhours }}",
              "status": "ignored",
              "details": "Timeoff type is not available in Replicon"
            }
        )

        if_request_startdaytype_not_contains_partialday_8=rail.IfOperator(
            task_id='if_request_startdaytype_not_contains_partialday_8',
            test='''{{ not (dag_run.conf.startdaytype | matches('Partial Day')) and not (dag_run.conf.startdaytype | matches('Full Day')) }}''',
            yes_task="assuranceagency_timeoffimport_logs_add_entry_9",
            no_task="date_split_12",
        )

        assuranceagency_timeoffimport_logs_add_entry_9=rail.WriteLogOperator(
            task_id='assuranceagency_timeoffimport_logs_add_entry_9',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="ignored",
            properties={
              "jobid": "{{dag_run.conf.callerjobid}}",
              "childjobid": "{{ dag_run_ecid() }}",
              "username": "{{ dag_run.conf.username }}",
              "employeeid": "{{ dag_run.conf.employeeid }}",
              "timeofftype": "{{ dag_run.conf.timeofftype }}",
              "startdate": "{{ dag_run.conf.startdate }}",
              "hours": "{{ dag_run.conf.startdayhours }}",
              "status": "ignored",
              "details": "Invalid Startdatetype value"
            }
        )

        def get_date_object(dag_run):
            dateobj = datetime.strptime(dag_run.conf['startdate'],'%Y-%m-%d')
            return {
              'day': dateobj.day,
              'month': dateobj.month,
              'year': dateobj.year
            }

        date_split_12=rail.PythonOperator(
            task_id='date_split_12',
            python_callable=get_date_object
        )

        get_timesheet_for_date2_13=rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_13',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "date": {
                "year": "{{result('date_split_12').year}}",
                "month": "{{result('date_split_12').month}}",
                "day": "{{result('date_split_12').day}}"
              },
              "timesheetGetOptionUri": null
            }
        )

        if_timesheet_uri_present_14=rail.IfOperator(
            task_id='if_timesheet_uri_present_14',
            test=lambda: rail.result('get_timesheet_for_date2_13') and rail.result('get_timesheet_for_date2_13')['timesheet'] and rail.result(
                'get_timesheet_for_date2_13')['timesheet']['uri'],
            yes_task="get_timesheet_details_15",
            no_task="if_request_startdaytype_contains_fullday_20",
        )

        get_timesheet_details_15=rail.RepliconServiceOperator(
            task_id='get_timesheet_details_15',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
              "timesheetUri": "{{ result('get_timesheet_for_date2_13').timesheet.uri }}"
            }
        )

        if_d_statusuri_ends_with_approved_16=rail.IfOperator(
            task_id='if_d_statusuri_ends_with_approved_16',
            #pylint: disable = line-too-long
            test='''{{ result('get_timesheet_details_15').statusUri | ends_with('approved')  or result('get_timesheet_details_15').statusUri | ends_with('waiting') }}''',
            yes_task="reopen_17",
            no_task="if_request_startdaytype_contains_fullday_20",
        )

        reopen_17=rail.RepliconServiceOperator(
            task_id='reopen_17',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data={
              "timesheetUri": "{{ result('get_timesheet_for_date2_13').timesheet.uri }}",
              "unitOfWorkId": "Reopen_{{ dag_run_ecid() }}",
              "comments": "Reopened by Replicon Integration"
            }
        )

        assuranceagency_timeoffimport_reopenedtimesheets_add_entry_18=rail.WriteLogOperator(
            task_id='assuranceagency_timeoffimport_reopenedtimesheets_add_entry_18',
            log="{{ dag_run.conf.reopenedtimesheetslookup }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "{{result('get_timesheet_details_15').statusUri}}",
                "timesheeturi": "{{result('get_timesheet_details_15').uri}}"
            }
        )

        if_request_startdaytype_contains_fullday_20=rail.IfOperator(
            task_id='if_request_startdaytype_contains_fullday_20',
            test='''{{ dag_run.conf.startdaytype | matches('Full Day') }}''',
            yes_task="get_time_off_type_assignments_for_user_21",
            no_task="if_request_startdaytype_contains_partialday_34",
        )

        get_time_off_type_assignments_for_user_21=rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user_21',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_checkif_timeofftypeisassignedtouser_22=rail.PythonOperator(
          task_id='log_checkif_timeofftypeisassignedtouser_22',
          python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
            'get_time_off_type_assignments_for_user_21'),'uri',dag_run.conf['timeoffuri'],'uri','')
        )

        if_log_checkif_timeofftypeisassignedtouser_22_present_23=rail.IfOperator(
            task_id='if_log_checkif_timeofftypeisassignedtouser_22_present_23',
            test='''{{ result('log_checkif_timeofftypeisassignedtouser_22') | is_truthy }}''',
            yes_task="get_time_off_details_for_user_and_date_range2_24",
            no_task="assuranceagency_timeoffimport_logs_add_entry_33",
        )

        get_time_off_details_for_user_and_date_range2_24=rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range2_24',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "dateRange": {
                "startDate": {
                "year": "{{result('date_split_12').year}}",
                "month": "{{result('date_split_12').month}}",
                "day": "{{result('date_split_12').day}}"
                },
                "endDate": {
                "year": "{{result('date_split_12').year}}",
                "month": "{{result('date_split_12').month}}",
                "day": "{{result('date_split_12').day}}"
                },
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        if_first_uri_present_25=rail.IfOperator(
            task_id='if_first_uri_present_25',
            test=lambda: bool(rail.result('get_time_off_details_for_user_and_date_range2_24') and rail.result(
              'get_time_off_details_for_user_and_date_range2_24')[0]['uri']),
            yes_task="delete_time_off_26",
            no_task="create_new_time_off_draft_27",
        )

        delete_time_off_26=rail.RepliconServiceOperator(
            task_id='delete_time_off_26',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
              "timeOffUri": "{{ result('get_time_off_details_for_user_and_date_range2_24')[0].uri }}"
            }
        )

        create_new_time_off_draft_27=rail.RepliconServiceOperator(
            task_id='create_new_time_off_draft_27',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
              "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_time_off2_28=rail.RepliconServiceOperator(
            task_id='put_time_off2_28',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data={
              "timeOff": {
                "target": {
                  "uri": "{{ result('create_new_time_off_draft_27') }}"
                },
                "owner": {
                  "uri": "{{ dag_run.conf.useruri }}",
                  "loginName": null,
                  "parameterCorrelationId": null
                },
                "timeOffType": {
                  "uri": "{{ dag_run.conf.timeoffuri }}",
                  "name": null
                },
                "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                "multiDayUsingStartEndDate": {
                  "timeOffStart": {
                    "date": {
                      "year": "{{result('date_split_12').year}}",
                      "month": "{{result('date_split_12').month}}",
                      "day": "{{result('date_split_12').day}}"
                    },
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

        publish_time_off_draft_29=rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_29',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
              "timeOff": "{{ result('create_new_time_off_draft_27') }}"
            }
        )

        force_approve_30=rail.RepliconServiceOperator(
            task_id='force_approve_30',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
              "timeOffUri": "{{ result('publish_time_off_draft_29').uri }}",
              "unitOfWorkId": "{{ dag_run_ecid() }}{{dag_run.conf.callerjobid}}",
              "comments": "Approved by Replicon Integration"
            }
        )

        assuranceagency_timeoffimport_logs_add_entry_31=rail.WriteLogOperator(
            task_id='assuranceagency_timeoffimport_logs_add_entry_31',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.username }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.startdayhours }}",
                "status": "success",
                "details": ''
            }
        )

        assuranceagency_timeoffimport_logs_add_entry_33=rail.WriteLogOperator(
            task_id='assuranceagency_timeoffimport_logs_add_entry_33',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="failed",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.username }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.startdayhours }}",
                "status": "ignored",
                "details": 'The timeoff "{{ dag_run.conf.formattedtimeofftype }}" is not assigned to user'
            }
        )

        if_request_startdaytype_contains_partialday_34=rail.IfOperator(
            task_id='if_request_startdaytype_contains_partialday_34',
            test='''{{ dag_run.conf.startdaytype | matches('Partial Day') }}''',
            yes_task="decimal_convert_35",
            no_task="catch_and_log_entry",
        )

        def get_hours_into_decimal(dag_run):
            startdayhours = float((dag_run.conf['startdayhours'].split(' '))[0])
            hours, remainder = divmod(startdayhours, 1)
            minutes, seconds = divmod(remainder * 3600, 60)
            return {
                "hours": int(hours),
                "minutes": int(minutes),
                "seconds": int(seconds)
            }

        decimal_convert_35=rail.PythonOperator(
            task_id='decimal_convert_35',
            python_callable=get_hours_into_decimal
        )

        get_time_off_type_assignments_for_user_36=rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user_36',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_checkif_timeofftypeisassignedtouser_37=rail.PythonOperator(
            task_id='log_checkif_timeofftypeisassignedtouser_37',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
              'get_time_off_type_assignments_for_user_36'),'uri',dag_run.conf['timeoffuri'],'uri','')
        )

        if_log_checkif_timeofftypeisassignedtouser_37_present_38=rail.IfOperator(
            task_id='if_log_checkif_timeofftypeisassignedtouser_37_present_38',
            test='''{{ result('log_checkif_timeofftypeisassignedtouser_37') | is_truthy }}''',
            yes_task="get_time_off_details_for_user_and_date_range2_39",
            no_task="assuranceagency_timeoffimport_logs_add_entry_48",
        )

        get_time_off_details_for_user_and_date_range2_39=rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range2_39',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "dateRange": {
                "startDate": {
                  "year": "{{result('date_split_12').year}}",
                  "month": "{{result('date_split_12').month}}",
                  "day": "{{result('date_split_12').day}}"
                },
                "endDate": {
                  "year": "{{result('date_split_12').year}}",
                  "month": "{{result('date_split_12').month}}",
                  "day": "{{result('date_split_12').day}}"
                },
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        if_first_uri_present_40=rail.IfOperator(
            task_id='if_first_uri_present_40',
            test=lambda: bool(rail.result('get_time_off_details_for_user_and_date_range2_39') and rail.result(
              'get_time_off_details_for_user_and_date_range2_39')[0]['uri']),
            yes_task="delete_time_off_41",
            no_task="create_new_time_off_draft_42",
        )

        delete_time_off_41=rail.RepliconServiceOperator(
            task_id='delete_time_off_41',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
              "timeOffUri": "{{ result('get_time_off_details_for_user_and_date_range2_39')[0].uri }}"
            }
        )

        create_new_time_off_draft_42=rail.RepliconServiceOperator(
            task_id='create_new_time_off_draft_42',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
              "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_time_off2_43=rail.RepliconServiceOperator(
            task_id='put_time_off2_43',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data={
              "timeOff": {
                "target": {
                  "uri": "{{ result('create_new_time_off_draft_42') }}"
                },
                "owner": {
                  "uri": "{{ dag_run.conf.useruri }}",
                  "loginName": null,
                  "parameterCorrelationId": null
                },
                "timeOffType": {
                  "uri": "{{ dag_run.conf.timeoffuri }}",
                  "name": null
                },
                "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                "multiDayUsingStartEndDate": {
                  "timeOffStart": {
                    "date": {
                      "year": "{{result('date_split_12').year}}",
                      "month": "{{result('date_split_12').month}}",
                      "day": "{{result('date_split_12').day}}"
                    },
                    "timeOfDay": null,
                    "relativeDuration": null,
                    "specificDuration": {
                      "hours": "{{result('decimal_convert_35').hours}}",
                      "minutes": "{{result('decimal_convert_35').minutes}}",
                      "seconds": "{{result('decimal_convert_35').seconds}}",
                      "milliseconds": "0",
                      "microseconds": "0"
                    }
                  },
                  "timeOffEnd": null
                },
                "userExplicitEntries": [ ],
                "comments": "Added by Replicon Integration",
              "customFieldValues": []
              }
            }
        )

        publish_time_off_draft_44=rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_44',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
              "timeOff": "{{ result('create_new_time_off_draft_42') }}"
            }
        )

        force_approve_45=rail.RepliconServiceOperator(
            task_id='force_approve_45',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
              "timeOffUri": "{{ result('publish_time_off_draft_44').uri }}",
              "unitOfWorkId": "{{ dag_run_ecid() }}{{dag_run.conf.callerjobid}}",
              "comments": "Approved by Replicon Integration"
            }
        )

        assuranceagency_timeoffimport_logs_add_entry_46=rail.WriteLogOperator(
            task_id='assuranceagency_timeoffimport_logs_add_entry_46',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.username }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.startdayhours }}",
                "status": "success",
                "details": ''
            }
        )

        assuranceagency_timeoffimport_logs_add_entry_48=rail.WriteLogOperator(
            task_id='assuranceagency_timeoffimport_logs_add_entry_48',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="failed",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.username }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.startdayhours }}",
                "status": "ignored",
                "details": 'The timeoff "{{ dag_run.conf.formattedtimeofftype }}" is not assigned to user'
            }
        )

        catch_and_log_entry=rail.WriteLogOperator(
            task_id='catch_and_log_entry',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            trigger_rule='one_failed',
            severity="failed",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.username }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "hours": "{{ dag_run.conf.startdayhours }}",
                "status": "failed",
                "details": "{{get_error_message()}}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule = 'all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_entry
        can_run_batch_task >> rail.Label('No') >> if_request_useruri_blank_2
        if_request_useruri_blank_2 >> rail.Label('Yes')  >> assuranceagency_timeoffimport_logs_add_entry_3 >> catch_and_log_entry
        if_request_useruri_blank_2 >> rail.Label('No') >> if_request_timeoffuri_blank_5
        if_request_timeoffuri_blank_5 >> rail.Label('Yes')  >> assuranceagency_timeoffimport_logs_add_entry_6 >> catch_and_log_entry
        if_request_timeoffuri_blank_5 >> rail.Label('No') >> if_request_startdaytype_not_contains_partialday_8
        if_request_startdaytype_not_contains_partialday_8 >> rail.Label('Yes')  >> assuranceagency_timeoffimport_logs_add_entry_9 >> catch_and_log_entry
        if_request_startdaytype_not_contains_partialday_8 >> rail.Label('No') >> date_split_12 >> get_timesheet_for_date2_13 >> if_timesheet_uri_present_14
        if_timesheet_uri_present_14 >> rail.Label('Yes')  >> get_timesheet_details_15 >> if_d_statusuri_ends_with_approved_16
        if_d_statusuri_ends_with_approved_16 >> rail.Label(
          'Yes') >> reopen_17 >> assuranceagency_timeoffimport_reopenedtimesheets_add_entry_18 >> if_request_startdaytype_contains_fullday_20
        if_d_statusuri_ends_with_approved_16 >> rail.Label('No') >> if_request_startdaytype_contains_fullday_20
        if_timesheet_uri_present_14 >> rail.Label('No') >> if_request_startdaytype_contains_fullday_20
        if_request_startdaytype_contains_fullday_20 >> rail.Label('Yes') >> get_time_off_type_assignments_for_user_21
        get_time_off_type_assignments_for_user_21 >> log_checkif_timeofftypeisassignedtouser_22 >> if_log_checkif_timeofftypeisassignedtouser_22_present_23
        if_log_checkif_timeofftypeisassignedtouser_22_present_23 >> rail.Label(
            'Yes')  >> get_time_off_details_for_user_and_date_range2_24 >> if_first_uri_present_25
        if_first_uri_present_25 >> rail.Label('Yes')  >> delete_time_off_26 >> create_new_time_off_draft_27
        if_first_uri_present_25 >> rail.Label('No') >> create_new_time_off_draft_27 >> put_time_off2_28 >> publish_time_off_draft_29 >> force_approve_30
        force_approve_30 >> assuranceagency_timeoffimport_logs_add_entry_31 >> if_request_startdaytype_contains_partialday_34
        if_log_checkif_timeofftypeisassignedtouser_22_present_23 >> rail.Label(
            'No') >> assuranceagency_timeoffimport_logs_add_entry_33 >> if_request_startdaytype_contains_partialday_34
        if_request_startdaytype_contains_fullday_20 >> rail.Label('No') >> if_request_startdaytype_contains_partialday_34
        if_request_startdaytype_contains_partialday_34 >> rail.Label('Yes') >> decimal_convert_35 >> get_time_off_type_assignments_for_user_36
        get_time_off_type_assignments_for_user_36 >> log_checkif_timeofftypeisassignedtouser_37 >> if_log_checkif_timeofftypeisassignedtouser_37_present_38
        if_log_checkif_timeofftypeisassignedtouser_37_present_38 >> rail.Label(
            'Yes')  >> get_time_off_details_for_user_and_date_range2_39 >> if_first_uri_present_40
        if_first_uri_present_40 >> rail.Label('Yes')  >> delete_time_off_41 >> create_new_time_off_draft_42
        if_first_uri_present_40 >> rail.Label('No') >> create_new_time_off_draft_42 >> put_time_off2_43 >> publish_time_off_draft_44 >> force_approve_45
        force_approve_45 >> assuranceagency_timeoffimport_logs_add_entry_46 >> catch_and_log_entry
        if_log_checkif_timeofftypeisassignedtouser_37_present_38 >> rail.Label('No') >> assuranceagency_timeoffimport_logs_add_entry_48 >> catch_and_log_entry
        if_request_startdaytype_contains_partialday_34 >> rail.Label('No') >> catch_and_log_entry >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
