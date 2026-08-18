from datetime import timedelta
import rail
from pwcglobal.user_import_v1 import request_payload
from pwcglobal.user_import_v1 import custom_method
from pwcglobal.user_import_v1.task.update_user_setting import get_update_user_setting
from pwcglobal.user_import_v1.task.put_table_view_setting import get_put_table_view_setting
from pwcglobal.user_import_v1.task.update_supervisor import get_update_supervisor


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwcglobal_user_import_add_user_child_{config.instance}_v1',
        description=f'PwCGlobal_User_Import_Child_User Add {config.instance} V1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        null = None
        user_uri = '{{ result("create_user").uri }}'

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_timesheetperiod_add',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        has_timesheetperiod_add = rail.IfOperator(
            task_id='has_timesheetperiod_add',
            test=lambda: bool(request_payload.get_conf()
                              ['timesheetperiodtype']),
            yes_task='get_timesheetperiodtype_uri',
            no_task='get_location_details',
        )

        get_timesheetperiodtype_uri = rail.RepliconServiceOperator(
            task_id='get_timesheetperiodtype_uri',
            endpoint='/services/TimesheetPeriodService2.svc/GetPageOfTimesheetPeriodsBySearchParameter',
            data={
                "page": "1",
                "pageSize": "1000",
                "timesheetPeriodSearch": {
                    "statusOptionUri": None,
                    "textSearch": {
                        "queryText": "{{ dag_run.conf.timesheetperiodtype }}",
                        "searchInDisplayText": "true",
                        "searchInName": "true",
                        "searchInDescription": "false"
                    }
                }
            },
            response_filter=custom_method.map_timesheetperiod_search_result
        )

        get_location_details = rail.RepliconServiceOperator(
            task_id='get_location_details',
            endpoint='/services/LocationService1.svc/GetLocationDetails',
            data={"locationUri": "{{ dag_run.conf.countriesgroupuri }}"}
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint='/services/importservice1.svc/PutUser2',
            data=request_payload.get_create_user_data
        )

        unassign_product = rail.RepliconServiceOperator(
            task_id='unassign_product',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data={
                "user": {
                    "uri": user_uri,
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "productAssignmentsToApply": {
                        "productUrisToUnassign": [
                            "urn:replicon-saas:product:time-intelligence"
                        ]
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        update_user_setting = get_update_user_setting(user_uri)

        put_timeoff_policy_dataaccessscopes_add = rail.RepliconServiceOperator(
            task_id='put_timeoff_policy_dataaccessscopes_add',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=request_payload.get_put_timeoff_policy_datascope_param
        )

        put_user_policy_dataaccessscopes_add = rail.RepliconServiceOperator(
            task_id='put_user_policy_dataaccessscopes_add',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=request_payload.get_put_userpolicy_datascope_param
        )

        put_user_notification_preferences = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences',
            endpoint='/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences',
            data=request_payload.get_user_notification_preference
        )

        put_table_view_settings = get_put_table_view_setting(user_uri, 'user')

        remove_timeoff_assignment = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignment',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                "userUri": user_uri,
                "timeOffTypeUris": []
            }
        )

        (update_supervisor_task, _) = get_update_supervisor(user_uri)

        has_timeofftype_uri = rail.IfOperator(
            task_id='has_timeofftype_uri',
            test=lambda: bool(request_payload.get_conf()['timeofftypeuri']),
            yes_task='add_timeofftype',
            no_task='get_exception_logs',
        )

        add_timeofftype = rail.RepliconServiceOperator(
            task_id='add_timeofftype',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                "userUri": user_uri,
                "timeOffTypeUris": ["{{ dag_run.conf.timeofftypeuri }}"]
            }
        )

        def do_get_exception_logs():
            logs = []
            location_info = rail.result('get_location_details') or {}
            if not location_info.get('code'):
                logs.append(
                    'Display name defaulted since Country code not available in the instance')
            if rail.result('get_timesheetperiodtype_uri', 'log'):
                logs.append(rail.result('get_timesheetperiodtype_uri', 'log'))
            if len(request_payload.get_conf().get('validationlog', [])) > 0:
                logs.extend(list(
                    map(lambda item: item['message'], request_payload.get_conf()['validationlog'])))
            return logs

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=do_get_exception_logs
        )

        has_exception_logs = rail.IfOperator(
            task_id='has_exception_logs',
            test=lambda: len(rail.result('get_exception_logs')) > 0,
            yes_task='write_exception_logs',
            no_task='write_success_log',
        )

        write_exception_logs = rail.WriteLogOperator(
            task_id='write_exception_logs',
            log="{{ dag_run.conf.log }}",
            message='User created with exception {{ result("get_exception_logs") | join(",")}}',
            severity='Exception',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'status': 'Exception',
                'message': 'User created with exception {{ result("get_exception_logs") | join(",")}}',
                'action': 'Add',
            }
        )

        write_success_log = rail.WriteLogOperator(
            task_id='write_success_log',
            log="{{ dag_run.conf.log }}",
            message='User created successfully',
            severity='Success',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'status': 'Success',
                'message': 'User created successfully',
                'action': 'Add',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            severity="Error",
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'status': "Error",
                'action': 'Add',
                'message': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        batch_task >> has_timesheetperiod_add
        batch_task >> catch_and_log_errors

        has_timesheetperiod_add >> rail.Label(
            'Yes') >> get_timesheetperiodtype_uri >> get_location_details
        has_timesheetperiod_add >> rail.Label(
            'No') >> get_location_details
        get_location_details >> create_user >> unassign_product >> update_user_setting >> \
            put_timeoff_policy_dataaccessscopes_add >> put_user_policy_dataaccessscopes_add >>\
            put_user_notification_preferences >> put_table_view_settings >> \
            remove_timeoff_assignment >> update_supervisor_task >> has_timeofftype_uri
        has_timeofftype_uri >> rail.Label(
            'Yes') >> add_timeofftype >> get_exception_logs
        has_timeofftype_uri >> rail.Label(
            'No') >> get_exception_logs
        get_exception_logs >> has_exception_logs
        has_exception_logs >> rail.Label(
            'yes') >> write_exception_logs >> catch_and_log_errors
        has_exception_logs >> rail.Label(
            'no') >> write_success_log >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
