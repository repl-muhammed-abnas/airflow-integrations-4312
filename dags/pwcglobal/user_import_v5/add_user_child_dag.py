import json
from datetime import timedelta
import rail
from pwcglobal.user_import_v5.utils import request_payload, custom_method
from pwcglobal.user_import_v5.task.update_user_setting import get_update_user_setting
from pwcglobal.user_import_v5.task.put_table_view_setting import get_put_table_view_setting
from pwcglobal.user_import_v5.task.update_supervisor import get_update_supervisor
from pwcglobal.user_import_v5.task.put_line_manager import put_line_manager_udf
from pwcglobal.user_import_v5.task.assign_default_toil_to_policy import add_toil_default_policy

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_add_dag_id,
        description=f'PwCGlobal_User_Import_Child_User Add',
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
            retries=1,
            retry_delay=timedelta(minutes=5)
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
            endpoint='/services/importservice1.svc/PutUser3',
            data=lambda : request_payload.get_create_user_data(config)
        )

        if_user_belongs_to_zt_country = rail.IfOperator(
            task_id="if_user_belongs_to_zt_country",
            test=lambda: bool(request_payload.get_conf()[
                              "zerotimeuserpermissionseturi"]),
            yes_task="start_user_setting",
            no_task="unassign_product"
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

        start_user_setting = rail.EmptyOperator(task_id="start_user_setting")

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

        put_time_entry_approval_path = rail.RepliconServiceOperator(
            task_id="put_time_entry_approval_path",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/UpdateApprovalPathForUser",
            data=request_payload.get_time_entry_path
        )

        get_all_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_all_timeofftypes',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
        )

        has_timeofftype_uri = rail.IfOperator(
            task_id='has_timeofftype_uri',
            test=lambda: bool((request_payload.get_conf()['toiltimeofftypeuri'] and request_payload.get_conf()["toil"] == "Y") or
                              request_payload.get_conf()['timeofftypeuri']),
            yes_task='add_timeofftypes',
            no_task='has_line_manager',
        )

        def get_time_off_req():
            time_off_types = []
            time_off_types.append(request_payload.get_conf()['timeofftypeuri'])
            if request_payload.get_conf()['toiltimeofftypeuri'] and request_payload.get_conf()["toil"] == "Y":
                time_off_types.append(
                    request_payload.get_conf()['toiltimeofftypeuri'])
            return {
                "userUri": rail.result("create_user")["uri"],
                "timeOffTypeUris": time_off_types
            }

        add_timeofftypes = rail.RepliconServiceOperator(
            task_id="add_timeofftypes",
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=get_time_off_req
        )

        has_line_manager = rail.IfOperator(
            task_id="has_line_manager",
            test=lambda: bool(
                request_payload.get_conf()["linemanagerpartyid"]),
            yes_task="line_manager_start",
            no_task="has_payrule"
        )

        line_manager_start = rail.EmptyOperator(task_id="line_manager_start")

        put_line_manager = put_line_manager_udf("add")

        has_payrule = rail.IfOperator(
            task_id="has_payrule",
            test=lambda: bool(request_payload.get_conf()[
                              "payrule"] and request_payload.get_conf()["payruleuri"]),
            yes_task="assign_payrules_for_user",
            no_task="has_ftepercent"
        )

        assign_payrules_for_user = rail.RepliconServiceOperator(
            task_id="assign_payrules_for_user",
            endpoint="services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda: {
                    "userUri": rail.result("create_user")["uri"],
                    "scheduleEntries": [
                        {
                            "effectiveDate": null,
                            "payRuleScript": {
                                "uri": request_payload.get_conf()["payruleuri"]
                            }
                        }
                    ]
            }
        )

        has_ftepercent = rail.IfOperator(
            task_id="has_ftepercent",
            test=lambda: bool(request_payload.get_conf()["ftepercent"]),
            yes_task="update_ftepercent_udf",
            no_task="if_toil_time_off"
        )

        update_ftepercent_udf = rail.RepliconServiceOperator(
            task_id='update_ftepercent_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateNumericValue',
            data=lambda dag_run: {
                "objectUri": rail.result("create_user")["uri"],
                "customFieldUri": dag_run.conf["customfielduri"]["ftepercenturi"],
                "value": request_payload.get_conf()['ftepercent']
            }
        )

        put_key_value_to_ftevalue_space = rail.RepliconServiceOperator(
            task_id="put_key_value_to_ftevalue_space",
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data=lambda: {
                    "keyNamespace": config.keynamespace,
                    "keyValue": {
                        "key": rail.result("create_user")["uri"],
                        "jsonValue": json.dumps(request_payload.get_ftevalue_json_request()
                                                )
                    }
            }
        )

        if_toil_time_off = rail.IfOperator(
            task_id="if_toil_time_off",
            test=lambda: request_payload.get_conf(
            )['toiltimeofftypeuri'] and request_payload.get_conf()["toil"] == "Y",
            yes_task="start_default_policy",
            no_task="end_default_policy"
        )

        start_default_policy = rail.EmptyOperator(
            task_id="start_default_policy")

        process_toil_default = add_toil_default_policy(user_uri)

        end_default_policy = rail.EmptyOperator(task_id="end_default_policy")

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=custom_method.do_get_exception_logs
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

        batch_task >> has_timesheetperiod_add
        batch_task >> catch_and_log_errors

        has_timesheetperiod_add >> rail.Label(
            'Yes') >> get_timesheetperiodtype_uri >> get_location_details
        has_timesheetperiod_add >> rail.Label(
            'No') >> get_location_details
        get_location_details >> create_user >> if_user_belongs_to_zt_country
        if_user_belongs_to_zt_country >> rail.Label(
            "Yes") >> start_user_setting
        if_user_belongs_to_zt_country >> rail.Label("No") >>\
            unassign_product >>\
            start_user_setting >> update_user_setting >> \
            put_timeoff_policy_dataaccessscopes_add >> put_user_policy_dataaccessscopes_add >>\
            put_user_notification_preferences >> put_table_view_settings >> \
            remove_timeoff_assignment >> update_supervisor_task >> put_time_entry_approval_path >>\
            get_all_timeofftypes >>\
            has_timeofftype_uri >> rail.Label(
            'Yes') >>\
            add_timeofftypes >> has_line_manager
        has_timeofftype_uri >> rail.Label(
            'No') >>\
            has_line_manager >> rail.Label(
                "Yes") >> line_manager_start >> put_line_manager >> has_payrule
        has_line_manager >> rail.Label("No") >>\
            has_payrule >> rail.Label("Yes") >>\
            assign_payrules_for_user >> has_ftepercent
        has_payrule >> rail.Label("No") >>\
            has_ftepercent >> rail.Label(
                "Yes") >>\
            update_ftepercent_udf >> put_key_value_to_ftevalue_space >> if_toil_time_off
        has_ftepercent >> rail.Label("No") >>\
            if_toil_time_off >> rail.Label("No") >> end_default_policy
        if_toil_time_off >> rail.Label("Yes") >> start_default_policy >> process_toil_default >> end_default_policy >>\
            get_exception_logs >> has_exception_logs
        has_exception_logs >> rail.Label(
            'yes') >> write_exception_logs >> catch_and_log_errors
        has_exception_logs >> rail.Label(
            'no') >> write_success_log >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
