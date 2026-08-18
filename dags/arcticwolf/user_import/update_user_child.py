from datetime import timedelta
from airflow.models import Variable
import rail
from arcticwolf.user_import.utils import request_payload, response_filter


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.user_update_child_dagid,
        description=f'Arctic Wolf User Update Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_runs,
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
            no_task='declare_list_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_logs',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_logs = rail.SetVariableOperator(
            task_id='declare_list_logs',
            append=False,
            name='logs',
            value=[]
        )

        declare_list_exception = rail.SetVariableOperator(
            task_id='declare_list_exception',
            append=False,
            name='Exception',
            value=[]
        )

        bulk_get_users3 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "parameterCorrelationId": null,
                        "employeeId": "{{dag_run.conf.employeeid}}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_effective_user_group_membership = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
            }
        )

        update_user = rail.RepliconServiceOperator(
            task_id='update_user',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.user_update_payload_schema(
                dag_run, config)
        )

        if_request_supervisor_present = rail.IfOperator(
            task_id='if_request_supervisor_present',
            test='''{{ dag_run.conf.supervisor | is_truthy }}''',
            yes_task="if_request_loginname_not_equals_to_supervisor",
            no_task="finish",
        )

        if_request_loginname_not_equals_to_supervisor = rail.IfOperator(
            task_id='if_request_loginname_not_equals_to_supervisor',
            test='''{{ dag_run.conf.employeeid != dag_run.conf.supervisor }}''',
            yes_task="search_supervisor",
            no_task="if_request_loginname_equals_to_supervisor",
        )

        search_supervisor = rail.RepliconServiceOperator(
            task_id='search_supervisor',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_supervisor_payload,
            data_handler=response_filter.get_supervisor_uri_and_status
        )

        if_search_supervisor_uri_blank = rail.IfOperator(
            task_id='if_search_supervisor_uri_blank',
            test=lambda: not bool(rail.result(
                'search_supervisor')['uri']),
            yes_task="add_to_supervisor_assignment_queue",
            no_task="if_search_supervisor_present",
        )

        add_to_supervisor_assignment_queue = rail.WriteLogOperator(
            task_id='add_to_supervisor_assignment_queue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="Queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.emailaddress }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "supervisor": "{{ dag_run.conf.supervisor }}",
                "action": "Update",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "Queued"
            }
        )

        if_search_supervisor_present = rail.IfOperator(
            task_id='if_search_supervisor_present',
            test=lambda: bool(rail.result(
                'search_supervisor')['uri']),
            yes_task="get_supervisor_assignment_details",
            no_task="if_request_loginname_equals_to_supervisor",
        )

        get_supervisor_assignment_details = rail.RepliconServiceOperator(
            task_id='get_supervisor_assignment_details',
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "asOfDate": null
            }
        )

        if_supervisor_already_assigned = rail.IfOperator(
            task_id='if_supervisor_already_assigned',
            test=lambda: rail.result('get_supervisor_assignment_details') and (rail.result('get_supervisor_assignment_details')[
                'supervisor']['uri'] != rail.result('search_supervisor')['uri']),
            yes_task="get_assigned_permission_sets_for_user",
            no_task="if_get_supervisor_assignment_details_blank",
        )

        get_assigned_permission_sets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{result('search_supervisor').uri}}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.name', '') if response and response[0]['policyUri'] else ''
        )

        if_get_assigned_permission_sets_for_user_present = rail.IfOperator(
            task_id='if_get_assigned_permission_sets_for_user_present',
            test=lambda: bool(rail.result(
                'get_assigned_permission_sets_for_user')),
            yes_task="update_supervisor_assignment_schedule_over_date_range_1",
            no_task="if_get_assigned_permission_sets_for_user_blank",
        )

        update_supervisor_assignment_schedule_over_date_range_1 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_1',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{result('search_supervisor').uri}}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.rundate.year }}",
                        "month": "{{ dag_run.conf.rundate.month }}",
                        "day": "{{ dag_run.conf.rundate.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_get_assigned_permission_sets_for_user_blank = rail.IfOperator(
            task_id='if_get_assigned_permission_sets_for_user_blank',
            test=lambda: not bool(rail.result(
                'get_assigned_permission_sets_for_user')),
            yes_task="assign_permission_set_to_user_supervisor",
            no_task="if_get_supervisor_assignment_details_blank",
        )

        assign_permission_set_to_user_supervisor = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_supervisor',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data={
                "userUri": "{{result('search_supervisor').uri}}",
                "permissionSetUris": [
                    "{{ dag_run.conf.supervisorpermissionuri }}"
                ]
            }
        )

        update_supervisor_assignment_schedule_over_date_range_2 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_2',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{result('search_supervisor').uri}}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.rundate.year }}",
                        "month": "{{ dag_run.conf.rundate.month }}",
                        "day": "{{ dag_run.conf.rundate.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_get_supervisor_assignment_details_blank = rail.IfOperator(
            task_id='if_get_supervisor_assignment_details_blank',
            test='''{{result('get_supervisor_assignment_details') | is_falsy }}''',
            yes_task="if_supervisor_permission_assigned",
            no_task="if_request_loginname_equals_to_supervisor",
        )

        if_supervisor_permission_assigned = rail.RepliconServiceOperator(
            task_id='if_supervisor_permission_assigned',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{result('search_supervisor').uri}}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.name', '') if response and response[0]['policyUri'] else ''
        )

        if_supervisor_permission_assigned_present = rail.IfOperator(
            task_id='if_supervisor_permission_assigned_present',
            test=lambda: bool(rail.result(
                'if_supervisor_permission_assigned')),
            yes_task="update_supervisor_assignment_schedule_over_date_range_3",
            no_task="if_if_supervisor_permission_assigned_blank",
        )

        update_supervisor_assignment_schedule_over_date_range_3 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_3',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{result('search_supervisor').uri}}",
                "dateRange": {
                    "startDate": null,
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_if_supervisor_permission_assigned_blank = rail.IfOperator(
            task_id='if_if_supervisor_permission_assigned_blank',
            test=lambda: not bool(rail.result(
                'if_supervisor_permission_assigned')),
            yes_task="assign_permission_set_to_user_supervisor_2",
            no_task="if_request_loginname_equals_to_supervisor",
        )

        assign_permission_set_to_user_supervisor_2 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_supervisor_2',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data={
                "userUri": "{{result('search_supervisor').uri}}",
                "permissionSetUris": [
                    "{{ dag_run.conf.supervisorpermissionuri }}"
                ]
            }
        )

        update_supervisor_assignment_schedule_over_date_range_4 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_4',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{result('search_supervisor').uri}}",
                "dateRange": {
                    "startDate": null,
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_loginname_equals_to_supervisor = rail.IfOperator(
            task_id='if_request_loginname_equals_to_supervisor',
            test='''{{ dag_run.conf.employeeid == dag_run.conf.supervisor }}''',
            yes_task="insert_to_exception",
            no_task="finish",
        )

        insert_to_exception = rail.SetVariableOperator(
            task_id='insert_to_exception',
            append=True,
            name='{{ result("declare_list_logs").name }}',
            value={
                "value": "Supervisor not assigned/updated since the user and supervisor are same"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        add_final_log_for_updated_user = rail.WriteLogOperator(
            task_id='add_final_log_for_updated_user',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity=lambda: "Exception" if rail.get_dag_run_var(
                'Exception') else "Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "action": "Update",
                "status": "Exception" if rail.get_dag_run_var('Exception') else "Success",
                "details": "Partialy Updated - " + ';'.join([excpetion['value'] for excpetion in rail.get_dag_run_var('Exception')]) if rail.get_dag_run_var(
                    'Exception') else "Successfully updated",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname']
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.userimportlogslookup }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Update",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> declare_list_logs >> declare_list_exception >>\
            bulk_get_users3 >> get_effective_user_group_membership >> update_user >> if_request_supervisor_present
        if_request_supervisor_present >> rail.Label(
            'Yes') >> if_request_loginname_not_equals_to_supervisor
        if_request_supervisor_present >> rail.Label('No') >> finish
        if_request_loginname_not_equals_to_supervisor >> rail.Label(
            'Yes') >> search_supervisor >> if_search_supervisor_uri_blank
        if_search_supervisor_uri_blank >> rail.Label(
            'Yes') >> add_to_supervisor_assignment_queue >> finish
        if_search_supervisor_uri_blank >> rail.Label(
            'No') >> if_search_supervisor_present
        if_search_supervisor_present >> rail.Label(
            'Yes') >> get_supervisor_assignment_details >> if_supervisor_already_assigned
        if_supervisor_already_assigned >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user >> if_get_assigned_permission_sets_for_user_present
        if_get_assigned_permission_sets_for_user_present >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_1 >> finish
        if_get_assigned_permission_sets_for_user_present >> rail.Label(
            'No') >> if_get_assigned_permission_sets_for_user_blank
        if_get_assigned_permission_sets_for_user_blank >> rail.Label(
            'Yes') >> assign_permission_set_to_user_supervisor >> update_supervisor_assignment_schedule_over_date_range_2 >> finish
        if_get_assigned_permission_sets_for_user_blank >> rail.Label(
            'No') >> if_get_supervisor_assignment_details_blank
        if_supervisor_already_assigned >> rail.Label(
            'No') >> if_get_supervisor_assignment_details_blank
        if_get_supervisor_assignment_details_blank >> rail.Label(
            'Yes') >> if_supervisor_permission_assigned >> if_supervisor_permission_assigned_present
        if_supervisor_permission_assigned_present >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_3 >> finish
        if_supervisor_permission_assigned_present >> rail.Label(
            'No') >> if_if_supervisor_permission_assigned_blank
        if_if_supervisor_permission_assigned_blank >> rail.Label(
            'Yes') >> assign_permission_set_to_user_supervisor_2 >> update_supervisor_assignment_schedule_over_date_range_4 >> finish
        if_if_supervisor_permission_assigned_blank >> rail.Label(
            'No') >> if_request_loginname_equals_to_supervisor
        if_get_supervisor_assignment_details_blank >> rail.Label(
            'No') >> if_request_loginname_equals_to_supervisor
        if_search_supervisor_present >> rail.Label(
            'No') >> if_request_loginname_equals_to_supervisor
        if_request_loginname_not_equals_to_supervisor >> rail.Label(
            'No') >> if_request_loginname_equals_to_supervisor
        if_request_loginname_equals_to_supervisor >> rail.Label(
            'Yes') >> insert_to_exception >> finish
        if_request_loginname_equals_to_supervisor >> rail.Label(
            'No') >> finish >> add_final_log_for_updated_user

        add_final_log_for_updated_user >> catch_and_log_error >> log_to_sumo
        return dag


rail.for_each_instance(create_dag)
