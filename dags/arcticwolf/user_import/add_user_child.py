
from datetime import timedelta
from airflow.models import Variable
import rail
from arcticwolf.user_import.utils import request_payload, response_filter, python_callable_methods


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.user_add_child_dagid,
        description=f'Arctic Wolf User Add Child {config.instance}',
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
            no_task='declare_list_exceptionlogger'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_exceptionlogger',
            end_task='if_create_user_failed_and_already_exists',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_exceptionlogger = rail.SetVariableOperator(
            task_id='declare_list_exceptionlogger',
            append=False,
            name='exceptionlogger',
            value=[]
        )

        get_all_enabled_activities = rail.RepliconServiceOperator(
            task_id='get_all_enabled_activities',
            endpoint='/services/ActivityService1.svc/GetEnabledActivities',
        )

        if_status_active = rail.IfOperator(
            task_id='if_status_active',
            test="{{dag_run.conf.status == 'Active'}}",
            yes_task='create_user',
            no_task='log_skipped_record_status_not_active'
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint='/services/ImportService2.svc/CreateUserOrApplyModifications',
            data=lambda dag_run: request_payload.user_add_payload_schema(
                dag_run, config)
        )

        log_skipped_record_status_not_active = rail.WriteLogOperator(
            task_id='log_skipped_record_status_not_active',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity="Info",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Add",
                "status": "Info",
                "details": "user is skipped, since status is not Active",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )
        if_request_supervisor_present = rail.IfOperator(
            task_id='if_request_supervisor_present',
            test='''{{ dag_run.conf.supervisor | is_truthy }}''',
            yes_task="if_request_user_equals_to_supervisor",
            no_task="add_final_log_for_user_created",
        )

        if_request_user_equals_to_supervisor = rail.IfOperator(
            task_id='if_request_user_equals_to_supervisor',
            test='''{{ dag_run.conf.employeeid == dag_run.conf.supervisor }}''',
            yes_task="insert_to_exceptionlogger",
            no_task="search_supervisor",
        )

        insert_to_exceptionlogger = rail.SetVariableOperator(
            task_id='insert_to_exceptionlogger',
            append=True,
            name='{{ result("declare_list_exceptionlogger").name }}',
            value={
                "log": "Supervisor not assigned since the user and supervisor are same"
            }
        )

        search_supervisor = rail.RepliconServiceOperator(
            task_id='search_supervisor',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_supervisor_payload,
            data_handler=response_filter.get_supervisor_uri_and_status
        )

        if_supervisoruri_is_blank = rail.IfOperator(
            task_id='if_supervisoruri_is_blank',
            test=lambda: not bool(rail.result('search_supervisor')['uri']),
            yes_task="add_to_supervisorassignment_queue",
            no_task="if_supervisor_status_not_equals_to_true",
        )

        add_to_supervisorassignment_queue = rail.WriteLogOperator(
            task_id='add_to_supervisorassignment_queue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.emailaddress }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "useruri": "{{ result('create_user')['user'].uri }}",
                "supervisor": "{{ dag_run.conf.supervisor }}",
                "action": "Add",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "queued"
            }
        )

        if_supervisor_status_not_equals_to_true = rail.IfOperator(
            task_id='if_supervisor_status_not_equals_to_true',
            test=lambda: rail.result('search_supervisor')['status'] != 'True',
            yes_task="add_to_supervisor_assignmentqueue",
            no_task="get_assigned_permission_sets_for_user",
        )

        add_to_supervisor_assignmentqueue = rail.WriteLogOperator(
            task_id='add_to_supervisor_assignmentqueue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.emailaddress }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "useruri": "{{ result('create_user')['user'].uri }}",
                "supervisor": "{{ dag_run.conf.supervisor }}",
                "action": "Add",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "queued"
            }
        )

        get_assigned_permission_sets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{result('search_supervisor').uri}}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.name', '') if response else ''
        )

        if_assigned_permissionset_blank = rail.IfOperator(
            task_id='if_assigned_permissionset_blank',
            test=lambda: not bool(rail.result(
                'get_assigned_permission_sets_for_user')),
            yes_task="assign_permission_set_to_user_supervisor",
            no_task="assign_supervisor",
        )

        assign_permission_set_to_user_supervisor = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_supervisor',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data={
                "userUri": "{{result('search_supervisor').uri}}",
                "permissionSetUris": [
                    "{{ dag_run.conf.supervisorpermissionuri }}",
                ]
            }
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user')['user'].uri }}",
                "supervisorUri": "{{result('search_supervisor').uri}}",
                "dateRange": null
            }
        )

        add_final_log_for_user_created = rail.WriteLogOperator(
            task_id='add_final_log_for_user_created',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity=lambda: "exception" if rail.get_dag_run_var(
                'exceptionlogger') else "Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "action": "Add",
                "status": "exception" if rail.get_dag_run_var('exceptionlogger') else "Success",
                "details": "user created ;" + rail.get_dag_run_var('exceptionlogger')[0]['log'] if rail.get_dag_run_var(
                    'exceptionlogger') else "User successfully created",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_create_user_failed_and_already_exists = rail.IfOperator(
            task_id = 'if_create_user_failed_and_already_exists',
            trigger_rule='one_failed',
            test=python_callable_methods.is_create_user_failed_and_already_exists,
            yes_task='log_user_already_exists_exception',
            no_task='catch_and_log_error'
        )

        log_user_already_exists_exception  = rail.WriteLogOperator(
            task_id='log_user_already_exists_exception',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity="Exception",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Add",
                "status": "Exception",
                "details": "The specified user already exists",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity="Error",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Add",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> if_create_user_failed_and_already_exists
        can_run_batch_task >> rail.Label('No') >> declare_list_exceptionlogger
        declare_list_exceptionlogger >> get_all_enabled_activities >> if_status_active
        if_status_active >> rail.Label(
            'Yes') >> create_user >> if_request_supervisor_present
        if_status_active >> rail.Label(
            'No') >> log_skipped_record_status_not_active >> add_final_log_for_user_created
        if_request_supervisor_present >> rail.Label(
            'Yes') >> if_request_user_equals_to_supervisor
        if_request_user_equals_to_supervisor >> rail.Label(
            'Yes') >> insert_to_exceptionlogger >> add_final_log_for_user_created
        if_request_user_equals_to_supervisor >> rail.Label(
            'No') >> search_supervisor >> if_supervisoruri_is_blank
        if_supervisoruri_is_blank >> rail.Label(
            'yes') >> add_to_supervisorassignment_queue >> add_final_log_for_user_created
        if_supervisoruri_is_blank >> rail.Label(
            'No') >> if_supervisor_status_not_equals_to_true
        if_supervisor_status_not_equals_to_true >> rail.Label(
            'Yes') >> add_to_supervisor_assignmentqueue >> add_final_log_for_user_created
        if_supervisor_status_not_equals_to_true >> rail.Label(
            'No') >> get_assigned_permission_sets_for_user >> if_assigned_permissionset_blank
        if_assigned_permissionset_blank >> rail.Label(
            'Yes') >> assign_permission_set_to_user_supervisor >> assign_supervisor
        if_assigned_permissionset_blank >> rail.Label(
            'No') >> assign_supervisor >> add_final_log_for_user_created
        if_request_supervisor_present >> rail.Label(
            'No') >> add_final_log_for_user_created >> if_create_user_failed_and_already_exists
        
        if_create_user_failed_and_already_exists >> rail.Label('Yes') >> log_user_already_exists_exception
        if_create_user_failed_and_already_exists >> rail.Label('No') >> catch_and_log_error


        

    return dag


rail.for_each_instance(create_dag)
