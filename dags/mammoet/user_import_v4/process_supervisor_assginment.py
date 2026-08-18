from datetime import timedelta
from pendulum import datetime
import rail
from mammoet.user_import_v4.utils import custom_methods
from airflow.models import Variable


null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_import_process_supervisor_assignment_dag_id,
        description="Mammoet User Import Process Supervisor assignment",
        start_date=datetime(2023, 9, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_supervisor_assignment_max_active_runs,
    ) as dag:


        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_managerid_and_employee_id_same'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_managerid_and_employee_id_same',
            end_task='catch_and_log_error',
        )

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        is_managerid_and_employee_id_same = rail.IfOperator(
            task_id="is_managerid_and_employee_id_same",
            test="{{ dag_run.conf.employee_id == dag_run.conf.supervisor_id }}",
            yes_task="log_employee_id_manager_id_same",
            no_task="get_supervisor_details"
        )

        log_employee_id_manager_id_same = rail.WriteLogOperator(
            task_id="log_employee_id_manager_id_same",
            severity="Exception",
            message="Supervisor assignment skipped as Manager & employee Id are same",
            log="{{dag_run.conf.user_log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_record_index}}',
                "status": "Exception",
                "action": "{{dag_run.conf.action}}",
                "details": "Supervisor assignment skipped as Manager & employee Id are same"
            }
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": "{{dag_run.conf.supervisor_id}}",
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: res[0] if res else []
        )

        is_supervisor_found = rail.IfOperator(
            task_id="is_supervisor_found",
            test=lambda: bool(rail.result('get_supervisor_details')),
            yes_task="is_user_disable",
            no_task="log_supervisor_not_found"
        )

        log_supervisor_not_found = rail.WriteLogOperator(
            task_id="log_supervisor_not_found",
            severity="Exception",
            message="Supervisor not present in Replicon",
            log="{{dag_run.conf.user_log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_record_index}}',
                "status": "Exception",
                "action": "{{dag_run.conf.action}}",
                "details": "Supervisor not present in Replicon"
            }
        )

        is_user_disable = rail.IfOperator(
            task_id="is_user_disable",
            test="{{ not result('get_supervisor_details').userDetails.isEnabled }}",
            yes_task="enable_login",
            no_task="get_user_permissions"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": "{{ result('get_supervisor_details').userDetails.uri }}"
            }
        )

        get_user_permissions = rail.RepliconServiceOperator(
            task_id="get_user_permissions",
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('get_supervisor_details').userDetails.uri }}"
            }
        )

        def get_permissions_to_assign(permission, permission_uri=None):
            if not rail.find_first_by_attr_and_get_attr(rail.result('get_user_permissions'), 'policyUri', permission):
                return {
                    'userUri': rail.result('get_supervisor_details')['userDetails']['uri'],
                    'permissionSetUri': permission_uri['uri']
                }
            return null

        def should_assign_permissions():
            if not rail.find_first_by_attr_and_get_attr(rail.result('get_user_permissions'), 'policyUri', 'urn:replicon:policy:supervision'):
                return True
            if not rail.find_first_by_attr_and_get_attr(rail.result('get_user_permissions'), 'policyUri', 'urn:replicon:policy:user'):
                return True
            return False

        any_permissions_missing = rail.IfOperator(
            task_id="any_permissions_missing",
            test=should_assign_permissions,
            yes_task="add_missing_permission_to_supervisor",
            no_task="is_new_user"
        )

        add_missing_permission_to_supervisor = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_permission_to_supervisor',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda dag_run: list(filter(None, [
                get_permissions_to_assign(
                    'urn:replicon:policy:supervision', dag_run.conf['user_permissions']['supervisor']),
                get_permissions_to_assign('urn:replicon:policy:user', dag_run.conf['user_permissions']['basic'])])),
            data=lambda item: item
        )

        is_new_user = rail.IfOperator(
            task_id="is_new_user",
            test="{{dag_run.conf.action | lower() == 'add'}}",
            yes_task="add_supervisor_schedule_for_user",
            no_task="get_effective_supervisor_of_user"
        )

        add_supervisor_schedule_for_user = rail.RepliconServiceOperator(
            task_id="add_supervisor_schedule_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "supervisorUri": rail.result('get_supervisor_details')['userDetails']['uri'],
                "dateRange": {
                    "startDate": custom_methods.get_replicon_date_from_str(dag_run.conf['effective_date'])
                }
            }
        )

        log_supervisor_added = rail.WriteLogOperator(
            task_id="log_supervisor_added",
            severity="Success",
            message="Supervisor added successfully",
            log="{{dag_run.conf.user_log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_record_index}}',
                "status": "Success",
                "action": "{{dag_run.conf.action}}",
                "details": "Supervisor added successfully"
            }
        )

        get_effective_supervisor_of_user = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data={
                "userUri": "{{ dag_run.conf.user_uri}}",
                "asOfDate": custom_methods.get_today_date()
            }
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=lambda: rail.result('get_supervisor_details')['userDetails']['uri'] != rail.result(
                'get_effective_supervisor_of_user')['supervisor']['user']['uri']
            if rail.result('get_effective_supervisor_of_user') else True,
            yes_task='is_effective_date_present',
            no_task='log_same_supervisor_already_assigned'
        )

        log_same_supervisor_already_assigned = rail.WriteLogOperator(
            task_id="log_same_supervisor_already_assigned",
            severity="Exception",
            message="No change in supervisor",
            log="{{dag_run.conf.user_log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_record_index}}',
                "status": "Exception",
                "action": "{{dag_run.conf.action}}",
                "details": "No change in supervisor"
            }
        )

        is_effective_date_present = rail.IfOperator(
            task_id="is_effective_date_present",
            test="{{dag_run.conf.effective_date | is_truthy}}",
            yes_task="update_supervisor_schedule_for_user",
            no_task="log_supervisor_not_assigned_effective_date_not_present"
        )

        log_supervisor_not_assigned_effective_date_not_present = rail.WriteLogOperator(
            task_id="log_supervisor_not_assigned_effective_date_not_present",
            severity="Exception",
            message="Supervisor updated skipped as effective date not present",
            log="{{dag_run.conf.user_log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_record_index}}',
                "status": "Exception",
                "action": "{{dag_run.conf.action}}",
                "details": "Supervisor updated skipped as effective date not present"
            }
        )

        update_supervisor_schedule_for_user = rail.RepliconServiceOperator(
            task_id="update_supervisor_schedule_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "supervisorUri": rail.result('get_supervisor_details')['userDetails']['uri'],
                "dateRange": {
                    "startDate": custom_methods.get_replicon_date_from_str(dag_run.conf['effective_date'])
                }
            }
        )

        log_supervisor_updated = rail.WriteLogOperator(
            task_id='log_supervisor_updated',
            severity="Success",
            message="Supervisor updated successfully",
            log="{{dag_run.conf.user_log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_record_index}}',
                "status": "Success",
                "action": "{{dag_run.conf.action}}",
                "details": "Supervisor updated successfully"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            severity="Error",
            message="{{get_error_message()}}",
            trigger_rule='one_failed',
            log="{{dag_run.conf.user_log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_record_index}}',
                "status": "Error",
                "action": "{{dag_run.conf.action}}",
                "details": "{{get_error_message()}}"
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> is_managerid_and_employee_id_same

        is_managerid_and_employee_id_same >> rail.Label(
            "yes") >> log_employee_id_manager_id_same >> rail.Label("On Error") >> catch_and_log_error
        is_managerid_and_employee_id_same >> rail.Label(
            "No") >> get_supervisor_details
        get_supervisor_details >> is_supervisor_found >> rail.Label(
            "No") >> log_supervisor_not_found >> rail.Label("On Error") >> catch_and_log_error
        is_supervisor_found >> rail.Label("Yes") >> is_user_disable >> rail.Label("No") >> get_user_permissions >> any_permissions_missing\
            >> rail.Label("Yes") >> add_missing_permission_to_supervisor >> is_new_user
        is_user_disable >> rail.Label(
            "Yes") >> enable_login >> get_user_permissions
        any_permissions_missing >> rail.Label("No") >> is_new_user

        is_new_user >> rail.Label("Yes") >> add_supervisor_schedule_for_user >> log_supervisor_added >> rail.Label(
            "On Error") >> catch_and_log_error
        is_new_user >> rail.Label(
            "No") >> get_effective_supervisor_of_user >> is_supervisor_changed

        is_supervisor_changed >> rail.Label("Yes") >> is_effective_date_present >> rail.Label("Yes")\
            >> update_supervisor_schedule_for_user >> log_supervisor_updated >> rail.Label("On Error") >> catch_and_log_error
        is_effective_date_present >> rail.Label(
            "No") >> log_supervisor_not_assigned_effective_date_not_present >> rail.Label("On Error") >> catch_and_log_error
        is_supervisor_changed >> rail.Label(
            "No") >> log_same_supervisor_already_assigned >> rail.Label("On Error") >> catch_and_log_error

    return dag


rail.for_each_instance(create_main_dag)
