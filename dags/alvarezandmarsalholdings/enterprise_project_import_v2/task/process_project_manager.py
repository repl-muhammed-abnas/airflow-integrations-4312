from datetime import timedelta
import rail
from alvarezandmarsalholdings.enterprise_project_import_v2.utils import request_payload, response_filter, python_callable

null = None

def process_project_manager_task_group(config):
    with rail.TaskGroup(group_id='process_project_manager_task', prefix_group_id=False):

        is_project_managerid_present = rail.IfOperator(
            task_id='is_project_managerid_present',
            test=lambda dag_run: bool(dag_run.conf['ProjectManager']),
            yes_task="get_user_info_on_empid",
            no_task="log_project_manager_not_present"
        )

        log_project_manager_not_present = rail.PythonOperator(
            task_id='log_project_manager_not_present',
            python_callable=lambda: 'Project Manager is not present in payload'
        )

        get_user_info_on_empid = rail.RepliconServiceOperator(
            task_id="get_user_info_on_empid",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                    "employeeId": dag_run.conf['ProjectManager']
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=response_filter.get_filtered_user_info
        )

        is_project_manager_available = rail.IfOperator(
            task_id='is_project_manager_available',
            test=lambda: bool(rail.result('get_user_info_on_empid')) and bool(rail.result('get_user_info_on_empid')['isenabled']),
            yes_task="log_project_manager_present_and_enabled",
            no_task="log_project_manager_disabled_or_not_present"
        )

        log_project_manager_disabled_or_not_present = rail.PythonOperator(
            task_id='log_project_manager_disabled_or_not_present',
            python_callable=lambda dag_run: f'Project Manager {dag_run.conf["ProjectManager"]} disabled or not present in Replicon'
        )

        log_project_manager_present_and_enabled = rail.PythonOperator(
            task_id="log_project_manager_present_and_enabled",
            python_callable=lambda: "Present"
        )

        get_project_manager_permissions_to_assign = rail.PythonOperator(
            task_id="get_project_manager_permissions_to_assign",
            python_callable=python_callable.get_project_manager_permission_to_assign
        )

        assign_project_manager_permission_set = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_project_manager_permission_set',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_project_manager_permissions_to_assign'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda item: item
        )

        finish_process_project_manager = rail.EmptyOperator(
            task_id="finish_process_project_manager",
        )

        is_project_managerid_present >> rail.Label("Yes") >> get_user_info_on_empid
        is_project_managerid_present >> rail.Label("Yes") >> log_project_manager_not_present >> finish_process_project_manager

        get_user_info_on_empid >> is_project_manager_available >> rail.Label(
            "Yes") >> log_project_manager_present_and_enabled >> get_project_manager_permissions_to_assign >> \
            assign_project_manager_permission_set >> finish_process_project_manager
        is_project_manager_available >> rail.Label(
            "No") >> log_project_manager_disabled_or_not_present >> finish_process_project_manager

    return is_project_managerid_present, finish_process_project_manager
