from datetime import timedelta
from airflow.models import Variable
import rail
from bearingpoint.project_import.utils import request_payload,response_filter

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.client_child_dag_id,
        description='Bearingpoint Process Clients Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_clients_in_replicon'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_clients_in_replicon',
            end_task='catch_and_log_errors',
        )

        get_clients_in_replicon = rail.RepliconServiceOperator(
            task_id = 'get_clients_in_replicon',
            endpoint="/services/ClientListService1.svc/GetData",
            data = request_payload.get_client_data,
            data_handler=response_filter.get_client_data_from_list_service
        )

        is_client_rep_has_not_valid_permission = rail.IfOperator(
            task_id = 'is_client_rep_has_not_valid_permission',
            test=lambda dag_run: dag_run.conf['client_manager'] and config.permission_sets['client_manager'] not in dag_run.conf[
                'client_manager_permission_set'].split(','),
            yes_task= 'assign_client_rep_permission_set',
            no_task= 'check_client_is_available'
        )

        assign_client_rep_permission_set = rail.RepliconServiceOperator(
            task_id= "assign_client_rep_permission_set",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['client_manager'],
                "permissionSetUri": dag_run.conf['client_permission_uri']
            }
        )

        check_client_is_available = rail.IfOperator(
            task_id='check_client_is_available',
            test='{{ result("get_clients_in_replicon") | is_truthy }}',
            yes_task='check_client_name_is_not_same',
            no_task='create_client'
        )

        check_client_name_is_not_same = rail.IfOperator(
            task_id='check_client_name_is_not_same',
            test='{{ result("get_clients_in_replicon").name != dag_run.conf.client_name }}',
            yes_task='update_client_name',
            no_task='is_client_rep_is_available'
        )

        update_client_name = rail.RepliconServiceOperator(
            task_id='update_client_name',
            endpoint='/services/ClientService1.svc/UpdateName',
            data={
                    "clientUri": '{{ result("get_clients_in_replicon").uri }}',
                    "name": '{{ dag_run.conf.client_name }}'
                }
        )

        is_client_rep_is_available = rail.IfOperator(
            task_id='is_client_rep_is_available',
            test='{{ dag_run.conf.client_manager | is_truthy }}',
            yes_task='update_client_rep',
            no_task='catch_and_log_errors'
        )

        update_client_rep = rail.RepliconServiceOperator(
            task_id='update_client_rep',
            endpoint='/services/ClientService1.svc/PutClientRepresentativeAssignments',
            data={
            "clientUri": '{{ result("get_clients_in_replicon").uri if result("get_clients_in_replicon") else result("create_client").uri }}',
                "clientRepresentativeUris": [
                    "{{ dag_run.conf.client_manager }}"
                ]
            }
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.get_create_client_payload
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log= '{{ dag_run.conf.log }}',
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties={
                'projectcode': '',
                'projectname': '',
                'clientcode':'{{ dag_run.conf.client_code }}',
                'taskname': '',
                "parenttaskname": '',
                'action': 'Add',
                'status': "error",
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> get_clients_in_replicon >> is_client_rep_has_not_valid_permission

        is_client_rep_has_not_valid_permission >> rail.Label(
            "Yes") >> assign_client_rep_permission_set >> check_client_is_available

        is_client_rep_has_not_valid_permission >> rail.Label(
            "No") >> check_client_is_available

        check_client_is_available >> rail.Label(
            "Yes") >> check_client_name_is_not_same

        check_client_is_available >> rail.Label(
            "No") >> create_client >> is_client_rep_is_available

        check_client_name_is_not_same >> rail.Label(
            "Yes") >> update_client_name >> is_client_rep_is_available

        check_client_name_is_not_same >> rail.Label(
            "No") >> is_client_rep_is_available

        is_client_rep_is_available >> rail.Label(
            "Yes") >> update_client_rep >> catch_and_log_errors

        is_client_rep_is_available >> rail.Label(
            "No") >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag_wbs)
