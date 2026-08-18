
from datetime import timedelta
from airflow.models import Variable
import rail
from capefoxcorporation.project_sync.utils import request_payload, response_filters, custom_methods

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_each_root_project_child_dag_id,
        description=f'Capefoxcorporation Deltek Costpoint Project Sync Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_costpoint_conn_id,
        },
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log",
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ImportService1.svc/BulkGetProjects2",
            data=request_payload.get_project_details_payload,
            data_handler=response_filters.filter_project_details_response
        )

        get_costpoint_projects = rail.DeltekCostPointServiceOperator(
            task_id='get_costpoint_projects',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company="{{ dag_run.conf.item.data[0] | attr_or_default('_company') | sn }}",
            data=request_payload.get_costpoint_projects_payload,
            data_handler=response_filters.filter_costpoint_projects_response,
        )

        get_workforce_user_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_workforce_user_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company="{{ dag_run.conf.item.data[0] | attr_or_default('_company') | sn }}",
            data=request_payload.get_workforce_user_costpoint_payload,
            data_handler=response_filters.filter_workforce_user_costpoint_response,
        )

        get_users_from_replicon = rail.RepliconServiceOperator(
            task_id='get_users_from_replicon',
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=request_payload.get_bulk_users_payload,
            data_handler=response_filters.do_user_data_handler
        )

        get_project_leader_info_from_replicon = rail.RepliconServiceOperator(
            task_id='get_project_leader_info_from_replicon',
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=request_payload.get_project_leader_users_payload,
            data_handler=response_filters.filter_project_leader_response
        )

        assign_project_leader_permission = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_project_leader_permission',
            items=custom_methods.should_create_project_leader_permission,
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=request_payload.get_assign_permission_payload
        )

        get_task_info_from_replicon = rail.PythonOperator(
            task_id='get_task_info_from_replicon',
            python_callable=response_filters.do_get_task_info_from_replicon
        )

        rename_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='rename_tasks',
            items=custom_methods.filter_tasks_needing_rename,
            endpoint="/services/TaskService1.svc/UpdateName",
            data=request_payload.get_update_task_name_payload
        )

        add_project_and_task = rail.RepliconServiceOperator(
            task_id='add_project_and_task',
            endpoint="/services/ImportService1.svc/PutProject3",
            data=lambda dag_run: request_payload.get_add_project_and_task_param(dag_run, config.date_time_format, config)
        )

        add_log_entry = rail.WriteLogOperator(
            task_id='add_log_entry',
            log="{{ result('create_log') }}",
            message="Project {{ 'created' if result('get_project_details') | is_falsy else 'updated' }} successfully",
            severity="Success",
            items=custom_methods.get_dag_run_data_items,
            properties={
                "proj_id": "{{ item.row.data.PROJ_ID }}",
                "proj_name":  "{{ item.row.data.PROJ_NAME }}",
                "action": "{{ 'Add'  if result('get_project_details') | is_falsy else 'Update' }}",
                "status": "Success",
                "details": "Project and tasks synced from Costpoint to Replicon",
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            message="Failed to sync project from Costpoint to Replicon",
            severity="Error",
            items=custom_methods.get_dag_run_data_items,
            properties={
                "proj_id": "{{ item.row.data.PROJ_ID }}",
                "proj_name":  "{{ item.row.data.PROJ_NAME }}",
                "action": "{{ 'Add'  if result('get_project_details') | is_falsy else 'Update' }}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_log
        create_log >> get_project_details >> get_costpoint_projects >> get_workforce_user_costpoint >> get_users_from_replicon >> \
            get_project_leader_info_from_replicon >> assign_project_leader_permission >> get_task_info_from_replicon >> rename_tasks >> \
            add_project_and_task >> add_log_entry >> finish
        finish >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
