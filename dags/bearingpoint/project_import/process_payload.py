from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from bearingpoint.project_import.utils import custom_method, request_payload, response_filter

# pylint:disable = too-many-statements
def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_payload_dagid,
        description=f'Project data sync_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.master_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='process_log_generation',
        )

        create_log = rail.CreateLogOperator(
            task_id = 'create_log'
        )

        should_process_project = rail.IfOperator(
            task_id='should_process_project',
            test=lambda dag_run: not bool(custom_method.get_missing_field(dag_run)['valid_project']),
            yes_task='get_all_permission_sets',
            no_task='log_project_exception'
        )

        log_project_exception = rail.WriteLogOperator(
            task_id="log_project_exception",
            log= "{{ result('create_log') }}",
            message="mandatory fields are missing in the payload",
            properties= custom_method.check_project_fileds
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: {
                'client_permission_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'Client Representative', 'uri'),
                'manager_permission_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'Project Manager', 'uri'),
                'co_manager_permission_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'Co Manager', 'uri')
            }
        )

        get_all_project_oef_details = rail.RepliconServiceOperator(
            task_id='get_all_project_oef_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={
                "bindingContextUri": "urn:replicon:object-type:project"
            },
            data_handler=lambda response: {
                'project_id_oef_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'Project ID', 'uri'),
                'project_name_oef_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'Project Desc', 'uri'),
                'project_category_oef_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'Project Category', 'uri'),
                'controlling_area_oef_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'ControllingArea', 'uri')
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/userlistService1.svc/GetData",
            data= request_payload.get_users_payload,
            data_handler= response_filter.get_user_data_from_list_service
        )

        get_user_permission_details = rail.RepliconServiceOperator(
            task_id="get_user_permission_details",
            endpoint="/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers",
            data=lambda: {
                "userUris": [item['uri'] for item in rail.result("get_user_details")['project_users']]
            },
            data_handler= response_filter.get_user_permissions_data
        )

        map_users_with_permission = rail.PythonOperator(
            task_id = 'map_users_with_permission',
            python_callable= custom_method.get_users_permission
        )

        should_process_client = rail.IfOperator(
            task_id="should_process_client",
            test='{{ dag_run.conf.Customer | is_truthy and dag_run.conf.CustomerName | is_truthy }}',
            yes_task='process_clients',
            no_task='get_all_service_center_details'
        )

        process_clients = rail.TriggerDagRunOperator(
            task_id='process_clients',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id= config.client_child_dag_id,
            conf=lambda dag_run: {
                "client_name": dag_run.conf['CustomerName'],
                "client_code": dag_run.conf['Customer'],
                "log": rail.result("create_log"),
                "client_manager": rail.find_first_by_attr_and_get_attr(rail.result(
                    "get_user_details")['project_users'],"employee_id", dag_run.conf['ClientRepresentative'], 'uri'),
                "client_manager_permission_set": rail.find_first_by_attr_and_get_attr(rail.result(
                    "map_users_with_permission"),"employee_id", dag_run.conf['ClientRepresentative'], 'permission'),
                "client_permission_uri": rail.result("get_all_permission_sets")['client_permission_uri']
            }
        )

        wait_for_process_clients = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_process_clients',
            dag_runs= '{{ result("process_clients") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_all_service_center_details = rail.RepliconServiceOperator(
            task_id="get_all_service_center_details",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        get_all_cost_center_details = rail.RepliconServiceOperator(
            task_id="get_all_cost_center_details",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        get_all_billing_rates = rail.RepliconServiceOperator(
            task_id="get_all_billing_rates",
            endpoint="/services/BillingRateService1.svc/GetAllBillingRates",
        )

        process_projects = rail.TriggerDagRunOperator(
            task_id='process_projects',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id= config.process_project_dag_id,
            conf= custom_method.get_child_conf
        )

        wait_for_process_projects = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_process_projects',
            dag_runs= '{{ result("process_projects") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id= config.process_log_dag_id,
            conf=lambda dag_run: {
                'logs': rail.result('create_log'),
                "master_ecid": dag_run.conf['master_ecid']
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> process_log_generation

        can_run_batch_task >> rail.Label(
            'No') >> create_log >> should_process_project >> rail.Label(
                "Yes") >> get_all_permission_sets

        should_process_project >> rail.Label(
            "No") >> log_project_exception >> process_log_generation

        get_all_permission_sets >> get_all_project_oef_details >> \
            get_user_details >> get_user_permission_details >> map_users_with_permission >> should_process_client

        should_process_client >> rail.Label(
            "Yes") >> process_clients >> wait_for_process_clients >> get_all_service_center_details

        should_process_client >> rail.Label(
            "No") >> get_all_service_center_details

        get_all_service_center_details >> get_all_cost_center_details >> get_all_billing_rates >>\
                process_projects >> wait_for_process_projects >> process_log_generation

    return dag

rail.for_each_instance(create_main_airflow_dag)
