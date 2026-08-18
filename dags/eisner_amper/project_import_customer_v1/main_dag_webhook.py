from datetime import datetime, timedelta
import rail

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'eisner_amper_project_import_customer_records_{config.instance}_old',
        description='Eisner Amper Project Import Customer',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_client_available = rail.IfOperator(
            task_id='is_client_available',
            test=lambda dag_run: bool(
                dag_run.conf['webhook']['data']['ProjectSet']),
            yes_task="get_client_custom_fields"
        )

        get_client_custom_fields = rail.RepliconServiceOperator(
            task_id="get_client_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={"objectUri": "urn:replicon:object-type:client"},
            data_handler= lambda udfs:{
                'eaclientnameuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'EA Client Name', 'uri'),
                'eaclientcodeuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'EA Client Code', 'uri'),
            }
        )

        get_project_object_extension_fields = rail.RepliconServiceOperator(
            task_id="get_project_object_extension_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"},
            data_handler=lambda oefs: {
                'projectprofiledefinitionuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Project Profile', 'uri'),
                'projecttypedefinitionuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Project Type', 'uri'),
            },
        )

        get_oef_drop_down_values_project_type = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_project_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_project_object_extension_fields').projecttypedefinitionuri }}"},
        )

        get_oef_drop_down_values_project_profile = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_project_profile",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_project_object_extension_fields').projectprofiledefinitionuri }}"},
        )

        get_permission_sets_for_project_manager = rail.RepliconServiceOperator(
            task_id='get_permission_sets_for_project_manager',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler= lambda response: {
                'project_manager_permissionuri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Project Manager', 'uri'),
            }
        )

        process_each_client = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_client',
            items=lambda dag_run: [dag_run.conf['webhook']['data']['ProjectSet']['Project']] if isinstance(
                dag_run.conf['webhook']['data']['ProjectSet']['Project'], (dict)) else dag_run.conf['webhook']['data']['ProjectSet']['Project'],
            trigger_dag_id= f'eisner_amper_project_import_customer_records_process_each_client_{config.instance}',
            conf=lambda item:{
                'item': item,
                'eaclientnameudfuri': rail.result('get_client_custom_fields')['eaclientnameuri'],
                'eaclientcodeudfuri': rail.result('get_client_custom_fields')['eaclientcodeuri'],
                'projectprofiledefinitionuri': rail.result('get_project_object_extension_fields')['projectprofiledefinitionuri'],
                'projecttypedefinitionuri': rail.result('get_project_object_extension_fields')['projecttypedefinitionuri'],
                'projectprofiletaguri': rail.find_first_by_attr_and_get_attr(
                    rail.result("get_oef_drop_down_values_project_profile")['tags'], "name", "P001", "uri"),
                'projecttypetaguri': rail.find_first_by_attr_and_get_attr(rail.result("get_oef_drop_down_values_project_type")['tags'], "name", "NA", "uri"),
                'projectmanagerpermissionuri': rail.result('get_permission_sets_for_project_manager')['project_manager_permissionuri']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_each_client = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_client',
            dag_runs='{{ result("process_each_client") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_client_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_client_logs',
            dag_runs='{{ result("process_each_client") }}',
            dagrun_task_id='create_client_log',
            flatten=True
        )

        gather_each_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_each_project_logs',
            dag_runs='{{ result("process_each_client") }}',
            dagrun_task_id='gather_project_logs',
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'eisner_amper_project_import_customer_records_log_generation_{config.instance}',
            conf=lambda: {
                'client_logs': rail.result('gather_client_logs'),
                'project_logs': rail.result('gather_each_project_logs'),
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        is_client_available >> rail.Label("Yes") >> get_client_custom_fields >> get_project_object_extension_fields
        get_project_object_extension_fields >> [get_oef_drop_down_values_project_type,
            get_oef_drop_down_values_project_profile] >> get_permission_sets_for_project_manager
        get_permission_sets_for_project_manager >> process_each_client >> wait_for_process_each_client >> gather_client_logs
        gather_client_logs >> gather_each_project_logs >> process_log_generation >> log_to_sumo
        log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag

rail.for_each_instance(create_main_airflow_dag)
