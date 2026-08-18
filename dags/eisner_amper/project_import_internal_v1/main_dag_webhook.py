from datetime import timedelta
from pendulum import datetime as dt
import rail
from eisner_amper.project_import_internal_v1.utils.response_filter import get_project_type_tag_uris, get_project_profile_tag_uris

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'eisner_amper_project_import_internal_records_{config.instance}_old',
        description='Eisner Amper Project Import Internal Record Webhooks',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1),
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
                dag_run.conf['webhook']['data']['A_EnterpriseProject']['A_EnterpriseProjectType']),
            yes_task="get_project_object_extension_fields",
            no_task= 'finish'
        )

        get_project_object_extension_fields = rail.RepliconServiceOperator(
            task_id="get_project_object_extension_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"},
            data_handler=lambda oefs: {
                'projectprofiledefinitionuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Project Profile', 'uri'),
                'projecttypedefinitionuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Project Type', 'uri'),
                'timeentrycodedefinitionuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Time Entry Code', 'uri')
            },
        )

        get_oef_drop_down_values_project_type = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_project_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_project_object_extension_fields').projecttypedefinitionuri }}"},
            response_filter= get_project_type_tag_uris
        )

        get_oef_drop_down_values_project_profile = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_project_profile",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_project_object_extension_fields').projectprofiledefinitionuri }}"},
            response_filter= get_project_profile_tag_uris
        )

        process_each_client = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_client',
            items=lambda dag_run: [dag_run.conf['webhook']['data']['A_EnterpriseProject']['A_EnterpriseProjectType']] if isinstance(
                dag_run.conf['webhook']['data']['A_EnterpriseProject']['A_EnterpriseProjectType'], (dict)) else dag_run.conf[
                    'webhook']['data']['A_EnterpriseProject']['A_EnterpriseProjectType'],
            trigger_dag_id= f'eisner_amper_project_import_internal_records_process_each_client_{config.instance}',
            conf=lambda item:{
                'item': item,
                'projectprofiledefinitionuri': rail.result('get_project_object_extension_fields')['projectprofiledefinitionuri'],
                'projecttypedefinitionuri': rail.result('get_project_object_extension_fields')['projecttypedefinitionuri'],
                'timeentrycodedefinitionuri': rail.result('get_project_object_extension_fields')['timeentrycodedefinitionuri'],
                'projectprofiletaguri': rail.result("get_oef_drop_down_values_project_profile"),
                'projecttypetaguri': rail.result("get_oef_drop_down_values_project_type")
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
            trigger_dag_id=f'eisner_amper_project_import_internal_records_log_generation_{config.instance}',
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

        finish = rail.EmptyOperator(
            task_id = 'finish'
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

        is_client_available >> rail.Label(
            "Yes") >> get_project_object_extension_fields >> get_oef_drop_down_values_project_type >> get_oef_drop_down_values_project_profile >> \
                process_each_client >> wait_for_process_each_client >> gather_client_logs

        is_client_available >> rail.Label(
            "Yes") >> finish

        gather_client_logs >> gather_each_project_logs >> process_log_generation >> log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag

rail.for_each_instance(create_main_airflow_dag)
