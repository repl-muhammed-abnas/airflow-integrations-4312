from datetime import timedelta
import rail
from airflow.models import Variable
from eisner_amper.project_import_internal_add_api.utils import request_payload
from eisner_amper.project_import_internal_add_api.utils import response_filter

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_client,
        description='Eisner Amper Project Data Import - internal Records Process Each Clients',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_clients,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_client_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_client_log',
            end_task='catch_and_log_errors',
        )

        create_client_log = rail.CreateLogOperator(
             task_id='create_client_log'
        )

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.get_all_mandatory_check_clients,
            yes_task="search_client_in_replicon",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_madatory_fields_not_present',
            log='{{ result("create_client_log") }}',
            message=lambda dag_run :request_payload.get_exception_message(dag_run, request_payload.MANDATORY_FIELDS['client_fields']),
            severity='Exception',
            properties= lambda dag_run: {
                'clientcode': dag_run.conf['item']['ClientCode'],
                'projectcode': '',
                'taskname': '',
                'taskcode': '',
                'action': 'Validation',
                'status': 'Exception',
            }
        )

        search_client_in_replicon = rail.RepliconServiceOperator(
            task_id="search_client_in_replicon",
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.get_client_payload,
            data_handler=response_filter.get_filtered_client_data
        )

        create_clientorapply_modifications = rail.RepliconServiceOperator(
            task_id="create_clientorapply_modifications",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=request_payload.apply_client_modifications_payload,
        )

        def get_projects_to_process_list(dag_run):
            a_enterprise_project_element = dag_run.conf['item']['A_EnterpriseProjectElement']
            if not a_enterprise_project_element:
                return []
            if isinstance(a_enterprise_project_element['A_EnterpriseProjectElementType'], (dict)):
                return [a_enterprise_project_element['A_EnterpriseProjectElementType']]
            return a_enterprise_project_element['A_EnterpriseProjectElementType']

        process_each_project =  rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_project',
            items=get_projects_to_process_list,
            trigger_dag_id= config.process_each_project,
            conf=lambda dag_run,item: {
                'item': item,
                'projectprofiledefinitionuri': dag_run.conf['projectprofiledefinitionuri'],
                'projecttypedefinitionuri':  dag_run.conf['projecttypedefinitionuri'],
                'timeentrycodedefinitionuri': dag_run.conf['timeentrycodedefinitionuri'],
                'projectprofiletaguri': dag_run.conf['projectprofiletaguri'],
                'projecttypetaguri':  dag_run.conf['projecttypetaguri'],
                'clienturi': rail.result('create_clientorapply_modifications')['uri'],
                'clientcode': dag_run.conf['item']['ClientCode'],
                'tenant_wide_log' : dag_run.conf['tenant_wide_log']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_each_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_project',
            dag_runs='{{ result("process_each_project") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_project_logs',
            dag_runs='{{ result("process_each_project") }}',
            dagrun_task_id='create_project_log',
            flatten=True
        )

        log_client_success = rail.WriteLogOperator(
            task_id='log_client_success',
            log='{{ result("create_client_log") }}',
            message=lambda: "Client Updated Successfully" if request_payload.does_client_exist() else "Client Added Successfully",
            severity='Success',
            properties=lambda dag_run: {
                'clientcode': dag_run.conf['item']['ClientCode'],
                'projectcode': '',
                'taskname': '',
                'taskcode': '',
                'action': 'Update' if request_payload.does_client_exist() else "Add",
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_client_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'clientcode': dag_run.conf['item']['ClientCode'],
                'projectcode': '',
                'taskname': '',
                'taskcode': '',
                'action': 'Sync',
                'status': 'Error',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )


        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_client_log

        create_client_log >> has_mandatory_fields >> rail.Label('No') >> log_madatory_fields_not_present >> catch_and_log_errors
        has_mandatory_fields >> rail.Label('Yes') >> search_client_in_replicon >> create_clientorapply_modifications >> process_each_project
        process_each_project >> wait_for_process_each_project >> gather_project_logs >> log_client_success >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag_wbs)
