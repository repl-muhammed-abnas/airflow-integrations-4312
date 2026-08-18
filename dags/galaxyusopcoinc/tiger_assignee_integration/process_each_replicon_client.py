from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.macros import get_error_message

from galaxyusopcoinc.tiger_assignee_integration.utils import python_callable_method
from galaxyusopcoinc.tiger_assignee_integration.utils import response_filter
from galaxyusopcoinc.tiger_assignee_integration.utils import request_payload


def create_child_dag_wbs(config):
    client_dags =[]

    for idx in range(0, config.BATCH_SIZE_CLIENT):
        with rail.create_airflow_dag(
            dag_id=f'vialtopartners_tiger_assignee_integration_child_process_each_replicon_client_{config.instance}' \
                if idx ==0 else f'vialtopartners_tiger_assignee_integration_child_process_each_replicon_client_{config.instance}_batch_{idx}',
            description='Vialto Partners Tiger Assignee Integration Process Each Replicon Client',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_each_replicon_client,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            # create_success_log = rail.CreateLogOperator(
            #     task_id='create_success_log'
            # )

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='query_assignee_details_for_client'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.child_wait_execution_timeout_days),
                start_task='query_assignee_details_for_client',
                end_task='get_all_projects',
            )

            query_assignee_details_for_client = rail.QueryCollectionOperator(
                task_id="query_assignee_details_for_client",
                name='assigneedetails',
                query="""SELECT DISTINCT clientshortname, assigneeid, status FROM clientsinreplicon where clientname = "{{dag_run.conf.clientname}}" """
            )

            query_assignee_ids_uri_to_add = rail.QueryCollectionOperator(
                task_id='query_assignee_ids_uri_to_add',
                query="""SELECT assigneeuri FROM updatedrepliconassigneeids WHERE assigneeid IN
                        (SELECT assigneeid FROM assigneedetails WHERE status = 'ACTIVE')"""
            )

            tags_to_add_payload = rail.PythonOperator(
                task_id='tags_to_add_payload',
                python_callable=python_callable_method.tags_to_add_payload,
                show_return_value_in_logs= False
            )

            query_assignee_ids_uri_to_remove = rail.QueryCollectionOperator(
                task_id='query_assignee_ids_uri_to_remove',
                query="""SELECT assigneeuri FROM updatedrepliconassigneeids WHERE assigneeid IN
                        (SELECT assigneeid FROM assigneedetails WHERE status = 'EXPIRED')"""
            )

            tags_to_remove_payload = rail.PythonOperator(
                task_id='tags_to_remove_payload',
                python_callable=python_callable_method.tags_to_remove_payload,
                show_return_value_in_logs= False
            )

            get_all_projects = rail.RepliconServiceOperator(
                task_id="get_all_projects",
                endpoint="/services/ProjectListService1.svc/GetData",
                data=request_payload.get_all_projects,
                data_handler=response_filter.get_all_projects_filtered
            )

            has_projects = rail.IfOperator(
                task_id="has_projects",
                test=lambda: bool(rail.result('get_all_projects')),
                yes_task='dummy_process_each_project',
                no_task="log_no_projects_present"
            )

            log_no_projects_present = rail.WriteLogOperator(
                task_id='log_no_projects_present',
                log='{{ dag_run.conf.create_exception_log }}',
                items="{{result('query_assignee_details_for_client')}}",
                message='No Projects Associated with Client',
                severity='Exception',
                properties=lambda dag_run, item: {
                    "projectname": '',
                    "clientname": dag_run.conf['clientname'],
                    "clientshortname": item['clientshortname'],
                    'assigneeid': item['assigneeid'],
                    'assigneestatus': item['status'],
                    'details':'No Projects Associated with Client',
                    'status': 'Exception',
                    'jobid': get_dagrun_ecid(rail.get_current_context()['dag_run'])
                }
            )

            dummy_process_each_project = rail.EmptyOperator(
                task_id= "dummy_process_each_project"
            )

            def get_trigger_id_projects(item):
                if int(item['row'])%config.BATCH_SIZE_PROJECT == 0:
                    return f'vialtopartners_tiger_assignee_integration_child_process_each_project_{config.instance}'
                else:
                    return f'vialtopartners_tiger_assignee_integration_child_process_each_project_{config.instance}_batch_{int(item["row"])%config.BATCH_SIZE_PROJECT}'

            process_each_project = rail.trigger_parallel_dagrun(
                task_id='process_each_project',
                items="{{ result('get_all_projects') | to_json }}",
                parallel_count=config.trigger_parallel_dagrun_process_each_project,
                trigger_dag_id=lambda item: get_trigger_id_projects(item),
                execution_timeout=timedelta(
                    hours=config.child_execution_timeout_hours),
                conf=request_payload.get_process_each_project,
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{ dag_run.conf.create_error_log }}',
                trigger_rule='one_failed',
                severity='Error',
                items="{{result('query_assignee_details_for_client')}}",
                message='{{ get_error_message() }}',
                properties=lambda dag_run, item: {
                    "projectname": '',
                    "clientname": dag_run.conf['clientname'],
                    "clientshortname": item['clientshortname'],
                    'assigneeid': item['assigneeid'],
                    'assigneestatus': item['status'],
                    'details':get_error_message(rail.get_current_context()),
                    'status': 'Error',
                    'jobid': get_dagrun_ecid(rail.get_current_context()['dag_run'])
                },
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> get_all_projects
            can_run_batch_task >> rail.Label('No') >> query_assignee_details_for_client

            query_assignee_details_for_client >> query_assignee_ids_uri_to_add >> tags_to_add_payload >> query_assignee_ids_uri_to_remove
            query_assignee_ids_uri_to_remove >> tags_to_remove_payload >> get_all_projects >> has_projects >> rail.Label(
                'No') >> log_no_projects_present >> catch_and_log_errors
            has_projects >> rail.Label(
                'Yes') >> dummy_process_each_project >> process_each_project >> catch_and_log_errors

        client_dags.append(dag)
    
    return dag


rail.for_each_instance(create_child_dag_wbs)
