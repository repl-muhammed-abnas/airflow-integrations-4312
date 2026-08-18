from datetime import timedelta
from airflow.models import Variable
import rail
from lead3rllc.project_import.utils import request_payload


def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_missing_values_in_replicon_dag_id,
        description='LEAD3R LLC Project Import - Add Missing Values In Replicon Child',
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
            no_task='get_all_clients'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_all_clients',
            end_task='catch_and_log_error',
        )

        get_all_clients = rail.RepliconServiceOperator(
            task_id='get_all_clients',
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.payload_to_get_all_replicon_clients,
            data_handler=lambda data: list(map(lambda x: {
                'client_name': x['cells'][0]['textValue'],
                'client_uri': x['cells'][0]['uri'],
                'enabled': x['cells'][1]['textValue']}, data['rows'])) if data['rows'] else [{'client_name': '', 'client_uri': '', 'enabled': ''}]
        )

        create_replicon_client_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_client_collection',
            source=lambda: rail.result('get_all_clients'),
            name='replicon_clients'
        )

        query_clients_in_valid_records_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id='query_clients_in_valid_records_not_present_in_replicon',
            query="""SELECT DISTINCT valid_records.company_name FROM  valid_records 
                WHERE (LOWER(valid_records.company_name) NOT IN (SELECT LOWER(replicon_clients.client_name) FROM replicon_clients) AND 
                NULLIF(valid_records.company_name, '') IS NOT NULL)""",
            name='clients_to_add'
        )

        if_clients_not_in_replicon_present_in_inputfile = rail.IfOperator(
            task_id='if_clients_not_in_replicon_present_in_inputfile',
            test=lambda: rail.result(
                'query_clients_in_valid_records_not_present_in_replicon', 'length') > 0,
            yes_task='trigger_dag_add_clients',
            no_task='dummy_task_for_trigger'
        )

        trigger_dag_add_clients = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_add_clients',
            items="{{ result('query_clients_in_valid_records_not_present_in_replicon') }}",
            trigger_dag_id=config.child_add_client_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=lambda item, dag_run: request_payload.trigger_add_client_payload(
                item, dag_run)
        )

        wait_for_child_dag_add_clients = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dag_add_clients',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_add_clients") }}'
        )

        dummy_task_for_trigger = rail.EmptyOperator(
            task_id='dummy_task_for_trigger'
        )

        trigger_dag_add_or_enable_oef_dropdown_options = rail.TriggerDagRunOperator(
            task_id='trigger_dag_add_or_enable_oef_dropdown_options',
            trigger_dag_id=config.child_add_or_enable_dropdown_options_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_add_or_enable_customfield_dropdown_options_payload
        )

        wait_for_completion_trigger_dag_add_or_enable_oef_dropdown_options = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_add_or_enable_oef_dropdown_options',
            dag_runs="{{result('trigger_dag_add_or_enable_oef_dropdown_options')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_all_department_groups = rail.RepliconServiceOperator(
            task_id='get_all_department_groups',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_all_department_groups_payload,
            data_handler=lambda data: list(map(lambda x: {
                'department_group_name': x['cells'][0]['textValue'],
                'department_group_uri': x['cells'][0]['uri'],
                'enabled': x['cells'][1]['textValue']}, data['rows'])) if data['rows'] else [
                    {'department_group_name': '', 'department_group_uri': '', 'enabled': ''}]
        )

        get_parent_department_group_uri = rail.PythonOperator(
            task_id='get_parent_department_group_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_department_groups'), 'department_group_name', 'LEAD3R LLC', 'department_group_uri')
        )

        create_collection_all_replicon_department_groups = rail.CreateCollectionOperator(
            task_id='create_collection_all_replicon_department_groups',
            source=lambda: rail.result('get_all_department_groups'),
            name='replicon_department_groups'
        )

        query_department_groups_in_valid_records_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id='query_department_groups_in_valid_records_not_present_in_replicon',
            query="""SELECT DISTINCT valid_records.deal_type FROM valid_records 
                WHERE (LOWER(valid_records.deal_type) NOT IN (SELECT LOWER(replicon_department_groups.department_group_name) 
                FROM replicon_department_groups) AND NULLIF(valid_records.deal_type, '') IS NOT NULL)""",
            name='department_groups_to_add'
        )

        if_department_groups_not_in_replicon_present_in_inputfile = rail.IfOperator(
            task_id='if_department_groups_not_in_replicon_present_in_inputfile',
            test=lambda: rail.result(
                'query_department_groups_in_valid_records_not_present_in_replicon', 'length') > 0,
            yes_task='trigger_dag_to_add_department_groups',
            no_task='catch_and_log_error'
        )

        trigger_dag_to_add_department_groups = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_to_add_department_groups',
            items="{{ result('query_department_groups_in_valid_records_not_present_in_replicon') }}",
            trigger_dag_id=config.child_add_department_group_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=lambda item, dag_run: request_payload.add_department_group_payload(
                item, dag_run)
        )

        wait_for_child_dag_add_department_groups = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dag_add_department_groups',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_to_add_department_groups") }}'
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.missing_field_value_import_logs}}",
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf["parentjobid"],
                "action": "Missing field values Dag Error",
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_all_clients

        get_all_clients >> create_replicon_client_collection >> query_clients_in_valid_records_not_present_in_replicon \
            >> if_clients_not_in_replicon_present_in_inputfile

        if_clients_not_in_replicon_present_in_inputfile >> rail.Label(
            'Yes') >> trigger_dag_add_clients >> wait_for_child_dag_add_clients >> dummy_task_for_trigger
        if_clients_not_in_replicon_present_in_inputfile >> rail.Label(
            'No') >> dummy_task_for_trigger

        dummy_task_for_trigger >> trigger_dag_add_or_enable_oef_dropdown_options >> wait_for_completion_trigger_dag_add_or_enable_oef_dropdown_options \
            >> get_all_department_groups >> get_parent_department_group_uri >> create_collection_all_replicon_department_groups \
            >> query_department_groups_in_valid_records_not_present_in_replicon >> if_department_groups_not_in_replicon_present_in_inputfile

        if_department_groups_not_in_replicon_present_in_inputfile >> rail.Label(
            'No') >> catch_and_log_error
        if_department_groups_not_in_replicon_present_in_inputfile >> rail.Label('Yes') >> trigger_dag_to_add_department_groups \
            >> wait_for_child_dag_add_department_groups >> catch_and_log_error

    return dag


rail.for_each_instance(create_child_dag)
