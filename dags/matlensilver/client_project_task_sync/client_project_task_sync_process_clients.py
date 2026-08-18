import rail
from matlensilver.client_project_task_sync import request_payload
from matlensilver.client_project_task_sync import response_filter
from matlensilver.client_project_task_sync import python_callable_method

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/matlensilver/client_project_task_sync/config.py


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'matlensilver_client_project_task_sync_process_clients_{config.instance}',
        description='Matlen_Silver_Client_Project_Task_Sync_Process_Clients',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_clients,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        client_id = "{{ dag_run.conf.clientid }}"

        create_client_logs = rail.CreateLogOperator(
            task_id='create_client_logs',
        )

        has_mandatory_fields = rail.IfOperator(
            task_id="has_mandatory_fields",
            test=request_payload.get_all_mandatory_fields_check,
            yes_task="search_client_in_replicon",
            no_task="log_mandatory_fields_not_present"
        )

        log_mandatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_mandatory_fields_not_present',
            log='{{result("create_client_logs") }}',
            message='\
                {%- if dag_run.conf.clientname | is_falsy -%} \
                    Client Name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.clientid | is_falsy -%} \
                    Client Code is not present in payload, \
                {%- endif -%}',
            severity='Client_Exception',
        )

        search_client_in_replicon = rail.RepliconServiceOperator(
            task_id='search_client_in_replicon',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_search_param(client_id),
            response_filter=lambda response: response_filter.client_filter(
                response, request_payload.get_dag_run_conf()['clientid'])
        )

        apply_client_modifications = rail.RepliconServiceOperator(
            task_id='apply_client_modifications',
            endpoint='services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.get_client_mofifications_payload
        )

        has_currency = rail.IfOperator(
            task_id="has_currency",
            test=lambda dag_run: bool(dag_run.conf['currency']),
            yes_task="update_currency",
            no_task="log_client_success"
        )

        update_currency = rail.RepliconServiceOperator(
            task_id='update_currency',
            endpoint='services/ClientService1.svc/UpdateDefaultBillingCurrency',
            data=request_payload.get_update_currency_payload
        )

        log_client_success = rail.WriteLogOperator(
            task_id='log_client_success',
            log='{{ result("create_client_logs") }}',
            message='\
                {%- if result("search_client_in_replicon") | is_falsy -%} \
                    Client Added Successfully \
                {%- else -%} \
                    Client Updated Successfully \
                {%- endif -%}',
            severity='Client_Success',
            properties={
                'assignmentid': '',
                'assignmenttitle': '',
                'clientid': '{{dag_run.conf.clientid}}',
                'clientname': '{{dag_run.conf.clientname}}',
                'projectid': '',
                'projectname': '',
                'status': 'Success',
            },
        )

        get_client_success_status = rail.PythonOperator(
            task_id='get_client_success_status',
            python_callable=python_callable_method.get_client_success_status,
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{result("create_client_logs")}}',
            trigger_rule='one_failed',
            severity='Client_Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'assignmentid': '',
                'assignmenttitle': '',
                'clientid': '{{dag_run.conf.clientid}}',
                'clientname': '{{dag_run.conf.clientname}}',
                'projectid': '',
                'projectname': '',
                'status': 'Error',
            },
        )

        get_client_errors_status = rail.PythonOperator(
            task_id='get_client_errors_status',
            python_callable=python_callable_method.get_client_errors_status,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_client_logs >> has_mandatory_fields >> rail.Label(
            "Yes") >> search_client_in_replicon >> apply_client_modifications
        has_mandatory_fields >> rail.Label(
            "No") >> log_mandatory_fields_not_present >> get_client_success_status >> catch_and_log_errors
        apply_client_modifications >> has_currency >> rail.Label(
            "No") >> log_client_success >> get_client_success_status
        has_currency >> rail.Label(
            "Yes") >> update_currency >> log_client_success
        get_client_success_status >> catch_and_log_errors >> get_client_errors_status >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
