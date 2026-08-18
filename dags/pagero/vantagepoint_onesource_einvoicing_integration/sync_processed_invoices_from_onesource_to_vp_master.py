from datetime import datetime, timedelta, timezone
from airflow.models import Variable
import json

import rail
from pagero.vantagepoint_onesource_einvoicing_integration.utils import custom_methods

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_flow2_dag_id,
        description='Get processed invoices from Onesource and sync with Vantagepoint',
        schedule_interval=config.flow2_schedule_interval,
        integration_type="generic",
        company_key=config.company_key,
        replicon_conn_id=None,
        max_active_runs=config.max_active_runs,
        default_args={
            'vp_conn_id': config.vantagepoint_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        RESET_TIME_THRESHOLD_MINS = 30
        STANDARD_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


        def get_lastsync_time_variable(variable_name, date_format, initial_sync_time, reset_after_threshold, use_param_date_format=False):

            time_format = date_format if use_param_date_format else STANDARD_TIME_FORMAT

            def get_last_synctime(variable, last_synctime_string, current_time):
                last_synctime_datetime = datetime.strptime(
                    last_synctime_string, time_format)
                last_synctime_datetime = last_synctime_datetime.replace(tzinfo=timezone.utc)
                if last_synctime_datetime <= datetime.now(timezone.utc) - timedelta(minutes=RESET_TIME_THRESHOLD_MINS):
                    Variable.set(variable, current_time)
                return last_synctime_string
            current_time = datetime.now(timezone.utc).strftime(time_format)
            last_synctime_string = Variable.get(variable_name, default_var='')
            last_synctime = (get_last_synctime(variable_name, last_synctime_string, current_time
                                            ) if reset_after_threshold else last_synctime_string) if last_synctime_string else initial_sync_time
            try:
                last_synctime = (datetime.strptime(last_synctime, time_format)).strftime(date_format)
            except ValueError:
                if use_param_date_format:
                    # Try fallback to STANDARD_TIME_FORMAT when using custom format
                    try:
                        last_synctime = (datetime.strptime(last_synctime, STANDARD_TIME_FORMAT)).strftime(date_format)
                    except ValueError:
                        # Could not parse last_synctime, returning as-is
                        pass
            return {
                'last_synctime': last_synctime,
                'current_time': current_time
            }


        def set_lastsync_time_variable(variable_name, value_to_set):
            Variable.set(variable_name, value_to_set)

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.last_sync_time_var,
                date_format=config.time_format,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False
            )
        )

        auth_data = json.dumps({
            "client_id": f"{OPEN_BRACKETS}var.json.{config.client_id_secret_variable_name}.client_id {CLOSE_BRACKETS}",
            "scopes": "urn:tr:onesource:auth:api:einvoicing",
            "grant_type": "client_credentials",
            "client_secret": f"{OPEN_BRACKETS}var.json.{config.client_id_secret_variable_name}.client_secret {CLOSE_BRACKETS}"
        })

        onesource_authentication = rail.SimpleHttpOperator(
            task_id='onesource_authentication',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='oauth2/v1/token',
            headers={
                "Content-Type": "application/json"
            },
            data=auth_data,
            log_response=True
        )

        extract_token = rail.PythonOperator(
            task_id='extract_token',
            python_callable=custom_methods.extract_and_save_token,
        )

        get_onesource_company_details = rail.SimpleHttpOperator(
            task_id='get_onesource_company_details',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint='einvoicing/company/v1/companies',
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {OPEN_BRACKETS}ti.xcom_pull(task_ids='extract_token'){CLOSE_BRACKETS}"
            },
            log_response=True
        )

        def extract_company_id_by_name(company_name):
            """Extract company ID by matching company name from the list."""
            companies = rail.result('get_onesource_company_details')
            # SimpleHttpOperator returns JSON as string, need to parse it
            if isinstance(companies, str):
                companies = json.loads(companies)
            for company in companies:
                if company['name'] == company_name:
                    return company['id']
            raise ValueError(f"Company with name '{company_name}' not found in OneSource. Available companies: {[c['name'] for c in companies]}")

        extract_company_id = rail.PythonOperator(
            task_id='extract_company_id',
            python_callable=extract_company_id_by_name,
            op_kwargs={
                'company_name': config.onesource_company_name
            }
        )

        # Use Jinja templating for runtime values - these get resolved at task execution time
        # No documentType filter - retrieve all sent documents (Invoice, CreditNote, DebitNote)
        query_params = (
            f"?companyId={OPEN_BRACKETS}result('extract_company_id'){CLOSE_BRACKETS}"
            f"&direction=Sent"
            f"&modifiedTimeFrom={OPEN_BRACKETS}result('get_last_sync_time')['last_synctime']{CLOSE_BRACKETS}"
            f"&sort=modifiedTime"
        )

        get_document_details = rail.SimpleHttpOperator(
            task_id='get_document_details',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint=f'einvoicing/document/v1/documents{query_params}',
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {OPEN_BRACKETS}ti.xcom_pull(task_ids='extract_token'){CLOSE_BRACKETS}"
            },
            log_response=True
        )

        process_each_document = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_document",
            retries=0,
            items=lambda: json.loads(rail.result('get_document_details'))['items'],
            trigger_dag_id=config.child_flow2_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, token=f"{OPEN_BRACKETS}result('extract_token'){CLOSE_BRACKETS}": {
                "item": item,
                "token": token
            }
        )

        wait_process_each_document = rail.WaitForDagRunsSensor(
            task_id='wait_process_each_document',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_document") }}'
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda value_to_set: set_lastsync_time_variable(
                variable_name=config.last_sync_time_var,
                value_to_set=value_to_set
            ),
            op_kwargs={
                'value_to_set': f"{OPEN_BRACKETS}result('get_last_sync_time')['current_time']{CLOSE_BRACKETS}"
            }
        )

        # Task dependencies
        get_last_sync_time >> onesource_authentication >> extract_token >> get_onesource_company_details >> extract_company_id >> get_document_details
        get_document_details >> process_each_document >> wait_process_each_document >> set_last_sync_time
        

    return dag


rail.for_each_instance(create_dag)