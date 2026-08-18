import rail
import json
from pagero.vantagepoint_onesource_einvoicing_integration.utils import custom_methods

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Get invoice details from VP and send them to Onesource',
        max_active_runs=config.max_active_runs,
        integration_type="generic",
        company_key=config.company_key,
        replicon_conn_id=None,
        webhook_conf=rail.WebhookConf(
            basic_auth_username_var= config.basic_auth_username_pagero,
            basic_auth_password_var=config.basic_auth_pass_pagero),
        default_args={
            'vp_conn_id': config.vantagepoint_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        get_invoice_details_from_vantagepoint = rail.VantagepointAPIOperator(
            task_id="get_invoice_details_from_vantagepoint",
            request_method='POST',
            endpoint="/Utilities/InvokeCustom/" + config.invoice_details_sp,
            request_body=lambda dag_run: {
                "wbs1": dag_run.conf['webhook']['data']['WBS1'],
                "invoice": dag_run.conf['webhook']['data']['Invoice']
            }
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
            data=auth_data,  # Pass as JSON string
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

        convert_vantagepoint_to_puf = rail.PythonOperator(
            task_id="convert_vantagepoint_to_puf",
            python_callable=lambda: custom_methods.convert_vantagepoint_to_puf(
                rail.result('get_invoice_details_from_vantagepoint'),
                supplier_config=custom_methods.enrich_supplier_config(
                    custom_methods.parse_onesource_company_to_supplier_config(
                        rail.result('get_onesource_company_details'),
                        company_id=getattr(config, 'onesource_company_id', None),
                        company_name=getattr(config, 'onesource_company_name', None)
                    ),
                    overrides=getattr(config, 'supplier_config_overrides', None),
                    country_code=getattr(config, 'country_code', None),
                ),
                country_code=getattr(config, 'country_code', None),
                is_file_path=False,
                use_buyer_as_supplier=False
            )
        )

        prepare_multipart = rail.PythonOperator(
            task_id='prepare_multipart',
            python_callable=custom_methods.prepare_multipart_data,
            op_kwargs={
                'onesource_company_id': getattr(config, 'onesource_company_id', None)
            }
        )

        send_document_to_onesource = rail.SimpleHttpOperator(
            task_id='send_document_to_onesource',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='einvoicing/document/v1/documents',
            headers={
                "Content-Type": f"multipart/form-data; boundary={OPEN_BRACKETS}ti.xcom_pull(task_ids='prepare_multipart')['boundary']{CLOSE_BRACKETS}",
                "Authorization": f"Bearer {OPEN_BRACKETS}ti.xcom_pull(task_ids='prepare_multipart')['token']{CLOSE_BRACKETS}"
            },
            data=f"{OPEN_BRACKETS}ti.xcom_pull(task_ids='prepare_multipart')['body']{CLOSE_BRACKETS}",
            log_response=True
        )

        get_invoice_details_from_vantagepoint >> onesource_authentication >> extract_token >> get_onesource_company_details >> convert_vantagepoint_to_puf
        convert_vantagepoint_to_puf >> prepare_multipart >> send_document_to_onesource

    return dag


rail.for_each_instance(create_dag)
