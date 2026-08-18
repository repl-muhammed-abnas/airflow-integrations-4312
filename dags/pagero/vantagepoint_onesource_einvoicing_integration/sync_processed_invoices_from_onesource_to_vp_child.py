from datetime import datetime, timezone
import json

import rail
from pagero.vantagepoint_onesource_einvoicing_integration.utils import custom_methods


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_flow2_dag_id,
        description='Sync processed invoices from OneSource to Vantagepoint',
        integration_type="generic",
        company_key=config.company_key,
        replicon_conn_id=None,
        max_active_runs=config.max_active_runs,
        default_args={'vp_conn_id': config.vantagepoint_conn_id},
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Authenticate with OneSource to get a fresh token
        auth_data = json.dumps({
            "client_id": f"{{{{var.json.{config.client_id_secret_variable_name}.client_id }}}}",
            "scopes": "urn:tr:onesource:auth:api:einvoicing",
            "grant_type": "client_credentials",
            "client_secret": f"{{{{var.json.{config.client_id_secret_variable_name}.client_secret }}}}"
        })

        onesource_authentication = rail.SimpleHttpOperator(
            task_id='onesource_authentication',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='oauth2/v1/token',
            headers={"Content-Type": "application/json"},
            data=auth_data,
            log_response=True
        )

        extract_token = rail.PythonOperator(
            task_id='extract_token',
            python_callable=custom_methods.extract_and_save_token,
        )

        get_einvoice_log = rail.VantagepointAPIOperator(
            task_id="get_einvoice_log",
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint="/UDIC/UDIC_eInvoiceLog",
            request_method='GET',
            filters=lambda dag_run: (
                f"?filterHash[0][name]=CustInvoice"
                f"&filterHash[0][value]={dag_run.conf['item']['documentInfo']['documentIdentifier']}"
                f"&filterHash[0][opp]=="
                f"&filterHash[0][seq]=0"
            )
        )

        get_document_presentation = rail.PythonOperator(
            task_id='get_document_presentation',
            python_callable=custom_methods.download_pdf_from_onesource(config.http_conn_id),
        )

        upload_pdf = rail.PythonOperator(
            task_id="upload_pdf",
            python_callable=custom_methods.upload_pdf_to_vantagepoint(config.vantagepoint_conn_id),
        )

        prepare_data = rail.PythonOperator(
            task_id='prepare_update_data',
            python_callable=custom_methods.prepare_update_data,
        )

        update_project_attachment = rail.VantagepointAPIOperator(
            task_id="update_project_attachment",
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint="/project/{{result('prepare_update_data').CustProject}}",
            request_method='PUT',
            request_body=lambda: {
                "FW_ATTACHMENTS": [{
                    "Key1": rail.result('prepare_update_data')['CustProject'],
                    "FileID": rail.result('prepare_update_data')['fileID'],
                    "FileDescription": rail.result('prepare_update_data')['fileName'],
                }]
            }
        )

        update_einvoice_log = rail.VantagepointAPIOperator(
            task_id="update_einvoice_log",
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint="/UDIC/UDIC_eInvoiceLog/{{result('prepare_update_data').UDIC_UID}}",
            request_method='PUT',
            request_body=lambda: {
                "CustSentToOneSource": "Y",
                "CustLastProcessDate": datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                "CustRejected": "N",
                "CustMessage": "Invoice processed in OneSource and PDF added to Project"
            }
        )

        # Task dependencies
        # Auth and eInvoice lookup run in parallel, both needed before PDF download
        onesource_authentication >> extract_token
        [extract_token, get_einvoice_log] >> get_document_presentation
        get_document_presentation >> upload_pdf >> prepare_data
        prepare_data >> update_project_attachment >> update_einvoice_log

        return dag


rail.for_each_instance(create_dag)
