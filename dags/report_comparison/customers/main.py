import json
from datetime import timedelta
from pendulum import datetime as dt
import rail

from report_comparison.customers.utils.custom_methods import *

null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"maconomy_workbook customer report comparison {config.instance}",
        schedule_interval=config.schedule_interval,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        catchup=False,
        tags=["maconomy_workbook"],
    ) as dag:

        workato_mac_wb_paymentterms_api = rail.SimpleHttpOperator(
            task_id="workato_mac_wb_paymentterms_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_mac_wb_paymentterms_api,
            headers={
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "X-HTTP-Method-Override": "GET",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": "Bearer {{ var.value."
                + config.workato_token_var
                + " }}",
            },
        )

        get_workato_mac_wb_paymentterms_mapper = rail.PythonOperator(
            task_id="get_workato_mac_wb_paymentterms_mapper",
            python_callable=process_mac_wb_paymentterms_data
        )

        workato_mac_wb_business_unit_api = rail.SimpleHttpOperator(
            task_id="workato_mac_wb_business_unit_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_mac_wb_business_unit_api,
            headers={
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "X-HTTP-Method-Override": "GET",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": "Bearer {{ var.value."
                + config.workato_token_var
                + " }}",
            },
        )

        get_workato_mac_wb_business_unit_mapper =  rail.PythonOperator(
            task_id="get_workato_mac_wb_business_unit_mapper",
            python_callable=process_mac_wb_business_unit_data
        )

        workato_mac_wb_industry_api = rail.SimpleHttpOperator(
            task_id="workato_mac_wb_industry_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_mac_wb_industry_api,
            headers={
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "X-HTTP-Method-Override": "GET",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": "Bearer {{ var.value."
                + config.workato_token_var
                + " }}",
            },
        )

        get_workato_mac_wb_industry_mapper = rail.PythonOperator(
            task_id="get_workato_mac_wb_industry_mapper",
            python_callable=workato_mac_wb_industry_data
        )

        workbook_logout = rail.SimpleHttpOperator(
            task_id="workbook_logout",
            http_conn_id="workbook_http_connid",
            endpoint="api/auth/logout",
            method="GET"
        )

        workbook_data_api = rail.SimpleHttpOperator(
            task_id="workbook_data_api",
            method="POST",
            http_conn_id="workbook_http_connid",
            endpoint=config.workbook_api,
            headers={
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "X-HTTP-Method-Override": "GET",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": "Bearer {{ var.value."
                + config.workbook_token_var
                + " }}",
            },
            data=json.dumps({"DataboardId": config.databoard_id, "Parameters": {"1": "1"}}),
            response_filter=lambda response: json.loads(response.text),
        )

        workbook_data_python = rail.PythonOperator(
            task_id="workbook_data_python",
            python_callable=process_workbook_data
        )

        maconomy_data = rail.SimpleHttpOperator(
            task_id="maconomy_data",
            method="POST",
            http_conn_id="maconomy_http_connid",
            endpoint=config.maconomy_api,
            headers={
                "Maconomy-Authentication": "X-Reconnect",
                "Accept": "application/vnd.deltek.maconomy.containers+json; version=5.0",
                "Content-Type": "application/vnd.deltek.maconomy.containers+json; version=5.0",
            },
            data=json.dumps(
                {
                    "fields": "CustomerRemark3,CustomerRemark2,ActiveStatus,paymentterms,JobPriceList,currency,name2,name3,country,zipcode,postaldistrict,telephone,electronicmailaddress,customergroup,CustomerPopup3,CustomerPopup4,Specification1Name,Specification10Name",
                    "limit": 0,
                }
            ),
            response_filter=lambda response: json.loads(response.text)
        )

        maconomy_customer_data = rail.PythonOperator(
            task_id="maconomy_customer_data",
            python_callable=process_maconomy_customer_data
        )

        comparison_report = rail.PythonOperator(
            task_id="comparison_report",
            python_callable=comparison_details
        )

        generate_csv_report = rail.PythonOperator(
            task_id="generate_csv_report",
            python_callable=generate_test_report
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name='{{result("generate_csv_report")}}',
            output_file_name="Customer_Comparison_Report_{{ecid()|replace(':','_')}}.csv",
            expires_in_seconds=24 * 7 * 60 * 60,
        )

        send_report_complete_mail = rail.EmailOperator(
            task_id="send_report_complete_mail",
            to=config.tenant_email,
            subject="Customer Report Comparison is completed {{current_time_in_specified_tz()}}",
            html_content="templates/send_completion_mail.html",
        )

        (
            workato_mac_wb_paymentterms_api
            >> get_workato_mac_wb_paymentterms_mapper
            >> workbook_logout
        )

        workato_mac_wb_business_unit_api >> get_workato_mac_wb_business_unit_mapper >> workbook_logout
        workato_mac_wb_industry_api >> get_workato_mac_wb_industry_mapper >> workbook_logout
        workbook_logout >> workbook_data_api >> workbook_data_python
        workbook_data_python >> maconomy_data >> maconomy_customer_data
        maconomy_customer_data >> comparison_report
        comparison_report >> generate_csv_report
        generate_csv_report >> generate_download_link >> send_report_complete_mail

        return dag


rail.for_each_instance(create_airflow_dag)
