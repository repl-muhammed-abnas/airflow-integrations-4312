import json
from datetime import timedelta
from pendulum import datetime as dt
import rail


from report_comparison.jobs_data.utils.custom_methods import (
    comparison_details,
    generate_test_report,
    process_business_unit_data,
    process_dimensionfeetype_data,
    process_employee_department_data,
    process_maconomy_jobs_data,
    process_workbook_data,
    process_income_risk_data,
    process_vccp_company_data,
    process_specification6_data,
)

null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"maconomy_workbook jobs report comparison {config.instance}",
        schedule_interval=config.schedule_interval,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        catchup=False,
        tags=["maconomy_workbook"],
    ) as dag:

        workato_employee_department_api = rail.SimpleHttpOperator(
            task_id="workato_employee_department_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_employee_department_api,
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

        get_employee_department_mapper = rail.PythonOperator(
            task_id="get_employee_department_mapper",
            python_callable=process_employee_department_data
        )

        workato_dimensionfeetype_api = rail.SimpleHttpOperator(
            task_id="workato_dimensionfeetype_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_dimensionfeetype_api,
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

        get_dimensionfeetype_mapper = rail.PythonOperator(
            task_id="get_dimensionfeetype_mapper",
            python_callable=process_dimensionfeetype_data
        )

        workato_dimension_business_unit_api = rail.SimpleHttpOperator(
            task_id="workato_dimension_business_unit_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_dimension_business_unit_api,
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

        get_business_mapper = rail.PythonOperator(
            task_id="get_business_mapper",  
            python_callable=process_business_unit_data
        )

        workato_income_risk_api = rail.SimpleHttpOperator(
            task_id="workato_income_risk_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_dimension_income_risk_api,
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

        get_dimension_income_risk_mapper = rail.PythonOperator(
            task_id="get_dimension_income_risk_mapper", 
            python_callable=process_income_risk_data
        )

        workato_vccp_company_api = rail.SimpleHttpOperator(
            task_id="workato_vccp_company_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_vccp_company_api,
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

        get_vccp_company_mapper = rail.PythonOperator(
            task_id="get_vccp_company_mapper", 
            python_callable=process_vccp_company_data
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
            data=json.dumps({"DataboardId": 10056, "Parameters": {"1": "1"}}),
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
                    "fields": "jobgroup,customernumber,locationname,projectname,startingdate,expectedendingdate,salespersonnumber,jobname,text2,text5,text6,text7,text8,text9,jobpricelist,specification1name,companynumber,projectmanagernumber,specification5name,specification6name,specification10name,currency,blockedforamountregistrations,popup3",
                    "restriction": "closed=false and Status=JobStatusType'Order",
                    "limit": 0,
                }
            ),
            response_filter=lambda response: json.loads(response.text)
        )

        maconomy_jobs_data = rail.PythonOperator(
            task_id="maconomy_jobs_data",
            python_callable=process_maconomy_jobs_data
        )

        maconomy_specification6_api = rail.SimpleHttpOperator(
            task_id="maconomy_specification6_api",
            method="POST",
            http_conn_id="maconomy_http_connid",
            endpoint=config.maconomy_specification6_api,
            headers={
                "Maconomy-Authentication": "X-Reconnect",
                "Accept": "application/vnd.deltek.maconomy.containers+json; version=5.0",
                "Content-Type": "application/vnd.deltek.maconomy.containers+json; version=5.0",
            },
            data=json.dumps(
                {
                    "fields": "specification6name,description",
                    "limit": 0,
                }
            ),
            response_filter=lambda response: json.loads(response.text),
        )

        get_specification6_data = rail.PythonOperator(
            task_id="get_specification6_data",
            python_callable=process_specification6_data,
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
            output_file_name="Jobs_Comparison_report_{{ecid()|replace(':','_')}}.csv",
            expires_in_seconds=24 * 7 * 60 * 60,
        )

        send_report_complete_mail = rail.EmailOperator(
            task_id="send_report_complete_mail",
            to=config.tenant_email,
            subject="Jobs Report Comparison is completed {{current_time_in_specified_tz()}}",
            html_content="templates/send_completion_mail.html",
        )

        # Task dependencies
        (
            workato_employee_department_api
            >> get_employee_department_mapper
            >> workbook_logout
        )
        (
            workato_dimensionfeetype_api
            >> get_dimensionfeetype_mapper
            >> workbook_logout
        )

        workato_dimension_business_unit_api >> get_business_mapper >> workbook_logout
        workato_income_risk_api >> get_dimension_income_risk_mapper >> workbook_logout

        workato_vccp_company_api >> get_vccp_company_mapper >> workbook_logout
        
        workbook_logout >> workbook_data_api >> workbook_data_python
        workbook_data_python >> maconomy_data >> maconomy_jobs_data
        workbook_data_python >> maconomy_specification6_api >> get_specification6_data
        [maconomy_jobs_data, get_specification6_data] >> comparison_report
        comparison_report >> generate_csv_report
        generate_csv_report >> generate_download_link >> send_report_complete_mail

        return dag

rail.for_each_instance(create_airflow_dag)