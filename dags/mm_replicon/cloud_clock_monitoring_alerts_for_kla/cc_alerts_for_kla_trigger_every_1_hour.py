from datetime import timedelta
import rail
from mm_replicon.cloud_clock_monitoring_alerts_for_kla.utils import python_callable_method

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mm_replicon/cloud_clock_monitoring_alerts_for_kla/config.py


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id="cloud_clock_monitoring_alerts_KLA",
        description="Replicon Cloud Cloud Monitoring Alerts for KLA for every 1 Hour",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(hours=config.dag_run_schedule),
        max_active_runs=1,
        default_args={
            'http_conn_id': config.http_conn_id,
        },
    ) as dag:

        get_query_params = rail.PythonOperator(
            task_id="get_query_params",
            python_callable=python_callable_method.build_query_params
        )

        download_cloud_clock_data_file_from_url = rail.SimpleHttpOperator(
            task_id='download_cloud_clock_data_file_from_url',
            method='GET',
            endpoint="problem-clocks",
            data="minLastUpdate={{result('get_query_params').minLastUpdate}}&maxLastUpdate={{result('get_query_params').maxLastUpdate}}",
            headers={"Content-Type": "text/csv; charset=utf-8",
                     "sec-fetch-dest": "document"},
            dag=dag
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id="parse_csv",
            document='{{ result("download_cloud_clock_data_file_from_url") }}'
        )

        clock_data_collection = rail.CreateCollectionOperator(
            task_id="clock_data_collection",
            source="{{result('parse_csv')}}",
            name="cloud_clock_list",
        )

        if_cloud_clock_data_exist = rail.IfOperator(
            task_id="if_cloud_clock_data_exist",
            test="{{result('clock_data_collection','length') > 0}}",
            yes_task="query_to_filter_list_on_company",
            no_task="finish"
        )

        query_to_filter_list_on_company = rail.QueryCollectionOperator(
            task_id="query_to_filter_list_on_company",
            query='''SELECT * FROM cloud_clock_list WHERE Company=:client_company_name''',
            query_params={
                "client_company_name": config.client_company_name
            }
        )

        if_company_cloud_clock_data_exist = rail.IfOperator(
            task_id="if_company_cloud_clock_data_exist",
            test="{{result('query_to_filter_list_on_company','length') > 0}}",
            yes_task="get_cloud_clock_email_content",
            no_task="finish"
        )

        get_cloud_clock_email_content = rail.RenderTemplateOperator(
            task_id='get_cloud_clock_email_content',
            target='result',
            template_file='templates/email/output_template.html',
            dataset="{{result('query_to_filter_list_on_company')}}"
        )

        send_cloud_clock_data_in_mail = rail.EmailOperator(
            task_id='send_cloud_clock_data_in_mail',
            to=config.alert_email,
            subject='Replicon Support | CloudClock Sync Failure Alert ! - KLA - {{ current_time() }}',
            html_content='{{ result("get_cloud_clock_email_content")}}',
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        get_query_params >> download_cloud_clock_data_file_from_url >> parse_csv
        parse_csv >> clock_data_collection >> if_cloud_clock_data_exist >> rail.Label(
            "Yes") >> query_to_filter_list_on_company >> if_company_cloud_clock_data_exist

        if_company_cloud_clock_data_exist >> rail.Label(
            "Yes") >> get_cloud_clock_email_content >> send_cloud_clock_data_in_mail

        if_company_cloud_clock_data_exist >> rail.Label("No") >> finish

        if_cloud_clock_data_exist >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_main_dag)
