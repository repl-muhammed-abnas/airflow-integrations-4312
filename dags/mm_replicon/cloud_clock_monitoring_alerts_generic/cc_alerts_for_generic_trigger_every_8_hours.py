from datetime import timedelta
import rail
from mm_replicon.cloud_clock_monitoring_alerts_generic.utils import custom_methods

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mm_replicon/cloud_clock_monitoring_alerts_generic/config.py


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id="cloud_clock_monitoring_alerts_generic_main_dag",
        description="Replicon Cloud Cloud Monitoring Alerts Generic for every 8 Hours",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(hours=config.dag_run_schedule),
        max_active_runs=config.max_active_runs_main_dag,
        default_args={
            'http_conn_id': config.http_conn_id,
        },
    ) as dag:

        download_cloud_clock_data_from_url = rail.SimpleHttpOperator(
            task_id='download_cloud_clock_data_from_url',
            method='GET',
            endpoint="problem-clocks",
            headers={"Content-Type": "text/csv; charset=utf-8",
                     "sec-fetch-dest": "document"},
            dag=dag
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id="parse_csv",
            document='{{ result("download_cloud_clock_data_from_url") }}'
        )

        clock_data_collection = rail.CreateCollectionOperator(
            task_id="clock_data_collection",
            source="{{result('parse_csv')}}",
            name="cloud_clock_list",
        )

        if_cloud_clock_data_exist = rail.IfOperator(
            task_id="if_cloud_clock_data_exist",
            test="{{result('clock_data_collection','length') > 0}}",
            yes_task="query_to_filter_list_to_notify",
            no_task="finish"
        )

        query_to_filter_list_to_notify = rail.QueryCollectionOperator(
            task_id="query_to_filter_list_to_notify",
            query='''SELECT * FROM cloud_clock_list WHERE
                            Last_Update NOT LIKE '%month%' AND
                            Last_Update NOT LIKE '%year%' AND
                            Last_Update NOT LIKE '%day%'
                  '''
        )

        if_company_cloud_clock_data_exist = rail.IfOperator(
            task_id="if_company_cloud_clock_data_exist",
            test="{{result('query_to_filter_list_to_notify','length') > 0}}",
            yes_task="get_company_details_existed_in_monitoring_list",
            no_task="finish"
        )

        get_company_details_existed_in_monitoring_list = rail.PythonOperator(
            task_id='get_company_details_existed_in_monitoring_list',
            python_callable=lambda: custom_methods.get_existed_company_in_monitoring_list(
                config)
        )

        if_company_data_for_email_exists = rail.IfOperator(
            task_id="if_company_data_for_email_exists",
            test=lambda: len(rail.result(
                'get_company_details_existed_in_monitoring_list')) > 2,
            yes_task="call_child_dag_to_render_and_email",
            no_task="finish"
        )

        call_child_dag_to_render_and_email = rail.TriggerDagRunForEachItemOperator(
            task_id='call_child_dag_to_render_and_email',
            items="{{ result('get_company_details_existed_in_monitoring_list') }}",
            trigger_dag_id=config.child_dag,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "Company": item["Company"],
                "emailto": item["emailto"],
                "Clock": item["Clock"],
                "Last_Update": item["Last_Update"],
                "Unsent_Punches": item["Unsent_Punches"]
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        download_cloud_clock_data_from_url >> parse_csv
        parse_csv >> clock_data_collection >> if_cloud_clock_data_exist >> rail.Label(
            "Yes") >> query_to_filter_list_to_notify >> if_company_cloud_clock_data_exist
        if_cloud_clock_data_exist >> rail.Label("No") >> finish

        if_company_cloud_clock_data_exist >> rail.Label(
            "Yes") >> get_company_details_existed_in_monitoring_list >> if_company_data_for_email_exists
        if_company_cloud_clock_data_exist >> rail.Label("No") >> finish

        if_company_data_for_email_exists >> rail.Label(
            "Yes") >> call_child_dag_to_render_and_email
        if_company_data_for_email_exists >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_main_dag)
