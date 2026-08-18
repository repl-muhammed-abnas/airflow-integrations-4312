from datetime import timedelta
import functools
import pendulum
import rail
from airflow.models import Variable


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id = f"capgemini_deleted_timeoff_booking_log_load_test_{config.instance}",
        description=f"capgemini deleted timeoff booking log load test {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key
    ) as dag:


        get_date_value_from_variable = rail.PythonOperator(
            task_id = "get_date_value_from_variable",
            python_callable=lambda: Variable.get("capgemini_deleted_timeoff_booking_adhoc_test_date_value", "2023-10-06")
        )

        get_log_info = rail.CreateLogOperator(
            task_id = "get_log_info",
            tenant_wide_name="capgemini_deleted_timeoff_test_log",
            existing_log_mode="append"
        )


        create_log_collection = rail.CreateCollectionOperator(
            task_id = "create_log_collection",
            source="{{result('get_log_info')}}",
            name="log"
        )

        query_for_last_one_day_data = rail.QueryCollectionOperator(
            task_id = "query_for_last_one_day_data",
            query="""SELECT l.*, json_extract(l.properties, '$.timeoff_booking_uri') as timeoff_booking_uri,
              json_extract(l.properties, '$.total_working_days') FROM log l WHERE DATE(l."timestamp") = DATE(:date_value)""",
            name="last_one_day_log_data",
            query_params={
                "date_value": "{{result('get_date_value_from_variable')}}"
            }
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')["uri"],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        process_report_data = rail.EmptyOperator(
            task_id='process_report_data'
        )

        def get_batch_creation_datetime(response):
            creation_time = response["creationTime"]
            return pendulum.datetime(creation_time["year"], creation_time["month"], creation_time["day"],
                    creation_time["hour"], creation_time["minute"], creation_time["second"]).strftime("%d/%m/%Y %H:%M:%S")

        get_batch_creation_time = rail.RepliconServiceOperator(
                task_id='get_batch_creation_time',
                endpoint='/services/BatchManagementService1.svc/GetStatus',
                data={
                    "batchUri": '{{ result("run_report.create_report_run") }}'
                },
                data_handler=get_batch_creation_datetime
        )

        load_csv = rail.LoadCSVFileOperator(
                task_id='load_csv',
                document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
                headers=config.export_columns,
                delimiter=';'
        )

        @functools.lru_cache(maxsize=128)
        def get_deleted_timeoff_log():
            return rail.load_all_records(rail.result("get_log_info"))

        @functools.lru_cache(maxsize=128)
        def get_deleted_timeoff_query():
            return rail.load_all_records(rail.result('query_for_last_one_day_data'))

        def write_leave_data_row(item, deleted_timeoff_reference):
            if not item:
                return []
            if config.should_add_timeoff_balance:
                if deleted_timeoff_reference=='log':
                    working_balance  = rail.find_first_by_attr_and_get_attr(
                    get_deleted_timeoff_log(), "properties.timeoff_booking_uri", item['Leave Request ID'], "properties.total_working_days")
                else:
                    working_balance  = rail.find_first_by_attr_and_get_attr(
                    get_deleted_timeoff_query(), "timeoff_booking_uri", item['Leave Request ID'], "total_working_days")
                return [
                    item['Leave Request ID'],
                    item['Employee ID'],
                    item['Local Employee Number'],
                    item['Current Time Off Type'],
                    item['Current Start Date'],
                    item['Current End Date'],
                    working_balance,
                    item['Modified On'],
                ]

            return [
                    item['Leave Request ID'],
                    item['Employee ID'],
                    item['Local Employee Number'],
                    item['Current Time Off Type'],
                    item['Current Start Date'],
                    item['Current End Date'],
                    item['Modified On'],
                ]

        def get_headers():
            if config.should_add_timeoff_balance:
                return ['Leave Request ID', 'Employee ID',
                        'Local Employee Number', 'Current Time Off Type', 'Current Start Date', 'Current End Date', ' Time Off Days', 'Modified On']
            return config.export_columns

        write_leave_data_csv = rail.WriteCSVFileOperator(
            task_id='write_leave_data_csv',
            source="{{ result('load_csv') }}",
            delimiter=';',
            header=get_headers,
            row=  lambda item: write_leave_data_row(item, 'log'),
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv)
        )


        write_leave_data_csv_using_query = rail.WriteCSVFileOperator(
            task_id='write_leave_data_csv_using_query',
            source="{{ result('load_csv') }}",
            delimiter=';',
            header=get_headers,
            row= lambda item: write_leave_data_row(item, 'query'),
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv)
        )

        get_date_value_from_variable >> get_log_info >> create_log_collection >> query_for_last_one_day_data\
            >> get_report_details >> run_report_group_entry
        run_report_group_exit >>process_report_data >> get_batch_creation_time >> load_csv >> write_leave_data_csv\
            >> write_leave_data_csv_using_query


    return dag

rail.for_each_instance(create_main_dag)
