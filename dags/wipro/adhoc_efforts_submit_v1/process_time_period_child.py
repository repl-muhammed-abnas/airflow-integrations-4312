from datetime import timedelta
import rail
null = None
dag_created = []


def create_airflow_child_dag(config):
    for country in config.time_export_for_country:
        cnt=str(country).replace(" ","_")
        with rail.create_airflow_dag(
            dag_id=f"{config.process_time_period_child}_{cnt}_{config.instance}_v1",
            description=f"efforts submit to wipro child {country} {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_child_runs,
        ) as dag:
            rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

            create_collection_time_export = rail.CreateCollectionOperator(
                task_id="create_collection_t",
                source= '{{dag_run.conf.time_export_data  | load_all_records | to_json }}',
                name="time_export"
            )

            query_per_entry_and_timeperiod = rail.QueryCollectionOperator(
                task_id="query_per_entry_and_timeperiod",
                query="""SELECT * FROM  time_export WHERE
                        time_export.employee_id='{{dag_run.conf.employee_id}}'
                        AND time_export.timesheet_period='{{dag_run.conf.timesheet_period}}' """
            )

            create_collection_per_period = rail.CreateCollectionOperator(
                task_id="create_collection_per_period",
                source='{{result("query_per_entry_and_timeperiod")}}',
                name="collection_per_period"
            )

            query_distinct_entry_date = rail.QueryCollectionOperator(
                task_id="query_distinct_entry_date",
                query="""SELECT DISTINCT entry_date FROM  collection_per_period """,
                name="entry_date_records"
            )

            process_per_entry_date = rail.trigger_parallel_dagrun(
                task_id="process_per_entry_date",
                items='{{result("query_distinct_entry_date")}}',
                trigger_dag_id=f"{config.process_perday_child}_{cnt}_{config.instance}_v1",
                conf=lambda item:{
                    "entry_date": item["entry_date"],
                    "time_export_per_period": rail.result("create_collection_per_period"),
                    "time_export_name": rail.render_template('{{dag_run.conf.time_export_name}}')
                },
                parallel_count=config.max_active_parallel_runs,
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            catch_and_log_errors = rail.EmptyOperator(
                task_id="catch_and_log_errors",
                trigger_rule="one_failed"
            )

            create_collection_time_export >> query_per_entry_and_timeperiod >>\
            create_collection_per_period >> query_distinct_entry_date >>\
            process_per_entry_date >> catch_and_log_errors
        dag_created.append(dag)


rail.for_each_instance(create_airflow_child_dag)
