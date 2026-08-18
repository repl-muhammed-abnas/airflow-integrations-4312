from datetime import timedelta
import rail
from itvdaytime.schedule_sync.utils import custom_methods


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"itvdaytime_schedule_sync_process_data_by_batch_child_{config.instance}",
        description=f"iTV DayTime Schedule Sync from Replicon to Oracle {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        create_log = rail.CreateLogOperator(
            task_id="create_log"
        )

        get_raw_data = rail.QueryCollectionOperator(
            task_id="get_raw_data",
            name="get_raw_data_child_{{dag_run.conf.index}}",
            query="""SELECT * FROM raw_data"""
        )

        get_data_to_process = rail.QueryCollectionOperator(
            task_id="get_data_to_process",
            name="get_data_to_process_child_{{dag_run.conf.index}}",
            query="""SELECT * from unique_raw_data_with_index
                        WHERE CAST (ROW_NUM as int) BETWEEN {{dag_run.conf.record_start_index}} AND {{dag_run.conf.record_end_index}}"""
        )

        process_data = rail.PythonOperator(
            task_id="process_data",
            python_callable=lambda dag_run: custom_methods.process_data_child(
                dag_run, raw_data_collection=get_raw_data.task_id, data_to_process_collection=get_data_to_process.task_id),
            execution_timeout=timedelta(hours=5)
        )

        log_processed_data = rail.WriteLogOperator(
            task_id="log_processed_data",
            log="{{result('create_log')}}",
            severity="Info",
            items="{{result('process_data') | to_json}}",
            message="data_to_export by child {{dag_run.conf.index}} for range \
                        BETWEEN {{dag_run.conf.record_start_index}} AND {{dag_run.conf.record_end_index}}",
            properties=lambda item: {
                'resource_reference_type': item['resource_reference_type'],
                'period_start_date': item['period_start_date'],
                'period_end_date': item['period_end_date'],
                'publish': "Y",  # hardcoded to 'Y'
                'shift_number': item['shift_number'],
                'shift_actions': "",  # hardcoded to blank
                'reference_day': item['reference_day'],

                'shift_start_time': item['shift_start_time'],
                'shift_end_time': item['shift_end_time'],
                'shift_duration': item['shift_duration'],
                'shift_time_not_worked': item['shift_time_not_worked'],
                'shift_code': item['shift_code'],
                'shift_category': item['shift_category'],

                'shift_type': item["shift_type"],
                'allow_shift': "Y"  # hardcoded to 'Y'
            }
        )

        create_log >> [
            get_raw_data, get_data_to_process] >> process_data >> log_processed_data
    return dag


rail.for_each_instance(create_main_dag)
