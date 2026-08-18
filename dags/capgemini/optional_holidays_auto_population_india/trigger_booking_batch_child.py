from datetime import timedelta
import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_book_optional_holiday_trigger_booking_batch_child_{config.instance}',
        description=f'Capgemini Auto Population of Optional Holidays India Trigger Batch Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_trigger_booking_child,
        default_args={
            'retries': 0
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def get_trigger_id(dag_run):
            batch_num = dag_run.conf["master_index"] % config.MAX_BATCH_ALLOWED
            if batch_num == 0:
                return f'capgemini_book_optional_holiday_child_{config.instance}'
            return f'capgemini_book_optional_holiday_child_batch_{batch_num}_{config.instance}'

        trigger_booking_childs_batch = rail.trigger_parallel_dagrun(
           task_id='trigger_booking_childs_batch',
            items=lambda dag_run: dag_run.conf["items"],
            parallel_count=config.trigger_child_batch_parallel_dagrun_count,
            trigger_dag_id=get_trigger_id,
            conf=lambda item, dag_run: {
                "user_data": item,
                "master_index": dag_run.conf['master_index'],
                "properties": dag_run.conf["properties"],
                "optional_holiday_booking_date_json": dag_run.conf["optional_holiday_booking_date_json"],
                "optional_holiday_booking_date": dag_run.conf["optional_holiday_booking_date"],
                "log_artifact": dag_run.conf["log_artifact"]
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        trigger_booking_childs_batch

    return dag


rail.for_each_instance(create_child_dag)
