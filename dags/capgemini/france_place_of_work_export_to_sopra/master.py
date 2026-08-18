from datetime import timedelta
from pendulum import datetime
from capgemini.france_place_of_work_export_to_sopra.utils.custom_methods import get_entry_date_range_list
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Capgemini France Place of Work Export to SOPRA Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 6, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.TriggerDagRunForEachItemOperator(
            task_id='create_export_for_each_month',
            items=lambda: get_entry_date_range_list(config.time_zone, config.no_of_months_place_of_work_data_to_export, config.filename_prefix),
            trigger_dag_id=config.export_child_dag_id,
            conf=lambda item: item,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

    return dag

rail.for_each_instance(create_dag)
