from datetime import timedelta, datetime
import pendulum
import rail
from dxctechnology.compass_iwo_details_v4.utils import request_payload

null = None

# pylint: disable=too-many-statements


def create_iwo_details_wbs_update_reprocess_batch(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_iwo_wbs_update_reprocess_batch_{config.dag_id_postfix}',
        description=f'DXC_COMPASS_IWO_WBS_Update_Reprocess_Batch- V2.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 10, 10, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs_reprocess,
    ) as dag:

        get_reprocess_update_log = rail.CreateLogOperator(
            task_id='get_reprocess_update_log',
            tenant_wide_name=f'{config.reprocess_update_log}_{config.tenant_wide_log_postfix}',
            existing_log_mode='append',
        )

        def do_filter_log(log):
            current_time = pendulum.now(config.time_zone)
            jobs_created_since = current_time - \
                timedelta(hours=config.first_delta)
            jobs_created_till = current_time - \
                timedelta(hours=config.second_delta)
            timestamp = datetime.strptime(
                log['timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z')
            return jobs_created_till >= timestamp >= jobs_created_since

        filter_log = rail.FilterLogEntriesOperator(
            task_id='filter_log',
            log="{{ result('get_reprocess_update_log')}}",
            filter_callable=do_filter_log,
            remove_filtered_entries=True,
        )

        has_any_data = rail.HasDataOperator(
            task_id='has_any_data',
            source='{{ result("filter_log") }}',
            yes_task='process_iwo_wbs_update_reprocess',
            no_task='delete_this_dagrun'
        )

        process_iwo_wbs_update_reprocess = rail.TriggerDagRunForEachItemOperator(
            task_id='process_iwo_wbs_update_reprocess',
            retries=0,
            items=lambda: rail.result('filter_log'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_iwo_wbs_update_child_{config.dag_id_postfix}',
            conf=request_payload.get_iwo_wbs_update_reprocess
        )

        wait_for_process_iwo_wbs_update_reprocess = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_iwo_wbs_update_reprocess',
            dag_runs='{{ result("process_iwo_wbs_update_reprocess") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_reprocess_update_log >> filter_log >> has_any_data
        has_any_data >> rail.Label(
            'Yes') >> process_iwo_wbs_update_reprocess >> wait_for_process_iwo_wbs_update_reprocess
        has_any_data >> rail.Label('No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_iwo_details_wbs_update_reprocess_batch)
