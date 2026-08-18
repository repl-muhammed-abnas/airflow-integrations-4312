from datetime import timedelta, datetime
import pendulum
import rail
from dxctechnology.gsap_iwo_resource_assignment_v1.utils.python_callable_method import get_process_unique_wbs_conf_reprocess
from dxctechnology.gsap_iwo_resource_assignment_v1.task.trigger_parallel_dagrun_aycn import trigger_parallel_dagrun_async
null = None


def create_reprocess_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_reprocess_batch_dag_id,
        description=f'DXC_gsap_iwo_resource_assignment_Reprocess_Batch- V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 10, 10),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_conf")

        get_reprocess_update_log = rail.CreateLogOperator(
            task_id='get_reprocess_update_log',
            tenant_wide_name=config.reprocess_wbs_log_name,
            existing_log_mode='append',
        )

        def do_filter_log(log):
            current_time = pendulum.now()
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
            yes_task='dummy_reprocess_wbs',
            no_task='delete_this_dagrun'
        )

        dummy_reprocess_wbs = rail.EmptyOperator(
            task_id = "dummy_reprocess_wbs"
        )

        reprocess_wbs = trigger_parallel_dagrun_async(
            task_id='reprocess_wbs',
            parallel_count=10,
            items=lambda: rail.result('filter_log'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_wbs_dag_id,
            conf=lambda item, **context: get_process_unique_wbs_conf_reprocess(item, context)
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_reprocess_update_log >> filter_log >> has_any_data
        has_any_data >> rail.Label(
            'Yes') >> dummy_reprocess_wbs >> reprocess_wbs
        has_any_data >> rail.Label('No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_reprocess_dag)
