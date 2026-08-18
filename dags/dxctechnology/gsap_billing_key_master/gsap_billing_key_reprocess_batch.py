from datetime import timedelta, datetime
import itertools
import pendulum
import rail
from dxctechnology.gsap_billing_key_master.utils.custom_methods import get_process_unique_wbs_conf_reprocess
from dxctechnology.gsap_billing_key_master.tasks.trigger_parallel_dagrun_async import trigger_parallel_dagrun_async
from airflow.models import Variable

null = None


def create_reprocess_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_billing_key_reprocess_batch_{config.dag_id_postfix}',
        description=f'DXC_gsap_billing_key_Reprocess_Batch- V1.0 {config.dag_id_postfix}',
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
            job_created_since_time_delta = Variable.get(config.job_created_since_time_delta_variable_name, config.first_delta)
            job_created_till_time_delta = Variable.get(config.job_created_till_time_delta_variable_name, config.second_delta)
            current_time = pendulum.now()
            jobs_created_since = current_time - \
                timedelta(hours=float(job_created_since_time_delta))
            jobs_created_till = current_time - \
                timedelta(hours=float(job_created_till_time_delta))
            try:
                timestamp = datetime.strptime(
                            log['timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                timestamp = datetime.strptime(
                            log['timestamp'], '%Y-%m-%dT%H:%M:%S%z')
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

        reprocess_wbs = rail.trigger_parallel_dagrun(
            task_id='reprocess_wbs',
            parallel_count=10,
            items=lambda: rail.result('filter_log'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_billing_key_process_wbs_{config.dag_id_postfix}',
            conf=lambda item, **context: get_process_unique_wbs_conf_reprocess(item, context)
        )

        dummy_gather_all_run_ids = rail.EmptyOperator(
            task_id = "dummy_gather_all_run_ids"
        )

        gather_all_run_ids = rail.PythonOperator(
            task_id = "gather_all_run_ids",
            python_callable=lambda: list(itertools.chain(
                *list(filter(None, map(lambda x: rail.result(
                    f'reprocess_wbs_{x+1}'), range(10)))))),
        )

        gather_each_logs_for_missing_wbs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_each_logs_for_missing_wbs',
            dag_runs='{{ result("gather_all_run_ids") }}',
            dagrun_task_id='log_wbs_record_for_reprocessing',
            flatten=True
        )

        get_reprocess_log = rail.CreateLogOperator(
            task_id = "get_reprocess_log",
            tenant_wide_name=config.reprocess_wbs_log_name,
            existing_log_mode="append"
        )

        log_wbs_records_for_reprocessing = rail.WriteLogOperator(
            task_id = "log_wbs_records_for_reprocessing",
            log="{{result('get_reprocess_log')}}",
            items=lambda: rail.result(gather_each_logs_for_missing_wbs.task_id),
            message=lambda item : f"Logging WBS {item['properties']['wbs']} for reprocessing",
            severity="Reprocess",
            properties=lambda item: {
                **item['properties']
            }
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_reprocess_update_log >> filter_log >> has_any_data
        has_any_data >> rail.Label(
            'Yes') >> dummy_reprocess_wbs >> reprocess_wbs >> dummy_gather_all_run_ids
        has_any_data >> rail.Label('No') >> delete_this_dagrun

        dummy_gather_all_run_ids >> gather_all_run_ids >> gather_each_logs_for_missing_wbs >> get_reprocess_log >> log_wbs_records_for_reprocessing

    return dag


rail.for_each_instance(create_reprocess_dag)
