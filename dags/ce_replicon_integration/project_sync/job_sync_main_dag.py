from datetime import datetime, timedelta
import rail
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
null = None

def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.job_main_dag_id,
        description=f'{config.company_key} Computerease To Replicon Job Sync MAIN DAG',
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        multi_tenant=True
    ) as dag:
        
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='should_log_history',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_job_last_sync_time():
            company_key = rail.get_current_context()['dag_run'].conf['company_key']
            return get_lastsync_time_variable(
                variable_name=f'{config.job_last_sync_time_var}_{company_key}',
                date_format=config.ce_time_format,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False
            )

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=get_job_last_sync_time
        )

        fetch_computerease_jobs = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_jobs',
            endpoint='/catalog/job',
            request_method='GET',
            query_params={
                'gt~updated_at': '{{ result("get_last_sync_time")["last_synctime"] }}'
            },
            computerease_conn_id='{{ dag_run.conf.computerease_conn_id }}',
        )

        def set_job_last_sync_time():
            company_key = rail.get_current_context()['dag_run'].conf['company_key']
            return set_lastsync_time_variable(
                variable_name=f'{config.job_last_sync_time_var}_{company_key}',
                value_to_set=rail.result('get_last_sync_time')['current_time']
            )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=set_job_last_sync_time
        )

        has_jobs_to_sync = rail.IfOperator(
            task_id='has_jobs_to_sync',
            test='{{ result("fetch_computerease_jobs").data | length > 0 }}',
            yes_task='trigger_job_sync_child_dag',
            no_task='should_log_history'
        )

        def get_date_object(datestring):
            if datestring:
                date_obj = datetime.strptime(datestring, "%Y-%m-%d")
                return {
                    'day': date_obj.day,
                    'month': date_obj.month,
                    'year': date_obj.year
                }
            return null

        def parse_computerease_jobs(data):
            jobs = []
            for job in data:
                job_status = job.get('status','')
                job_status = "Completed" if job_status == "closed" else "Tentative" if job_status == "inactive" else "In Progress"
                jobs.append({                 
                    'code': job.get('code', ''),
                    'description': job.get('description', '') if job.get('description', '') else job.get('code', ''),
                    'status': job_status,
                    'first_payroll_date': job.get('first_payroll_date', ''),
                    'approval_team_uuid': job.get('approval_team_uuid', ''),
                    'jobdate_open': get_date_object(job.get('jobdate_open', '')),
                    'jobdate_due': get_date_object(job.get('jobdate_due', '')),
                    'company_uuid': job.get('company_uuid', ''),
                    'wbs_type': job.get('wbs_type', '')
                })
            return jobs
        
        trigger_job_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_job_sync_child_dag',
            items=lambda: parse_computerease_jobs(
                rail.result('fetch_computerease_jobs')['data']),
            trigger_dag_id=config.job_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run, item: {
                'job_data': item,
                'computerease_conn_id': dag_run.conf['computerease_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id']
            }
        )

        wait_for_job_sync_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_job_sync_completion',
            dag_runs='{{ result("trigger_job_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_job_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_job_errors',
            dag_runs="{{ result('trigger_job_sync_child_dag') }}",
            dagrun_task_id='catch_job_error',
            flatten=True
        )

        is_job_error = rail.IfOperator(
            task_id='is_job_error',
            test="{{ (get_task_state('gather_job_errors') == 'success' and result('gather_job_errors') | length > 0) }}",
            yes_task='fail_job_error',
            no_task='should_log_history'
        )

        fail_job_error = rail.FailOperator(
            task_id='fail_job_error',
            message="{{ result('gather_job_errors') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('has_jobs_to_sync') == 'success' and \
                    result('has_jobs_to_sync') != 'trigger_job_sync_child_dag') }}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name=config.provider,
            integration_type=config.workflow
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        batch_task >> get_last_sync_time >> fetch_computerease_jobs >> set_last_sync_time
        batch_task >> should_log_history

        set_last_sync_time >> has_jobs_to_sync

        has_jobs_to_sync >> rail.Label(
            'Yes') >> trigger_job_sync_child_dag >> wait_for_job_sync_completion >> gather_job_errors >> is_job_error
        has_jobs_to_sync >> rail.Label('No') >> should_log_history

        is_job_error >> rail.Label('Yes') >> fail_job_error >> should_log_history
        is_job_error >> rail.Label('No') >> should_log_history

        should_log_history >> rail.Label('Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label('No') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_dag_instance)
