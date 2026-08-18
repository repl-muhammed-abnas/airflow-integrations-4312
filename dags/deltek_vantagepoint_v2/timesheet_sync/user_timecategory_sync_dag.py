from datetime import timedelta, datetime
from pendulum import datetime as dt
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timecategory_sync_dag_id,
        description=f"{config.company_key} Sync Users' Time Category from Deltek Vantagepoint to an S3 bucket",
        company_key=config.company_key,
        start_date=dt(2025, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_users_from_vp',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_all_users_from_vp = rail.VantagepointAPIOperator(
            task_id='get_all_users_from_vp',
            endpoint='/employee',
            request_method='GET',
            filters='?fieldFilter=Employee,HomeCompany,TKGroup',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}'
        )

        trigger_get_timecategory_for_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_get_timecategory_for_user',
            items="{{result('get_all_users_from_vp') | to_json}}",
            retries=0,
            trigger_dag_id=config.timecategory_sync_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= lambda dag_run, item: {
                'loginname': item['Employee'],
                'homecompany': item['HomeCompany'],
                'tkgroup': item['TKGroup'] or '<allgroup>',
                'company_key': dag_run.conf['company_key'],
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id']
            }
        )

        wait_for_trigger_get_timecategory_for_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_get_timecategory_for_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_get_timecategory_for_user") }}'
        )

        gather_timecategories_for_users = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timecategories_for_users',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_get_timecategory_for_user') }}",
            dagrun_task_id='add_time_category_by_employee'
        )

        write_csv_file = rail.WriteCSVFileOperator(
            task_id = 'write_csv_file',
            source="{{result('gather_timecategories_for_users') | to_json}}",
            header=['loginname', 'timecategory'],
            row=lambda item: [item['loginname'], item['timecategory']]
        )

        upload_file_to_s3 = rail.S3UploadFileOperator(
            task_id = 'upload_file_to_s3',
            source="{{result('write_csv_file')}}",
            key_name=config.s3_upload_filepath + '{{ dag_run.conf.company_key }}' + config.timecategory_file_name,
            bucket_name=config.bucket_name,
            aws_conn_id=config.aws_conn_id,
            replace=True
        )

        update_last_run = rail.PythonOperator(
            task_id='update_last_run',
            python_callable=lambda dag_run: Variable.set(
                f'{config.timecategory_sync_last_run_var}_{dag_run.conf["company_key"]}',
                datetime.now().isoformat()
            ) or {}
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in user timecategory sync - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        batch_task >> get_all_users_from_vp
        batch_task >> catch_error
        get_all_users_from_vp >> trigger_get_timecategory_for_user >> wait_for_trigger_get_timecategory_for_user
        wait_for_trigger_get_timecategory_for_user >> gather_timecategories_for_users >> write_csv_file >> upload_file_to_s3 >> update_last_run >> catch_error

        return dag


rail.for_each_instance(create_dag)
