from datetime import timedelta
from pendulum import datetime as dt
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_user_timecategory_sync_main_{config.instance}',
        description="Sync Users' Time Category from Deltek Vantagepoint to an S3 bucket",
        company_key=config.company_key,
        start_date=dt(2025, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_time_category_sync,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_users_from_vp',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_all_users_from_vp = rail.VantagepointAPIOperator(
            task_id='get_all_users_from_vp',
            endpoint='/employee',
            request_method='GET',
            filters='?fieldFilter=Employee,HomeCompany,TKGroup'
        )

        trigger_get_timecategory_for_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_get_timecategory_for_user',
            items="{{result('get_all_users_from_vp') | to_json}}",
            retries=0,
            trigger_dag_id=f"deltek_vantagepoint_user_timecategory_sync_child_{config.instance}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= lambda item: {
                'loginname': item['Employee'],
                'homecompany': item['HomeCompany'],
                'tkgroup': item['TKGroup'] or '<allgroup>',
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
            key_name=config.s3_upload_filepath + config.company_key + config.timecategory_file_name,
            bucket_name=config.bucket_name,
            aws_conn_id=config.aws_conn_id,
            replace=True
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        batch_task >> get_all_users_from_vp
        batch_task >> log_to_sumo
        get_all_users_from_vp >> trigger_get_timecategory_for_user >> wait_for_trigger_get_timecategory_for_user
        wait_for_trigger_get_timecategory_for_user >> gather_timecategories_for_users >> write_csv_file >> upload_file_to_s3 >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
