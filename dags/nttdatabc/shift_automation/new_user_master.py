
from datetime import timedelta
from nttdatabc.shift_automation.tasks.send_logs import get_send_logs
from pendulum import datetime as dt
from dateutil.relativedelta import relativedelta
import pendulum
import rail
from airflow.models import Variable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'nttdatabc_new_user_default_shift_assignment_master_{config.instance}',
        description=f'NTTData BC New user default shift assignment master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2022, 12, 7, tz=config.time_zone),
        schedule_interval=config.new_user_schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        get_bucket_name = rail.PythonOperator(
            task_id='get_bucket_name',
            python_callable = lambda: Variable.get(
                config.aws_s3_bucket, default_var='replicon-airflow-dev-group')
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.shift_schedule_report_name,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_enabled_user_schedule_data',
            no_task= 'finish'
        )

        load_enabled_user_schedule_data = rail.LoadCSVFileOperator(
            task_id='load_enabled_user_schedule_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_enabled_user_schedule_data = rail.CreateCollectionOperator(
            task_id='create_enabled_user_schedule_data',
            source = "{{ result('load_enabled_user_schedule_data') }}",
            name = "enabled_user_schedule_data",
            columns = {
                'User Name':'username',
                'Login Name':'loginname',
                'useruri':'useruri',
                'Schedule Name (Current)':'schedulename',
                'Country':'country',
                'User Status':'status',
                'User Start Date':'userstartdate'
            }
        )

        download_file_from_s3 = rail.S3DownloadFileOperator(
            task_id='download_file_from_s3',
            bucket_name='{{ result("get_bucket_name") }}',
            key_name=config.output_reference_file_path,
            aws_conn_id=config.aws_conn_id
        )

        load_reference_file_data = rail.LoadCSVFileOperator(
            task_id='load_reference_file_data',
            document="{{ result('download_file_from_s3') }}",
        )

        create_reference_file_data = rail.CreateCollectionOperator(
            task_id='create_reference_file_data',
            source = "{{ result('load_reference_file_data') }}",
            name = "reference_file_data",
            columns = {
                'User Name':'username',
                'Login Name':'loginname',
                'useruri':'useruri',
                'Schedule Name (Current)':'schedulename',
                'Country':'country',
                'User Status':'status',
                'User Start Date':'userstartdate'
            }
        )

        query_get_all_users_with_shift_schedules = rail.QueryCollectionOperator(
            task_id='query_get_all_users_with_shift_schedules',
            query="""SELECT * FROM enabled_user_schedule_data WHERE loginname NOT IN (SELECT DISTINCT \
                        loginname FROM reference_file_data) AND schedulename='Shift Schedule'""",
        )

        start_date = (pendulum.now()+relativedelta(day=1)).date()
        end_date = (pendulum.now()+relativedelta(months=+13, day=31)).date()

        is_users_with_shift_schedules_exists = rail.IfOperator(
            task_id='is_users_with_shift_schedules_exists',
            test='{{ result("query_get_all_users_with_shift_schedules", "length") > 0 }}',
            yes_task='trigger_shift_assignment_per_user'
        )

        trigger_shift_assignment_per_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_shift_assignment_per_user',
            retries=0,
            items="{{ result('query_get_all_users_with_shift_schedules') }}",
            trigger_dag_id=f'nttdatabc_default_shift_assignment_per_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item : {
                "useruri": item["useruri"],
                "startdate": str(start_date),
                "enddate": str(end_date),
                "country": item["country"],
                "startdateday": start_date.strftime("%d"),
                "startdatemonth": start_date.strftime("%m"),
                "startdateyear": start_date.strftime("%Y"),
                "enddateday": end_date.strftime("%d"),
                "enddatemonth": end_date.strftime("%m"),
                "enddateyear": end_date.strftime("%Y"),
                "username": item["username"],
                "loginname": item["loginname"],
                "usertype": "New User"
            }
        )

        wait_for_shift_assignments = rail.WaitForDagRunsSensor(
            task_id='wait_for_shift_assignments',
            dag_runs='{{ result("trigger_shift_assignment_per_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=3,
        )

        upload_file_to_s3_archive = rail.S3UploadFileOperator(
            task_id='upload_file_to_s3_archive',
            source='{{ result("load_reference_file_data") }}',
            key_name=config.archive_file_path + "/{{ dag_run_ecid() | replace(':', '-') }}_{{ current_time('%d%m%Y%H%M%S') }}_oldreference.csv",
            bucket_name='{{ result("get_bucket_name") }}',
            aws_conn_id=config.aws_conn_id,
            replace=True
        )

        upload_file_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_file_to_s3',
            source='{{ result("load_enabled_user_schedule_data") }}',
            key_name=config.output_reference_file_path,
            bucket_name='{{ result("get_bucket_name") }}',
            aws_conn_id=config.aws_conn_id,
            replace=True
        )

        send_logs_enter,_ = get_send_logs(config)

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_bucket_name >> get_report_details >> run_my_report_entry
        run_my_report_exit >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> load_enabled_user_schedule_data >> create_enabled_user_schedule_data \
            >> download_file_from_s3 >> load_reference_file_data >> create_reference_file_data \
                >> query_get_all_users_with_shift_schedules >> is_users_with_shift_schedules_exists
        is_users_with_shift_schedules_exists >> rail.Label("Yes") >> trigger_shift_assignment_per_user >> wait_for_shift_assignments \
                    >> upload_file_to_s3_archive >> upload_file_to_s3 >> send_logs_enter

        report_has_data >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_dag)
