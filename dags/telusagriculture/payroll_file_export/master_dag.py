
from datetime import timedelta
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'telusagriculture_payroll_file_export_{config.instance}',
        description=f'TELUSagriculture Payroll File Export {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval = timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task = 'archive_to_secondary_sftp',
            no_task = 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        archive_to_secondary_sftp = rail.SFTPMoveFileOperator(
            task_id='archive_to_secondary_sftp',
            new_filename=config.archive_filepath+'/{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
            existing_filename=config.input_filepath+'/{{ result("new_file_sensor") | file_base }}.csv',
        )

        filename = config.output_filepath + "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_base }}.csv"

        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_sftp',
            content="{{ result('download_file') }}",
            sftp_conn_id=config.secondary_sftp_conn_id,
            remote_filepath=filename,
        )
        new_file_sensor >> download_file >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_to_secondary_sftp >> upload_to_sftp
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return dag

rail.for_each_instance(create_dag)
