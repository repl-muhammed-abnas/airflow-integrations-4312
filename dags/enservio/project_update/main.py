
from datetime import timedelta
import uuid
import rail
from rail.lib.artifact import existing_artifact

def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"enservio_project_status_update_master_{config.instance}",
        description=f"Enservio project status update Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.master_max_active_run
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=15),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task="fail_bad_file_format",
        )

        fail_bad_file_format = rail.FailOperator(
            task_id = "fail_bad_file_format",
            message= "File format is not in CSV"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_csv = rail.LoadCSVFileOperator(
            task_id = "load_csv",
            document="{{ result('download_file') }}"
        )

        def get_email_address_from_file():
            with existing_artifact(rail.result('download_fromaddress_file'), mode= 'r') as artifact:
                data = artifact.file.read()
                return data

        download_fromaddress_file = rail.SFTPDownloadFileOperator(
            task_id='download_fromaddress_file',
            remote_filepath=config.recipient_address_filepath + "/{{ result('new_file_sensor') | file_name | replace('csv', 'txt') }}",
        )

        get_toemail_from_file = rail.PythonOperator(
            task_id = "get_toemail_from_file",
            python_callable= get_email_address_from_file
        )

        for_each_project = rail.ForEachOperator(
            task_id = "for_each_project",
            items= "{{ result('load_csv')}}",
            start_task= "update_project_status",
            end_task= "for_each_end"
        )

        update_project_status = rail.RepliconServiceOperator(
            task_id= "update_project_status",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda: {
                "target": {
                    "name": rail.result('for_each_project')['Project Name']
                },
                "modifications": {
                    "statusToApply": {
                        "name": "Completed"
                    }

                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            },
            retries = 1
        )

        is_update_failed = rail.IfOperator(
            task_id = 'is_update_failed',
            trigger_rule = "one_failed",
            test= "{{ get_task_state('update_project_status') | lower == 'failed' }}",
            yes_task= "log_update_failed",
            no_task="for_each_end"
        )

        log_update_failed = rail.WriteLogOperator(
            task_id = "log_update_failed",
            message= "update failed",
            severity= "Error",
            properties=lambda: {
                "Project Name": rail.result('for_each_project')['Project Name'],
                "details": """Failed - {{ result('update_project_status', 'error') }}"""
            }
        )

        for_each_end = rail.EmptyOperator(
            task_id = "for_each_end"
        )

        send_success_email = rail.EmailOperator(
            task_id = "send_success_email",
            to = '{{result("get_toemail_from_file")}}',
            html_content= "templates/email/completed_successfully_email.html",
            subject= "Request  to update project status in Enservio - Completed"
        )


        new_file_sensor >> is_csv >> rail.Label("No") >> fail_bad_file_format
        is_csv >> rail.Label("Yes") >> download_file >> load_csv
        download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        load_csv >> download_fromaddress_file >> get_toemail_from_file >> for_each_project >> update_project_status >> is_update_failed\
            >> rail.Label("No") >> for_each_end
        is_update_failed >> rail.Label("Yes") >> log_update_failed >> for_each_end
        for_each_project >> for_each_end >> send_success_email

    return dag

rail.for_each_instance(create_master_dag)
