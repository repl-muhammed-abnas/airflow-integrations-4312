from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from pendulum import datetime as pendulum_datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_archive_workday_logs_post_2_days_master_{config.instance}",
        description="DXC Archive Workday Logs post 2 days",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=pendulum_datetime(2023, 9, 26),
        schedule_interval=config.schedule_interval,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        },
        max_active_runs=config.max_active_run_master
    ) as dag:

        list_dir = rail.SFTPListFilesOperator(
            task_id = "list_dir",
            paths=[
                config.log_path
            ]
        )

        has_any_files = rail.IfOperator(
            task_id = "has_any_files",
            test=lambda : bool(rail.result('list_dir').get(config.log_path)),
            yes_task="get_files_to_archive"
        )

        def get_files_to_archive_callable():
            files = rail.result("list_dir").get(config.log_path)
            archive_date = (datetime.now(tz=timezone.utc) - timedelta(days=3) ).timestamp()
            file_to_archive = []
            for file in files:
                if parse(file['modify']).timestamp() < archive_date:
                    file_to_archive.append(file)
            return file_to_archive

        get_files_to_archive = rail.PythonOperator(
            task_id = "get_files_to_archive",
            python_callable=get_files_to_archive_callable
        )

        for_each_file = rail.ForEachOperator(
            task_id = "for_each_file",
            items=lambda: rail.result("get_files_to_archive"),
            start_task="archive_file",
            end_task="for_each_end"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id = "archive_file",
            existing_filename=config.log_path + "/{{result('for_each_file').name}}",
            new_filename=config.log_archive_path + "/{{result('for_each_file').name}}"
        )


        for_each_end = rail.EmptyOperator(
            task_id="for_each_end"
        )

        list_dir >> has_any_files >> get_files_to_archive >> for_each_file >> for_each_end
        for_each_file >> archive_file >> for_each_end

    return dag

rail.for_each_instance(create_main_dag)
