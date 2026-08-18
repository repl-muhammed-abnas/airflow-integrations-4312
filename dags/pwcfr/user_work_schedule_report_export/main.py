from pendulum import datetime, now
from pwcfr.user_work_schedule_report_export.tasks.report_export import create_user_report_collections
import rail

# pylint: disable=too-many-statements


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcfr_user_work_schedule_report_export_master_{config.instance}",
        description="pwcfr user work schedule report",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 6, 16, tz=config.cest_time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
                    "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        scheduled_users_collection = create_user_report_collections(
            config, config.scheduled_users_report_name, config.scheduled_users_suffix)
        all_users_collection = create_user_report_collections(
            config, config.all_users_report_name, config.all_users_suffix)

        query_for_non_scheduled_users = rail.QueryCollectionOperator(
            task_id="query_for_non_scheduled_users",
            query="""SELECT * FROM user_report_collection_all_users WHERE loginname NOT IN
              (SELECT loginname FROM user_report_collection_scheduled_user)"""
        )

        create_non_scheduled_users_collection = rail.CreateCollectionOperator(
            task_id="create_non_scheduled_users_collection",
            source='{{result("query_for_non_scheduled_users")}}',
            name="non_scheduled_users_collection",
            columns=["employeeid", "schedulename", "loginname"]
        )

        query_combine_scheduled_and_non_scheduled_users = rail.QueryCollectionOperator(
            task_id="query_combine_scheduled_and_non_scheduled_users",
            query="""SELECT * FROM user_report_collection_scheduled_user UNION
                    SELECT * FROM non_scheduled_users_collection"""
        )

        if_combination_has_data = rail.IfOperator(
            task_id="if_combination_has_data",
            test='{{result("query_combine_scheduled_and_non_scheduled_users")| length > 0}}',
            yes_task="write_user_report_to_csv",
            no_task="send_no_user_data_email"
        )

        write_user_report_to_csv = rail.WriteCSVFileOperator(
            task_id="write_user_report_to_csv",
            source='{{result("query_combine_scheduled_and_non_scheduled_users")}}',
            header=["Employee ID", "Schedule Name", "Login Name"],
            row=lambda item:[
                item["employeeid"],
                item["schedulename"] if item["schedulename"] else '""',
                item["loginname"]
            ]
        )

        send_no_user_data_email = rail.EmailOperator(
            task_id="send_no_user_data_email",
            to=config.tenant_email,
            subject="{{get_company_key()}}| 2_monitoring_USER_work_schedule _Master | No data found in the report",
            html_content="<p>No data found in the report</p>"
        )

        csv_data_update = rail.PythonOperator(
            task_id="csv_data_update",
            python_callable=lambda: rail.write_artifact(rail.read_artifact(
                rail.result("write_user_report_to_csv")).replace('""""""', '""'))
        )

        upload_user_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_user_export_to_sftp",
            sftp_conn_id=config.sftp_conn_id,
            content='{{result("csv_data_update")}}',
            remote_filepath=config.sftp_export_file_path + config.user_export_name +
            now(tz=config.cest_time_zone).strftime("%d%m%Y") + ".csv"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        scheduled_users_collection >> query_combine_scheduled_and_non_scheduled_users
        all_users_collection >> query_for_non_scheduled_users >> \
            create_non_scheduled_users_collection >> \
            query_combine_scheduled_and_non_scheduled_users >> \
            if_combination_has_data >> rail.Label("Yes") >> write_user_report_to_csv >> csv_data_update >>\
            upload_user_export_to_sftp >> log_to_sumo >> can_fail_dag >> fail_dagrun
        if_combination_has_data >> rail.Label("No") >> send_no_user_data_email >> \
            log_to_sumo >> can_fail_dag >> fail_dagrun
        return dag


rail.for_each_instance(create_main_airflow_dag)
