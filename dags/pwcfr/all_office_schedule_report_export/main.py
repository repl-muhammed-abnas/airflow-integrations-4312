import json
import rail
from pendulum import datetime

def create_main_airflow_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f"pwcfr_all_office_schedule_report_export_{config.instance}",
        description="pwcfr all office schedule report export",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        max_active_runs = config.max_active_runs,
        start_date=datetime(2023,6,26, tz=config.cest_time_zone),
        default_args={
            "sftp_conn_id":config.sftp_conn_id
        }
    )as dag:

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            response_filter=lambda response:list(map(lambda uri:uri["uri"],response.json()["d"]))
        )

        get_details_of_each_office_schedule = rail.RepliconServiceOperator(
            task_id="get_details_of_each_office_schedule",
            endpoint="/services/OfficeScheduleService1.svc/BulkGetOfficeScheduleDetails",
            data=lambda :{"officeScheduleUris": rail.result("get_all_office_schedules")},
            data_handler=lambda response:'\n'.join(list(map(lambda res:json.dumps({"d":res["officeSchedule"]}).replace(": ", ":").replace(", ",","), response)))
        )

        upload_schedule_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_schedule_export_to_sftp",
            sftp_conn_id=config.sftp_conn_id,
            content="{{result('get_details_of_each_office_schedule')}}",
            remote_filepath=config.office_schedule_export_sftp_path+config.all_office_schedule_export_filename
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id = "can_fail_dag",
            test="{{get_error_message()|is_truthy}}",
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message="{{get_error_message()}}"
        )
        get_all_office_schedules >> get_details_of_each_office_schedule >> upload_schedule_export_to_sftp >> \
        log_to_sumo >> can_fail_dag >> fail_dagrun

        return dag

rail.for_each_instance(create_main_airflow_dag)
