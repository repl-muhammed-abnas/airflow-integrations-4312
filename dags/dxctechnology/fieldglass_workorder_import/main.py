from datetime import timedelta
from pendulum import datetime
import rail


def create_airflow_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description="dxctechnology fieldglass c1,compass workorder import",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 10, 16),
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_max_active_run,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.sftp_import_filepath,
            soft_fail_timeout=timedelta(minutes=config.sftp_time_out)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            test='{{get_task_state("new_file_sensor") == "success"}}',
            yes_task="if_c1_work_order",
            no_task="delete_dagrun"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        if_c1_work_order = rail.IfOperator(
            task_id="if_c1_work_order",
            test='{{result("new_file_sensor")|file_name | starts_with("C1_WORK_ORDER") | is_truthy}}',
            yes_task="process_c1_workorder_import",
            no_task="if_compass_workorder_import"
        )

        process_c1_workorder_import = rail.TriggerDagRunOperator(
            task_id="process_c1_workorder_import",
            trigger_dag_id=config.c1_file_to_replicon_dag_id,
            conf={
                "file_path": '{{result("new_file_sensor")}}'
            },
            wait_for_completion=True
        )

        if_compass_workorder_import = rail.IfOperator(
            task_id="if_compass_workorder_import",
            test='{{result("new_file_sensor")|file_name | starts_with("COMPASS_WORK_ORDER") | is_truthy}}',
            yes_task="process_compass_workorder_import",
            no_task="if_gsap_workorder_import"
        )

        process_compass_workorder_import = rail.TriggerDagRunOperator(
            task_id="process_compass_workorder_import",
            trigger_dag_id=config.compass_file_to_replicon_dag_id,
            conf={
                "file_path": '{{result("new_file_sensor")}}'
            },
            wait_for_completion=True
        )

        if_gsap_workorder_import = rail.IfOperator(
            task_id="if_gsap_workorder_import",
            test='{{result("new_file_sensor")|file_name | starts_with("GSAP_WORK_ORDER") | is_truthy}}',
            yes_task="process_gsap_workorder_import",
            no_task="archive_import_file"
        )

        process_gsap_workorder_import = rail.TriggerDagRunOperator(
            task_id="process_gsap_workorder_import",
            trigger_dag_id=config.gsap_file_to_replicon_dag_id,
            conf={
                "file_path": '{{result("new_file_sensor")}}'
            }
            ,
            wait_for_completion=True
        )

        archive_import_file = rail.SFTPMoveFileOperator(
            task_id="archive_import_file",
            new_filename=config.sftp_archive_filepath +
            '{{dag_run_ecid()}}_' + '{{result("new_file_sensor")|file_name}}',
            existing_filename='{{result("new_file_sensor")|file_name}}'
        )

        send_invalid_file_mail = rail.EmailOperator(
            task_id="send_invalid_file_mail",
            to=config.tenant_mail,
            bcc=config.alert_mail,
            subject="{{get_company_key()}}"+" | C1/COMPASS/GSAP Work order data sync to Replicon - Invalid file name -  " +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/invalid_file.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test="{{get_error_message()|is_truthy}}"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        new_file_sensor >>\
        was_new_file_found >> rail.Label("Yes") >>\
            if_c1_work_order >> rail.Label(
                "Yes") >> process_c1_workorder_import >> log_to_sumo
        if_c1_work_order >> rail.Label(
            "No") >> \
            if_compass_workorder_import >> rail.Label("Yes") >>\
            process_compass_workorder_import >> log_to_sumo
        if_compass_workorder_import >> rail.Label("No") >>\
            if_gsap_workorder_import >> rail.Label("Yes") >>\
            process_gsap_workorder_import >> log_to_sumo
        if_gsap_workorder_import >> rail.Label(
            "No") >> archive_import_file >> send_invalid_file_mail >> log_to_sumo
        log_to_sumo >> can_fail_dag >> fail_dagrun
        was_new_file_found >> rail.Label("No") >> delete_dagrun
        return dag


rail.for_each_instance(create_airflow_master_dag)
