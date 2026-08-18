import json
import rail
from datetime import datetime as dt
from airflow.models import Variable
from wipro.efforts_submit.custom_http_operator.CustomSimpleHttpOperator import CustomSimpleHttpOperator

null = None


def create_airflow_master(config):
    with rail.create_airflow_dag(
        dag_id=config.shift_assignment_export_child,
        description="Shift assignment export child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="process_shift_data"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="process_shift_data",
            end_task="finish_task"
        )

        process_shift_data = rail.PythonOperator(
            task_id="process_shift_data",
            python_callable=lambda dag_run:
                json.dumps({
                    "d": {
                        "PERNR": "",
                        "KEY_DATE": "",
                        "MESSAGE": "",
                        "PROJECTID": "",
                        "LOCATION": "",
                        "nav_shift_detail": list(map(lambda i: {
                            "Pernr": i["employee_id"],
                            "Shift_time": str(i["shift_start_time"]) + "-" + str(i["shift_end_time"]),
                            "Begda": dt.strptime(i["entry_date"], "%Y/%m/%d").strftime("%d.%m.%Y"),
                            "WBS_Element": "",
                            "Shift_type": i["shift_type"],
                            "Location": i["shift_location"],
                            "Markholiday": "",
                            "DWS": i["shift_dws"]
                        }, dag_run.conf["items"]))
                    }
                }, ensure_ascii=False)
        )

        is_trial_instance = rail.IfOperator(
            task_id="is_trial_instance",
            test=lambda: config.instance == "trial",
            yes_task="upload_data_to_sftp",
            no_task="submit_shift_data_to_wipro"
        )

        upload_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_data_to_sftp",
            remote_filepath=config.shift_data_upload_path +
            "/Shift_Export_{{dag_run_ecid()|replace(':','_')}}.json",
            content='{{result("process_shift_data")}}'
        )

        submit_shift_data_to_wipro = CustomSimpleHttpOperator(
            task_id="submit_shift_data_to_wipro",
            http_conn_id="wipro_http_effort_submit",
            endpoint="h2r/my-time/1.0.0/assign-shift",
            method="POST",
            auth_type=None,
            headers={
                "Authorization": "Bearer " + '{{var.value.wipro_efforts_submission_bearer_token_variable_'+config.instance+'}}',
                'Content-Type': 'application/json',
                "sourceSystemId": "REPLICON",
            },
            data=lambda: rail.result("process_shift_data")
        )

        finish_task = rail.EmptyOperator(task_id="finish_task")

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish_task
        can_run_batch_task >> rail.Label("No") >>\
            process_shift_data >> is_trial_instance >> rail.Label("Yes") >>\
            upload_data_to_sftp >> finish_task
        is_trial_instance >> rail.Label(
            "No") >> submit_shift_data_to_wipro >> finish_task

        return dag


rail.for_each_instance(create_airflow_master)
