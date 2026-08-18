import json
import uuid
from datetime import timedelta
from wipro.efforts_submit.custom_http_operator.CustomSimpleHttpOperator import CustomSimpleHttpOperator
import rail
null = None
dag_created = []


def create_airflow_child_dag(config):
    for country in config.time_export_for_country:
        cnt=str(country).replace(" ","_")
        with rail.create_airflow_dag(
            dag_id=f"{config.submit_data_child}_{cnt}_{config.instance}_v2",
            description=f"efforts submit to wipro child {country} {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.submit_time_child_max_active_runs,
            default_args={
                'retry_delay': timedelta(seconds=120)
            }
        ) as dag:
            rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

            if_instance_trial = rail.IfOperator(
                task_id='if_instance_trial',
                test=lambda: bool(config.instance == "trial"),
                yes_task="upload_log_to_sftp",
                no_task="efforts_submission_to_wipro",
            )

            efforts_submission_to_wipro = CustomSimpleHttpOperator(
                task_id="efforts_submission_to_wipro",
                http_conn_id="wipro_http_effort_submit",
                endpoint="h2r/my-time/1.0.0/time-sheet",
                method="POST",
                auth_type=None,
                headers={
                    "Authorization": "Bearer " + '{{var.value.wipro_efforts_submission_bearer_token_variable_'+config.instance+'}}',
                    'Content-Type': 'text/plain; charset=utf-8',
                    "sourceSystemId": "REPLICON",
                },
                data=lambda dag_run:json.dumps(dag_run.conf["data"]).encode("utf-8")
            )

            upload_log_to_sftp = rail.SFTPUploadFileOperator(
                task_id='upload_log_to_sftp',
                content="{{ dag_run.conf.data}}",
                sftp_conn_id=config.sftp_conn_id,
                remote_filepath=config.log_filepath + '/Log_{{ dag_run.conf.data.d.Pernr if dag_run.conf.data | is_truthy else "empty" }}_' + str(uuid.uuid4()) + '.json',
            )

            if_instance_trial >> rail.Label(
                "Yes") >> upload_log_to_sftp
            if_instance_trial >> rail.Label(
                "No") >> efforts_submission_to_wipro

        dag_created.append(dag)
    return dag_created


rail.for_each_instance(create_airflow_child_dag)
