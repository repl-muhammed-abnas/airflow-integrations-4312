from datetime import datetime
import os
from pytz import UTC
import shutil
import tempfile

import rail
from rail.lib.artifact import existing_artifact

import airflow
from airflow.models import DagModel
from airflow.exceptions import AirflowException
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.email import send_mime_email, build_mime_message
from system.get_dags_and_max_active_runs.config import replicon_conn_id, \
    FROM_EMAIL_ADDR, CC_EMAIL_ADDR, TO_EMAIL_ADDR, main_dag_id


with airflow.DAG(
    dag_id=main_dag_id,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['system_airflow_get_dag_and_max_active_runs_automation', 'system'],
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
        'replicon_conn_id': replicon_conn_id
    },
    default_view="graph",
    max_active_runs=1
) as airflow_dag:

    '''
    ToDo: To add a additional logic to get the owner who triggered the run.
    Currently we can identify it by going to `Dag Audit Log` and check `owner` for event `trigger`
    '''
    @provide_session
    def get_all_active_dag_list_and_max_active_runs_callable(session=NEW_SESSION):
        query_all_active_dags = session.query(
            DagModel.dag_id,
            DagModel.is_active,
            DagModel.is_paused,
            DagModel.owners,
            DagModel.fileloc,
            DagModel.max_active_runs
        ).filter(
            # Keeping the DagModel.is_active to get only those dags which are available in the UI
            #! DagModel.is_active may be removed in the future
            DagModel.is_active
        ).filter(DagModel.is_paused == False)

        query_all_active_dags_data = query_all_active_dags.all()
        if not query_all_active_dags_data:
            raise AirflowException("No active dags found in the environment")

        data = []
        for item in query_all_active_dags_data:
            data.append(
                {
                    "dag_id": item[0],
                    "is_active": item[1],
                    "is_paused": item[2],
                    "owners": item[3],
                    "fileloc": item[4],
                    "max_active_runs": item[5]
                }
            )

        rail.set_result(
            key="reg_env", val=f"{os.environ.get('REGION', 'unknown')}-{os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')}")
        return rail.write_json_artifact(data)

    get_all_active_dag_list_and_max_active_runs = rail.PythonOperator(
        task_id="get_all_active_dag_list_and_max_active_runs",
        python_callable=get_all_active_dag_list_and_max_active_runs_callable
    )

    prepare_csv_file = rail.WriteCSVFileOperator(
        task_id="prepare_csv_file",
        source=lambda: rail.load_json_artifact(rail.result(
            "get_all_active_dag_list_and_max_active_runs")),
        header=['DagID', 'IsActive', 'IsPaused', 'Owners',
                'FileLocation', 'Comments', 'MaxActiveRuns'],
        row=[
            "{{item.dag_id}}",
            "{{item.is_active}}",
            "{{item.is_paused}}",
            "{{item.owners}}",
            "{{item.fileloc}}",
            "IsPaused is the actual state of the integration. False: Dag is active. True: Dag is Inactive",
            "{{item.max_active_runs}}"
        ]
    )

    render_email_template_success = rail.RenderTemplateOperator(
        task_id="render_email_template_success",
        template_file="emails/email_body.html",
        target='result'
    )

    def send_standard_response_callable(from_email_addr, to_email_addr, cc_email_addr, region):
        subject = f"""Airflow-alert | Run is completed for getting dags and their max active runs for region {region} at {datetime.now(tz=UTC).isoformat()}"""
        _files = [(f'Dag_data_{region}.csv',
                   rail.result(prepare_csv_file.task_id))]

        to_attach = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            def copy_to_staging_dir(source, friendly_name):
                attachment_name = os.path.join(tmp_dir, friendly_name)
                shutil.copyfile(source, attachment_name)
                to_attach.append(attachment_name)

            for file in _files:
                with existing_artifact(file[1]) as artifact:
                    copy_to_staging_dir(artifact.local_filename, file[0])

            msg, recipients = build_mime_message(
                mail_from=from_email_addr,
                to=to_email_addr,
                cc=cc_email_addr,
                subject=subject,
                html_content=rail.result("render_email_template_success"),
                files=to_attach
            )

            send_mime_email(e_from=from_email_addr,
                            e_to=recipients, mime_msg=msg)
        return subject

    send_email = rail.PythonOperator(
        task_id="send_email",
        python_callable=send_standard_response_callable,
        op_args=[
            FROM_EMAIL_ADDR,
            TO_EMAIL_ADDR,
            CC_EMAIL_ADDR,
            f"{os.environ.get('REGION', 'unknown')}-{os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')}"
        ]
    )

    get_all_active_dag_list_and_max_active_runs >> prepare_csv_file >> render_email_template_success >> send_email
