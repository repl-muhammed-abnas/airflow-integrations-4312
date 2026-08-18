from datetime import datetime
import os
from pytz import UTC
import shutil
import tempfile

import rail
from rail.lib.artifact import existing_artifact

import airflow
from airflow.models import DagModel, Variable
from airflow.models.log import Log
from airflow.exceptions import AirflowException
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.email import send_mime_email, build_mime_message
from system.activate_deactivate_dags.config import replicon_conn_id, activate_deactivate_automation_var_name, \
    FROM_EMAIL_ADDR, CC_EMAIL_ADDR, TO_EMAIL_ADDR, deactivate_all_dags_dag_id


with airflow.DAG(
    dag_id=deactivate_all_dags_dag_id,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['system_airflow_activate_deactivate_automation', 'system'],
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
    def get_all_active_dag_list_callable(session=NEW_SESSION):
        query_all_active_dags = session.query(
            DagModel.dag_id,
            DagModel.is_active,
            DagModel.is_paused,
            DagModel.owners,
            DagModel.fileloc
        ).filter(
            # Keeping the DagModel.is_active to get only those dags which are available in the UI
            #! DagModel.is_active may be removed in the future
            DagModel.is_active,
            # This DAG is required to be excluded from the list as it is part of the system test on FTD
            DagModel.dag_id.notin_(
                ["system_test"]
            )
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
                    "fileloc": item[4]
                }
            )
        artifact_name = rail.write_json_artifact(data)
        current_variable_value = Variable.get(
            key=activate_deactivate_automation_var_name, deserialize_json=True)
        # Not loading the data here as the artifact might have been deleted in the db_cleanup runs
        print("Current variable data:", current_variable_value)
        print(
            f"Updating the variable `{activate_deactivate_automation_var_name}`")
        Variable.set(key=activate_deactivate_automation_var_name, value={"dag_data": artifact_name, "timestamp": datetime.now(
            tz=UTC).isoformat()}, serialize_json=True, description="DO NOT EDIT OR MODIFY")

        # to avoid pausing the current dag
        rail.set_result(key="dag_id_list", val=[
                        dag_details['dag_id'] for dag_details in data if dag_details['dag_id'] != airflow_dag.dag_id])
        rail.set_result(
            key="reg_env", val=f"{os.environ.get('REGION', 'unknown')}-{os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')}")
        return artifact_name

    get_all_active_dag_list = rail.PythonOperator(
        task_id="get_all_active_dag_list",
        python_callable=get_all_active_dag_list_callable
    )

    @provide_session
    def disable_active_dags_callable(session=NEW_SESSION):
        list_of_dag_ids = rail.result(
            get_all_active_dag_list.task_id, 'dag_id_list')
        print("Processing of disabling dags started......")
        _count = session.query(DagModel).filter(DagModel.dag_id.in_(
            list_of_dag_ids)).update({DagModel.is_paused: True})
        print("Processing of disabling dags Completed.....")
        if _count != len(list_of_dag_ids):
            raise AirflowException(
                "Something went wrong.... Active dag count doesn't match with the disabled dag count")
        # return the total number of dags disabled (from active to inactive)
        return _count

    disable_active_dags = rail.PythonOperator(
        task_id="disable_active_dags",
        python_callable=disable_active_dags_callable
    )

    prepare_csv_file = rail.WriteCSVFileOperator(
        task_id="prepare_csv_file",
        source=lambda: rail.load_json_artifact(
            rail.result("get_all_active_dag_list")),
        header=['DagID', 'IsActive', 'IsPaused', 'Owners',
                'FileLocation', 'Comments', 'PausedByIntegration'],
        row=[
            "{{item.dag_id}}",
            "{{item.is_active}}",
            "{{item.is_paused}}",
            "{{item.owners}}",
            "{{item.fileloc}}",
            "IsPaused is the actual state of the integration. False: Dag is active. True: Dag is Inactive",
            "Yes"
        ]
    )

    render_email_template_success = rail.RenderTemplateOperator(
        task_id="render_email_template_success",
        template_file="emails/deactivate_email_body.html",
        target='result'
    )

    def send_standard_response_callable(from_email_addr, to_email_addr, cc_email_addr, region):
        subject = f"""Airflow-alert | Deactivation of dags is completed for region {region} at {datetime.now(tz=UTC).isoformat()}"""
        _files = [(f'deactivated_dags_integration_data_{region}.csv', rail.result(
            prepare_csv_file.task_id))]

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

    get_all_active_dag_list >> disable_active_dags >> prepare_csv_file >> render_email_template_success >> send_email
