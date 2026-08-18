import json
import rail
from odessa.project_team_update_v2.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"odessa_jira_import_child_update_custom_field_v2_{config.instance}",
        description=f"odessa jira import child update custom field V2 {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        update_issue_in_jira = rail.SimpleHttpOperator(
            task_id='update_issue_in_jira',
            method='PUT',
            endpoint='rest/api/2/issue/{{ dag_run.conf.issuekey }}',
            http_conn_id='odessa_jira',
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            data=json.dumps(request_payload.get_data(config)),
            dag=dag,
        )

        send_error_email = rail.EmailOperator(
            task_id='send_error_email',
            to=config.error_email,
            bcc=config.internal_logs_email,
            trigger_rule= 'one_failed',
            subject='{{ get_company_key()}} | Error while updating the custom field on Jira Account',
            html_content="templates/error_email.html"
        )

        update_issue_in_jira >> rail.Label(
            "on_error") >> send_error_email

    return dag


rail.for_each_instance(create_child_dag)
