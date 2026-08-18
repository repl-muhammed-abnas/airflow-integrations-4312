from datetime import datetime
import json
import rail
from hostopia.webhook_for_jira_deletion.utils import custom_method
from hostopia.webhook_for_jira_deletion.tasks.issue_deleted import issue_deleted
from hostopia.webhook_for_jira_deletion.tasks.subtask_deleted import subtask_deleted


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'hostopia_webhooks_jira_issue_deleted_{config.instance}',
        description='Hostopia webhooks jira Issue Deleted',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
        webhook_conf=[rail.WebhookConf(
            query_access_token_var=f'hostopia_issue_deleted_webhooks_{config.instance}_token')],
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_sender_valid = rail.IfOperator(
            task_id="is_sender_valid",
            test="{{ dag_run.conf.webhook.data.user | is_truthy and dag_run.conf.webhook.data.user.accountType == 'atlassian'}}",
            yes_task="map_to_issue_schema",
            no_task="fail_not_valid_sender"
        )

        fail_not_valid_sender = rail.FailOperator(
            task_id="fail_not_valid_sender",
            message="Webhook sender is not valid expected 'atlassian' received {{ dag_run.conf.webhook.data.user.accountType}}"
        )

        def get_source_data(data):
            temp = []
            if not data:
                return temp
            temp.append(json.loads(json.dumps(data['webhook']['data']['issue'])))
            return temp

        map_to_issue_schema = rail.DataAdaptorOperator(
            task_id="map_to_issue_schema",
            source=lambda dag_run: get_source_data(dag_run.conf),
            columns=['key', 'subtaskstatus', 'projectname', 'taskname', 'parent'],
            data=custom_method.convert_input_data_to_task_data,
        )

        get_triggered_data = rail.PythonOperator(
            task_id='get_triggered_data',
            python_callable= lambda: rail.load_all_records(rail.result('map_to_issue_schema'))[0]
        )

        issue_deleted_in_jira = issue_deleted()

        subtask_deleted_in_jira = subtask_deleted()

        is_sender_valid >> rail.Label(
            "No") >> fail_not_valid_sender

        is_sender_valid >> rail.Label(
            "Yes") >> map_to_issue_schema >> get_triggered_data >> issue_deleted_in_jira >> subtask_deleted_in_jira

    return dag


rail.for_each_instance(create_dag)
