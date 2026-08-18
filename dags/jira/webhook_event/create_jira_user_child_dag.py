from datetime import timedelta
import rail
from airflow.models import Variable
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_jira_{config.region.replace('-', '_')}_user_export_child_dag_{config.instance}",
        description=f'Jira {config.region} User Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='user_detail_in_replicon'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='user_detail_in_replicon',
            end_task='log_dagrun_details_to_table',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_user_detail(response):
            user_detail = list(map(lambda item: {
                'name': item['cells'][0]['textValue'],
                'loginname': item['cells'][1]['textValue'],
                'uri': item['cells'][0]['uri'],
                'emailaddress': item['cells'][2].get('textValue'),
                'enabled': item['cells'][3].get('boolValue')
            }, response['rows']))
            return user_detail[0] if user_detail else []

        user_detail_in_replicon = rail.RepliconServiceOperator(
            task_id="user_detail_in_replicon",
            replicon_conn_id='{{ dag_run.conf.data.connector_details.replicon_conn_id }}',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:email-address",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "text": dag_run.conf['data']['event_details']['user']['loginName']
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_user_detail
        )

        create_user_in_jira = rail.JiraAPIOperator(
            task_id='create_user_in_jira',
            request_method='POST',
            endpoint="/rest/api/3/user",
            jira_conn_id='{{ dag_run.conf.data.connector_details.connector_conn_id }}',
            request_body=lambda: {
                "password": "Password@123",
                "displayName": rail.result('user_detail_in_replicon')['name'],
                "emailAddress": rail.result('user_detail_in_replicon')['emailaddress'],
                "products": []
            }
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.data.connector_details.company_key }}',
            connector_name='jira',
            integration_type='user_export'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_details_to_table
        can_run_batch_task >> rail.Label(
            'No') >> user_detail_in_replicon >> create_user_in_jira >> log_dagrun_details_to_table

    return dag


rail.for_each_instance(create_child_dag)
