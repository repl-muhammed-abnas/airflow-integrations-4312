from airflow.models import Variable
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.setup_dag_dagid,
        description='One time set up call',
        integration_type="generic",
        company_key=config.company_key,
        replicon_conn_id=None,
        max_active_runs=config.max_active_runs,
        default_args={
            'vp_conn_id': config.vantagepoint_conn_id
        }
    ) as dag:

        rail.VantagepointAPIOperator(
            task_id="set_up_airflow_trigger_webhook_in_vantagepoint",
            endpoint="/Workflow/Workflow",
            request_method='POST',
            request_body=lambda: {
                "WorkflowEvents": [
                    {
                        "EventID": "f623d105289b465cbc5a92e1085542ad",
                        "ApplicationName": "UDIC_eInvoiceLog",
                        "TableName": " ",
                        "EventType": "Insert",
                        "ApprovalType": "",
                        "PRLevel": "",
                        "Active": "Y",
                        "Description": "Records added to eInvoice log table",
                        "Conditions": "",
                        "EventOrder": 1,
                        "ApprovalTypeCode": "",
                        "ApplicationType": ""
                    }
                ],
                "WorkflowActions": [
                    {
                        "EventID": "f623d105289b465cbc5a92e1085542ad",
                        "ActionID": "0a933018b28e4eb89406fae4e0e18fc2",
                        "ActionOrder": 1,
                        "PRLevel": "",
                        "Active": "Y",
                        "ActionType": "Webhook",
                        "Description": "Send event to Airflow",
                        "Conditions": "",
                        "Language": "",
                        "ReadOnly": "N",
                        "ConditionMet": "Y"
                    }
                ],
                "WorkflowActionWebhook": [
                    {
                        "ActionID": "0a933018b28e4eb89406fae4e0e18fc2",
                        "WebhookURL": Variable.get(
                            config.airflow_master_dag_trigger_url),
                        "AuthUsername": Variable.get(config.basic_auth_username_pagero),
                        "AuthPassword": Variable.get(config.basic_auth_pass_pagero),
                        "ClientID": "",
                        "ClientSecret": "",
                        "RetryCount": 1,
                        "Timeout": 10,
                        "RunAfterSave": "N"
                    }
                ],
                "WorkflowActionWebhookArgs": [
                    {
                        "ActionID": "0a933018b28e4eb89406fae4e0e18fc2",
                        "ArgName": "Invoice",
                        "SQLExpression": "'[:UDIC_eInvoiceLog.CustInvoice]'",
                        "SQLIfExpression": "",
                        "SQLElseExpression": "",
                        "ArgOrder": 1
                    },
                    {
                        "ActionID": "0a933018b28e4eb89406fae4e0e18fc2",
                        "ArgName": "WBS1",
                        "SQLExpression": "'[:UDIC_eInvoiceLog.CustProject]'",
                        "SQLIfExpression": "",
                        "SQLElseExpression": "",
                        "ArgOrder": 2
                    }
                ]
            }
        )

    return dag

rail.for_each_instance(create_dag)
