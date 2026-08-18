from datetime import datetime, timedelta
import rail

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.project_import_customer_webhook_main_dag,
        description='Alvarez and Marsal Holdings Project Import Customer Webhook Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def update_workitems(data):
            for project in data.get("CommercialProject", []):
                workpackage = project['WorkpackageSet'].get('WorkPackage', []) if project.get('WorkpackageSet') else []
                for wp in workpackage:
                    wp_id = wp.get("WorkPackageID")
                    workitem_set = wp.get("WorkitemSet", {})
                    if isinstance(workitem_set, dict):
                        for item in workitem_set.get("Workitem", []):
                            original_workitem = item.get("Workitem")
                            if original_workitem and wp_id:
                                item["Workitem"] = f"{wp_id}{config.seperator}{original_workitem}"
            return data

        rail.TriggerDagRunOperator(
            task_id = 'process_customer_projects',
            trigger_dag_id= config.project_master_dag,
            conf= lambda dag_run: {
                    "payload": update_workitems(dag_run.conf['webhook']['data'])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda dag_run: {
                "Total Number of Records": len(dag_run.conf['webhook']['data'].get('CommercialProject', []))
            },
        )

    return dag


rail.for_each_instance(create_main_dag)
