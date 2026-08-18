import rail

def create_child_dag(config):
    dag_list = []
    for idx, log_name in enumerate(config.tenant_wide_log_list):
        with rail.create_airflow_dag(
            dag_id=f'{config.webhook_logging_child_dagid}_{idx+1}_v0',
            description=f'Capgemini Auto Population of Optional Holidays India for New Users using Webhook logging child v0 {config.instance} {idx+1}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_new_users,
            default_args={
                'retries': 0
            }
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            def get_log_properties(dag_run):
                webhook_details = dag_run.conf['webhook']['data']
                return {
                    "user_login_name": webhook_details['user']['loginName'],
                    "user_name": webhook_details['user']['displayText'],
                    "user_uri": webhook_details['user']['uri']
                }

            log_new_users_details = rail.WriteLogOperator(
                task_id="log_new_users_details",
                log=log_name,
                severity="Created",
                message="New User Created",
                properties=get_log_properties
            )

            log_new_users_details

        dag_list.append(dag)

    return dag

rail.for_each_instance(create_child_dag)
