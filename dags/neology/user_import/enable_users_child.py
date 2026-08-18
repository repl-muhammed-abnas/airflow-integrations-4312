import rail

def create_enable_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.enable_users_child_dag_id,
        description="Neology Enable User Child DAG - Enable users in Replicon",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.enable_user_child_max_active_runs
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        enable_user = rail.RepliconServiceOperator(
            task_id='enable_user',
            endpoint='services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            }
        )

        catch_enable_user_error = rail.PythonOperator(
            task_id='catch_enable_user_error',
            trigger_rule='one_failed',
            python_callable=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'useruri': dag_run.conf['useruri'],
                'startdate': dag_run.conf['userstartdate'],
                'enddate': dag_run.conf['userenddate'],
                'error': rail.result('enable_user', key='error')
            }
        )

        enable_user >> catch_enable_user_error

        return dag

rail.for_each_instance(create_enable_user_child_dag)
