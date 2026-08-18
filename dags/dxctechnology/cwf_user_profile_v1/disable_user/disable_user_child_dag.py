import rail

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/cwf_user_profile_v1/disable_user/config.py


def create_disabled_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dagid,
        description=f'DXC_Fieldglass CWFUserProfiles_Disable_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint='services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.uri }}'
            }
        )

        catch_disable_user_error = rail.PythonOperator(
            task_id='catch_disable_user_error',
            trigger_rule='one_failed',
            python_callable=lambda dag_run: {
                'user': dag_run.conf['user'],
                'useruri': dag_run.conf['uri'],
                'enddate': dag_run.conf['enddate'],
                'error': rail.result('disable_user', key='error')
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'user': '{{ dag_run.conf.user }}',
                'useruri': '{{ dag_run.conf.uri }}',
                'enddate': '{{ dag_run.conf.enddate }}',
                'error': '{{ get_error_message() }}'
            }
        )

        disable_user >> rail.Label(
            'On Error') >> catch_disable_user_error >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_disabled_user_child_dag)
