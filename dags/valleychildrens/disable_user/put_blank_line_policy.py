import rail
from valleychildrens.disable_user.utils import request_payload


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"valleychildrens_put_blank_policy_child_{config.instance}",
        description=f"Valletchildrens Put Blank Policy Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        put_time_off_policy = rail.RepliconServiceOperator(
            task_id="put_time_off_policy",
            endpoint="services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_time_off_policy,
        )

        catch_disable_user_error = rail.PythonOperator(
            task_id='catch_disable_user_error',
            trigger_rule='one_failed',
            python_callable=lambda dag_run: {
                'useruri': dag_run.conf['Useruri'],
                'enddate': dag_run.conf['Terminationdate'],
                'error': rail.result('put_time_off_policy', key='error')
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'useruri': '{{ dag_run.conf.Useruri }}',
                'enddate': '{{ dag_run.conf.Terminationdate }}',
                'error': '{{ get_error_message() }}'
            }
        )

        put_time_off_policy >> catch_disable_user_error >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
