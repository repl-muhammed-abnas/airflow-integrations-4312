from ipipeline.user_import_v2.utils.request_payload import get_disable_user_and_update_loginname_payload
import rail
null = None


def create_disable_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_users_child_dag_id,
        description="iPipeline Disable User Child DAG - Disable users in Replicon",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.disable_user_child_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint='/services/ImportService2.svc/CreateUserOrApplyModifications',
            data=get_disable_user_and_update_loginname_payload
        )

        catch_disable_user_error = rail.PythonOperator(
            task_id='catch_disable_user_error',
            trigger_rule='one_failed',
            python_callable=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'useruri': dag_run.conf['useruri'],
                'enddate': dag_run.conf['userenddate'],
                'error': rail.result('disable_user', key='error')
            }
        )

        disable_user >> catch_disable_user_error

        return dag


rail.for_each_instance(create_disable_user_child_dag)
