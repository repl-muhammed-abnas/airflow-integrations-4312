import rail
import pendulum
from datetime import timedelta, date

from repliconinc.internal_monthly_shift_roster.utils import custom_methods, request_payload


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"RepliconInc APAC Monthly Shift Roster - Master DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2026, 2, 22, tz=config.timezone),
        max_active_runs=config.max_active_run_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var
        ),
        default_args={
            "execution_timeout": timedelta(days=config.execution_timeout_days),
        },
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')

        get_end_date = rail.PythonOperator(
            task_id="get_end_date",
            python_callable=custom_methods.get_end_date
        )

        get_team_shift_roster = rail.PythonOperator(
            task_id='get_team_shift_roster',
            python_callable=request_payload.get_team_shift_roster_from_conf
        )  

        # Pass the required parameters to the child DAG for shift assignment
        # While migrating from workato to airflow, the unused variables are not passed to the child dag
        process_shift_assigment_for_each_user = rail.trigger_parallel_dagrun(
            task_id="process_shift_assigment_for_each_user",
            items=lambda: rail.result("get_team_shift_roster"),
            trigger_dag_id=config.process_each_user_child_dag_id,
            parallel_count=config.parallel_count_process_each_user,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **({
                    "user_uri": item["useruri"],
                    "shift_name": item["shift"],
                    "start_date_day": (first_day_of_next_month := custom_methods.get_first_day_of_next_month(date.today())).day,
                    "start_date_month": first_day_of_next_month.month,
                    "start_date_year": first_day_of_next_month.year,
                    "end_date_day": rail.result("get_end_date").day,
                    "end_date_month": rail.result("get_end_date").month,
                    "end_date_year": rail.result("get_end_date").year,
                })
            }
        )

        get_end_date >> get_team_shift_roster >> process_shift_assigment_for_each_user
        

    return dag

rail.for_each_instance(create_master_dag)