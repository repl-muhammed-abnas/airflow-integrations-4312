import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_book_optional_holiday_delete_timeoff_child_{config.instance}',
        description=f'Capgemini Auto Population of Optional Holidays India Delete Timeoff Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_delete_timeoff_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='delete_not_submitted_timeoff',
            endpoint='/services/TimeoffService1.svc/DeleteTimeOff',
            data={
                "timeOffUri": "{{ dag_run.conf.timeoff_uri }}"
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
