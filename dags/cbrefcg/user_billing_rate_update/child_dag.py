from datetime import timedelta
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f'cbrefcg_update_users_billing_rates_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        start_processing_billing_rates = rail.EmptyOperator(
            task_id = 'start_processing_billing_rates'
        )

        process_billing_rates = rail.trigger_parallel_dagrun(
           task_id='process_billing_rates',
            items="{{ dag_run.conf.projectdata | to_json }}",
            parallel_count=50,
            trigger_dag_id=config.process_billing_rates_dag_id,
            conf={
                "projecturi": "{{ item.projecturi }}",
                "useruri": "{{ item.useruri }}",
                "currentbillingrate": "{{ item.currentbillingrate }}",
                "currencyuri": "{{ item.currencyuri }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        start_processing_billing_rates >> process_billing_rates >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
