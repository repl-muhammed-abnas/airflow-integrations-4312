import rail
from dxctechnology.gsap_task_import.tasks.process_billing_key import process_billing_key


def create_process_each_billing_key_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_each_gsap_wbs_billing_key_dagid,
        description=f"DXCTechnology GSAP task import process each billing key {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_billing_keys_max_active_runs
    ) as dag:

        _, finish = process_billing_key(config,
                                        project_type="gsap",
                                        create_task_dag_id=config.gsap_wbs_create_task_dagid,
                                        update_dag_task_id=config.gsap_wbs_update_task_dagid
                                        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            items='{{ result("get_input_tasks_for_project") }}',
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.project_name }}',
                'task': '{{item.task_name}}',
                'status': "Error",
                # pylint: disable= line-too-long
                'details': '{{ get_error_message()}}' + " for {{dag_run.conf.billingkey_task_name}}"
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        finish >> rail.Label("On error") >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_process_each_billing_key_dag)
