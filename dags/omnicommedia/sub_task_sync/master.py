
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'omnicommedia_subtask_sync_{config.instance}',
        description=f'Omnicommedia_Autocreate Sub task in Replicon_Master V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=[rail.WebhookConf(
            hmac_secret_var=config.hmac_secret_var)],
        start_date=datetime(2023, 1, 1),
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_redirect_to_workato = rail.IfOperator(
            task_id='can_redirect_to_workato',
            test=lambda: Variable.get(
                config.can_redirect_to_workato_var_name, default_var='').lower() == 'true',
            yes_task='post_to_workato',
            no_task='can_run_batch_task',
        )

        post_to_workato = rail.SimpleHttpOperator(
            task_id='post_to_workato',
            method='POST',
            http_conn_id=config.workato_api_endpoint,
            headers={
                'Content-Type': 'application/json; charset=utf-8',
                'API-TOKEN': "{{ var.value." + config.workato_api_token_var_name + " }}"
            },
            data='{{ dag_run.conf.webhook.data | to_json }}',
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='was_triggered_by_omnicommedia'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='was_triggered_by_omnicommedia',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            end_task='log_to_sumo',
        )

        was_triggered_by_omnicommedia = rail.EmptyOperator(
            task_id='was_triggered_by_omnicommedia')

        check_task_and_loginname = rail.IfOperator(
            task_id='check_task_and_loginname',
            test=lambda dag_run: (dag_run.conf['webhook']['data']).get('task').get('displayText') and (
                dag_run.conf['webhook']['data']).get('authority').get('actingUser').get('loginName') != 'automation',
            yes_task='trigger_dag_run_live_omnincommedia_createsubtask_child_v1_04',
            no_task='log_to_sumo'
        )

        trigger_dag_run_live_omnincommedia_createsubtask_child_v1_04 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_omnincommedia_createsubtask_child_v1_04',
            retries=0,
            items=[0],
            trigger_dag_id=f'omnicommedia_createsubtask_child_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "actinguser": (dag_run.conf['webhook']['data']).get('authority').get('actingUser').get('loginName'),
                "taskuri": (dag_run.conf['webhook']['data']).get('task').get('uri'),
                "taskname": (dag_run.conf['webhook']['data']).get('task').get('displayText')
            }
        )

        wait_for_completion_trigger_dag_run_live_omnincommedia_createsubtask_child_v1_04 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_omnincommedia_createsubtask_child_v1_04',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_omnincommedia_createsubtask_child_v1_04") }}'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_redirect_to_workato >> rail.Label(
            'Yes') >> post_to_workato >> log_to_sumo
        can_redirect_to_workato >> rail.Label('No') >> can_run_batch_task

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> was_triggered_by_omnicommedia \
            >> check_task_and_loginname >> rail.Label('Yes') >> trigger_dag_run_live_omnincommedia_createsubtask_child_v1_04 \
            >> wait_for_completion_trigger_dag_run_live_omnincommedia_createsubtask_child_v1_04 >> log_to_sumo
        check_task_and_loginname >> rail.Label('No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
