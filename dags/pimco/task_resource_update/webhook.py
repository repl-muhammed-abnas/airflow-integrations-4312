from datetime import datetime, timedelta
import rail
from airflow.models import Variable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_task_resource_update_webook_{config.instance}',
        description=f'PIMCO Task Resource Update - Webook {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_webhook,
        webhook_conf=[rail.WebhookConf(
            hmac_secret_var=f'pimco_task_resource_update_webook_{config.instance}_secret')],
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_webhook, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='start'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='start',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        start = rail.EmptyOperator(
            task_id='start'
        )

        if config.debug:
            was_triggered_by_replicon = rail.EmptyOperator(
                task_id='was_triggered_by_replicon')
        else:
            was_triggered_by_replicon = rail.IfOperator(
                task_id="was_triggered_by_replicon",
                test='{{dag_run.conf.webhook.data.authority.actingUser.loginName == "admin"}}',
                yes_task="delete_this_dagrun",
                no_task='is_valid_webhookevent'
            )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        is_valid_webhookevent = rail.IfOperator(
            task_id = "is_valid_webhookevent",
            test = "{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] in ['TaskResourcesModified']}}",
            yes_task="get_task_details",
            no_task= "fail_invalid_webhookevent"
        )

        fail_invalid_webhookevent = rail.FailOperator(
            task_id = "fail_invalid_webhookevent",
            message= "Received invalid webhook trigger event: '{{dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type']}}'"
        )

        get_task_details=rail.RepliconServiceOperator(
            task_id='get_task_details',
            endpoint="/services/TaskService1.svc/GetTaskDetails",
            data={
                "taskUri": "{{dag_run.conf.webhook.data.task.uri}}"
            }
        )

        is_project_name_not_equal_pimcomodeltask=rail.IfOperator(
            task_id='is_project_name_not_equal_pimcomodeltask',
            test=lambda: bool(rail.result('get_task_details')['project']['name'] != config.project_name and rail.result(
                    'get_task_details')['project']['name'] != config.consultant_project_name),
            yes_task="delete_this_dagrun",
            no_task="create_task_status_and_resource_update_lookup_table",
        )

        create_task_status_and_resource_update_lookup_table = rail.CreateLogOperator(
            task_id="create_task_status_and_resource_update_lookup_table",
            tenant_wide_name="task_status_and_resource_update_lookup_table",
            existing_log_mode="append",
        )

        add_entry_task_status_and_resource_update_lookup=rail.WriteLogOperator(
            task_id='add_entry_task_status_and_resource_update_lookup',
            log="{{ result('create_task_status_and_resource_update_lookup_table') }}",
            message="na",
            properties=lambda: {
                "type": 'resource',
                "taskname": rail.result('get_task_details')['name'],
                "code": rail.result('get_task_details')['code'],
                "uri": rail.result('get_task_details')['uri'],
                "fullpath": rail.result('get_task_details')['displayText'],
                "processed": "No",
                "date": datetime.now().strftime("%d/%m/%Y"),
                'project_type': 'Consultant' if rail.result('get_task_details')['project']['name'] != config.project_name else 'FTE'
            }
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> start
        start >> was_triggered_by_replicon
        was_triggered_by_replicon >> rail.Label("Yes") >> delete_this_dagrun
        was_triggered_by_replicon >> rail.Label("No") >> is_valid_webhookevent >> rail.Label(
            "Yes") >> get_task_details >> is_project_name_not_equal_pimcomodeltask
        is_project_name_not_equal_pimcomodeltask >> rail.Label('Yes')  >> delete_this_dagrun >> finish
        is_project_name_not_equal_pimcomodeltask >> rail.Label(
            'No') >> create_task_status_and_resource_update_lookup_table >> add_entry_task_status_and_resource_update_lookup >> finish >> log_to_sumo
        log_to_sumo >> can_fail_dag >> fail_dagrun
        is_valid_webhookevent >> rail.Label("No") >> fail_invalid_webhookevent >> finish
    return dag

rail.for_each_instance(create_dag)
