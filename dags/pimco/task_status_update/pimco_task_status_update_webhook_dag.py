from datetime import timedelta
from pimco.task_status_update.utils import python_callable_method
import rail
from airflow.models import Variable

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_task_status_update_webhook_{config.instance}',
        description=f'PIMCO Task status update - Webhook {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_webhook,
        webhook_conf=[rail.WebhookConf(
            hmac_secret_var=f'pimco_task_status_update_webhook_{config.instance}_secret')],
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                            config.can_run_batch_task_webhook, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_pimco_task_table_for_model_project_lookup'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_pimco_task_table_for_model_project_lookup',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_pimco_task_table_for_model_project_lookup = rail.CreateLogOperator(
            task_id ='create_pimco_task_table_for_model_project_lookup',
            tenant_wide_name='pimco_task_table_for_model_project',
            existing_log_mode = 'append'
        )

        get_all_entries_pimco_task_table_for_model_project = rail.FilterLogEntriesOperator(
            task_id = 'get_all_entries_pimco_task_table_for_model_project',
            log='{{result("create_pimco_task_table_for_model_project_lookup")}}'
        )

        create_pimco_consultant_task_project_lookup = rail.CreateLogOperator(
            task_id ='create_pimco_consultant_task_project_lookup',
            tenant_wide_name='pimco_task_table_for_consultant_model_project',
            existing_log_mode = 'append'
        )

        get_all_entries_pimco_consultant_task_project = rail.FilterLogEntriesOperator(
            task_id = 'get_all_entries_pimco_consultant_task_project',
            log='{{result("create_pimco_consultant_task_project_lookup")}}'
        )

        if config.debug:
            was_triggered_by_replicon = rail.EmptyOperator(
                task_id='was_triggered_by_replicon')
        else:
            was_triggered_by_replicon = rail.IfOperator(
                task_id="was_triggered_by_replicon",
                test=lambda dag_run: python_callable_method.is_name_for_uri_present(dag_run.conf['webhook']['data']['task']['uri']),
                yes_task="is_valid_webhookevent",
                no_task='delete_this_dagrun'
            )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        is_valid_webhookevent = rail.IfOperator(
            task_id = "is_valid_webhookevent",
            test = "{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] in ['TaskStatusChanged']}}",
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
                "taskUri": "{{ dag_run.conf.webhook.data.task.uri }}"
            }
        )

        is_project_name_not_equal_pimcomodeltask=rail.IfOperator(
            task_id='is_project_name_not_equal_pimcomodeltask',
            test=lambda: bool(rail.result('get_task_details')['project']['name'] != config.project_name and rail.result(
                    'get_task_details')['project']['name'] != config.consultant_project_name),
            yes_task="delete_this_dagrun",
            no_task="get_task_status_and_resource_update_lookup_table",
        )

        get_task_status_and_resource_update_lookup_table = rail.CreateLogOperator(
            task_id="get_task_status_and_resource_update_lookup_table",
            tenant_wide_name="task_status_and_resource_update_lookup_table",
            existing_log_mode="append",
        )

        search_entries_task_status_and_resource_update_lookup=rail.FilterLogEntriesOperator(
            task_id = 'search_entries_task_status_and_resource_update_lookup',
            log= "{{ result('get_task_status_and_resource_update_lookup_table') }}",
            properties={
                'type': 'status',
                'taskname': "{{ dag_run.conf.webhook.data.task.name }}",
                'code': "{{ dag_run.conf.webhook.data.task.code }}",
                'uri': "{{ dag_run.conf.webhook.data.task.uri }}",
                'fullpath': "{{ dag_run.conf.webhook.data.task.displayText }}"
            }
        )

        if_entries_not_present=rail.IfOperator(
            task_id='if_entries_not_present',
            test='''{{ result('search_entries_task_status_and_resource_update_lookup',"length") == 0 }}''',
            yes_task="add_entry_task_status_and_resource_update_lookup",
            no_task="if_entrys_processed_value_not_equal_isclosed",
        )

        add_entry_task_status_and_resource_update_lookup=rail.WriteLogOperator(
            task_id='add_entry_task_status_and_resource_update_lookup',
            log="{{ result('get_task_status_and_resource_update_lookup_table') }}",
            message="na",
            properties=lambda: {
                'type': 'status',
                'taskname': "{{ dag_run.conf.webhook.data.task.name }}",
                'code': "{{ dag_run.conf.webhook.data.task.code }}",
                'uri': "{{ dag_run.conf.webhook.data.task.uri }}",
                'fullpath': "{{ dag_run.conf.webhook.data.task.displayText }}",
                'processed': "{{ dag_run.conf.webhook.data.isClosed }}",
                'date': "{{ current_time('%d/%m/%Y')}}",
                'project_type': 'Consultant' if rail.result('get_task_details')['project']['name'] != config.project_name else 'FTE'
            }
        )

        if_entrys_processed_value_not_equal_isclosed=rail.IfOperator(
            task_id='if_entrys_processed_value_not_equal_isclosed',
            test=lambda dag_run: python_callable_method.check_value_of_processed(dag_run.conf['webhook']['data']['isClosed']),
            yes_task="delete_entry_task_status_and_resource_update_lookup",
            no_task="finish",
        )

        delete_entry_task_status_and_resource_update_lookup=rail.FilterLogEntriesOperator(
            task_id = 'delete_entry_task_status_and_resource_update_lookup',
            log= "{{ result('get_task_status_and_resource_update_lookup_table') }}",
            properties={
                'type': 'status',
                'taskname': "{{ dag_run.conf.webhook.data.task.name }}",
                'code': "{{ dag_run.conf.webhook.data.task.code }}",
                'uri': "{{ dag_run.conf.webhook.data.task.uri }}",
                'fullpath': "{{ dag_run.conf.webhook.data.task.displayText }}"
            },
            remove_filtered_entries=True
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> create_pimco_task_table_for_model_project_lookup >> get_all_entries_pimco_task_table_for_model_project
        get_all_entries_pimco_task_table_for_model_project >> create_pimco_consultant_task_project_lookup >> get_all_entries_pimco_consultant_task_project >> \
            was_triggered_by_replicon
        was_triggered_by_replicon >> rail.Label("No") >> delete_this_dagrun >> finish
        was_triggered_by_replicon >> rail.Label(
            "Yes") >> is_valid_webhookevent >> rail.Label("Yes") >> get_task_details >> is_project_name_not_equal_pimcomodeltask
        is_project_name_not_equal_pimcomodeltask >> rail.Label('Yes')  >> delete_this_dagrun >> finish
        is_project_name_not_equal_pimcomodeltask >> rail.Label(
            'No') >> get_task_status_and_resource_update_lookup_table >> search_entries_task_status_and_resource_update_lookup >> if_entries_not_present
        if_entries_not_present >> rail.Label('Yes')  >> add_entry_task_status_and_resource_update_lookup >> finish
        if_entries_not_present >> rail.Label('No') >> if_entrys_processed_value_not_equal_isclosed
        if_entrys_processed_value_not_equal_isclosed >> rail.Label('Yes')  >> delete_entry_task_status_and_resource_update_lookup >> finish
        if_entrys_processed_value_not_equal_isclosed >> rail.Label('No') >> finish
        is_valid_webhookevent >> rail.Label("No") >> fail_invalid_webhookevent >> finish
    return dag

rail.for_each_instance(create_dag)
