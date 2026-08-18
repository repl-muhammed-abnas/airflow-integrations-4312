from pendulum import datetime
import rail
from cbrefcg.oef_update.utils import request_payload
from cbrefcg.oef_update.utils import custom_method
from cbrefcg.oef_update.tasks.process_each_oef_task import process_oef

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'cbrefcg_user_modified_webhook_event_{config.instance}',
        description=f'cbrefcg_User_Modified_Webhook event {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
        webhook_conf=[rail.WebhookConf(
            hmac_secret_var=f'cbrefcg_user_modified_webhooks_{config.instance}_secret')],
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        is_valid_webhookevent = rail.IfOperator(
            task_id = "is_valid_webhookevent",
            test = "{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] == 'UserModified' }}",
            yes_task="create_log",
            no_task= "fail_invalid_webhookevent"
        )

        fail_invalid_webhookevent = rail.FailOperator(
            task_id = "fail_invalid_webhookevent",
            message= "Received invalid webhook trigger event: '{{dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type']}}'"
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log",
            tenant_wide_name="cbre_webhook_user_modified_data",
            existing_log_mode="append",
        )

        get_effective_user_group_membership= rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.webhook.data.user.uri }}"
            },
            data_handler= request_payload.get_group_memberships
        )

        check_employee_type = rail.IfOperator(
            task_id = 'check_employee_type',
            test='{{ result("get_effective_user_group_membership") | is_truthy }}',
            yes_task= 'get_user_details',
            no_task= 'finish'
        )

        get_user_details= rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                    "uri": "{{ dag_run.conf.webhook.data.user.uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        filter_logs_by_user = rail.FilterLogEntriesOperator(
            task_id = 'filter_logs_by_user',
            log="{{ result('create_log') }}",
            properties={
                'useruri': "{{ dag_run.conf.webhook.data.user.uri }}"
            }
        )

        has_filtered_data_present = rail.IfOperator(
            task_id = 'has_filtered_data_present',
            test= '{{ result("filter_logs_by_user", "length") > 0 }}',
            yes_task= 'is_custom_field_not_same',
            no_task= 'add_user_group_start'
        )

        is_custom_field_not_same = rail.IfOperator(
            task_id = 'is_custom_field_not_same',
            test=lambda: custom_method.check_custom_fielddata()['value'],
            yes_task= 'update_user_group_start',
            no_task= 'finish'
        )

        update_user_group_start = rail.EmptyOperator(
            task_id = 'update_user_group_start'
        )

        remove_existing_log = rail.FilterLogEntriesOperator(
            task_id = 'remove_existing_log',
            log="{{ result('create_log') }}",
            properties={
                'useruri': "{{ dag_run.conf.webhook.data.user.uri }}"
            },
            remove_filtered_entries = True
        )

        process_update_user  = process_oef(config,'update')

        add_user_group_start = rail.EmptyOperator(
            task_id = 'add_user_group_start'
        )

        process_add_user = process_oef(config,'add')

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )


        is_valid_webhookevent >> rail.Label(
            "Yes") >> create_log

        is_valid_webhookevent >> rail.Label(
            "No") >> fail_invalid_webhookevent

        create_log >> get_effective_user_group_membership >> check_employee_type

        check_employee_type >> rail.Label(
            "Yes") >> get_user_details >> filter_logs_by_user >> has_filtered_data_present

        check_employee_type >> rail.Label(
            "Yes") >> finish

        has_filtered_data_present >> rail.Label(
            "Yes") >> is_custom_field_not_same

        has_filtered_data_present >> rail.Label(
            "No") >> add_user_group_start >> process_add_user >> finish

        is_custom_field_not_same >> rail.Label(
            "Yes") >> update_user_group_start >> remove_existing_log >> process_update_user >> finish

        is_custom_field_not_same >> rail.Label(
            "No") >> finish

        finish >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
