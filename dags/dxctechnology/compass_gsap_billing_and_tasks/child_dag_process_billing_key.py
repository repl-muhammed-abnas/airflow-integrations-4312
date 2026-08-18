import rail
from dxctechnology.compass_gsap_billing_and_tasks.create_billing_key_task import get_create_billingkey_task
from dxctechnology.compass_gsap_billing_and_tasks.update_billing_key_task import get_update_billingkey_task


def create_child_dag_billing_key(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_gsap_billing_and_tasks_import_{config.sub_erp_name}_child_billingkey',
        description=f'DXC COMPASS GSAP Billing and Tasks Child BillingKey - {config.sub_erp_name}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_concurrent_billingkey_task_imports,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        has_all_required_fields = rail.IfOperator(
            task_id='has_all_required_fields',
            test='{{ dag_run.conf.BillingKey | attr_or_default("StartDate", "") | length > 0 and \
                dag_run.conf.BillingKey | attr_or_default("EndDate", "") | length > 0 and \
                dag_run.conf.BillingKey | attr_or_default("Name", "") | length > 0 }}',
            yes_task='create_log',
            no_task='log_missing_required_fields',
        )

        def get_log_missing_required_fields_msg(dag_run):
            msg = []
            msg.append(
                "Billing Key is not present" if not dag_run.conf['BillingKey']['Name'] else None)
            msg.append(
                "Billing Key Start Date is not present" if not dag_run.conf['BillingKey']['StartDate'] else None)
            msg.append(
                "Billing Key End Date is not present" if not dag_run.conf['BillingKey']['EndDate'] else None)
            return ", ".join([m for m in msg if m is not None])
        log_missing_required_fields = rail.WriteLogOperator(
            task_id="log_missing_required_fields",
            message=get_log_missing_required_fields_msg,
            severity='Error',
            properties={
                'WBS': '{{ dag_run.conf.BillingKey.WBS }}',
                'BillingKey': '{{ dag_run.conf.BillingKey.Name }}',
            }
        )

        create_log = rail.CreateLogOperator(task_id='create_log')

        does_task_exist_in_replicon = rail.IfOperator(
            task_id='does_task_exist_in_replicon',
            test='{{ dag_run.conf.BillingTasks | length > 0 }}',
            yes_task='updating_billingkey_task',
            no_task='put_billingkey_tasks',
        )

        def get_completion_log_severity():
            logs = rail.load_all_records(rail.result('create_log'))
            if any(filter(lambda e: e['severity'] == 'Error', logs)):
                return 'Error'
            if any(filter(lambda e: e['severity'] == 'Exception', logs)):
                return 'Exception'
            return 'Success'
        log_successful_completion = rail.WriteLogOperator(
            task_id='log_successful_completion',
            # pylint: disable=line-too-long
            message='{{ result("create_log") | load_all_records | map_to_attr("message") | join(" | ") | default("Billing Key processed successfully", True) }}',
            severity=get_completion_log_severity,
            properties={
                'WBS': '{{ dag_run.conf.BillingKey.WBS }}',
                'BillingKey': '{{ dag_run.conf.BillingKey.Name }}',
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                'WBS': '{{ dag_run.conf.BillingKey.WBS }}',
                'BillingKey': '{{ dag_run.conf.BillingKey.Name }}',
            }
        )

        create_billingkey_task_group_entry, create_billingkey_task_group_exit = get_create_billingkey_task()
        update_billingkey_task_group_entry, update_billingkey_task_group_exit = get_update_billingkey_task()

        has_all_required_fields >> rail.Label("Yes") >> create_log >> does_task_exist_in_replicon >> rail.Label(
            "Yes") >> update_billingkey_task_group_entry
        has_all_required_fields >> rail.Label(
            "No") >> log_missing_required_fields >> catch_and_log_errors
        does_task_exist_in_replicon >> rail.Label(
            "No") >> create_billingkey_task_group_entry
        [create_billingkey_task_group_exit, update_billingkey_task_group_exit] >> log_successful_completion >> rail.Label(
            "On error") >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag_billing_key)
