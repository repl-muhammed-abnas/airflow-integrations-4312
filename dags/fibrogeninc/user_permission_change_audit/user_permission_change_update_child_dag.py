from fibrogeninc.user_permission_change_audit.utils import request_payload
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'fibrogeninc_user_permission_change_update_child_{config.instance}',
        description=f'Fibrogeninc user permission change update - Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        if_request_dateaccountlastchanged_present_3=rail.IfOperator(
            task_id='if_request_dateaccountlastchanged_present_3',
            test='{{ dag_run.conf.date_account_last_changed | is_truthy }}',
            yes_task="update_text_value_date_account_last_changed_4",
            no_task="if_request_permissionname_present_7",
        )

        update_text_value_date_account_last_changed_4=rail.RepliconServiceOperator(
            task_id='update_text_value_date_account_last_changed_4',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=request_payload.get_update_text_value_account_changed_payload
        )

        update_text_value_description_5=rail.RepliconServiceOperator(
            task_id='update_text_value_description_5',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: request_payload.get_update_text_value_description_payload(dag_run, "Permission Modification")
        )

        log_permission_modification_audit = rail.WriteLogOperator(
            task_id='log_permission_modification_audit',
            message="Permission Modification Audit Successful",
            severity="Success",
            properties={
                "username": '{{ dag_run.conf.username }}',
                "permissionname": '{{ dag_run.conf.permission_name }}',
                "status": "Success",
                "details": "Permission Modification Audit Successful"
            }
        )

        if_request_permissionname_present_7=rail.IfOperator(
            task_id='if_request_permissionname_present_7',
            test='{{ dag_run.conf.permission_name | is_truthy }}',
            yes_task="update_text_value_date_account_last_changed_8",
            no_task="log_audit_skipped",
        )

        update_text_value_date_account_last_changed_8=rail.RepliconServiceOperator(
            task_id='update_text_value_date_account_last_changed_8',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=request_payload.get_update_text_value_account_changed_payload
        )

        update_text_value_description_9=rail.RepliconServiceOperator(
            task_id='update_text_value_description_9',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: request_payload.get_update_text_value_description_payload(dag_run, "Permission Assigned")
        )

        log_permission_assigned_audit = rail.WriteLogOperator(
            task_id='log_permission_assigned_audit',
            message="Permission Assigned Audit Successful",
            severity="Success",
            properties={
                "username": '{{ dag_run.conf.username }}',
                "permissionname": '{{ dag_run.conf.permission_name }}',
                "status": "Success",
                "details": "Permission Assigned Audit Successful"
            }
        )

        log_audit_skipped = rail.WriteLogOperator(
            task_id='log_audit_skipped',
            message="Permission Change Audit Skipped",
            severity="Skipped",
            properties={
                "username": '{{ dag_run.conf.username }}',
                "permissionname": '{{ dag_run.conf.permission_name }}',
                "status": "Skipped",
                "details": "Permission Change Audit Skipped"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties={
                "username": '{{ dag_run.conf.username }}',
                "permissionname": '{{ dag_run.conf.permission_name }}',
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        if_request_dateaccountlastchanged_present_3
        if_request_dateaccountlastchanged_present_3 >> rail.Label('Yes')  >> update_text_value_date_account_last_changed_4 >> update_text_value_description_5 \
            >> log_permission_modification_audit >> catch_and_log_errors
        if_request_dateaccountlastchanged_present_3 >> rail.Label('No') >> if_request_permissionname_present_7
        if_request_permissionname_present_7 >> rail.Label('Yes')  >> update_text_value_date_account_last_changed_8 >> update_text_value_description_9 \
            >> log_permission_assigned_audit >> catch_and_log_errors
        if_request_permissionname_present_7 >> rail.Label('No') >> log_audit_skipped >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
