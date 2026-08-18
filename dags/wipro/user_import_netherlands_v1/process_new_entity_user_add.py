from airflow.models import Variable
from wipro.user_import_netherlands_v1.task import put_user_and_table_settings, put_supervisor_table_settings
from wipro.user_import_netherlands_v1.utils import request_payload, custom_methods
import rail
null = None


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.add_new_entity_user_dag_id,
        description="wipro User import process record",
        company_key=config.company_key,
        max_active_runs=config.max_active_run_sub_child,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_process_batch_task, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_netherlands_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_netherlands_user",
            end_task="catch_and_log_errors"
        )

        create_netherlands_user = rail.RepliconServiceOperator(
            task_id="create_netherlands_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_new_entity_user_create_payload(
                dag_run, config.USER_TEMPLATE_NAME),
            data_handler=lambda response: response["user"]["uri"] if response else null
        )

        if_supervisor_details_in_feed = rail.IfOperator(
            task_id="if_supervisor_details_in_feed",
            test=lambda dag_run: bool(dag_run.conf["primary_supervisor_id"]),
            yes_task="write_supervisor_pending_logs",
            no_task="unassign_products_for_user"
        )

        write_supervisor_pending_logs = rail.WriteLogOperator(
            task_id="write_supervisor_pending_logs",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor",
            severity="Pending",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "primary_supervisor_id": dag_run.conf["primary_supervisor_id"],
                "primary_supervisor_adid": dag_run.conf["primary_supervisor_adid"],
                "primary_supervisor_mailid": dag_run.conf["primary_supervisor_mailid"],
                "Add_Update": "Add",
                "useruri": rail.result("create_netherlands_user"),
                "project_supervisor_id": dag_run.conf["project_supervisor_id"] if
                    dag_run.conf["project_supervisor_id"] and dag_run.conf['personnel_subarea_text'] == 'DOP Portugal' else None
            }
        )

        unassign_products_for_user = rail.RepliconServiceOperator(
            task_id='unassign_products_for_user',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda: {
                "user": {
                    "uri": rail.result("create_netherlands_user"),
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "productAssignmentsToApply": {
                        "productUrisToUnassign": [
                            "urn:replicon-saas:product:time-intelligence",
                            "urn:replicon-saas:product:time-bill-plus",
                        ]
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        put_view_settings_for_user = put_user_and_table_settings.get_put_table_view_setting(
            '{{result("create_netherlands_user")}}', "netherlands", 'user')

        if_primary_manager_or_project_manager_permission = rail.IfOperator(
            task_id="if_primary_manager_or_project_manager_permission",
            test='{{dag_run.conf.primary_manager_flg == "Y" or dag_run.conf.project_manager_flg == "Y"}}',
            yes_task="start_table_setting",
            no_task="end_table_setting"
        )

        start_table_setting = rail.EmptyOperator(task_id="start_table_setting")
        put_manager_table_view = put_supervisor_table_settings.get_put_table_view_setting_supervisor(
            '{{result("create_netherlands_user")}}', 'supervisor_user', '{{dag_run.conf.project_manager_flg}}')

        end_table_setting = rail.EmptyOperator(task_id="end_table_setting")

        remove_all_time_off_types = rail.RepliconServiceOperator(
            task_id="remove_all_time_off_types",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                    "userUri": '{{result("create_netherlands_user")}}',
                    "timeOffTypeUris": []
            }
        )

        write_added_user_logs = rail.WriteLogOperator(
            task_id="write_added_user_logs",
            log='{{dag_run.conf.lookuptable}}',
            message="User created",
            severity="Success",
            trigger_rule="all_success",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Success",
                "action": "Add",
                "details": "User successfully created"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="User not processed for the following reason/s",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Failed",
                "action": "Add",
                "details": "User not processed for the following reason/s" + custom_methods.get_error_message(),


            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
        create_netherlands_user >>\
        if_supervisor_details_in_feed >> rail.Label(
                "No") >> unassign_products_for_user
        if_supervisor_details_in_feed >> rail.Label("Yes") >>\
            write_supervisor_pending_logs >> unassign_products_for_user >>\
            put_view_settings_for_user >>\
            if_primary_manager_or_project_manager_permission >> rail.Label(
            "Yes") >> start_table_setting >> put_manager_table_view >>\
            end_table_setting >> remove_all_time_off_types >>\
            write_added_user_logs >> catch_and_log_errors
        if_primary_manager_or_project_manager_permission >> rail.Label("No") >>\
            end_table_setting
        return dag


rail.for_each_instance(create_airflow_child)
