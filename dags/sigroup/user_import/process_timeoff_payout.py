from sigroup.user_import.utils import custom_methods
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_user_import_timeoff_type_for_update_user_payout_user,
        description="sigroup user import update user  payout timeoff types child",
        max_active_runs=config.child_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_new_policy_line = rail.PythonOperator(
            task_id="create_new_policy_line",
            python_callable=custom_methods.get_new_payout_policy_line
        )

        create_all_valid_policies_list = rail.PythonOperator(
            task_id="create_all_valid_policies_list",
            python_callable=custom_methods.get_existing_policies_list
        )

        put_user_account_policy_schedule = rail.RepliconServiceOperator(
            task_id="put_user_account_policy_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
            "timeOffAccount": {
                "userUri": dag_run.conf["user_uri"],
                "timeOffTypeUri": dag_run.conf["timeoffuri"]
            },
            "policySetScheduleEntries": rail.result("create_all_valid_policies_list")
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="User update failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf.get("employeeid", ""),
                "Username": (dag_run.conf.get("firstname") or "") + (dag_run.conf.get("lastname") or ""),
                "Action": "Update",
                "Status": "Error",
                "Details": rail.render_template('{{get_error_message()}}'),
                
            }
        )

        create_new_policy_line >> create_all_valid_policies_list >>\
        put_user_account_policy_schedule >> catch_and_log_errors
        return dag


rail.for_each_instance(create_airflow_dag)
