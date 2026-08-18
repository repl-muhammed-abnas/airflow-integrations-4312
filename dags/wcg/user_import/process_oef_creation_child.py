from airflow.models import Variable
import rail


def create_oef_dropdown_value_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_oef_dropdown_value_child_dag_id,
        description="WCG User Import - Manage OEF Dropdown Values (Workato Steps 238-239 pattern)",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="get_enabled_dropdown_options"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_enabled_dropdown_options",
            end_task="catch_and_log_errors"
        )

        get_enabled_dropdown_options = rail.RepliconServiceOperator(
            task_id="get_enabled_dropdown_options",
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.subsidary_oef_uri }}"
            }
        )

        check_if_dropdown_value_exists = rail.IfOperator(
            task_id="check_if_dropdown_value_exists",
            test=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_enabled_dropdown_options"),"displayText",dag_run.conf.get("field_value"),"uri"),
            yes_task="catch_and_log_errors",
            no_task="put_dropdown_options"
        )

        def get_dropdown_create_payload(dag_run):
            updated_options = [
                {
                    "name": option.get("displayText"),
                    "target": {
                        "uri": option.get("uri"),
                        "name": option.get("displayText")
                    },
                    "isEnabled": option.get("isEnabled", "true")
                }
                for option in rail.result("get_enabled_dropdown_options", [])
            ]
            updated_options.append({
                "name": dag_run.conf["field_value"],
                "isEnabled": "true"
            })

            return {
                "customFieldUri": dag_run.conf["subsidary_oef_uri"],
                "customFieldDropDownOptionUris": updated_options
            }

        put_dropdown_options = rail.RepliconServiceOperator(
            task_id="put_dropdown_options",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=get_dropdown_create_payload
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.log_artifact }}',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule="one_failed",
            properties={
                "employeeid": "",
                "firstname": "",
                "lastname": "",
                "action": "Validation",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_enabled_dropdown_options >> check_if_dropdown_value_exists

        check_if_dropdown_value_exists >> rail.Label("Yes") >> catch_and_log_errors
        check_if_dropdown_value_exists >> rail.Label("No") >> put_dropdown_options >> catch_and_log_errors

    return dag


rail.for_each_instance(create_oef_dropdown_value_child_dag)
