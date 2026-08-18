from datetime import timedelta
from datetime import datetime as dt
from pendulum import datetime as pdt
from wipro.user_import_spain_v3.utils import custom_methods
from wipro.user_import_spain_v3.utils import request_payload
import rail
null = None


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"wipro_terminationbalance_user_child_{config.instance}_v3",
        description="spain termination balance user",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pdt(2023, 12, 18, tz=config.time_zone),
        max_active_runs=config.master_max_active_run,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")
        
        get_update_user_details = rail.RepliconServiceOperator(
            task_id="get_update_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": dag_run.conf["useruri"],
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0]
        )
        
        

        get_user_annual_accrued_leaves  = rail.RepliconServiceOperator(
            task_id="get_user_annual_accrued_leaves",
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data= lambda dag_run:  {
                "userUri": dag_run.conf["useruri"],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["user_start_date"], "%Y-%m-%d"),
                    "endDate": rail.parse_date(dag_run.conf["enddate"], "%Y-%m-%d") 
                }
            },  
            data_handler=lambda response: custom_methods.get_user_annual_leaves_taken(config, response)
        )

        set_termination_time_off_balance_policy = rail.RepliconServiceOperator(
            task_id="set_termination_time_off_balance_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: request_payload.get_spain_annual_acquistion_terminated_user_payload(dag_run)
        )
        
        

        update_termination_oef_field = rail.RepliconServiceOperator(
            task_id="update_termination_oef_field",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_update_termination_oef_payload(dag_run),
            trigger_rule="all_success"
        )

        write_log_user_processed_in_replicon = rail.WriteLogOperator(
            task_id="write_log_user_processed_in_replicon",
            log='{{dag_run.conf.disable_log}}',
            message="User processed",
            trigger_rule="all_success",
            properties=lambda dag_run:{
                "employee_id": dag_run.conf["employee_id"],
                "enddate": dag_run.conf["enddate"],
                "employee_first_name": dag_run.conf["first_name"],
                "employee_last_name": dag_run.conf["last_name"],
                "status": "Success",
                "details": "User termination balance processed successfully",
                "ecid": '{{dag_run_ecid()}}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.disable_log}}',
            message="User processed",
            trigger_rule="one_failed",
            severity="Error",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "enddate": dag_run.conf["enddate"],
                "employee_first_name": dag_run.conf["first_name"],
                "employee_last_name": dag_run.conf["last_name"],
                "status": "Failure",
                "details": "User not processed" + rail.render_template('{{get_error_message()}}'),
                "ecid": rail.render_template('{{dag_run_ecid()}}')
            }
        )

        get_update_user_details  >> get_user_annual_accrued_leaves>>\
        set_termination_time_off_balance_policy >> update_termination_oef_field >>\
        write_log_user_processed_in_replicon >> catch_and_log_errors
    
    return dag

rail.for_each_instance(create_airflow_child_dag)