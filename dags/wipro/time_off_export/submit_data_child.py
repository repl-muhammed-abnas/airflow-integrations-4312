import json
from wipro.efforts_submit.custom_http_operator.CustomSimpleHttpOperator2 import CustomSimpleHttpOperator2
import rail


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.submit_timeoff_data_dag_id,
        description=f"Wipro Timeoff Export Submit Timeoff data child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_second_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        instance = "uat" if "uat" in  config.instance else  config.instance

        timeoff_export_to_wipro = CustomSimpleHttpOperator2(
            task_id="timeoff_export_to_wipro",
            http_conn_id="wipro_http_effort_submit",
            endpoint="h2r/my-time/1.0.0/leaveTransaction",
            method="POST",
            auth_type=None,
            headers={
                "Authorization": "Bearer " + '{{var.value.wipro_efforts_submission_bearer_token_variable_'+instance+'}}',
                'Content-Type': 'application/json',
                "sourceSystemId": "REPLICON",
            },
            data=lambda dag_run: json.dumps(dag_run.conf["data"], ensure_ascii=False).encode("utf-8"),
            log_response = True
        )

        def get_response():
            data = json.loads(rail.result('timeoff_export_to_wipro'))
            if not data:
                return []
            return {
                "status": "Failed" if data['leaveTransactionSubmit'].get('message',{}) else 'Success',
                "response": data['leaveTransactionSubmit']['message'] if data['leaveTransactionSubmit'].get(
                    'message',{}) else data['leaveTransactionSubmit']['d'].get('ReturnMessage', '')
            }

        log_timeoff_export_sucess = rail.WriteLogOperator(
            task_id="log_timeoff_export_sucess",
            log='{{ dag_run.conf.log }}',
            message="Time off data exported successfully",
            properties=lambda dag_run:{
                "employee_id": dag_run.conf["employee_id"],
                "work_item_id": dag_run.conf["work_item_iD"],
                "booking_start_date": dag_run.conf["booking_start_date"],
                "booking_end_date": dag_run.conf["booking_end_date"],
                "event": dag_run.conf["event"],
                "status": get_response()['status'],
                "response": get_response()['response']
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            extra_info= lambda dag_run:{
                "employee_id": dag_run.conf["employee_id"],
                "work_item_id": dag_run.conf["work_item_iD"],
                "booking_start_date": dag_run.conf["booking_start_date"],
                "booking_end_date": dag_run.conf["booking_end_date"],
                "event": dag_run.conf["event"],
                "status": get_response()['status'],
                "response": get_response()['response']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "work_item_id": dag_run.conf["work_item_iD"],
                "booking_start_date": dag_run.conf["booking_start_date"],
                "booking_end_date": dag_run.conf["booking_end_date"],
                "event": dag_run.conf["event"],
                "status": "Error",
                "response": rail.render_template('{{ get_error_message() }}')
            }
        )

        timeoff_export_to_wipro >> log_timeoff_export_sucess >> log_to_sumo >> catch_and_log_errors

    return dag

rail.for_each_instance(create_airflow_child_dag)
