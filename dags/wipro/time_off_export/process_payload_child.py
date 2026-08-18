from datetime import timedelta
import rail
from airflow.models import Variable
from wipro.time_off_export.utils import custom_methods,response_filter

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f'Wipro TimeOff Export Process Payload Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_process_batch_task, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_log",
            end_task="catch_and_log_errors"
        )

        create_log = rail.CreateLogOperator(
             task_id='create_log'
        )

        if_timeoff_status_not_deleted = rail.IfOperator(
            task_id = 'if_timeoff_status_not_deleted',
            test=lambda dag_run: "approved" in (dag_run.conf['data'].get("timeOffStatusUri","")).lower() or \
                "waiting" in (dag_run.conf['data'].get("timeOffStatusUri","")).lower() or \
                    "rejected" in (dag_run.conf['data'].get("timeOffStatusUri","")).lower(),
            yes_task= 'get_non_deleted_timeoff_details',
            no_task= 'get_deleted_timeoff_details'
        )

        get_non_deleted_timeoff_details = rail.RepliconServiceOperator(
            task_id = 'get_non_deleted_timeoff_details',
            endpoint= '/services/TimeOffService1.svc/GetTimeOffDetails2',
            data= {
                "timeOffUri": '{{ dag_run.conf.data.timeOff.uri }}'
            },
            data_handler= response_filter.get_non_deleted_timeoff_details
        )

        get_approval_history_details = rail.RepliconServiceOperator(
            task_id = 'get_approval_history_details',
            endpoint= '/services/TimeOffApprovalService1.svc/BulkGetApprovalHistoryDetails',
            data= {
                "timeOffUris": ['{{ dag_run.conf.data.timeOff.uri }}']
            },
            data_handler= response_filter.get_approval_history_details
        )

        get_deleted_timeoff_details = rail.PythonOperator(
            task_id = 'get_deleted_timeoff_details',
            python_callable= response_filter.get_deleted_timeoff_details
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id = 'get_user_details',
            endpoint= '/services/ImportService2.svc/GetUserDetails',
            data=lambda dag_run: {
                "user": {
                    "uri": rail.result("get_non_deleted_timeoff_details")['user_uri'] if rail.result(
                        "get_non_deleted_timeoff_details") else dag_run.conf['data']['owner']['uri']
                },
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler= response_filter.get_user_details
        )

        is_employee_id_blank = rail.IfOperator(
            task_id = 'is_employee_id_blank',
            test= '{{ result("get_user_details").employee_id | is_falsy }}',
            yes_task= 'log_employee_id_exception',
            no_task= 'get_manager_details_in_replicon'
        )

        log_employee_id_exception = rail.WriteLogOperator(
            task_id="log_employee_id_exception",
            log='{{ result("create_log") }}',
            message="employee id is blank in replicon",
            properties=lambda dag_run:{
                "employee_id": '',
                "work_item_id": dag_run.conf['data']["timeOff"]["uri"],
                "booking_start_date": rail.result("get_non_deleted_timeoff_details")['start_date'] if rail.result(
                    "get_non_deleted_timeoff_details") else rail.result("get_deleted_timeoff_details")['start_date'],
                "booking_end_date": rail.result("get_non_deleted_timeoff_details")['end_date'] if rail.result(
                    "get_non_deleted_timeoff_details") else rail.result("get_deleted_timeoff_details")['end_date'],
                "event": custom_methods.get_event(dag_run),
                "status": "Skipped",
                "response": "employee id is blank in replicon"
            }
        )

        get_manager_details_in_replicon = rail.RepliconServiceOperator(
            task_id = 'get_manager_details_in_replicon',
            endpoint= '/services/ImportService2.svc/GetUserDetails',
            data= {
                "user": {
                    "uri": '{{ result("get_user_details").manager_uri }}'
                },
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler= lambda response: {
                    "employee_id": response['employeeId'],
                    "name": response['firstName'] +" "+ response['lastName']
                }
            )
        
        get_country_details = rail.RepliconServiceOperator(
            task_id = 'get_country_details',
            endpoint= '/services/ServiceCenterService1.svc/GetServiceCenterDetails',
            data= {
                "serviceCenterUri": '{{ result("get_user_details").country_uri }}'
            }
        )

        get_acting_user_empid_in_replicon = rail.RepliconServiceOperator(
            task_id = 'get_acting_user_empid_in_replicon',
            endpoint= '/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: {
                "users": [
                    {
                        "loginName": dag_run.conf['data']['authority']['actingUser']['loginName'] if dag_run.conf[
                            'data']['authority'].get('actingUser',{}) else ''
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler= lambda response: response[0]['userDetails']['employeeId'] if response else []
        )

        process_timeoff_data_to_submit = rail.TriggerDagRunOperator(
            task_id="process_timeoff_data_to_submit",
            trigger_dag_id=config.submit_timeoff_data_dag_id,
            conf= custom_methods.get_submit_child_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.PythonOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            python_callable=custom_methods.catch_and_log_errors
        )

        should_log_error = rail.IfOperator(
            task_id='should_log_error',
            test=lambda: rail.result('catch_and_log_errors')['should_log'],
            yes_task='write_error_log'
        )

        write_error_log = rail.WriteLogOperator(
            task_id='write_error_log',
            log='{{ result("create_log") }}',
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties=lambda: rail.result('catch_and_log_errors')['properties']
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label("No") >>\
            create_log >> if_timeoff_status_not_deleted >> rail.Label(
            "Yes") >> get_non_deleted_timeoff_details >> get_approval_history_details >> get_user_details

        if_timeoff_status_not_deleted >> rail.Label(
            "No") >> get_deleted_timeoff_details >> get_user_details >> is_employee_id_blank
        
        is_employee_id_blank >> rail.Label(
            "Yes") >> log_employee_id_exception >> catch_and_log_errors
        
        is_employee_id_blank >> rail.Label(
            "No") >> get_manager_details_in_replicon >> get_country_details >> \
                get_acting_user_empid_in_replicon >> process_timeoff_data_to_submit >> catch_and_log_errors

        catch_and_log_errors >> should_log_error >> rail.Label("Yes") >> write_error_log

    return dag

rail.for_each_instance(create_child_dag_wbs)
