from datetime import timedelta
import rail
from airflow.models import Variable
from tsystems.office_schedule_api_import_v2.utils import request_payload, custom_methods



def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.assign_schedule_dag_id,
        description=f'{config.company_key} office schedule file import assign schedule child{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.schedule_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_assignment_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_assignment_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_assignment_log = rail.CreateLogOperator(
            task_id="create_assignment_log"
        )
        
        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": None,
                        "loginName": None,
                        "employeeId": dag_run.conf["employee_id"],
                        "parameterCorrelationId": None
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
        )

        if_user_availablein_replicon = rail.IfOperator(
            task_id='if_user_availablein_replicon',
            test=lambda: bool(rail.result('get_user_details')),
            yes_task='if_valid_from_date_is_less_than_user_start_date',
            no_task='log_exception_user_not_available'
        )

        log_exception_user_not_available = rail.WriteLogOperator(
            task_id='log_exception_user_not_available',
            log='{{ result("create_assignment_log") }}',
            message="User not available in Replicon",
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employee_id'],
                'schedule_name': dag_run.conf['schedule_name'],
                'holiday_calendar_name': dag_run.conf.get('holiday_calendar_name') or '',
                'action': 'Validation',
                'status': 'Exception',
                "details": "User not available in Replicon"
            }
        )

        if_valid_from_date_is_less_than_user_start_date = rail.IfOperator(
            task_id='if_valid_from_date_is_less_than_user_start_date',
            test=custom_methods.check_if_valid_from_date_is_less_than_user_start_date,
            yes_task='update_user_with_schedule',
            no_task='log_exception_before_start_date'
        )

        update_user_with_schedule = rail.RepliconServiceOperator(
            task_id="update_user_with_schedule",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_update_user_payload(
                dag_run)
        )

        log_exception_before_start_date = rail.WriteLogOperator(
            task_id='log_exception_before_start_date',
            log='{{ result("create_assignment_log") }}',
            message="Valid from date can not be prior to user start date",
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employee_id'],
                'schedule_name': dag_run.conf['schedule_name'],
                'holiday_calendar_name': dag_run.conf.get('holiday_calendar_name') or '',
                'action': 'Validation',
                'status': 'Exception',
                "details": "Valid from date can not be prior to user start date"
            }
        )

        log_assignment_result = rail.WriteLogOperator(
            task_id='log_assignment_result',
            log='{{ result("create_assignment_log") }}',
            message="User schedule and holiday calendar assignment result",
            severity=custom_methods.get_assignment_log_severity,
            properties=custom_methods.get_assignment_log_properties
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_assignment_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employee_id': '{{ dag_run.conf["employee_id"] }}',
                'schedule_name': '{{ dag_run.conf["schedule_name"] }}',
                'holiday_calendar_name': '{{ dag_run.conf.get("holiday_calendar_name", "") }}',
                'action': 'Schedule and Holiday Calendar Assignment',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            },
        )


        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_assignment_log >> get_user_details >> if_user_availablein_replicon
        if_user_availablein_replicon >> rail.Label('Yes') >> if_valid_from_date_is_less_than_user_start_date

        if_valid_from_date_is_less_than_user_start_date >> rail.Label('Yes') >>update_user_with_schedule >> log_assignment_result >> catch_and_log_errors
        if_valid_from_date_is_less_than_user_start_date >> rail.Label('No') >> log_exception_before_start_date >> catch_and_log_errors

        if_user_availablein_replicon >> rail.Label('No') >> log_exception_user_not_available >> catch_and_log_errors

        return dag


rail.for_each_instance(create_dag)