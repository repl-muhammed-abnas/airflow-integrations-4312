from datetime import timedelta
import rail
from avenu.user_import.utils import request_payload
from avenu.user_import.utils import python_callable_method
from avenu.user_import.utils import response_filter
from airflow.models import Variable

null = None


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'avenu_user_sync_process_time_off_policy_rehire_user_{config.instance}_child',
        description='Avenu User Sync Process Time off Policy For Rehire user',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_policy_update_rehire_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "get_specific_user_time_off_policy_summary"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_specific_user_time_off_policy_summary',
            end_task="catch_and_log_errors",
        )

        get_specific_user_time_off_policy_summary = rail.RepliconServiceOperator(
            task_id="get_specific_user_time_off_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_time_off_policy_summary,
            response_filter=response_filter.get_specific_user_time_off_assigned
        )

        get_default_timeoff_policy_set_schedule_for_timeofftype_rehire = rail.RepliconServiceOperator(
            task_id="get_default_timeoff_policy_set_schedule_for_timeofftype_rehire",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=request_payload.get_default_timeoff_policy_set_schedule_for_timeofftype
        )

        has_any_default_policy = rail.IfOperator(
            task_id = "has_any_default_policy",
            test="{{ result('get_default_timeoff_policy_set_schedule_for_timeofftype_rehire') | is_truthy }}",
            yes_task="get_user_info"
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri":  '{{ dag_run.conf.useruri }}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            response_filter=lambda res: res.json()['d'][0]
        )

        get_tenure = rail.PythonOperator(
            task_id='get_tenure',
            python_callable=python_callable_method.get_tenure
        )

        get_default_policy_set_rehire = rail.PythonOperator(
            task_id='get_default_policy_set_rehire',
            python_callable=python_callable_method.get_default_policy_set_rehire
        )

        put_user_timeoff_policy_schedule_rehire = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy_schedule_rehire",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_user_timeoff_policy_schedule_rehire
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log_error}}",
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Error',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_specific_user_time_off_policy_summary \
            >> get_default_timeoff_policy_set_schedule_for_timeofftype_rehire >> has_any_default_policy
        has_any_default_policy >> rail.Label("Yes") >> get_user_info
        get_user_info >> get_tenure >> get_default_policy_set_rehire
        get_default_policy_set_rehire >> put_user_timeoff_policy_schedule_rehire >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
