from datetime import timedelta
from airflow.models import Variable
import rail

from crl.user_import_canada_v6.utils import request_payload
from crl.user_import_canada_v6.utils.response_filter import get_filtered_time_off_types, get_policy_to_assign
from crl.user_import_canada_v6.utils.python_callable_methods import get_required_time_off_type_details


def create_child_dag(config):
    timeoff_type_dags = []

    for idx in range(0, config.BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.process_timeoff_type_assignment_new_user_dagid}{get_postfix}",
            description='CRL User Import - Process TIme Off Type Assignment- New User',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_time_off_type_assignment_new_user,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='get_all_time_off_types'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='get_all_time_off_types',
                end_task='catch_and_log_errors',
            )

            get_all_time_off_types = rail.RepliconServiceOperator(
                task_id='get_all_time_off_types',
                endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
                data_handler=get_filtered_time_off_types
            )

            get_required_time_off_type_details_to_assign = rail.PythonOperator(
                task_id='get_required_time_off_type_details_to_assign',
                python_callable=lambda dag_run: get_required_time_off_type_details(dag_run.conf['time_off_types_to_assign'], 'add')
            )

            put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
                task_id="put_timeoff_assignment_for_user",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=request_payload.put_timeoff_assignment_for_user
            )

            for_each_time_off_assign_default_policy = rail.ForEachOperator(
                task_id="for_each_time_off_assign_default_policy",
                items=lambda: rail.result('get_required_time_off_type_details_to_assign')['result'],
                start_task='is_montreal_vacation',
                end_task='for_each_time_off_assign_default_policy_end'
            )

            is_montreal_vacation = rail.IfOperator(
                task_id='is_montreal_vacation',
                test=lambda: rail.result("for_each_time_off_assign_default_policy")['timeoff_type_name'] == "[CAN] Vacances/Vacation",
                yes_task='process_time_off_types_montreal_vacation_new_user',
                no_task='is_service_anniversary_timeoff'
            )

            process_time_off_types_montreal_vacation_new_user = rail.TriggerDagRunOperator(
                task_id='process_time_off_types_montreal_vacation_new_user',
                trigger_dag_id=config.process_timeoff_type_assignment_montreal_vacation_new_user_dagid,
                conf=lambda dag_run:{
                    "timeofftype_name": rail.result("for_each_time_off_assign_default_policy")['timeoff_type_name'],
                    "timeoff_type_uri": rail.result("for_each_time_off_assign_default_policy")['timeoff_type_uri'],
                    "placeholder_timeoff_type_uri": request_payload.get_montreal_vacation_reference_timeoff_uri(dag_run,
                        rail.result("for_each_time_off_assign_default_policy")['timeoff_type_uri']),
                    "adjusted_hire_date": dag_run.conf['adjusted_hire_date'],
                    "useruri": dag_run.conf['useruri'],
                    "std_hrs": dag_run.conf['std_hrs'],
                    "full_part": dag_run.conf['full_part']
                }
                ,
                execution_timeout=timedelta(days=config.execution_timeout_days),
                retries=0,
            )

            is_service_anniversary_timeoff = rail.IfOperator(
                task_id='is_service_anniversary_timeoff',
                test=lambda: rail.result("for_each_time_off_assign_default_policy")['timeoff_type_name'] == "[CAN] Anniversaire de service/ Service Anniversary",
                yes_task='process_time_off_types_service_anniversary_new_user',
                no_task='get_default_time_off_policy_schedule'
            )

            process_time_off_types_service_anniversary_new_user = rail.TriggerDagRunOperator(
                task_id='process_time_off_types_service_anniversary_new_user',
                trigger_dag_id=config.process_timeoff_type_assignment_service_anniversary_new_user_dagid,
                conf=lambda dag_run:{
                    "timeofftype_name": rail.result("for_each_time_off_assign_default_policy")['timeoff_type_name'],
                    "timeoff_type_uri": rail.result("for_each_time_off_assign_default_policy")['timeoff_type_uri'],
                    "placeholder_timeoff_type_uri": request_payload.get_service_anniversary_reference_timeoff_uri(dag_run),
                    "adjusted_hire_date": dag_run.conf['adjusted_hire_date'],
                    "useruri": dag_run.conf['useruri'],
                    "std_hrs": dag_run.conf['std_hrs'],
                }
                ,
                execution_timeout=timedelta(days=config.execution_timeout_days),
                retries=0,
            )

            get_default_time_off_policy_schedule = rail.RepliconServiceOperator(
                task_id="get_default_time_off_policy_schedule",
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
                data=lambda dag_run: request_payload.get_default_timeoff_policy_schedule_payload(
                    dag_run, config.REFERENCE_BASED_TIME_OFF_TYPES,'for_each_time_off_assign_default_policy', config.PERSONAL_DAYS_REFERENCE_TIME_OFF_TYPES),
                data_handler=lambda response, dag_run:get_policy_to_assign(
                    response, config.TIMEOFF_TYPE_POLICY_MODIFY, dag_run,'for_each_time_off_assign_default_policy')
            )

            is_policy_present = rail.IfOperator(
                task_id='is_policy_present',
                test=lambda: bool(rail.result(
                    'get_default_time_off_policy_schedule')),
                yes_task='put_user_timeoff_policy',
                no_task='no_default_timeoff_policy_available'
            )

            put_user_timeoff_policy = rail.RepliconServiceOperator(
                task_id="put_user_timeoff_policy",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data=lambda dag_run:request_payload.get_user_timeoff_policy_payload(dag_run,'for_each_time_off_assign_default_policy')
            )

            no_default_timeoff_policy_available = rail.EmptyOperator(
                task_id='no_default_timeoff_policy_available'
            )

            for_each_time_off_assign_default_policy_end = rail.EmptyOperator(
                task_id='for_each_time_off_assign_default_policy_end'
            )

            is_vacation_to_wait_required = rail.IfOperator(
                task_id='is_vacation_to_wait_required',
                test=lambda: bool(rail.result("process_time_off_types_montreal_vacation_new_user")),
                yes_task='wait_for_process_time_off_types_montreal_vacation_new_user',
                no_task='is_service_anniversary_wait_required'
            )

            wait_for_process_time_off_types_montreal_vacation_new_user = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_time_off_types_montreal_vacation_new_user',
                dag_runs='{{ result("process_time_off_types_montreal_vacation_new_user") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            is_service_anniversary_wait_required = rail.IfOperator(
                task_id='is_service_anniversary_wait_required',
                test=lambda: bool(rail.result("process_time_off_types_service_anniversary_new_user")),
                yes_task='wait_for_process_time_off_types_service_anniversary_new_user',
                no_task='is_any_timeoff_type_to_assign_not_available'
            )

            wait_for_process_time_off_types_service_anniversary_new_user = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_time_off_types_service_anniversary_new_user',
                dag_runs='{{ result("process_time_off_types_service_anniversary_new_user") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            is_any_timeoff_type_to_assign_not_available = rail.IfOperator(
                task_id='is_any_timeoff_type_to_assign_not_available',
                test="{{result('get_required_time_off_type_details_to_assign').time_off_type_exception_log | is_truthy }}",
                yes_task='log_time_off_type_not_available',
                no_task='catch_and_log_errors'
            )

            log_time_off_type_not_available = rail.WriteLogOperator(
                task_id='log_time_off_type_not_available',
                log = '{{ dag_run.conf.user_log }}',
                message="{{result('get_required_time_off_type_details_to_assign').time_off_type_exception_log }}",
                severity="Exception",
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    'first_name': '{{dag_run.conf.first_name}}',
                    'last_name': '{{dag_run.conf.last_name}}',
                    "action": "Add",
                    "status": 'Exception',
                    'details': "{{result('get_required_time_off_type_details_to_assign').time_off_type_exception_log }}",
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log="{{ dag_run.conf.user_log}}",
                trigger_rule='one_failed',
                severity='Error',
                message='{{"User Added Partially, Error: "}}{{ get_error_message() }}',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    'first_name': '{{dag_run.conf.first_name}}',
                    'last_name': '{{dag_run.conf.last_name}}',
                    'action': "Add",
                    'status': 'Error',
                    'details': '{{"User Added Partially, Error: "}}{{ get_error_message() }}'
                },
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done',
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> get_all_time_off_types

            get_all_time_off_types >> get_required_time_off_type_details_to_assign >> put_timeoff_assignment_for_user >> for_each_time_off_assign_default_policy
            for_each_time_off_assign_default_policy >> for_each_time_off_assign_default_policy_end

            for_each_time_off_assign_default_policy >> is_montreal_vacation

            is_montreal_vacation >> rail.Label('Yes') >> process_time_off_types_montreal_vacation_new_user
            process_time_off_types_montreal_vacation_new_user >> for_each_time_off_assign_default_policy_end
            is_montreal_vacation >> rail.Label('No') >> is_service_anniversary_timeoff

            is_service_anniversary_timeoff >> rail.Label('Yes') >> process_time_off_types_service_anniversary_new_user
            process_time_off_types_service_anniversary_new_user >> for_each_time_off_assign_default_policy_end
            is_service_anniversary_timeoff >> rail.Label('No') >> get_default_time_off_policy_schedule


            get_default_time_off_policy_schedule >> is_policy_present
            is_policy_present >> rail.Label('No') >> no_default_timeoff_policy_available >> for_each_time_off_assign_default_policy_end
            is_policy_present >> rail.Label( 'Yes') >> put_user_timeoff_policy >> for_each_time_off_assign_default_policy_end

            for_each_time_off_assign_default_policy_end >> is_vacation_to_wait_required
            is_vacation_to_wait_required >> rail.Label( 'Yes') >> wait_for_process_time_off_types_montreal_vacation_new_user
            is_vacation_to_wait_required >> rail.Label( 'No') >> is_service_anniversary_wait_required

            is_service_anniversary_wait_required >> rail.Label( 'Yes') >> wait_for_process_time_off_types_service_anniversary_new_user
            wait_for_process_time_off_types_service_anniversary_new_user >> is_any_timeoff_type_to_assign_not_available
            is_service_anniversary_wait_required >> rail.Label( 'No') >> is_any_timeoff_type_to_assign_not_available

            wait_for_process_time_off_types_montreal_vacation_new_user >> is_service_anniversary_wait_required
            is_any_timeoff_type_to_assign_not_available >> rail.Label('Yes') >> log_time_off_type_not_available
            log_time_off_type_not_available >> catch_and_log_errors

            is_any_timeoff_type_to_assign_not_available >> rail.Label('No') >> catch_and_log_errors >> log_to_sumo

        timeoff_type_dags.append(dag)

    return timeoff_type_dags


rail.for_each_instance(create_child_dag)
