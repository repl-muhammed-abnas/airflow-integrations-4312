
from datetime import timedelta
import json
from airflow.models import Variable
import rail

from crl.user_import_usa_v4.utils import request_payload, python_callable_methods
from crl.user_import_usa_v4.utils.response_filter import get_filtered_time_off_types, assigned_timeoffs_types_to_user, get_policy_to_assign_for_timeoff

# pylint: disable=too-many-statements
def create_child_dag(config):
    timeoff_type_dags = []

    for idx in range(0, config.BATCH_COUNT):

        with rail.create_airflow_dag(
            dag_id=f"{config.process_timeoff_type_assignment_update_rehire_user_dagid}_batch_{idx+1}",
            description='CRL User Import USA - Process TIme Off Type Assignment- Update User',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_time_off_type_assignment_update_rehire_user,
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

            get_user_time_off_policy_summary = rail.RepliconServiceOperator(
                task_id="get_user_time_off_policy_summary",
                endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
                data={
                    "userUri": "{{ dag_run.conf.useruri }}"
                },
                data_handler= assigned_timeoffs_types_to_user
            )

            has_time_off_assignment = rail.IfOperator(
                task_id='has_time_off_assignment',
                test="{{ dag_run.conf.time_off_types_to_assign | is_truthy }}",
                yes_task='get_required_time_off_type_details_to_assign',
                no_task='is_rehire_user'
            )

            is_rehire_user = rail.IfOperator(
                task_id="is_rehire_user",
                test=lambda dag_run: bool(dag_run.conf['rehire']=='Yes'),
                yes_task="catch_and_log_errors",
                no_task="process_time_off_type_no_accrual"
            )

            process_time_off_type_no_accrual= rail.TriggerDagRunOperator(
                task_id='process_time_off_type_no_accrual',
                trigger_dag_id=config.process_timeoff_type_no_accrual_dagid,
                conf=lambda dag_run:{
                    "employee_id": dag_run.conf['employee_id'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    'end_date': dag_run.conf['end_date'],
                    'useruri': dag_run.conf['useruri'],
                    'starting_balance_script_uri': dag_run.conf['starting_balance_script_uri'],
                    'prevent_balance_overdraw_uri': dag_run.conf['prevent_balance_overdraw_uri'],
                    "user_log": dag_run.conf['user_log'],
                    'todays_date':dag_run.conf['todays_date'],
                    "action": 'update',
                    "change_effective_date": dag_run.conf['change_effective_date'],
                    'event': dag_run.conf['event'],
                    'event_reason_code':dag_run.conf['event_reason_code'],

                },
                execution_timeout=timedelta(hours=config.execution_timeout_days),
                retries=0,
            )

            wait_for_process_time_off_type_no_accrual = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_time_off_type_no_accrual',
                dag_runs='{{ result("process_time_off_type_no_accrual") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            gather_time_off_type_error_logs_disable_user = rail.GatherResultsFromDagRunsOperator(
                task_id='gather_time_off_type_error_logs_disable_user',
                dag_runs='{{ result("process_time_off_type_no_accrual") }}',
                dagrun_task_id='catch_and_log_errors',
                flatten=True,
            )

            has_any_error_present = rail.IfOperator(
                task_id="has_any_error_present",
                test="{{ result('gather_time_off_type_error_logs_disable_user') | is_truthy }}",
                yes_task= 'log_error_present',
                no_task='catch_and_log_errors'
            )

            log_error_present = rail.EmptyOperator(
                task_id='log_error_present'
            )

            get_required_time_off_type_details_to_assign = rail.PythonOperator(
                task_id='get_required_time_off_type_details_to_assign',
                python_callable=lambda dag_run: python_callable_methods.get_required_time_off_type_details(
                    dag_run.conf['time_off_types_to_assign'],"update", config.MANNUAL_TIMEOFF_TYPES),
            )

            is_time_off_type_availabe_in_replicon = rail.IfOperator(
                task_id='is_time_off_type_availabe_in_replicon',
                test="{{result('get_required_time_off_type_details_to_assign').result | is_truthy }}",
                yes_task='put_timeoff_assignment_for_user',
                no_task='log_time_off_type_not_available'
            )

            put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
                task_id="put_timeoff_assignment_for_user",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=request_payload.put_timeoff_assignment_for_user
            )

            assigned_time_offs_types = rail.PythonOperator(
                task_id='assigned_time_offs_types',
                python_callable=python_callable_methods.assigned_time_offs_types
            )

            time_off_types_to_be_disabled = rail.PythonOperator(
                task_id='time_off_types_to_be_disabled',
                python_callable=python_callable_methods.time_off_types_to_be_disabled
            )

            is_time_off_types_to_be_disabled = rail.IfOperator(
                task_id = "is_time_off_types_to_be_disabled",
                test=lambda: bool(rail.result('time_off_types_to_be_disabled')),
                yes_task = "for_each_time_off_type_no_accural",
                no_task = "time_off_types_to_be_assigned"
            )

            for_each_time_off_type_no_accural = rail.ForEachOperator(
                task_id="for_each_time_off_type_no_accural",
                items=lambda: rail.result('time_off_types_to_be_disabled'),
                start_task='get_balance_summary_for_user',
                end_task='for_each_time_off_type_no_accural_end'
            )

            get_balance_summary_for_user = rail.RepliconServiceOperator(
                task_id="get_balance_summary_for_user",
                endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
                data=lambda dag_run:{
                    "account": {
                        "userUri": dag_run.conf['useruri'],
                        "timeOffTypeUri": rail.result('for_each_time_off_type_no_accural')['timeoff_type_uri']
                    },
                    "asOfDate": request_payload.get_replicon_date(dag_run.conf['change_effective_date'])
                    }
            )

            get_historical_policy_to_assign_list_disable_user = rail.PythonOperator(
                task_id='get_historical_policy_to_assign_list_disable_user',
                python_callable=lambda dag_run: python_callable_methods.get_historical_policy_to_assign_list(
                    dag_run,'update', 'for_each_time_off_type_no_accural', config)
            )

            get_no_accrual_policy_line = rail.PythonOperator(
                task_id='get_no_accrual_policy_line',
                python_callable=lambda dag_run: python_callable_methods.get_no_accrual_policy_line(dag_run, 'update')
            )

            get_all_policy_to_assign_for_disable_user = rail.PythonOperator(
                task_id='get_all_policy_to_assign_for_disable_user',
                python_callable=python_callable_methods.get_all_policy_to_assign_for_disable_user
            )

            put_user_timeoff_policy_schedule_blank_policy = rail.RepliconServiceOperator(
                task_id="put_user_timeoff_policy_schedule_blank_policy",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data=lambda dag_run:{
                    "timeOffAccount": {
                        "userUri": dag_run.conf['useruri'],
                        "timeOffTypeUri": rail.result('for_each_time_off_type_no_accural')['timeoff_type_uri']
                    },
                    "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign_for_disable_user'))
                }
            )

            for_each_time_off_type_no_accural_end = rail.EmptyOperator(
                task_id='for_each_time_off_type_no_accural_end'
            )

            time_off_types_to_be_assigned = rail.PythonOperator(
                task_id='time_off_types_to_be_assigned',
                python_callable=lambda dag_run: python_callable_methods.time_off_types_to_be_assigned_update(
                    dag_run,config)
            )

            is_time_off_types_to_be_assigned = rail.IfOperator(
                task_id = "is_time_off_types_to_be_assigned",
                test=lambda: bool(rail.result('time_off_types_to_be_assigned')),
                yes_task = "for_each_time_off_type_policy",
                no_task = "is_any_timeoff_type_to_assign_not_available"
            )

            for_each_time_off_type_policy = rail.ForEachOperator(
                task_id="for_each_time_off_type_policy",
                items=lambda: rail.result('time_off_types_to_be_assigned'),
                start_task='is_vacation_to_type',
                end_task='for_each_time_off_policy_end'
            )

            is_vacation_to_type = rail.IfOperator(
                task_id = "is_vacation_to_type",
                test=lambda: rail.result("for_each_time_off_type_policy")['timeoff_type_name'] == "[USA] Vacation",
                yes_task = "get_balance_summary_for_user_vacation_to_type",
                no_task = "get_historical_policy_to_assign_list"
            )

            get_balance_summary_for_user_vacation_to_type = rail.RepliconServiceOperator(
                task_id="get_balance_summary_for_user_vacation_to_type",
                endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
                data=lambda dag_run:{
                    "account": {
                        "userUri": dag_run.conf['useruri'],
                        "timeOffTypeUri": rail.result('for_each_time_off_type_policy')['timeoff_type_uri']
                    },
                    "asOfDate": request_payload.get_replicon_date(dag_run.conf['change_effective_date'])
                    }
            )

            get_historical_policy_to_assign_list = rail.PythonOperator(
                task_id='get_historical_policy_to_assign_list',
                python_callable=lambda dag_run: python_callable_methods.get_historical_policy_to_assign_list(
                    dag_run,'update' if dag_run.conf['rehire']!='Yes' else 'rehire','for_each_time_off_type_policy', config)
            )

            get_default_time_off_policy_schedule = rail.RepliconServiceOperator(
                task_id="get_default_time_off_policy_schedule",
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
                data=lambda dag_run: request_payload.get_default_timeoff_policy_set_schedule_for_timeofftype(
                    dag_run, config,'for_each_time_off_type_policy'),
                data_handler=lambda response, dag_run:get_policy_to_assign_for_timeoff(
                    response,dag_run, 'for_each_time_off_type_policy',config)
            )

            get_all_policy_to_assign = rail.PythonOperator(
                task_id='get_all_policy_to_assign',
                python_callable= python_callable_methods.get_all_policy_to_assign_update
            )

            is_policy_to_assign_present = rail.IfOperator(
                task_id='is_policy_to_assign_present',
                test=lambda: bool(rail.result('get_all_policy_to_assign')),
                yes_task='put_user_timeoff_policy',
                no_task='no_timeoff_policy_line_available'
            )

            put_user_timeoff_policy = rail.RepliconServiceOperator(
                task_id="put_user_timeoff_policy",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data=lambda dag_run:request_payload.get_update_user_timeoff_policy_payload(dag_run,'for_each_time_off_type_policy')
            )

            no_timeoff_policy_line_available = rail.EmptyOperator(
                task_id='no_timeoff_policy_line_available'
            )

            for_each_time_off_policy_end = rail.EmptyOperator(
                task_id='for_each_time_off_policy_end'
            )

            get_hidden_oefs_payload = rail.PythonOperator(
                task_id= "get_hidden_oefs_payload",
                python_callable= lambda dag_run:request_payload.get_oefs(dag_run,("update" if dag_run.conf['rehire']!='Yes' else "rehire"))
            )

            is_update_hidden_oefs_needed = rail.IfOperator(
                task_id="is_update_hidden_oefs_needed",
                test=lambda: bool(rail.result("get_hidden_oefs_payload")),
                yes_task="update_placeholder_to_hidden_oefs_values",
                no_task="is_any_timeoff_type_to_assign_not_available"
            )

            update_placeholder_to_hidden_oefs_values = rail.RepliconServiceOperator(
                task_id='update_placeholder_to_hidden_oefs_values',
                endpoint='/services/ImportService1.svc/ApplyUserModifications3',
                data=lambda dag_run: {
                    "user": {
                        "uri": dag_run.conf['useruri']
                    },
                    "modifications": {
                        "objectExtensionFieldsToApply": rail.result("get_hidden_oefs_payload")
                        },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
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
                    "action": "Update",
                    "status": 'Exception',
                    'details': "{{result('get_required_time_off_type_details_to_assign').time_off_type_exception_log }}",
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log="{{ dag_run.conf.user_log }}",
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    'first_name': '{{dag_run.conf.first_name}}',
                    'last_name': '{{dag_run.conf.last_name}}',
                    'action': 'Update',
                    'status': 'Error',
                    'details': '{{ get_error_message() }}',
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

            get_all_time_off_types >> get_user_time_off_policy_summary >> has_time_off_assignment >> rail.Label('No') >> is_rehire_user

            is_rehire_user >> rail.Label('Yes') >> catch_and_log_errors
            is_rehire_user >> rail.Label('No') >> process_time_off_type_no_accrual >> wait_for_process_time_off_type_no_accrual
            wait_for_process_time_off_type_no_accrual >> gather_time_off_type_error_logs_disable_user
            gather_time_off_type_error_logs_disable_user >> has_any_error_present >> rail.Label('Yes') >> log_error_present >> catch_and_log_errors
            has_any_error_present >> rail.Label('No') >> catch_and_log_errors

            has_time_off_assignment >> rail.Label('Yes') >> get_required_time_off_type_details_to_assign
            get_required_time_off_type_details_to_assign >> is_time_off_type_availabe_in_replicon

            is_time_off_type_availabe_in_replicon >> rail.Label('No') >> log_time_off_type_not_available

            is_time_off_type_availabe_in_replicon >> rail.Label('Yes') >> put_timeoff_assignment_for_user >> assigned_time_offs_types
            assigned_time_offs_types >> time_off_types_to_be_disabled >> is_time_off_types_to_be_disabled

            is_time_off_types_to_be_disabled >> rail.Label('Yes') >> for_each_time_off_type_no_accural >> get_balance_summary_for_user
            get_balance_summary_for_user >> get_historical_policy_to_assign_list_disable_user
            get_historical_policy_to_assign_list_disable_user >> get_no_accrual_policy_line >> get_all_policy_to_assign_for_disable_user
            get_all_policy_to_assign_for_disable_user >> put_user_timeoff_policy_schedule_blank_policy
            put_user_timeoff_policy_schedule_blank_policy >> for_each_time_off_type_no_accural_end

            for_each_time_off_type_no_accural >> for_each_time_off_type_no_accural_end >> time_off_types_to_be_assigned

            is_time_off_types_to_be_disabled >> rail.Label('No') >> time_off_types_to_be_assigned >> is_time_off_types_to_be_assigned

            is_time_off_types_to_be_assigned >> rail.Label('Yes') >> for_each_time_off_type_policy
            is_time_off_types_to_be_assigned >> rail.Label('No') >> is_any_timeoff_type_to_assign_not_available

            for_each_time_off_type_policy >> for_each_time_off_policy_end

            for_each_time_off_type_policy >> is_vacation_to_type >> rail.Label("No") >> get_historical_policy_to_assign_list
            is_vacation_to_type >> rail.Label("Yes") >> get_balance_summary_for_user_vacation_to_type >> get_historical_policy_to_assign_list

            get_historical_policy_to_assign_list >> get_default_time_off_policy_schedule >> get_all_policy_to_assign

            get_all_policy_to_assign >> is_policy_to_assign_present >> rail.Label('Yes') >> put_user_timeoff_policy
            is_policy_to_assign_present >> rail.Label('No') >> no_timeoff_policy_line_available >> for_each_time_off_policy_end

            put_user_timeoff_policy >> for_each_time_off_policy_end

            for_each_time_off_policy_end >> get_hidden_oefs_payload >> is_update_hidden_oefs_needed

            is_update_hidden_oefs_needed >> rail.Label('Yes') >> update_placeholder_to_hidden_oefs_values >> is_any_timeoff_type_to_assign_not_available
            is_update_hidden_oefs_needed >> rail.Label('No') >> is_any_timeoff_type_to_assign_not_available

            is_any_timeoff_type_to_assign_not_available >> rail.Label('Yes') >> log_time_off_type_not_available
            is_any_timeoff_type_to_assign_not_available >> rail.Label('No') >> catch_and_log_errors

            log_time_off_type_not_available >> catch_and_log_errors >> log_to_sumo

        timeoff_type_dags.append(dag)

    return timeoff_type_dags

rail.for_each_instance(create_child_dag)
