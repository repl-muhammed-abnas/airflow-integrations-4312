from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_timeoff_add_rehire_user_update_dag_id,
        description=f'Momentive_Japan_Timeoff_add_rehire_user_update_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='parse_hire_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='parse_hire_date',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        parse_hire_date = rail.PythonOperator(
            task_id='parse_hire_date',
            python_callable=lambda dag_run: {
                'day': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').day,
                'month': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').month,
                'year': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').year
            }
        )

        create_child_trigger_list = rail.SetVariableOperator(
            task_id='create_child_trigger_list',
            name='childtriggeredlist',
            append=False,
            value=[]
        )

        check_workshift_changed_date_present = rail.IfOperator(
            task_id='check_workshift_changed_date_present',
            test='{{ dag_run.conf.workshift_changeddate | is_truthy }}',
            yes_task='parse_workshift_changed_date',
            no_task='extract_employment_month'
        )

        parse_workshift_changed_date = rail.PythonOperator(
            task_id='parse_workshift_changed_date',
            python_callable=lambda dag_run: {
                'day': datetime.strptime(dag_run.conf['workshift_changeddate'], '%Y-%m-%d').day,
                'month': datetime.strptime(dag_run.conf['workshift_changeddate'], '%Y-%m-%d').month,
                'year': datetime.strptime(dag_run.conf['workshift_changeddate'], '%Y-%m-%d').year
            }
        )

        extract_employment_month = rail.PythonOperator(
            task_id='extract_employment_month',
            python_callable=lambda dag_run: {
                'employment_month': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').month
            }
        )

        determine_yearly_accrual = rail.PythonOperator(
            task_id='determine_yearly_accrual',
            python_callable=lambda dag_run: {
                'yearly_accrual': python_callable.get_timeoff_accrual_hours(
                    rail.result('extract_employment_month')['employment_month'])
            }
        )

        check_timeoff_uri_present = rail.IfOperator(
            task_id='check_timeoff_uri_present',
            test=lambda dag_run: dag_run.conf['timeoffuri'] is not None and dag_run.conf['timeoffuri'] != '',
            yes_task='check_timeoff_name_new_hire',
            no_task='catch_error'
        )

        check_timeoff_name_new_hire = rail.IfOperator(
            task_id='check_timeoff_name_new_hire',
            test=lambda dag_run: dag_run.conf.get('timeoffname', '') == '02. JPN_年次有給休暇 Annual Paid Leave (New Hire)',
            yes_task='catch_error',
            no_task='check_continuous_service_date_present'
        )

        check_continuous_service_date_present = rail.IfOperator(
            task_id='check_continuous_service_date_present',
            test=lambda dag_run: dag_run.conf['continuous_service_date'] is not None and dag_run.conf['continuous_service_date'] != '',
            yes_task='check_timeoff_name_fixed_term_standard',
            no_task='check_eligible_for_default_policy'
        )

        check_timeoff_name_fixed_term_standard = rail.IfOperator(
            task_id='check_timeoff_name_fixed_term_standard',
            test=lambda dag_run: dag_run.conf.get('timeoffname', '') == '05. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Standard)',
            yes_task='trigger_standard_parttime_policy_assignment_update',
            no_task='check_timeoff_name_fixed_term_parttime'
        )

        trigger_standard_parttime_policy_assignment_update = rail.TriggerDagRunOperator(
            task_id='trigger_standard_parttime_policy_assignment_update',
            trigger_dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_fixed_term_standard_parttime_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf.get('loginname', ''),
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "add",
                "timeoffuri": dag_run.conf['timeoffuri'],
                "timeofftype": dag_run.conf['timeoffname'],
                "yoss": dag_run.conf.get('continuous_service_date'),
                "yos": dag_run.conf.get('continuous_service_date')
            }
        )

        insert_childid_to_wait_list_1 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_1',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_standard_parttime_policy_assignment_update')}}"
        )

        check_timeoff_name_fixed_term_parttime = rail.IfOperator(
            task_id='check_timeoff_name_fixed_term_parttime',
            test=lambda dag_run: dag_run.conf.get('timeoffname', '') == '04. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Part Time)',
            yes_task='trigger_parttime_policy_assignment_update',
            no_task='check_timeoff_name_regular'
        )

        trigger_parttime_policy_assignment_update = rail.TriggerDagRunOperator(
            task_id='trigger_parttime_policy_assignment_update',
            trigger_dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_fixed_term_parttime_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf.get('loginname', ''),
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "add",
                "timeoffuri": dag_run.conf['timeoffuri'],
                "timeofftype": dag_run.conf['timeoffname'],
                "yoss": dag_run.conf.get('continuous_service_date'),
                "yos": dag_run.conf.get('continuous_service_date')
            }
        )

        insert_childid_to_wait_list_2 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_2',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_parttime_policy_assignment_update')}}"
        )

        check_timeoff_name_regular = rail.IfOperator(
            task_id='check_timeoff_name_regular',
            test=lambda dag_run: dag_run.conf.get('timeoffname', '') == '01. JPN_年次有給休暇 Annual Paid Leave (Regular)',
            yes_task='trigger_regular_policy_assignment_update',
            no_task='check_timeoff_name_fixed_term_rehire'
        )

        trigger_regular_policy_assignment_update = rail.TriggerDagRunOperator(
            task_id='trigger_regular_policy_assignment_update',
            trigger_dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_regular_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf.get('loginname', ''),
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "add",
                "timeoffuri": dag_run.conf['timeoffuri'],
                "timeofftype": dag_run.conf['timeoffname'],
                "yoss": dag_run.conf.get('continuous_service_date'),
                "yos": dag_run.conf.get('continuous_service_date')
            }
        )

        insert_childid_to_wait_list_3 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_3',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_regular_policy_assignment_update')}}"
        )

        check_timeoff_name_fixed_term_rehire = rail.IfOperator(
            task_id='check_timeoff_name_fixed_term_rehire',
            test=lambda dag_run: dag_run.conf.get('timeoffname', '') == '03. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Rehire after Retirement)',
            yes_task='trigger_rehire_policy_assignment_update',
            no_task='child_dag_ids'
        )

        trigger_rehire_policy_assignment_update = rail.TriggerDagRunOperator(
            task_id='trigger_rehire_policy_assignment_update',
            trigger_dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_fixed_term_rehire_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf.get('loginname', ''),
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "add",
                "timeoffuri": dag_run.conf['timeoffuri'],
                "timeofftype": dag_run.conf['timeoffname'],
                "yoss": dag_run.conf.get('continuous_service_date'),
                "yos": dag_run.conf.get('continuous_service_date')
            }
        )

        insert_childid_to_wait_list_4 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_4',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_rehire_policy_assignment_update')}}"
        )

        child_dag_ids = rail.PythonOperator(
            task_id='child_dag_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('childtriggeredlist')] if rail.get_dag_run_var('childtriggeredlist') else []
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('child_dag_ids') | to_json}}"
        )

        gather_responses_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_child',
            dag_runs='{{ result("child_dag_ids") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.responses_from_child_timeout),
            flatten=True
        )

        filter_error_responses = rail.PythonOperator(
            task_id='filter_error_responses',
            python_callable=lambda: [item for item in rail.result(
                'gather_responses_from_child') if item]
        )

        # Workato Step 27: Check if timeoff type should get default policy assignment
        check_eligible_for_default_policy = rail.IfOperator(
            task_id='check_eligible_for_default_policy',
            test=lambda dag_run: dag_run.conf.get('timeoffname', '') not in [
                '01. JPN_年次有給休暇 Annual Paid Leave (Regular)',
                '03. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Rehire after Retirement)',
                '04. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Part Time)',
                '05. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Standard)',
                '06. JPN_個別休日 - 誕生日休日 Shift Worker Holiday - Birthday Holiday',
                '07. JPN_個別休日 - 個別連休（上期） Shift Worker Holiday - Consecutive Holiday - 1st Half',
                '08. JPN_個別休日 - 個別連休（下期） Shift Worker Holiday - Consecutive Holiday - 2nd Half',
                '09. JPN_個別休日 - 個別休日 Shift Worker Holiday - Inconsecutive Holiday',
                '10. JPN_個別休暇 - 夏期 Shift Worker Special Vacation (Summer)'
            ],
            yes_task='get_default_timeoff_policy_schedule',
            no_task='check_shift_worker_timeoff_types'
        )

        # Workato Step 29: GetDefaultTimeOffTypePolicyScheduleForUser
        get_default_timeoff_policy_schedule = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                }
            }
        )

        # Workato Step 31: default policy assigned only when the first entry is dated
        check_default_policy_exists = rail.IfOperator(
            task_id='check_default_policy_exists',
            test=lambda: bool(((rail.result('get_default_timeoff_policy_schedule') or [{}])[0].get('effectiveDate') or {}).get('day')),
            yes_task='transform_default_policy_data',
            no_task='check_shift_worker_timeoff_types'
        )

        # Workato Step 32: Transform policy data (script → scriptTarget conversion)
        transform_default_policy_data = rail.PythonOperator(
            task_id='transform_default_policy_data',
            python_callable=lambda: python_callable.convert_policy_set_with_script_target(
                rail.result('get_default_timeoff_policy_schedule')
            )
        )

        # Workato Step 33: PutUserTimeOffAccountPolicySetSchedule
        put_user_policy_schedule = rail.RepliconServiceOperator(
            task_id='put_user_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('transform_default_policy_data')
            }
        )


        # Workato Step 36: Check if shift worker holiday types
        check_shift_worker_timeoff_types = rail.IfOperator(
            task_id='check_shift_worker_timeoff_types',
            test=lambda dag_run: dag_run.conf.get('timeoffname', '') in [
                '06. JPN_個別休日 - 誕生日休日 Shift Worker Holiday - Birthday Holiday',
                '07. JPN_個別休日 - 個別連休（上期） Shift Worker Holiday - Consecutive Holiday - 1st Half',
                '08. JPN_個別休日 - 個別連休（下期） Shift Worker Holiday - Consecutive Holiday - 2nd Half',
                '09. JPN_個別休日 - 個別休日 Shift Worker Holiday - Inconsecutive Holiday',
                '10. JPN_個別休暇 - 夏期 Shift Worker Special Vacation (Summer)'
            ],
            yes_task='get_default_shift_worker_policy',
            no_task='catch_error'
        )

        # Workato Step 38: GetDefaultTimeOffPolicySetScheduleForTimeOffType
        get_default_shift_worker_policy = rail.RepliconServiceOperator(
            task_id='get_default_shift_worker_policy',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda dag_run: {
                "timeOffTypeUri": dag_run.conf['timeoffuri']
            }
        )

        # Workato Step 39: Check if offset value exists
        check_offset_value_exists = rail.IfOperator(
            task_id='check_offset_value_exists',
            test=lambda: rail.result('get_default_shift_worker_policy') and
                        len(rail.result('get_default_shift_worker_policy')) > 0 and
                        rail.result('get_default_shift_worker_policy')[0].get('startOffset', {}).get('offsetValue') is not None,
            yes_task='parse_workshift_changed_date_for_policy',
            no_task='catch_error'
        )

        # Parse workshift_changeddate for policy assignment
        parse_workshift_changed_date_for_policy = rail.PythonOperator(
            task_id='parse_workshift_changed_date_for_policy',
            python_callable=lambda dag_run: {
                'day': datetime.strptime(dag_run.conf['workshift_changeddate'], '%Y-%m-%d').day,
                'month': datetime.strptime(dag_run.conf['workshift_changeddate'], '%Y-%m-%d').month,
                'year': datetime.strptime(dag_run.conf['workshift_changeddate'], '%Y-%m-%d').year
            } if dag_run.conf.get('workshift_changeddate') else {
                'day': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').day,
                'month': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').month,
                'year': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').year
            }
        )

        # Workato Steps 40-48: Build shift worker policy schedule
        build_shift_worker_policy_schedule = rail.PythonOperator(
            task_id='build_shift_worker_policy_schedule',
            python_callable=lambda dag_run: python_callable.build_shift_worker_policy_schedule(
                rail.result('get_default_shift_worker_policy'),
                rail.result('parse_workshift_changed_date_for_policy'),
                dag_run.conf.get('workshift_changeddate', dag_run.conf['startdate'])
            )
        )

        # Workato Step 49: PutUserTimeOffAccountPolicySetSchedule for shift workers
        put_shift_worker_policy_schedule = rail.RepliconServiceOperator(
            task_id='put_shift_worker_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('build_shift_worker_policy_schedule')
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Rehire user time off update dag ; {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else (rail.result('filter_error_responses') or null)
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> parse_hire_date >> create_child_trigger_list

        create_child_trigger_list >> check_workshift_changed_date_present

        check_workshift_changed_date_present >> rail.Label('Yes') >> parse_workshift_changed_date >> extract_employment_month
        check_workshift_changed_date_present >> rail.Label('No') >> extract_employment_month

        extract_employment_month >> determine_yearly_accrual >> check_timeoff_uri_present

        check_timeoff_uri_present >> rail.Label('Yes') >> check_timeoff_name_new_hire
        check_timeoff_uri_present >> rail.Label('No') >> catch_error

        check_timeoff_name_new_hire >> rail.Label('Yes') >> catch_error
        check_timeoff_name_new_hire >> rail.Label('No') >> check_continuous_service_date_present

        check_continuous_service_date_present >> rail.Label('Yes') >> check_timeoff_name_fixed_term_standard
        check_continuous_service_date_present >> rail.Label('No') >> check_eligible_for_default_policy

        check_timeoff_name_fixed_term_standard >> rail.Label('Yes') >> trigger_standard_parttime_policy_assignment_update >> insert_childid_to_wait_list_1 >> check_timeoff_name_fixed_term_parttime
        check_timeoff_name_fixed_term_standard >> rail.Label('No') >> check_timeoff_name_fixed_term_parttime

        check_timeoff_name_fixed_term_parttime >> rail.Label('Yes') >> trigger_parttime_policy_assignment_update >> insert_childid_to_wait_list_2 >> check_timeoff_name_regular
        check_timeoff_name_fixed_term_parttime >> rail.Label('No') >> check_timeoff_name_regular

        check_timeoff_name_regular >> rail.Label('Yes') >> trigger_regular_policy_assignment_update >> insert_childid_to_wait_list_3 >> check_timeoff_name_fixed_term_rehire
        check_timeoff_name_regular >> rail.Label('No') >> check_timeoff_name_fixed_term_rehire

        check_timeoff_name_fixed_term_rehire >> rail.Label('Yes') >> trigger_rehire_policy_assignment_update >> insert_childid_to_wait_list_4 >> child_dag_ids
        check_timeoff_name_fixed_term_rehire >> rail.Label('No') >> child_dag_ids
        child_dag_ids >> wait_for_child_dags >> gather_responses_from_child  >> filter_error_responses >> check_eligible_for_default_policy
        
        # Workato Step 27: Default Policy Eligibility Flow
        check_eligible_for_default_policy >> rail.Label('Yes') >> get_default_timeoff_policy_schedule
        check_eligible_for_default_policy >> rail.Label('No') >> check_shift_worker_timeoff_types

        # Workato Steps 29-33: Default Policy Assignment Flow
        get_default_timeoff_policy_schedule >> check_default_policy_exists
        check_default_policy_exists >> rail.Label('Yes') >> transform_default_policy_data >> put_user_policy_schedule >> check_shift_worker_timeoff_types
        check_default_policy_exists >> rail.Label('No') >> check_shift_worker_timeoff_types

        # Workato Steps 36-49: Shift Worker Policy Management Flow
        check_shift_worker_timeoff_types >> rail.Label('Yes') >> get_default_shift_worker_policy
        check_shift_worker_timeoff_types >> rail.Label('No') >> catch_error

        get_default_shift_worker_policy >> check_offset_value_exists
        check_offset_value_exists >> rail.Label('Yes') >> parse_workshift_changed_date_for_policy >> build_shift_worker_policy_schedule >> \
            put_shift_worker_policy_schedule >> catch_error
        
        check_offset_value_exists >> rail.Label('No') >> catch_error

        return dag


rail.for_each_instance(create_dag)