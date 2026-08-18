from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_fixed_term_rehire_assignment_dag_id,
        description=f'Momentive_user_sync_child_annual_leave_policy_fixed_term_rehire_assignment_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
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
            no_task='get_req_timeoff_type_uri'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_req_timeoff_type_uri',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_req_timeoff_type_uri = rail.RepliconServiceOperator(
            task_id='get_req_timeoff_type_uri',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', "03. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Rehire after Retirement) - DUMMY - REHIRE MONTH", 'uri')
        )

        extract_employment_month = rail.PythonOperator(
            task_id='extract_employment_month',
            python_callable=lambda dag_run: datetime.strptime(dag_run.conf['yoss'], '%Y-%m-%d').strftime('%B').lower()
        )

        split_startdate = rail.PythonOperator(
            task_id='split_startdate',
            python_callable=lambda dag_run: python_callable.split_date_string(dag_run.conf['startdate'])
        )

        split_rehire_date = rail.PythonOperator(
            task_id='split_rehire_date',
            python_callable=lambda dag_run: python_callable.split_date_string(dag_run.conf['yoss'])
        )

        # Validate yoss is a real date (not just present). yoss (continuous service date)
        # can arrive as "0"/blank when the source value is missing; the downstream steps
        # strptime it, so a non-date must route to the No path (graceful skip) instead of
        # crashing the task. Mirrors the recipe's #9 present-guard, hardened for strict
        # Python parsing.
        if_rehire_date_present = rail.IfOperator(
            task_id='if_rehire_date_present',
            test=lambda dag_run: python_callable.is_valid_date(dag_run.conf.get('yoss')),
            yes_task='extract_employment_month',
            no_task='catch_error'
        )

        get_default_policy_for_rehire_month = rail.RepliconServiceOperator(
            task_id='get_default_policy_for_rehire_month',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('get_req_timeoff_type_uri')
            }
        )

        extract_and_convert_rehire_policy = rail.PythonOperator(
            task_id='extract_and_convert_rehire_policy',
            python_callable=lambda dag_run: python_callable.build_rehire_timeoff_policy_with_offset_check(
                rail.result('get_default_policy_for_rehire_month'), dag_run
            )
        )

        if_rehire_policy_entries_present = rail.IfOperator(
            task_id='if_rehire_policy_entries_present',
            test=lambda: bool(rail.result('extract_and_convert_rehire_policy')),
            yes_task='assign_annual_assignment_leave_policy_rehire',
            no_task='catch_error'
        )

        assign_annual_assignment_leave_policy_rehire = rail.RepliconServiceOperator(
            task_id='assign_annual_assignment_leave_policy_rehire',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('extract_and_convert_rehire_policy')
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Fixed Term Rehire Annual Leave Assignment for user ; {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_req_timeoff_type_uri


        # Gate on a valid yoss BEFORE any date parsing. On Yes, run the parsing/build steps
        # (which strptime yoss); on No (blank/non-date yoss), skip to catch_error without
        # a task failure.
        get_req_timeoff_type_uri >> if_rehire_date_present

        if_rehire_date_present >> rail.Label('Yes') >> extract_employment_month >> split_startdate >> split_rehire_date \
            >> get_default_policy_for_rehire_month >> extract_and_convert_rehire_policy >> \
            if_rehire_policy_entries_present >> rail.Label('Yes') >> assign_annual_assignment_leave_policy_rehire >> catch_error
        if_rehire_policy_entries_present >> rail.Label('No') >> catch_error
        if_rehire_date_present >> rail.Label('No') >> catch_error

    return dag


for_each_instance = rail.for_each_instance(create_dag)
