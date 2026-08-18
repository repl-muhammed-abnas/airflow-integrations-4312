from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import json
import rail
from assuredpartnersinc.user_import_v3.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_user_timeoff_policy_update_for_start_date_update_time_off_types_dag_id,
        description=f'Assured Partners User Import Timeoff policy update for start date update timeoff type Child{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_timeoffuri_present_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_timeoffuri_present_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_timeoffuri_present_3 = rail.IfOperator(
            task_id='if_request_timeoffuri_present_3',
            test='''{{ dag_run.conf.timeoffuri | is_truthy }}''',
            yes_task="log_effectivedatederived_and_tenure",
            no_task="catch_and_log_error",
        )

        def get_effectivedatederived_and_tenure(dag_run):
            effective_date_derived = python_callable.get_split_date(
                dag_run.conf['startdate'], 'no_split')
            return {
                'effective_date_derived': python_callable.get_split_date(effective_date_derived, 'int'),
                'tenure': float(abs(
                    (datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).date() - effective_date_derived).days / 365))
            }

        log_effectivedatederived_and_tenure = rail.PythonOperator(
            task_id='log_effectivedatederived_and_tenure',
            python_callable=get_effectivedatederived_and_tenure
        )

        get_defaultpolicyfromgloballevel_10 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_10',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        get_user_time_off_type_policy_summary_11 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_11',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        def add_relevant_historical_policies(user_timeoff_policysetschedule_for_given_timeoff_type, dag_run):
            modified_policysetschedule = []
            for item in user_timeoff_policysetschedule_for_given_timeoff_type:
                effective_date_in_policy_line = python_callable.dict_date_to_datetime(
                    item['effectiveDate'])
                if effective_date_in_policy_line < datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).date():
                    modified_policysetschedule.append({
                        "description": item['description'],
                        "effectiveDate": item['effectiveDate'],
                        "policySet": item['policySet']
                    })
            return modified_policysetschedule

        def get_policy_sets_counts_list(default_policy, effective_date_and_user_tenure, dag_run):
            policy_sets_counts_list = []
            for item in default_policy:
                if int(item['startOffset']['offsetValue']) > int(effective_date_and_user_tenure['tenure']):
                    eff_date = python_callable.get_split_date(python_callable.dict_date_to_datetime(
                        effective_date_and_user_tenure['effective_date_derived']) + relativedelta(months=int(item['startOffset']['offsetValue']) * 12), 'int')
                    policy_sets_counts_list.append({
                        'effective_date': eff_date,
                        'policyset': item['policySet']
                    })

            if not (policy_sets_counts_list):
                if default_policy:
                    policy_sets_counts_list.append({
                        'effective_date': python_callable.get_split_date(dag_run.conf['integration_run_date'], 'int'),
                        'policyset': default_policy[-1]['policySet']
                    })

            return policy_sets_counts_list

        log_add_relevant_historical_policies_to_policies_list_and_policy_sets_counts_list = rail.PythonOperator(
            task_id='log_add_relevant_historical_policies_to_policies_list_and_policy_sets_counts_list',
            python_callable=lambda dag_run: {
                'modified_policysetschedule': add_relevant_historical_policies(rail.result('get_user_time_off_type_policy_summary_11'), dag_run) if rail.result('get_user_time_off_type_policy_summary_11') else [],
                'policy_sets_counts_list': get_policy_sets_counts_list(rail.result('get_defaultpolicyfromgloballevel_10'), rail.result('log_effectivedatederived_and_tenure'), dag_run)
            }
        )

        def final_policysetschedule(modified_policysetschedule_and_policy_sets_counts_list, dag_run):
            final_policysetschedule_list = modified_policysetschedule_and_policy_sets_counts_list[
                'modified_policysetschedule']
            for item in modified_policysetschedule_and_policy_sets_counts_list['policy_sets_counts_list']:
                if item == modified_policysetschedule_and_policy_sets_counts_list['policy_sets_counts_list'][0]:
                    final_policysetschedule_list.append({
                        "description": "Effective on - " + dag_run.conf['integration_run_date'],
                        "effectiveDate": python_callable.get_split_date(dag_run.conf['integration_run_date'], 'int'),
                        "policySet": item['policyset']
                    })
                elif item != modified_policysetschedule_and_policy_sets_counts_list['policy_sets_counts_list'][0]:
                    final_policysetschedule_list.append({
                        "description": "Effective on - " + str(item['effective_date']['month']) + "-" + str(item['effective_date']['day']) + "-" + str(item['effective_date']['year']),
                        "effectiveDate": item['effective_date'],
                        "policySet": item['policyset']
                    })
            final_policyset_schedule = json.loads(json.dumps(final_policysetschedule_list, ensure_ascii=False).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))

            return final_policyset_schedule

        log_final_policysetschedule = rail.PythonOperator(
            task_id='log_final_policysetschedule',
            python_callable=lambda dag_run: final_policysetschedule(rail.result(
                'log_add_relevant_historical_policies_to_policies_list_and_policy_sets_counts_list'), dag_run)
        )

        assign_time_offpolicy_40 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_40',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policysetschedule')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Timeoff policy update for start date Update timeoff type ({{dag_run.conf.timeofftypename}}) : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or "Success"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> final_response_from_dag
        can_run_batch_task >> rail.Label(
            'No') >> if_request_timeoffuri_present_3

        if_request_timeoffuri_present_3 >> rail.Label(
            'No') >> catch_and_log_error
        if_request_timeoffuri_present_3 >> rail.Label('Yes') >> log_effectivedatederived_and_tenure >> get_defaultpolicyfromgloballevel_10 \
            >> get_user_time_off_type_policy_summary_11 >> log_add_relevant_historical_policies_to_policies_list_and_policy_sets_counts_list

        log_add_relevant_historical_policies_to_policies_list_and_policy_sets_counts_list >> log_final_policysetschedule \
            >> assign_time_offpolicy_40 >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
