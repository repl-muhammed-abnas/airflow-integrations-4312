from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import json
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v3.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_dag_id,
        description=f'Assured Partners User Import rehire/transfer time off policy assignment for non pto1 time off types {config.instance}',
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
            yes_task="log_effective_date_derived",
            no_task="catch_and_log_error",
        )

        log_effective_date_derived = rail.PythonOperator(
            task_id='log_effective_date_derived',
            python_callable=lambda dag_run: python_callable.get_split_date(dag_run.conf['startdate'] if dag_run.conf['type'].lower() == "rehire" else (
                (dag_run.conf['loaend'] or dag_run.conf['ChangeEffectiveDate']) if dag_run.conf['type'].lower() == "loa" else (
                    (datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).date() - timedelta(days=1)) if dag_run.conf['type'].lower() == "transfer" else dag_run.conf['integration_run_date'])), 'int')
        )

        get_defaultpolicyfromgloballevel_9 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_9',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        get_user_time_off_type_policy_summary_10 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_10',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        log_relevant_historical_policies = rail.PythonOperator(
            task_id='log_relevant_historical_policies',
            python_callable=lambda: python_callable.get_relevant_historical_policies(rail.result('get_user_time_off_type_policy_summary_10'), rail.result(
                'log_effective_date_derived'))
        )

        def get_default_policy_set_modified(timeoff_default_policyset, effective_date_derived):
            modified_default_policy_set = []
            default_policyset_gsubbed = json.loads(json.dumps(timeoff_default_policyset, ensure_ascii=False).replace('"null"', '"effective"').replace(
                '"script"', '"scriptTarget"'))
            for item in default_policyset_gsubbed:
                modified_default_policy_set.append({
                    'description': "Effective on - " + datetime.strftime(python_callable.dict_date_to_datetime(effective_date_derived) + relativedelta(years=int(item['startOffset']['offsetValue'])), config.DATE_DEFAULT_FORMAT),
                    'effectiveDate': python_callable.get_split_date(python_callable.dict_date_to_datetime(effective_date_derived) + relativedelta(years=int(item['startOffset']['offsetValue'])), 'int'),
                    'policySet': item['policySet']
                })
            return modified_default_policy_set

        log_default_policy_set_modified = rail.PythonOperator(
            task_id='log_default_policy_set_modified',
            python_callable=lambda: get_default_policy_set_modified(rail.result(
                "get_defaultpolicyfromgloballevel_9"), rail.result('log_effective_date_derived'))
        )

        if_request_startdate_equals_to_previousstartdate_36 = rail.IfOperator(
            task_id='if_request_startdate_equals_to_previousstartdate_36',
            test=lambda dag_run: dag_run.conf['startdate'] == dag_run.conf[
                'previousstartdate'] and dag_run.conf['type'] == 'rehire',
            yes_task="if_relevant_historical_policy_present_37",
            no_task="if_request_type_not_equals_to_rehire_39",
        )

        if_relevant_historical_policy_present_37 = rail.IfOperator(
            task_id='if_relevant_historical_policy_present_37',
            test=lambda: 'urn' in json.dumps(
                rail.result("log_relevant_historical_policies")),
            yes_task="catch_and_log_error",
            no_task="if_request_type_not_equals_to_rehire_39",
        )

        if_request_type_not_equals_to_rehire_39 = rail.IfOperator(
            task_id='if_request_type_not_equals_to_rehire_39',
            test=lambda dag_run:  dag_run.conf['type'] != 'rehire' and dag_run.conf['timeofftypename'] != 'Sick Pay-H',
            yes_task="if_to_s_contains_urn_ifhistoricalpolicyispresent_40",
            no_task="log_final_policy_set",
        )

        if_to_s_contains_urn_ifhistoricalpolicyispresent_40 = rail.IfOperator(
            task_id='if_to_s_contains_urn_ifhistoricalpolicyispresent_40',
            test=lambda: 'urn' in json.dumps(
                rail.result("log_relevant_historical_policies")),
            yes_task="catch_and_log_error",
            no_task="log_final_policy_set",
        )

        def get_final_policy_set(relevant_historical_policies, modified_default_policyset):
            if 'urn' in json.dumps(relevant_historical_policies):
                for item in relevant_historical_policies:
                    modified_default_policyset.append({
                        'description': item['description'],
                        'effectiveDate': item['effectiveDate'],
                        'policySet': item['policySet']
                    })
            return modified_default_policyset

        log_final_policy_set = rail.PythonOperator(
            task_id='log_final_policy_set',
            python_callable=lambda dag_run: get_final_policy_set(rail.result(
                "log_relevant_historical_policies"), rail.result("log_default_policy_set_modified"))
        )

        assign_time_offpolicy_48 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_48',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policy_set')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Rehire/Transfer User Timeoff Policy Assignment For Non-PTO1 time off types: {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or "Success"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> if_request_timeoffuri_present_3

        if_request_timeoffuri_present_3 >> rail.Label(
            'No') >> catch_and_log_error
        if_request_timeoffuri_present_3 >> rail.Label(
            'Yes') >> log_effective_date_derived

        log_effective_date_derived >> get_defaultpolicyfromgloballevel_9 >> get_user_time_off_type_policy_summary_10 >> log_relevant_historical_policies >> log_default_policy_set_modified >> if_request_startdate_equals_to_previousstartdate_36

        if_request_startdate_equals_to_previousstartdate_36 >> rail.Label(
            'No') >> if_request_type_not_equals_to_rehire_39
        if_request_startdate_equals_to_previousstartdate_36 >> rail.Label(
            'Yes') >> if_relevant_historical_policy_present_37

        if_relevant_historical_policy_present_37 >> rail.Label(
            'Yes') >> catch_and_log_error
        if_relevant_historical_policy_present_37 >> rail.Label(
            'No') >> if_request_type_not_equals_to_rehire_39

        if_request_type_not_equals_to_rehire_39 >> rail.Label(
            'No') >> log_final_policy_set
        if_request_type_not_equals_to_rehire_39 >> rail.Label(
            'Yes') >> if_to_s_contains_urn_ifhistoricalpolicyispresent_40

        if_to_s_contains_urn_ifhistoricalpolicyispresent_40 >> rail.Label(
            'No') >> log_final_policy_set
        if_to_s_contains_urn_ifhistoricalpolicyispresent_40 >> rail.Label(
            'Yes') >> catch_and_log_error

        log_final_policy_set >> assign_time_offpolicy_48 >> catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
