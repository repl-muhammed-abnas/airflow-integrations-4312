
# pylint: disable=line-too-long
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from momentive.user_import_thailand.utils import request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_thailand_user_sync_child_put_remaining_balance_for_payout_dag_id,
        description=f'Momentive_Put remaining balance for payout child {config.instance}',
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
            no_task='log_balancevalue'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_balancevalue',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        # Recipe [2]-[6]: decompose the balance into its integer (decimal_left) and
        # fractional (decimal_right) parts; pad a single-digit fraction to two digits.
        def log_balance_value(dag_run):
            decimal_right = str(float(dag_run.conf['balance'])).split(".")[1]
            return {
                "decimal_left": str(float(dag_run.conf['balance'])).split(".")[0],
                "decimal_right": decimal_right,
                "log_valueendingwith0_9": (str(decimal_right) + "0") if len(decimal_right) == 1 else null
            }

        log_balancevalue = rail.PythonOperator(
            task_id='log_balancevalue',
            python_callable=log_balance_value
        )

        # Recipe [3]: balance_amount holds the rounded balance to be applied.
        declare_variable_4 = rail.SetVariableOperator(
            task_id='declare_variable_4',
            append=False,
            name='balance_amount',
            value=None
        )

        # Recipe [11]: the two-digit fraction used to decide rounding.
        log_finalbalanceamounttobeadded_12 = rail.PythonOperator(
            task_id='log_finalbalanceamounttobeadded_12',
            python_callable=lambda: rail.result('log_balancevalue')['log_valueendingwith0_9'] if rail.result(
                'log_balancevalue')['log_valueendingwith0_9'] else rail.result('log_balancevalue')['decimal_right']
        )

        # Recipe [12]: fraction "00"/"0" -> whole number, ".0".
        if_log_finalbalanceamounttobeadded_12_equals_to_00_13 = rail.IfOperator(
            task_id='if_log_finalbalanceamounttobeadded_12_equals_to_00_13',
            test=lambda: bool(rail.result('log_finalbalanceamounttobeadded_12') == "00" or rail.result(
                'log_finalbalanceamounttobeadded_12') == "0"),
            yes_task="update_variable_14",
            no_task="if_log_finalbalanceamounttobeadded_12_equals_to_99_15",
        )

        update_variable_14 = rail.SetVariableOperator(
            task_id='update_variable_14',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value=lambda: rail.result('log_balancevalue')[
                "decimal_left"] + ".0"
        )

        # Recipe [14]: fraction "99" -> round up to next whole number.
        if_log_finalbalanceamounttobeadded_12_equals_to_99_15 = rail.IfOperator(
            task_id='if_log_finalbalanceamounttobeadded_12_equals_to_99_15',
            test=lambda: bool(
                int(rail.result('log_finalbalanceamounttobeadded_12')) == 99),
            yes_task="update_variable_16",
            no_task="if_12_to_i_greater_than_0_17",
        )

        update_variable_16 = rail.SetVariableOperator(
            task_id='update_variable_16',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value=lambda: int(rail.result(
                'log_balancevalue')["decimal_left"]) + 1
        )

        # Recipe [16]: fraction 1-25 -> ".0".
        if_12_to_i_greater_than_0_17 = rail.IfOperator(
            task_id='if_12_to_i_greater_than_0_17',
            test=lambda: bool(int(rail.result('log_finalbalanceamounttobeadded_12')) > 0 and int(
                rail.result('log_finalbalanceamounttobeadded_12')) < 26),
            yes_task="update_variable_18",
            no_task="if_12_to_i_greater_than_25_19",
        )

        update_variable_18 = rail.SetVariableOperator(
            task_id='update_variable_18',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value=lambda: rail.result('log_balancevalue')[
                "decimal_left"] + ".0"
        )

        # Recipe [18]: fraction 26-72 -> ".5".
        if_12_to_i_greater_than_25_19 = rail.IfOperator(
            task_id='if_12_to_i_greater_than_25_19',
            test=lambda: bool(int(rail.result('log_finalbalanceamounttobeadded_12')) > 25 and int(
                rail.result('log_finalbalanceamounttobeadded_12')) < 73),
            yes_task="update_variable_20",
            no_task="if_12_to_i_greater_than_72_21",
        )

        update_variable_20 = rail.SetVariableOperator(
            task_id='update_variable_20',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value=lambda: str(rail.result(
                'log_balancevalue')['decimal_left']) + ".5"
        )

        # Recipe [20]: fraction 73-98 -> round up to next whole number, ".0".
        if_12_to_i_greater_than_72_21 = rail.IfOperator(
            task_id='if_12_to_i_greater_than_72_21',
            test=lambda: bool(int(rail.result('log_finalbalanceamounttobeadded_12')) > 72 and int(
                rail.result('log_finalbalanceamounttobeadded_12')) < 99),
            yes_task="update_variable_22",
            no_task="getassignedpolicyforthetimeofftype_24",
        )

        update_variable_22 = rail.SetVariableOperator(
            task_id='update_variable_22',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value=lambda: str(int(rail.result(
                'log_balancevalue')['decimal_left']) + 1) + ".0"
        )

        # Recipe [23]: get the user's assigned policy schedule for the time off type.
        getassignedpolicyforthetimeofftype_24 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_24',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule')
        )

        # Recipe [26]: a policy schedule must exist for this time off type.
        if_policysetschedule_exists = rail.IfOperator(
            task_id='if_policysetschedule_exists',
            test="{{result('getassignedpolicyforthetimeofftype_24') | is_truthy}}",
            yes_task='get_past_policysetschedule_entries',
            no_task='catch_error'
        )

        # Recipe [27]-[35]: keep only schedule entries effective before the termination date.
        def get_policysetschedule_entries_from_past(dag_run):
            policysetschedule_entries_list = []
            effective_date = None
            for item in rail.result("getassignedpolicyforthetimeofftype_24"):
                effective_date = str(item['effectiveDate']['month']) + "/" + str(
                    item['effectiveDate']['day']) + "/" + str(item['effectiveDate']['year'])
                if (datetime.strptime(effective_date, "%m/%d/%Y") < datetime.strptime(dag_run.conf['terminationdate'], "%d/%m/%Y")):
                    policysetschedule_entries_list.append(item)

            final_list_with_scripttarget = json.loads(json.dumps(policysetschedule_entries_list).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
            return final_list_with_scripttarget

        get_past_policysetschedule_entries = rail.PythonOperator(
            task_id='get_past_policysetschedule_entries',
            python_callable=get_policysetschedule_entries_from_past
        )

        # Recipe [36]: only proceed if the (resolved) past entries reference real URIs.
        if_urn_in_scheduleentries = rail.IfOperator(
            task_id="if_urn_in_scheduleentries",
            test=lambda: bool("urn" in json.dumps(
                rail.result("get_past_policysetschedule_entries"))),
            yes_task="append_new_entry_to_be_added_in_policysetschedule",
            no_task="log_no_policy_error"
        )

        # Recipe [39]: no policy schedule -> nothing to back-fill.
        log_no_policy_error = rail.PythonOperator(
            task_id="log_no_policy_error",
            python_callable=lambda: "No policy, hence no 0 balance required"
        )

        append_new_entry_to_be_added_in_policysetschedule = rail.PythonOperator(
            task_id="append_new_entry_to_be_added_in_policysetschedule",
            python_callable=request_payload.final_policyset_schedule_entry
        )

        # Recipe [37]: write the policy schedule with the remaining balance as the initial balance.
        put_timeoffpolicy_with_initial_balance_as_remaining_balance_38 = rail.RepliconServiceOperator(
            task_id='put_time_offpolicywithinitialbalanceasremainingbalance_38',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result("append_new_entry_to_be_added_in_policysetschedule")
            }
        )

        # Recipe [40]/[41]: capture any error and return it as the recipe response.
        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Put remaining balance flow; {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> log_balancevalue

        log_balancevalue >> declare_variable_4 >> log_finalbalanceamounttobeadded_12

        log_finalbalanceamounttobeadded_12 >> if_log_finalbalanceamounttobeadded_12_equals_to_00_13

        if_log_finalbalanceamounttobeadded_12_equals_to_00_13 >> rail.Label(
            'No') >> if_log_finalbalanceamounttobeadded_12_equals_to_99_15
        if_log_finalbalanceamounttobeadded_12_equals_to_00_13 >> rail.Label(
            'Yes') >> update_variable_14 >> if_log_finalbalanceamounttobeadded_12_equals_to_99_15

        if_log_finalbalanceamounttobeadded_12_equals_to_99_15 >> rail.Label(
            'No') >> if_12_to_i_greater_than_0_17
        if_log_finalbalanceamounttobeadded_12_equals_to_99_15 >> rail.Label(
            'Yes') >> update_variable_16 >> if_12_to_i_greater_than_0_17

        if_12_to_i_greater_than_0_17 >> rail.Label(
            'Yes') >> update_variable_18 >> if_12_to_i_greater_than_25_19
        if_12_to_i_greater_than_0_17 >> rail.Label(
            'No') >> if_12_to_i_greater_than_25_19

        if_12_to_i_greater_than_25_19 >> rail.Label(
            'No') >> if_12_to_i_greater_than_72_21
        if_12_to_i_greater_than_25_19 >> rail.Label(
            'Yes') >> update_variable_20 >> if_12_to_i_greater_than_72_21

        if_12_to_i_greater_than_72_21 >> rail.Label(
            'No') >> getassignedpolicyforthetimeofftype_24
        if_12_to_i_greater_than_72_21 >> rail.Label(
            'Yes') >> update_variable_22 >> getassignedpolicyforthetimeofftype_24

        getassignedpolicyforthetimeofftype_24 >> if_policysetschedule_exists

        if_policysetschedule_exists >> rail.Label('No') >> catch_error
        if_policysetschedule_exists >> rail.Label(
            'Yes') >> get_past_policysetschedule_entries >> if_urn_in_scheduleentries

        if_urn_in_scheduleentries >> rail.Label(
            'No') >> log_no_policy_error >> catch_error
        if_urn_in_scheduleentries >> rail.Label(
            'Yes') >> append_new_entry_to_be_added_in_policysetschedule

        append_new_entry_to_be_added_in_policysetschedule >> put_timeoffpolicy_with_initial_balance_as_remaining_balance_38 >> catch_error

    return dag


rail.for_each_instance(create_dag)
