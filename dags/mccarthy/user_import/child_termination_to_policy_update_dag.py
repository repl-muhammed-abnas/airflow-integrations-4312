from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


def create_termination_policy_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_termination_to_policy_update_child_{config.instance}',
        description=f'Mccarthy| Termination_TO_Policy_Update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='apply_end_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='apply_end_date',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_replicon_date(date_str, fmt='%m/%d/%Y'):
            datetime_obj = datetime.strptime(date_str, fmt)
            return {
                'year': datetime_obj.year,
                'month': datetime_obj.month,
                'day': datetime_obj.day
            }
        apply_end_date = rail.RepliconServiceOperator(
            task_id='apply_end_date',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "modifications": {
                    "userDetailsToApply": {
                        "employmentEndDate": {
                            "date": get_replicon_date(dag_run.conf['enddate'])
                        }
                    }
                }
            }
        )

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_startingbalancesetto_timeoffbalance_script = rail.RepliconServiceOperator(
            task_id='get_startingbalancesetto_timeoffbalance_script',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        get_preventbalanceoverdraw_timeoffvalidation_script = rail.RepliconServiceOperator(
            task_id='get_preventbalanceoverdraw_timeoffvalidation_script',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Prevent balance overdraw', 'uri', '')
        )

        def get_timeofftypes_assigned(response):
            policies_by_timeoff_types = response['policiesByTimeOffType']
            return list(filter(lambda x: bool(x['enabled']), map(lambda item: {
                'name': item['timeOffType']['name'],
                'enabled': item['isTimeOffAllowedAgainstThisTimeOffType'],
                'uri': item['timeOffType']['uri'],
                'policy': json.loads(json.dumps(item['policySetSchedule'], ensure_ascii=False).replace(
                    "[[{", "[{").replace("}]]", "}]"))
            }, policies_by_timeoff_types))) if policies_by_timeoff_types else []
        get_assigned_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_assigned_timeoff_types',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_timeofftypes_assigned
        )

        is_assigned_timeofftypes = rail.IfOperator(
            task_id='is_assigned_timeofftypes',
            test="{{ result('get_assigned_timeoff_types') | map_to_attr('policy') | remove_empty | \
                length > 0 }}",
            yes_task='trigger_timeoff_assignment_no_accrual',
            no_task='write_to_policyupdate_log'
        )

        trigger_timeoff_assignment_no_accrual = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_assignment_no_accrual',
            retries=0,
            items=lambda: rail.result('get_assigned_timeoff_types'),
            trigger_dag_id=f'mccarthy_user_import_timeoff_assignment_policy_update_no_accrual_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": item['uri'],
                "policyset": item['policy'],
                "enddate": dag_run.conf['enddate'],
                "startingbalancesettouri": rail.result('get_startingbalancesetto_timeoffbalance_script'),
                "preventbalanceoverdrawuri": rail.result('get_preventbalanceoverdraw_timeoffvalidation_script'),
                "loginname": dag_run.conf['loginname']
            }
        )

        wait_for_timeoff_assignment_no_accrual = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_assignment_no_accrual',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_assignment_no_accrual") }}'
        )

        write_to_policyupdate_log = rail.WriteLogOperator(
            task_id='write_to_policyupdate_log',
            log="{{ dag_run.conf.log }}",
            message="User Terminated",
            severity="Success",
            properties={
                'loginname': '{{ dag_run.conf.loginname }}',
                'email': '{{ dag_run.conf.Email }}',
                'action': 'Update',
                'status': 'Success',
                'details': 'User Terminated'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> apply_end_date >> disable_user >> get_startingbalancesetto_timeoffbalance_script >> \
            get_preventbalanceoverdraw_timeoffvalidation_script >> get_assigned_timeoff_types >> is_assigned_timeofftypes
        is_assigned_timeofftypes >> rail.Label(
            'Yes') >> trigger_timeoff_assignment_no_accrual >> wait_for_timeoff_assignment_no_accrual >> write_to_policyupdate_log
        is_assigned_timeofftypes >> rail.Label(
            'No') >> write_to_policyupdate_log
        write_to_policyupdate_log >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_termination_policy_dag)
