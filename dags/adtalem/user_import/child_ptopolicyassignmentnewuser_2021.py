from datetime import timedelta
import json
from airflow.models import Variable
import rail
from adtalem.user_import.utils.python_callable_method import get_pto_policy_assignments


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_pto_policy_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_ptopolicyassignmentnewuser_2021_{config.instance}',
        description=f'PTO policy assignment new user_2021 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_pto_policyset'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_pto_policyset',
            end_task='dagrun_log_to_sumo',
        )

        get_pto_policyset = rail.RepliconServiceOperator(
            task_id='get_pto_policyset',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            },
            data_handler=lambda response: json.loads(json.dumps(
                response, ensure_ascii=False).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        )

        is_rft_rpt_policyname = rail.IfOperator(
            task_id='is_rft_rpt_policyname',
            test=lambda dag_run: dag_run.conf['policyname'] in ('RFT', 'RPT'),
            yes_task='get_rft_rpt_policies',
            no_task='get_maxbal_script_uri'
        )

        get_rft_rpt_policies = rail.PythonOperator(
            task_id='get_rft_rpt_policies',
            python_callable=get_pto_policy_assignments,
            op_args=['rft_rpt']
        )

        get_maxbal_script_uri = rail.RepliconServiceOperator(
            task_id='get_maxbal_script_uri',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Max Balance Limit', 'uri', '')
        )

        is_rftca_rptca_policyname = rail.IfOperator(
            task_id='is_rftca_rptca_policyname',
            test=lambda dag_run: dag_run.conf['policyname'] in (
                'RFT-CA', 'RPT-CA'),
            yes_task='get_rftca_rptca_policies',
            no_task='is_rftch_rptch_policyname'
        )

        get_rftca_rptca_policies = rail.PythonOperator(
            task_id='get_rftca_rptca_policies',
            python_callable=get_pto_policy_assignments,
            op_args=['rftca_rptca']
        )

        is_rftch_rptch_policyname = rail.IfOperator(
            task_id='is_rftch_rptch_policyname',
            test=lambda dag_run: dag_run.conf['policyname'] in (
                'RFT-CH', 'RPT-CH'),
            yes_task='get_rftch_rptch_policies',
            no_task='is_rftchca_rptchca_policyname'
        )

        get_rftch_rptch_policies = rail.PythonOperator(
            task_id='get_rftch_rptch_policies',
            python_callable=get_pto_policy_assignments,
            op_args=['rftch_rptch']
        )

        is_rftchca_rptchca_policyname = rail.IfOperator(
            task_id='is_rftchca_rptchca_policyname',
            test=lambda dag_run: dag_run.conf['policyname'] in (
                'RFT-CH-CA', 'RPT-CH-CA'),
            yes_task='get_rftchca_rptchca_policies',
            no_task='dagrun_log_to_sumo'
        )

        get_rftchca_rptchca_policies = rail.PythonOperator(
            task_id='get_rftchca_rptchca_policies',
            python_callable=get_pto_policy_assignments,
            op_args=['rftchca_rptchca']
        )

        assign_time_offpolicy_assign_defaultpolicy = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_assign_defaultpolicy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_rft_rpt_policies') or rail.result(
                    'get_rftca_rptca_policies') or rail.result('get_rftch_rptch_policies') or rail.result(
                        'get_rftchca_rptchca_policies')
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
            'No') >> get_pto_policyset

        get_pto_policyset >> is_rft_rpt_policyname

        is_rft_rpt_policyname >> rail.Label(
            'Yes') >> get_rft_rpt_policies >> assign_time_offpolicy_assign_defaultpolicy

        is_rft_rpt_policyname >> rail.Label(
            'No') >> get_maxbal_script_uri >> is_rftca_rptca_policyname

        is_rftca_rptca_policyname >> rail.Label(
            'Yes') >> get_rftca_rptca_policies >> assign_time_offpolicy_assign_defaultpolicy

        is_rftca_rptca_policyname >> rail.Label(
            'No') >> is_rftch_rptch_policyname

        is_rftch_rptch_policyname >> rail.Label(
            'Yes') >> get_rftch_rptch_policies >> assign_time_offpolicy_assign_defaultpolicy

        is_rftch_rptch_policyname >> rail.Label(
            'No') >> is_rftchca_rptchca_policyname

        is_rftchca_rptchca_policyname >> rail.Label(
            'Yes') >> get_rftchca_rptchca_policies >> assign_time_offpolicy_assign_defaultpolicy

        is_rftchca_rptchca_policyname >> rail.Label(
            'No') >> dagrun_log_to_sumo

        assign_time_offpolicy_assign_defaultpolicy >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_pto_policy_assignment_dag)
