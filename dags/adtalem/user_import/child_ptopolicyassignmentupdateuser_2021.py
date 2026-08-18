from datetime import timedelta
import json
from airflow.models import Variable
import rail
from adtalem.user_import.utils.python_callable_method import get_pto_policy_assignments_update
from adtalem.user_import.utils.request_payload import get_user_tenure


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_ptopolicyassignmentupdateuser_2021_{config.instance}',
        description=f'PTO policy assignment update user_2021 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

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
            end_task='log_to_sumo',
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

        get_usertenure_servicedate = rail.PythonOperator(
            task_id='get_usertenure_servicedate',
            python_callable=get_user_tenure,
            op_args=['{{ dag_run.conf.servicedate }}']
        )

        get_maxbal_script_uri = rail.RepliconServiceOperator(
            task_id='get_maxbal_script_uri',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Max Balance Limit', 'uri', '')
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
            python_callable=get_pto_policy_assignments_update,
            op_args=['rftch_rptch']
        )

        is_rftchca_rptchca_policyname = rail.IfOperator(
            task_id='is_rftchca_rptchca_policyname',
            test=lambda dag_run: dag_run.conf['policyname'] in (
                'RFT-CH-CA', 'RPT-CH-CA'),
            yes_task='get_rftchca_rptchca_policies',
            no_task='user_tenure_lessthan_5'
        )

        get_rftchca_rptchca_policies = rail.PythonOperator(
            task_id='get_rftchca_rptchca_policies',
            python_callable=get_pto_policy_assignments_update,
            op_args=['rftchca_rptchca']
        )

        def get_policySetScheduleEntries(previous_policy):
            new_policy_sets = rail.result('get_rftch_rptch_policies') or rail.result(
                'get_rftchca_rptchca_policies')
            if previous_policy:
                return previous_policy + new_policy_sets
            return new_policy_sets
        assign_time_offpolicy_assign_defaultpolicy = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_assign_defaultpolicy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": get_policySetScheduleEntries(dag_run.conf['previouspolicy'])
            }
        )

        user_tenure_lessthan_5 = rail.IfOperator(
            task_id='user_tenure_lessthan_5',
            test="{{ result('get_usertenure_servicedate') <= 5 }}",
            yes_task="is_rft_rpt_policyname2",
            no_task="if_get_usertenure_servicedate_greater_than_5_208",
        )

        is_rft_rpt_policyname2 = rail.IfOperator(
            task_id='is_rft_rpt_policyname2',
            test=lambda dag_run: dag_run.conf['policyname'] in ('RFT', 'RPT'),
            yes_task='get_rft_rpt_policies2',
            no_task='is_rftca_rptca_policyname2'
        )

        get_rft_rpt_policies2 = rail.PythonOperator(
            task_id='get_rft_rpt_policies2',
            python_callable=get_pto_policy_assignments_update,
            op_args=['rft_rpt_<=5']
        )

        is_rftca_rptca_policyname2 = rail.IfOperator(
            task_id='is_rftca_rptca_policyname2',
            test=lambda dag_run: dag_run.conf['policyname'] in (
                'RFT-CA', 'RPT-CA'),
            yes_task='get_rftca_rptca_policies2',
            no_task='if_get_usertenure_servicedate_greater_than_5_208'
        )

        get_rftca_rptca_policies2 = rail.PythonOperator(
            task_id='get_rftca_rptca_policies2',
            python_callable=get_pto_policy_assignments_update,
            op_args=['rftca_rptca<=5']
        )

        if_get_usertenure_servicedate_greater_than_5_208 = rail.IfOperator(
            task_id='if_get_usertenure_servicedate_greater_than_5_208',
            test="{{ 5 < result('get_usertenure_servicedate') < 10 }}",
            yes_task="is_rft_rpt_policyname3",
            no_task="if_get_usertenure_servicedate_greater_than_10_259",
        )

        is_rft_rpt_policyname3 = rail.IfOperator(
            task_id='is_rft_rpt_policyname3',
            test=lambda dag_run: dag_run.conf['policyname'] in ('RFT', 'RPT'),
            yes_task='get_rft_rpt_policies3',
            no_task='is_rftca_rptca_policyname3'
        )

        get_rft_rpt_policies3 = rail.PythonOperator(
            task_id='get_rft_rpt_policies3',
            python_callable=get_pto_policy_assignments_update,
            op_args=['rft_rpt_5-10']
        )

        is_rftca_rptca_policyname3 = rail.IfOperator(
            task_id='is_rftca_rptca_policyname3',
            test=lambda dag_run: dag_run.conf['policyname'] in (
                'RFT-CA', 'RPT-CA'),
            yes_task='get_rftca_rptca_policies3',
            no_task='if_get_usertenure_servicedate_greater_than_10_259'
        )

        get_rftca_rptca_policies3 = rail.PythonOperator(
            task_id='get_rftca_rptca_policies3',
            python_callable=get_pto_policy_assignments_update,
            op_args=['rftca_rptca_5-10']
        )

        if_get_usertenure_servicedate_greater_than_10_259 = rail.IfOperator(
            task_id='if_get_usertenure_servicedate_greater_than_10_259',
            test="{{ result('get_usertenure_servicedate') > 10 }}",
            yes_task="is_rft_rpt_policyname4",
            no_task="log_to_sumo",
        )

        is_rft_rpt_policyname4 = rail.IfOperator(
            task_id='is_rft_rpt_policyname4',
            test=lambda dag_run: dag_run.conf['policyname'] in ('RFT', 'RPT'),
            yes_task='get_rft_rpt_policies4',
            no_task='is_rftca_rptca_policyname4'
        )

        get_rft_rpt_policies4 = rail.PythonOperator(
            task_id='get_rft_rpt_policies4',
            python_callable=get_pto_policy_assignments_update,
            op_args=['rft_rpt_>10']
        )

        is_rftca_rptca_policyname4 = rail.IfOperator(
            task_id='is_rftca_rptca_policyname4',
            test=lambda dag_run: dag_run.conf['policyname'] in (
                'RFT-CA', 'RPT-CA'),
            yes_task='get_rftca_rptca_policies4',
            no_task='log_to_sumo'
        )

        get_rftca_rptca_policies4 = rail.PythonOperator(
            task_id='get_rftca_rptca_policies4',
            python_callable=get_pto_policy_assignments_update,
            op_args=['rftca_rptca_>10']
        )

        def get_policySetScheduleEntries2(previous_policy):
            new_policy_sets = rail.result('get_rft_rpt_policies2') or rail.result(
                'get_rftca_rptca_policies2') or rail.result('get_rft_rpt_policies3') or rail.result(
                'get_rftca_rptca_policies3') or rail.result('get_rft_rpt_policies4') or rail.result(
                'get_rftca_rptca_policies4')
            if previous_policy:
                return previous_policy + new_policy_sets
            return new_policy_sets
        assign_time_offpolicy_assign_defaultpolicy2 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_assign_defaultpolicy2',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": get_policySetScheduleEntries2(dag_run.conf['previouspolicy'])
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_pto_policyset

        get_pto_policyset >> get_usertenure_servicedate >> \
            get_maxbal_script_uri >> is_rftch_rptch_policyname

        is_rftch_rptch_policyname >> rail.Label(
            'Yes') >> get_rftch_rptch_policies >> assign_time_offpolicy_assign_defaultpolicy

        is_rftch_rptch_policyname >> rail.Label(
            'No') >> is_rftchca_rptchca_policyname

        is_rftchca_rptchca_policyname >> rail.Label(
            'Yes') >> get_rftchca_rptchca_policies >> assign_time_offpolicy_assign_defaultpolicy

        is_rftchca_rptchca_policyname >> rail.Label(
            'No') >> user_tenure_lessthan_5

        assign_time_offpolicy_assign_defaultpolicy >> user_tenure_lessthan_5

        user_tenure_lessthan_5 >> rail.Label(
            'Yes') >> is_rft_rpt_policyname2
        is_rft_rpt_policyname2 >> rail.Label(
            'Yes') >> get_rft_rpt_policies2 >> assign_time_offpolicy_assign_defaultpolicy2
        is_rft_rpt_policyname2 >> rail.Label(
            'No') >> is_rftca_rptca_policyname2
        is_rftca_rptca_policyname2 >> rail.Label(
            'Yes') >> get_rftca_rptca_policies2 >> assign_time_offpolicy_assign_defaultpolicy2
        is_rftca_rptca_policyname2 >> rail.Label(
            'No') >> if_get_usertenure_servicedate_greater_than_5_208
        user_tenure_lessthan_5 >> rail.Label(
            'No') >> if_get_usertenure_servicedate_greater_than_5_208
        if_get_usertenure_servicedate_greater_than_5_208 >> rail.Label(
            'Yes') >> is_rft_rpt_policyname3
        is_rft_rpt_policyname3 >> rail.Label(
            'Yes') >> get_rft_rpt_policies3 >> assign_time_offpolicy_assign_defaultpolicy2
        is_rft_rpt_policyname3 >> rail.Label(
            'No') >> is_rftca_rptca_policyname3
        is_rftca_rptca_policyname3 >> rail.Label(
            'Yes') >> get_rftca_rptca_policies3 >> assign_time_offpolicy_assign_defaultpolicy2
        is_rftca_rptca_policyname3 >> rail.Label(
            'No') >> if_get_usertenure_servicedate_greater_than_10_259
        if_get_usertenure_servicedate_greater_than_5_208 >> rail.Label(
            'No') >> if_get_usertenure_servicedate_greater_than_10_259
        if_get_usertenure_servicedate_greater_than_10_259 >> rail.Label(
            'Yes') >> is_rft_rpt_policyname4
        is_rft_rpt_policyname4 >> rail.Label(
            'Yes') >> get_rft_rpt_policies4 >> assign_time_offpolicy_assign_defaultpolicy2
        is_rft_rpt_policyname4 >> rail.Label(
            'No') >> is_rftca_rptca_policyname4
        is_rftca_rptca_policyname4 >> rail.Label(
            'Yes') >> get_rftca_rptca_policies4 >> assign_time_offpolicy_assign_defaultpolicy2
        is_rftca_rptca_policyname4 >> rail.Label(
            'No') >> log_to_sumo
        if_get_usertenure_servicedate_greater_than_10_259 >> rail.Label(
            'No') >> log_to_sumo

        assign_time_offpolicy_assign_defaultpolicy2 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
