from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.task.process_child_addupdate_dags import process_child_addupdate_task_group
from adtalem.user_import.task.process_mappers import process_mappers_task_group
from adtalem.user_import.utils.request_payload import get_employeeid_from_emp_number, get_search_user_param
from adtalem.user_import.utils.response_filter import get_user_uri_by_empid


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_caribbean_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_caribbean_process_user_{config.instance}',
        description=f'Adtalem Child Caribbean Process User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_user',
            end_task='dagrun_log_to_sumo'
        )

        process_user = rail.CreateLogOperator(
            task_id='process_user'
        )

        is_user_in_paygroup = rail.IfOperator(
            task_id='is_user_in_paygroup',
            test=lambda dag_run: dag_run.conf['paygroup'] in (
                'BRBLOC', 'STKITT', 'BRBEXP', 'STKLOC'),
            yes_task='process_empnumber',
            no_task='dagrun_log_to_sumo',
        )

        process_empnumber = rail.EmptyOperator(
            task_id='process_empnumber'
        )

        is_empnumber_present = rail.IfOperator(
            task_id='is_empnumber_present',
            test='{{ dag_run.conf.employeenumber | is_truthy}}',
            yes_task='create_caribbean_userlog',
            no_task='dagrun_log_to_sumo',
        )

        create_caribbean_userlog = rail.CreateLogOperator(
            task_id='create_caribbean_userlog'
        )

        (mapper_paygroup_jobcode, search_jobcode_in_mapper) = process_mappers_task_group(
            'caribbean_user')

        is_caribbean_paygroup_present_jobcode_not_present = rail.IfOperator(
            task_id='is_caribbean_paygroup_present_jobcode_not_present',
            test=lambda: bool(len(rail.result('search_paygroup_in_mapper')) > 0 and len(
                rail.result('search_jobcode_in_mapper')) == 0),
            yes_task='get_employeeid',
            no_task='write_ignored_user'
        )

        get_employeeid = rail.PythonOperator(
            task_id='get_employeeid',
            python_callable=get_employeeid_from_emp_number,
            op_args=['{{ dag_run.conf.employeenumber }}']
        )

        get_required_useruri = rail.RepliconServiceOperator(
            task_id='get_required_useruri',
            endpoint='/services/UserListService1.svc/GetData',
            data=get_search_user_param,
            data_handler=get_user_uri_by_empid
        )

        (is_user_not_present_caribbean, wait_for_adduser_caribbean_child,
         wait_for_updateuser_caribbean_child) = process_child_addupdate_task_group(
            config.execution_timeout_days, config.instance, 'caribbean')

        write_ignored_user = rail.WriteLogOperator(
            task_id='write_ignored_user',
            log="{{ result('create_caribbean_userlog') }}",
            message='Not in allowed pay group and Job Code combination.',
            severity='Ignored',
            properties={
                'login_name': '{{ dag_run.conf.dnumber }}',
                'status': 'Ignored',
                'failure_reason': 'Not in allowed pay group and Job Code combination.'
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
            'No') >> process_user >> is_user_in_paygroup

        is_user_in_paygroup >> rail.Label(
            'Yes') >> process_empnumber >> is_empnumber_present

        is_empnumber_present >> rail.Label(
            'Yes') >> create_caribbean_userlog >> mapper_paygroup_jobcode

        search_jobcode_in_mapper >> is_caribbean_paygroup_present_jobcode_not_present

        is_caribbean_paygroup_present_jobcode_not_present >> rail.Label(
            'Yes') >> get_employeeid >> get_required_useruri >> is_user_not_present_caribbean

        wait_for_adduser_caribbean_child >> dagrun_log_to_sumo

        wait_for_updateuser_caribbean_child >> dagrun_log_to_sumo

        is_caribbean_paygroup_present_jobcode_not_present >> rail.Label(
            'No') >> write_ignored_user >> dagrun_log_to_sumo

        is_empnumber_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_user_in_paygroup >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_caribbean_user_child_dag)
