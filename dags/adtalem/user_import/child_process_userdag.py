from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.task.process_child_addupdate_dags import process_child_addupdate_task_group
from adtalem.user_import.task.process_mappers import process_mappers_task_group
from adtalem.user_import.utils.python_callable_method import is_us_user_based_on_paygroup
from adtalem.user_import.utils.request_payload import get_employeeid_from_emp_number, get_search_user_param
from adtalem.user_import.utils.response_filter import get_user_uri_by_empid


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_process_user_{config.instance}',
        description=f'Adtalem Child Process User {config.instance}',
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
            no_task='get_employeeid'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_employeeid',
            end_task='finish',
        )

        get_employeeid = rail.PythonOperator(
            task_id='get_employeeid',
            python_callable=get_employeeid_from_emp_number,
            op_args=['{{ dag_run.conf.employeenumber }}']
        )

        is_employeenumber_present = rail.IfOperator(
            task_id='is_employeenumber_present',
            test='{{ dag_run.conf.employeenumber | is_truthy  }}',
            yes_task='create_userlog',
            no_task='finish',
        )

        create_userlog = rail.CreateLogOperator(
            task_id='create_userlog'
        )

        (mapper_paygroup_jobcode,
         search_jobcode_in_mapper) = process_mappers_task_group()

        get_required_useruri = rail.RepliconServiceOperator(
            task_id='get_required_useruri',
            endpoint='/services/UserListService1.svc/GetData',
            data=get_search_user_param,
            data_handler=get_user_uri_by_empid
        )

        is_paygroup_present_jobcode_not_present = rail.IfOperator(
            task_id='is_paygroup_present_jobcode_not_present',
            test=lambda: bool(len(rail.result('search_paygroup_in_mapper')) > 0 and len(
                rail.result('search_jobcode_in_mapper')) == 0),
            yes_task='us_user_or_not',
            no_task='process_disableuser'
        )

        us_user_or_not = rail.PythonOperator(
            task_id='us_user_or_not',
            python_callable=is_us_user_based_on_paygroup,
            op_args=['{{ dag_run.conf.paygroup }}']
        )

        is_user_canbwk_crrngt_paygroup = rail.IfOperator(
            task_id='is_user_canbwk_crrngt_paygroup',
            test=lambda dag_run: dag_run.conf['paygroup'] in (
                'CANBWK', 'CRRNGT'),
            yes_task='process_adduser_active2020_cr40',
            no_task='process_adduser_v2_2021',
        )

        process_adduser_active2020_cr40 = rail.EmptyOperator(
            task_id='process_adduser_active2020_cr40'
        )

        (is_user_not_present_active2020_cr40, wait_for_adduser_active2020_cr40_child,
         wait_for_updateuser_active2020_cr40_child) = process_child_addupdate_task_group(
            config.execution_timeout_days, config.instance, 'active2020_crv4.0')

        process_adduser_v2_2021 = rail.EmptyOperator(
            task_id='process_adduser_v2_2021'
        )

        (is_user_not_present_crv2, wait_for_adduser_crv2_child,
         wait_for_updateuser_crv2_child) = process_child_addupdate_task_group(
            config.execution_timeout_days, config.instance, 'v2_2021')

        process_disableuser = rail.EmptyOperator(
            task_id='process_disableuser'
        )

        is_paygroup_present_jobcode_present = rail.IfOperator(
            task_id='is_paygroup_present_jobcode_present',
            test=lambda: bool(len(rail.result('search_paygroup_in_mapper')) > 0 and len(
                rail.result('search_jobcode_in_mapper')) > 0),
            yes_task='process_adduser_in_disabledstatus',
            no_task='write_exception_log',
        )

        process_adduser_in_disabledstatus = rail.EmptyOperator(
            task_id='process_adduser_in_disabledstatus'
        )

        (is_user_not_present_disabledstatus, wait_for_adduser_disabledstatus_child,
         wait_for_updateuser_disabledstatus_child) = process_child_addupdate_task_group(
            config.execution_timeout_days, config.instance, 'disabledstatus')

        write_exception_log = rail.WriteLogOperator(
            task_id='write_exception_log',
            log="{{ result('create_userlog') }}",
            message='Not in allowed pay group.',
            severity='Ignored',
            properties={
                'login_name': '{{ dag_run.conf.dnumber }}',
                'status': 'Ignored',
                'failure_reason': 'Not in allowed pay group.'
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> get_employeeid

        get_employeeid >> is_employeenumber_present

        is_employeenumber_present >> rail.Label(
            'Yes') >> create_userlog >> mapper_paygroup_jobcode

        search_jobcode_in_mapper >> get_required_useruri >> is_paygroup_present_jobcode_not_present

        is_paygroup_present_jobcode_not_present >> rail.Label(
            'Yes') >> us_user_or_not >> is_user_canbwk_crrngt_paygroup

        is_user_canbwk_crrngt_paygroup >> rail.Label(
            'Yes') >> process_adduser_active2020_cr40 >> is_user_not_present_active2020_cr40

        wait_for_adduser_active2020_cr40_child >> finish

        wait_for_updateuser_active2020_cr40_child >> finish

        is_user_canbwk_crrngt_paygroup >> rail.Label(
            'No') >> process_adduser_v2_2021 >> is_user_not_present_crv2

        wait_for_adduser_crv2_child >> finish

        wait_for_updateuser_crv2_child >> finish

        is_paygroup_present_jobcode_not_present >> rail.Label(
            'No') >> process_disableuser >> is_paygroup_present_jobcode_present

        is_paygroup_present_jobcode_present >> rail.Label(
            'Yes') >> process_adduser_in_disabledstatus >> is_user_not_present_disabledstatus

        wait_for_adduser_disabledstatus_child >> finish

        wait_for_updateuser_disabledstatus_child >> finish

        is_paygroup_present_jobcode_present >> rail.Label(
            'No') >> write_exception_log >> finish

        is_employeenumber_present >> rail.Label(
            'No') >> finish

        return dag


rail.for_each_instance(create_user_child_dag)
