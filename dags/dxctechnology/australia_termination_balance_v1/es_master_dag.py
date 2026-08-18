from datetime import timedelta
from pendulum import datetime as dt
import rail
from dxctechnology.australia_termination_balance_v1.utils import request_payload, response_filter, python_callable_method
from dxctechnology.australia_termination_balance_v1.mapper.company_code_mapper_aus import COMPANY_CODE_MAP_AUS

null = None
def create_main_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_aus_termination_balance_es_master_{config.instance}_v1",
        description=f"DXC - AUS_termination_balance_ES_Master - {config.instance}",
        company_key=config.company_key,
         start_date=dt(2022, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_dag_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        get_all_scripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
        )

        get_all_timeOffTypes = rail.RepliconServiceOperator(
            task_id="get_all_timeOffTypes",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        dxc_payroll_extract_mapper_aus_search_entries = rail.PythonOperator(
            task_id='dxc_payroll_extract_mapper_aus_search_entries',
            python_callable=lambda:  list(
                filter(lambda x: x["Parent_Company_code"] == "ES", COMPANY_CODE_MAP_AUS))
        )

        has_mapper_data = rail.IfOperator(
            task_id='has_mapper_data',
            test=lambda: bool(list(filter(lambda x: x, map(lambda x: rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts'), 'displayText', x['fileformat_name'], 'uri'), rail.result(
                'dxc_payroll_extract_mapper_aus_search_entries'))))) and rail.result('dxc_payroll_extract_mapper_aus_search_entries') and rail.result('dxc_payroll_extract_mapper_aus_search_entries')[0]['type'] == 'Compass',
            yes_task="get_enabled_divisions",
            no_task="finish",
        )

        get_enabled_divisions = rail.RepliconServiceOperator(
            task_id='get_enabled_divisions',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        get_required_data = rail.PythonOperator(
            task_id = 'get_required_data',
            python_callable= python_callable_method.get_required_data
        )

        get_all_terminated_users = rail.RepliconServiceOperator(
            task_id='get_all_terminated_users',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: request_payload.get_terminated_users(config.cutoff_date),
            data_handler= response_filter.get_all_terminated_users_list
        )

        create_collection_create_list_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv',
            source="{{ result('get_all_terminated_users') | to_json }}",
            name="terminateduserslist",
            columns={
                'username': 'username',
                'useruri': 'useruri',
                'employeeid': 'employeeid',
                'companycode': 'companycode',
                'enddate': 'enddate',
                'userid': 'userid',
                'companycodeinlist': 'iscompanycodeallowed'
            }
        )

        query_list_users_with_allowed_companycode = rail.QueryCollectionOperator(
            task_id='query_list_users_with_allowed_companycode',
            query="""SELECT * FROM  terminateduserslist WHERE  terminateduserslist.iscompanycodeallowed="Yes" """,
            name="validatedterminateduserslist",
        )

        if_query_list_users_with_allowed_companycode = rail.IfOperator(
            task_id='if_query_list_users_with_allowed_companycode',
            test='''{{ result('query_list_users_with_allowed_companycode','length') > 0 }}''',
            yes_task="query_list_terminated_users",
            no_task="finish",
        )

        query_list_terminated_users = rail.QueryCollectionOperator(
            task_id='query_list_terminated_users',
            query="""SELECT * FROM  validatedterminateduserslist""",
        )

        process_terminated_users = rail.TriggerDagRunOperator(
            task_id='process_terminated_users',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_terminated_export_child_{config.instance}_v1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.get_child_dagrun_conf('ES')
        )

        wait_for_process_terminated_users = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_terminated_users',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_terminated_users") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        get_all_scripts >> get_all_timeOffTypes >> dxc_payroll_extract_mapper_aus_search_entries >> has_mapper_data
        has_mapper_data >> rail.Label(
            'Yes') >> get_enabled_divisions >> get_required_data >> get_all_terminated_users >> create_collection_create_list_from_csv >> query_list_users_with_allowed_companycode >> if_query_list_users_with_allowed_companycode
        if_query_list_users_with_allowed_companycode >> rail.Label(
            'Yes') >> query_list_terminated_users >> process_terminated_users >> wait_for_process_terminated_users >> finish
        if_query_list_users_with_allowed_companycode >> rail.Label(
            'No') >> finish >> log_to_sumo
        has_mapper_data >> rail.Label(
            'Yes') >> finish

    return dag

rail.for_each_instance(create_main_dag)
