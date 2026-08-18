from datetime import timedelta
import rail
from airflow.models import Variable
from adtalem.user_import.task.process_mappers import process_mappers_task_group
from adtalem.user_import.utils.python_callable_method import load_supervisor_from_collection
from adtalem.user_import.utils.request_payload import get_employeeid_from_emp_number, get_search_user_param
from adtalem.user_import.utils.response_filter import get_user_uri_by_loginname, get_user_uri_by_empid

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_caribbean_supervisor_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_caribbean_supervisor_{config.instance}',
        description=f'Adtalem Child Caribbean Supervisor {config.instance}',
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
            no_task='query_newuser_from_managerdnumber'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='query_newuser_from_managerdnumber',
            end_task='dagrun_log_to_sumo'
        )

        query_newuser_from_managerdnumber = rail.QueryCollectionOperator(
            task_id='query_newuser_from_managerdnumber',
            query="""SELECT * FROM rawdatacollectioncaribbean WHERE dnumber=:loginname""",
            query_params={
                'loginname': '{{ dag_run.conf.loginname }}'
            }
        )

        is_newuser_as_manager_present = rail.IfOperator(
            task_id='is_newuser_as_manager_present',
            test="{{ result('query_newuser_from_managerdnumber','length') > 0 }}",
            yes_task='get_required_supervisor_useruri',
            no_task='dagrun_log_to_sumo'
        )

        get_required_supervisor_useruri = rail.RepliconServiceOperator(
            task_id='get_required_supervisor_useruri',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:login-name'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': '{{ dag_run.conf.loginname }}'
                        }
                    }
                },
            },
            data_handler=get_user_uri_by_loginname
        )

        is_required_supervisor_useruri_present = rail.IfOperator(
            task_id='is_required_supervisor_useruri_present',
            test="{{ result('get_required_supervisor_useruri') | is_truthy }}",
            yes_task='get_new_supervisor',
            no_task='dagrun_log_to_sumo'
        )

        get_new_supervisor = rail.PythonOperator(
            task_id='get_new_supervisor',
            python_callable=load_supervisor_from_collection
        )

        is_user_in_paygroup = rail.IfOperator(
            task_id='is_user_in_paygroup',
            test=lambda: rail.result('get_new_supervisor')['paygroup'] in (
                'BRBLOC', 'STKITT', 'BRBEXP', 'STKLOC'),
            yes_task='process_empid_loginname',
            no_task='dagrun_log_to_sumo',
        )

        process_empid_loginname = rail.EmptyOperator(
            task_id='process_empid_loginname'
        )

        is_empid_loginname_present = rail.IfOperator(
            task_id='is_empid_loginname_present',
            test="{{ result('get_new_supervisor').employeeid | sn | is_truthy and \
                result('get_new_supervisor').loginname | sn | is_truthy }}",
            yes_task='create_caribbean_userlog',
            no_task='dagrun_log_to_sumo',
        )

        create_caribbean_userlog = rail.CreateLogOperator(
            task_id='create_caribbean_userlog'
        )

        (mapper_paygroup_jobcode, search_jobcode_in_mapper) = process_mappers_task_group(
            'caribbean_supervisor')

        is_caribbean_paygroup_present_jobcode_not_present = rail.IfOperator(
            task_id='is_caribbean_paygroup_present_jobcode_not_present',
            test=lambda: bool(len(rail.result('search_paygroup_in_mapper')) > 0 and len(
                rail.result('search_jobcode_in_mapper')) == 0),
            yes_task='get_employeeid',
            no_task='process_ignored_user'
        )

        get_employeeid = rail.PythonOperator(
            task_id='get_employeeid',
            python_callable=get_employeeid_from_emp_number,
            op_args=["{{ result('get_new_supervisor').employeeid }}"]
        )

        get_required_useruri = rail.RepliconServiceOperator(
            task_id='get_required_useruri',
            endpoint='/services/UserListService1.svc/GetData',
            data=get_search_user_param,
            data_handler=get_user_uri_by_empid
        )

        is_user_not_present = rail.IfOperator(
            task_id='is_user_not_present',
            test="{{ result('get_required_useruri') | is_falsy  }}",
            yes_task='trigger_caribbean_adduser_production_child',
            no_task='process_ignored_user'
        )

        trigger_caribbean_adduser_production_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_caribbean_adduser_production_child',
            retries=0,
            items=lambda: [rail.result('get_new_supervisor')],
            trigger_dag_id=f'adtalem_userimport_caribbean_child_add_user_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda dag_run, item: {
                **{
                    k: v.strip() if v else '' for k, v in item.items()
                },
                **{
                    'log': rail.result('create_caribbean_userlog'),
                    'supervisorpermissionuri': dag_run.conf['supervisorpermissionuri'],
                    'enduserpermissionuri': dag_run.conf['enduserpermissionuri']
                }
            }
        )

        wait_for_caribbean_adduser_production_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_caribbean_adduser_production_child',
            dag_runs="{{ result('trigger_caribbean_adduser_production_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        process_ignored_user = rail.EmptyOperator(
            task_id='process_ignored_user'
        )

        is_caribbean_paygroup_not_present_jobcode_present = rail.IfOperator(
            task_id='is_caribbean_paygroup_not_present_jobcode_present',
            test=lambda: bool(len(rail.result('search_paygroup_in_mapper')) == 0 and len(
                rail.result('search_jobcode_in_mapper')) > 0),
            yes_task='write_ignored_user',
            no_task='dagrun_log_to_sumo'
        )

        write_ignored_user = rail.WriteLogOperator(
            task_id='write_ignored_user',
            log="{{ result('create_caribbean_userlog') }}",
            message='Not in allowed pay group and Job Code combination.',
            severity='Ignored',
            properties={
                'login_name': "{{ result('get_new_supervisor').loginname }}",
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
            'No') >> query_newuser_from_managerdnumber >> is_newuser_as_manager_present

        is_newuser_as_manager_present >> rail.Label(
            'Yes') >> get_required_supervisor_useruri >> is_required_supervisor_useruri_present

        is_required_supervisor_useruri_present >> rail.Label(
            'Yes') >> get_new_supervisor >> is_user_in_paygroup

        is_user_in_paygroup >> rail.Label(
            'Yes') >> process_empid_loginname >> is_empid_loginname_present

        is_empid_loginname_present >> rail.Label(
            'Yes') >> create_caribbean_userlog >> mapper_paygroup_jobcode

        search_jobcode_in_mapper >> is_caribbean_paygroup_present_jobcode_not_present

        is_caribbean_paygroup_present_jobcode_not_present >> rail.Label(
            'Yes') >> get_employeeid >> get_required_useruri >> is_user_not_present

        is_user_not_present >> rail.Label(
            'Yes') >> trigger_caribbean_adduser_production_child >> \
            wait_for_caribbean_adduser_production_child >> dagrun_log_to_sumo

        is_user_not_present >> rail.Label(
            'No') >> process_ignored_user

        is_caribbean_paygroup_present_jobcode_not_present >> rail.Label(
            'No') >> process_ignored_user

        process_ignored_user >> is_caribbean_paygroup_not_present_jobcode_present

        is_caribbean_paygroup_not_present_jobcode_present >> rail.Label(
            'Yes') >> write_ignored_user >> dagrun_log_to_sumo

        is_caribbean_paygroup_not_present_jobcode_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_empid_loginname_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_user_in_paygroup >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_required_supervisor_useruri_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_newuser_as_manager_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_caribbean_supervisor_child_dag)
