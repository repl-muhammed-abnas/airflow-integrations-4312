from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


def create_usersync_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_user_sync_child_{config.instance}',
        description=f'LIVE | Mccarthy User Sync_Child {config.instance}',
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
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        def get_user_uri_by_loginname(response, dag_run):
            user_uris = [item['cells'][0]['uri'] for item in response['rows']
                         if item['cells'][1]['textValue'] == dag_run.conf['Loginname']] if response['rows'] else []
            return rail.smartjoin_by_delim(user_uris) if user_uris else ''
        search_user_by_loginname = rail.RepliconServiceOperator(
            task_id='search_user_by_loginname',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{ dag_run.conf.Loginname }}"
                        }
                    }
                }
            },
            data_handler=get_user_uri_by_loginname
        )

        is_userpresent = rail.IfOperator(
            task_id='is_userpresent',
            test="{{ result('search_user_by_loginname') | is_truthy }}",
            yes_task="trigger_update_child_dag",
            no_task="is_mandatoryfield_present"
        )

        trigger_update_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_update_child_dag',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'mccarthy_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{k: v for k, v in item.items() if k not in ('_ancestry', '_ecid', '_replication_position')},
                **{
                    'useruri': rail.result('search_user_by_loginname'),
                    'log': rail.result('create_log')
                }
            }
        )

        wait_for_update_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_update_child_dag") }}'
        )

        is_mandatoryfield_present = rail.IfOperator(
            task_id='is_mandatoryfield_present',
            test='{{ dag_run.conf.Firstname | is_truthy and dag_run.conf.Lastname | is_truthy \
                and dag_run.conf.Loginname | is_truthy }}',
            yes_task="trigger_add_child_dag",
            no_task="write_mandatoryfield_exception"
        )

        trigger_add_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_add_child_dag',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'mccarthy_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{k: v for k, v in item.items() if k not in ('_ancestry', '_ecid', '_replication_position')},
                **{
                    'log': rail.result('create_log')
                }
            }
        )

        wait_for_add_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_add_child_dag") }}'
        )

        write_mandatoryfield_exception = rail.WriteLogOperator(
            task_id='write_mandatoryfield_exception',
            log="{{ result('create_log') }}",
            message='User could not be import as one or more mandatory fields are missing',
            severity='Skipped',
            properties={
                'loginname': '{{ dag_run.conf.Loginname }}',
                'email': '{{ dag_run.conf.Email }}',
                'action': 'Add',
                'status': 'Skipped',
                'details': 'User could not be import as one or more mandatory fields are missing'
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
            'No') >> create_log
        create_log >> search_user_by_loginname >> is_userpresent
        is_userpresent >> rail.Label(
            'Yes') >> trigger_update_child_dag >> wait_for_update_child_dag >> dagrun_log_to_sumo
        is_userpresent >> rail.Label(
            'No') >> is_mandatoryfield_present
        is_mandatoryfield_present >> rail.Label(
            'Yes') >> trigger_add_child_dag >> wait_for_add_child_dag >> dagrun_log_to_sumo
        is_mandatoryfield_present >> rail.Label(
            'No') >> write_mandatoryfield_exception >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_usersync_child_dag)
