from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_processuser_{config.instance}',
        description=f'TerraconConsultants User import Child Process User {config.instance}',
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
            no_task='create_userlog'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_userlog',
            end_task='dagrun_log_to_sumo',
        )

        create_userlog = rail.CreateLogOperator(
            task_id='create_userlog'
        )

        is_actiontype_disable = rail.IfOperator(
            task_id='is_actiontype_disable',
            test=lambda dag_run: dag_run.conf.get(
                'actiontype', '') == 'disable',
            yes_task="get_useradminpermission_if_present",
            no_task="is_employeenumber_present",
        )

        get_useradminpermission_if_present = rail.RepliconServiceOperator(
            task_id='get_useradminpermission_if_present',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                'userUri': '{{ dag_run.conf.uri }}'
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:administration', 'policyUri', '')
        )

        is_admin_permission_present = rail.IfOperator(
            task_id='is_admin_permission_present',
            test="{{ result('get_useradminpermission_if_present') | is_falsy }}",
            yes_task="trigger_disable_user_dag",
            no_task="write_disableuser_exception",
        )

        trigger_disable_user_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_disable_user_dag',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'terraconconsultants_userimport_child_disable_user_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{k.lower(): v for k, v in item.items() if k not in ('_ancestry', '_ecid', '_replication_position')},
                'log': rail.result('create_userlog')
            }
        )

        wait_for_disable_user_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_user_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_disable_user_dag") }}'
        )

        write_disableuser_exception = rail.WriteLogOperator(
            task_id='write_disableuser_exception',
            log="{{ result('create_userlog') }}",
            message='User not disabled, has admin permission assigned | {{ dag_run.conf.uri }}',
            severity='Exception',
            properties={
                'loginname': '{{ dag_run.conf.loginname }}',
                'uri': '{{ dag_run.conf.uri }}',
                'action': 'Disable',
                'status': 'Exception',
                'reason': 'User not disabled, has admin permission assigned'
            }
        )

        is_employeenumber_present = rail.IfOperator(
            task_id='is_employeenumber_present',
            test="{{ dag_run.conf.employeenumber | is_truthy }}",
            yes_task="is_date_in_correctformat",
            no_task="write_employeeid_exception_log",
        )

        is_date_in_correctformat = rail.IfOperator(
            task_id='is_date_in_correctformat',
            test=lambda dag_run: '/' in dag_run.conf['startdate'] and '/' in
            (dag_run.conf['service_date']
             if dag_run.conf['service_date'] else '/'),
            yes_task="is_user_present_in_replicon",
            no_task="write_invalid_date_format_log",
        )

        is_user_present_in_replicon = rail.IfOperator(
            task_id='is_user_present_in_replicon',
            test="{{ dag_run.conf.useruri | is_truthy }}",
            yes_task="trigger_terraconconsultants_user_sync_update_v3",
            no_task="trigger_terraconconsultants_user_sync_addasync",
        )

        trigger_terraconconsultants_user_sync_update_v3 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_terraconconsultants_user_sync_update_v3',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'terraconconsultants_userimport_child_updateuser_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{k.lower(): v for k, v in item.items() if k not in ('_ancestry', '_ecid', '_replication_position')},
                'log': rail.result('create_userlog')
            }
        )

        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user',
            dag_runs="{{ result('trigger_terraconconsultants_user_sync_update_v3') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        trigger_terraconconsultants_user_sync_addasync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_terraconconsultants_user_sync_addasync',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'terraconconsultants_userimport_child_adduser_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{k.lower(): v for k, v in item.items() if k not in ('_ancestry', '_ecid', '_replication_position')},
                'log': rail.result('create_userlog')
            }
        )

        wait_for_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_user',
            dag_runs="{{ result('trigger_terraconconsultants_user_sync_addasync') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        write_invalid_date_format_log = rail.WriteLogOperator(
            task_id='write_invalid_date_format_log',
            log="{{ result('create_userlog') }}",
            message='EmployeeID not present in the feed file',
            severity='Exception',
            properties={
                'loginname': '{{ dag_run.conf.employeenumber }}',
                'uri': '',
                'action': 'Invalid',
                'status': 'Exception',
                'reason': 'Invalid Date format received.'
            }
        )

        write_employeeid_exception_log = rail.WriteLogOperator(
            task_id='write_employeeid_exception_log',
            log="{{ result('create_userlog') }}",
            message='EmployeeID not present in the feed file',
            severity='Exception',
            properties={
                'loginname': '{{ dag_run.conf.firstname }}, {{ dag_run.conf.lastname }}',
                'uri': '',
                'action': 'Invalid',
                'status': 'Exception',
                'reason': 'EmployeeID not present in the feed file'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> create_userlog

        create_userlog >> is_actiontype_disable

        is_actiontype_disable >> rail.Label(
            'Yes') >> get_useradminpermission_if_present >> is_admin_permission_present

        is_admin_permission_present >> rail.Label(
            'Yes') >> trigger_disable_user_dag >> wait_for_disable_user_dag >> \
            dagrun_log_to_sumo

        is_admin_permission_present >> rail.Label(
            'No') >> write_disableuser_exception >> dagrun_log_to_sumo

        is_actiontype_disable >> rail.Label(
            'No') >> is_employeenumber_present

        is_employeenumber_present >> rail.Label(
            'Yes') >> is_date_in_correctformat

        is_date_in_correctformat >> rail.Label(
            'Yes') >> is_user_present_in_replicon

        is_user_present_in_replicon >> rail.Label(
            'Yes') >> trigger_terraconconsultants_user_sync_update_v3 >> wait_for_update_user >> \
            dagrun_log_to_sumo

        is_user_present_in_replicon >> rail.Label(
            'No') >> trigger_terraconconsultants_user_sync_addasync >> wait_for_add_user >> \
            dagrun_log_to_sumo

        is_date_in_correctformat >> rail.Label(
            'No') >> write_invalid_date_format_log >> dagrun_log_to_sumo

        is_employeenumber_present >> rail.Label(
            'No') >> write_employeeid_exception_log >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_user_child_dag)
