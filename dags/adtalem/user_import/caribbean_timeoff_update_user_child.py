from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils import python_callable_method


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_timeoff_updateuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_caribbean_child_timeoff_update_user_{config.instance}',
        description=f'Adtalem Carribean Update User - Time Off_Prod Final {config.instance}',
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
            no_task='get_salary'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_salary',
            end_task='dagrun_log_to_sumo'
        )

        get_salary = rail.PythonOperator(
            task_id='get_salary',
            python_callable=python_callable_method.get_salary_timeoff
        )

        get_mapper_entries = rail.PythonOperator(
            task_id='get_mapper_entries',
            python_callable=python_callable_method.get_mapper_entries_from_adtalem_caribbean_mapperfile,
            op_args=["{{ dag_run.conf.paygroup }}",
                     "{{ dag_run.conf.jobcode }}"]
        )

        get_timeofftypes_from_mapper = rail.PythonOperator(
            task_id='get_timeofftypes_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Time Off Types']
        )

        get_assigned_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_assigned_timeofftypes',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers",
            data={
                "userUris": [
                    "{{ dag_run.conf.useruri }}"
                ]
            },
            data_handler=lambda response: response[0] if response else ''
        )

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        is_mapper_timeoff_types_present = rail.IfOperator(
            task_id='is_mapper_timeoff_types_present',
            test="{{ result('get_timeofftypes_from_mapper') | is_truthy }}",
            yes_task="get_timeofftype_uris_to_assign",
            no_task="dagrun_log_to_sumo"
        )

        get_timeofftype_uris_to_assign = rail.PythonOperator(
            task_id='get_timeofftype_uris_to_assign',
            python_callable=python_callable_method.get_timeofftype_uris_caribbean
        )

        assign_required_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_required_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('get_timeofftype_uris_to_assign')
            }
        )

        get_timeoff_previously_not_newly_assigned = rail.PythonOperator(
            task_id='get_timeoff_previously_not_newly_assigned',
            python_callable=python_callable_method.get_timeoffs_not_in_newset
        )

        is_timeoff_previously_not_newly_assigned_present = rail.IfOperator(
            task_id='is_timeoff_previously_not_newly_assigned_present',
            test="{{ result('get_timeoff_previously_not_newly_assigned') | length > 0 }}",
            yes_task="trigger_put_0_balance_caribbean",
            no_task="is_userstatus_disabled",
        )

        trigger_put_0_balance_caribbean = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_put_0_balance_caribbean',
            retries=0,
            items=lambda: rail.result(
                'get_timeoff_previously_not_newly_assigned'),
            trigger_dag_id=f'adtalem_userimport_caribbean_child_put_0_balance_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "timeoffuri": "{{ item }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "terminationdate": "{{ current_time('%m/%d/%Y') }}",
            }
        )

        is_userstatus_disabled = rail.IfOperator(
            task_id='is_userstatus_disabled',
            test="{{ dag_run.conf.userstatus == 'Disabled' }}",
            yes_task="trigger_assignpolicy_rehireusers_disabled",
            no_task="is_userstatus_enabled",
        )

        trigger_assignpolicy_rehireusers_disabled = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_assignpolicy_rehireusers_disabled',
            retries=0,
            items=lambda: rail.result(
                'get_timeofftype_uris_to_assign'),
            trigger_dag_id=f'adtalem_userimport_caribbean_child_assign_policy_for_rehire_update_users_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "status": "{{ dag_run.conf.userstatus }}",
                "timeoffuri": "{{ item }}",
                "useruri": "{{ dag_run.conf.useruri }}"
            }
        )

        is_userstatus_enabled = rail.IfOperator(
            task_id='is_userstatus_enabled',
            test="{{ dag_run.conf.userstatus == 'Enabled' }}",
            yes_task="get_timeoff_new_assigned_not_oldset",
            no_task="dagrun_log_to_sumo"
        )

        get_timeoff_new_assigned_not_oldset = rail.PythonOperator(
            task_id='get_timeoff_new_assigned_not_oldset',
            python_callable=python_callable_method.get_timeoffs_not_in_oldset
        )

        trigger_assignpolicy_rehireusers_enabled = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_assignpolicy_rehireusers_enabled',
            retries=0,
            items=lambda: rail.result(
                'get_timeoff_new_assigned_not_oldset'),
            trigger_dag_id=f'adtalem_userimport_caribbean_child_assign_policy_for_rehire_update_users_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "status": "{{ dag_run.conf.userstatus }}",
                "timeoffuri": "{{ item }}",
                "useruri": "{{ dag_run.conf.useruri }}"
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
            'No') >> get_salary

        get_salary >> get_mapper_entries >> get_timeofftypes_from_mapper >> get_assigned_timeofftypes >> \
            get_alltimeoff_types >> is_mapper_timeoff_types_present
        is_mapper_timeoff_types_present >> rail.Label(
            'Yes') >> get_timeofftype_uris_to_assign >> assign_required_timeofftypes >> \
            get_timeoff_previously_not_newly_assigned >> is_timeoff_previously_not_newly_assigned_present

        is_timeoff_previously_not_newly_assigned_present >> rail.Label(
            'Yes') >> trigger_put_0_balance_caribbean >> is_userstatus_disabled
        is_timeoff_previously_not_newly_assigned_present >> rail.Label(
            'No') >> is_userstatus_disabled
        is_userstatus_disabled >> rail.Label(
            'Yes') >> trigger_assignpolicy_rehireusers_disabled >> dagrun_log_to_sumo
        is_userstatus_disabled >> rail.Label(
            'No') >> is_userstatus_enabled
        is_userstatus_enabled >> rail.Label(
            'Yes') >> get_timeoff_new_assigned_not_oldset >> \
            trigger_assignpolicy_rehireusers_enabled >> dagrun_log_to_sumo
        is_userstatus_enabled >> rail.Label(
            'No') >> dagrun_log_to_sumo
        is_mapper_timeoff_types_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_timeoff_updateuser_child_dag)
