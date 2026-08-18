from datetime import timedelta, date
from pendulum import datetime, now
from airflow.models import Variable
import rail
from pwcglobal.ord_department_hierarchy_sync.utils import python_callable_method

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'pwc_ord_department_group_hierarchy_sync_master_nzl_v10_{config.instance}',
        description=f'PwC | ORD Department Group Hierarchy Sync Master - NZL V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1, tz=config.schedule_timezone_Aukland),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_current_time_in_tz'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_current_time_in_tz',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_current_time_in_tz = rail.PythonOperator(
            task_id='get_current_time_in_tz',
            python_callable=lambda: now(
                config.schedule_timezone_Aukland).isoformat()
        )

        get_enabled_department_groups_4 = rail.RepliconServiceOperator(
            task_id='get_enabled_department_groups_4',
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",
            data=None
        )

        pwc_ord_mapper_search_entries_7 = rail.PythonOperator(
            task_id='pwc_ord_mapper_search_entries_7',
            python_callable=python_callable_method.get_ord_mapper_specific_values,
            op_args=[config.ord_mapper]
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_child_v1_0async_9 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_child_v1_0async_9',
            retries=0,
            items="{{ result('pwc_ord_mapper_search_entries_7') | to_json }}",
            trigger_dag_id=f'pwc_ord_department_group_hierarchy_sync_child_v10_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "datemodified": (date.today() - timedelta(days=1)).strftime("%Y%m%d") if not item['startdate'] else item['startdate'],
                "ordlevel1": item['level_1'],
                "prefix": item['prefix'],
                "id": item['id'],
                # pylint: disable=line-too-long
                "ordlevel1uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_department_groups_4'), 'displayText', item['level_1'], 'uri'),
                "rooturi": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_department_groups_4'), 'displayText', 'PwC', 'uri'),
                "jobcreatedtime": rail.result('get_current_time_in_tz')
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_child_v1_0async_9 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_child_v1_0async_9',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_child_v1_0async_9") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_current_time_in_tz
        get_current_time_in_tz >> get_enabled_department_groups_4 >> pwc_ord_mapper_search_entries_7 \
            >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_child_v1_0async_9 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_child_v1_0async_9 \
            >> finish

        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
