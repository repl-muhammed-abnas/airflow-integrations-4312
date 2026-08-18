from datetime import timedelta
from airflow.models import Variable
from ge.user_sync_poland.utils import custom_methods
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_schedule_add_dag_id,
        description=f'GE POLAND User Import Schedule Add Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_inputfilerawdata_for_records'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_inputfilerawdata_for_records',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_inputfilerawdata_for_records = rail.QueryCollectionOperator(
            task_id='query_inputfilerawdata_for_records',
            name='records_to_process',
            query="""SELECT * FROM inputfilerawdata"""
        )

        get_all_replicon_office_schedules_7 = rail.RepliconServiceOperator(
            task_id='get_all_replicon_office_schedules_7',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        ge_poland_user_sync_master_mapper_search_entries_9 = rail.PythonOperator(
            task_id='ge_poland_user_sync_master_mapper_search_entries_9',
            python_callable=lambda:  list(
                filter(lambda x: x["type"] == "Default Schedule", config.POLAND_MASTER_MAPPER))
        )

        get_schedules_to_assign_for_all_records_10_17 = rail.PythonOperator(
            task_id='get_schedules_to_assign_for_all_records_10_17',
            python_callable=lambda: custom_methods.get_schedules_to_assign(rail.result('get_all_replicon_office_schedules_7'), rail.result(
                'ge_poland_user_sync_master_mapper_search_entries_9'))
        )

        create_collection_for_schedules_to_assign_list_18 = rail.CreateCollectionOperator(
            task_id='create_collection_for_schedules_to_assign_list_18',
            source="{{result('get_schedules_to_assign_for_all_records_10_17') | to_json}}",
            columns={
                'schedulename': 'schedulename',
                'scheduleuri': 'scheduleuri'
            },
            name='schedules_to_assign'
        )

        query_collection_schedules_to_assign_for_new_schedules_19 = rail.QueryCollectionOperator(
            task_id='query_collection_schedules_to_assign_for_new_schedules_19',
            name='schedules_to_create',
            query="""SELECT DISTINCT schedulename FROM schedules_to_assign WHERE NULLIF(scheduleuri, '') IS NULL """
        )

        trigger_dag_run_ge_poland_sub_child_schedule_add_21 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_poland_sub_child_schedule_add_21',
            retries=0,
            items="{{ result('query_collection_schedules_to_assign_for_new_schedules_19') }}",
            trigger_dag_id=config.sub_child_schedule_add_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda item: {
                "name": item['schedulename'],
                "monday": item['schedulename'].split("|")[0],
                "tuesday": item['schedulename'].split("|")[1],
                "wednesday": item['schedulename'].split("|")[2],
                "thursday": item['schedulename'].split("|")[3],
                "friday": item['schedulename'].split("|")[4],
                "saturday": item['schedulename'].split("|")[5],
                "sunday": item['schedulename'].split("|")[6]
            }
        )

        wait_for_completion_trigger_dag_run_ge_poland_sub_child_schedule_add_21 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_poland_sub_child_schedule_add_21',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_ge_poland_sub_child_schedule_add_21") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> query_inputfilerawdata_for_records

        query_inputfilerawdata_for_records >> get_all_replicon_office_schedules_7 >> ge_poland_user_sync_master_mapper_search_entries_9 >>\
            get_schedules_to_assign_for_all_records_10_17 >> create_collection_for_schedules_to_assign_list_18 >>\
            query_collection_schedules_to_assign_for_new_schedules_19 >> trigger_dag_run_ge_poland_sub_child_schedule_add_21 >>\
            wait_for_completion_trigger_dag_run_ge_poland_sub_child_schedule_add_21 >> finish

    return dag


rail.for_each_instance(create_dag)
