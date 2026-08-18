from datetime import timedelta
import rail
from transparentbpo.time_entry_import.utils import custom_methods, request_payload, response_filters

def create_child_dag(config):
    append_dags = []
    for idx in range(0, config.PROCESS_USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f"{config.process_each_entry_date_child}_batch_{idx+1}",
            description=f'TransparentBPO Time Import Child - Process Each Entry {config.instance}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            query_project_task_records = rail.QueryCollectionOperator(
                task_id="query_project_task_records",
                query="""SELECT * from final_valid_records
                where work_date = '{{dag_run.conf.work_date}}'
                AND employee_id = '{{dag_run.conf.employee_id}}'"""
            )

            get_all_user_punches_for_date = rail.QueryCollectionOperator(
                task_id="get_all_user_punches_for_date",
                query="""SELECT * from all_user_records
                where work_date = '{{dag_run.conf.work_date}}'"""
            )
    
            get_aggregate_seconds_for_activity = rail.PythonOperator(
                task_id="get_aggregate_seconds_for_activity", 
                python_callable=custom_methods.get_aggregate_seconds_for_activity,
            )

            add_time_entry = rail.RepliconServiceCallForEachItemOperator(
                task_id="add_time_entry",
                items = request_payload.put_time_entry_payload,
                endpoint="/services/TimeEntryRevisionGroupService1.svc/BulkPutTimeEntryRevisionGroups",
                data=lambda item: {**item}
            )

            put_punch_entries = rail.RepliconServiceCallForEachItemOperator(
                task_id="put_punch_entries",
                items = request_payload.get_bulk_put_time_punch_payload,
                endpoint="/services/TimePunchService1.svc/BulkPutTimePunch4",
                data=lambda item: {**item}
            )

            log_success = rail.WriteLogOperator(
                task_id="log_success",
                log='{{ dag_run.conf.user_log }}',
                items='{{result("query_project_task_records")}}',
                severity="Success",
                message="Time entry Added successfully",
                properties={
                    'employee_id': '{{ item.employee_id }}',
                    'work_date': '{{ item.work_date }}',
                    'project': '{{ item.project }}',
                    'task': '{{ item.task }}',
                    'activity': '{{ item.activity }}',
                    'status': 'Success',
                    'action': 'Add',
                    'details': 'Time entry added successfully'
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log = '{{dag_run.conf.user_log}}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    'employee_id': '{{ dag_run.conf.employee_id }}',
                    'work_date': '{{ dag_run.conf.work_date }}',
                    'project': '',
                    'task': '',
                    'activity': '',
                    'status': 'Error',
                    'action': 'Add',
                    'details': '{{ get_error_message() }}'
                },
            )

            query_project_task_records >>\
            get_all_user_punches_for_date >>\
            get_aggregate_seconds_for_activity>>\
            add_time_entry >> put_punch_entries >>\
            log_success >> catch_and_log_errors

        append_dags.append(dag)
    return append_dags


rail.for_each_instance(create_child_dag)
