from datetime import timedelta
import rail
from bearingpoint.sap_h4s4_timeoff_booking_import.utils import request_payload, response_filter
from airflow.models import Variable

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_delete_timeoff_dag_id,
        description=f'Bearingpoint Timeoff Booking Sync Process Delete Timeoff Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_min_max_dates_from_valid_records'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_min_max_dates_from_valid_records',
            end_task='catch_and_log_errors',
        )

        get_min_max_dates_from_valid_records = rail.QueryCollectionOperator(
            task_id='get_min_max_dates_from_valid_records',
            query="""SELECT MIN(startdate) as start_date, MAX(enddate) as end_date from rawdata""",
        )

        get_all_timeoff_bookings_for_min_max_dates = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_bookings_for_min_max_dates',
            endpoint='/services/TimeOffListService1.svc/GetData',
            data=request_payload.get_timeoff_bookings_for_min_max_dates,
            data_handler=response_filter.get_timeoff_booking_details
        )

        has_any_timeoff_for_min_max_dates = rail.IfOperator(
            task_id='has_any_timeoff_for_min_max_dates',
            test='{{ result("get_all_timeoff_bookings_for_min_max_dates") | is_truthy }}',
            yes_task='create_time_off_collection',
            no_task='finish'
        )

        create_time_off_collection = rail.CreateCollectionOperator(
            task_id='create_time_off_collection',
            source='{{ result("get_all_timeoff_bookings_for_min_max_dates") | to_json }}',
            name='timeoffdata'
        )

        query_records_not_in_feed_file = rail.QueryCollectionOperator(
            task_id='query_records_not_in_feed_file',
            query='SELECT timeoff_uri FROM timeoffdata WHERE booking_id not in (SELECT booking_id FROM rawdata)'
        )

        has_any_timeoff_to_delete = rail.IfOperator(
            task_id='has_any_timeoff_to_delete',
            test='{{ result("query_records_not_in_feed_file", "length") > 0 }}',
            yes_task='create_delete_timeoff_batch',
            no_task='finish'
        )

        create_delete_timeoff_batch = rail.RepliconServiceOperator(
            task_id="create_delete_timeoff_batch",
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=lambda: {
                "timeOffUris": [item['timeoff_uri'] for item in rail.load_all_records(
                    rail.result("query_records_not_in_feed_file"))]
            }
        )

        execute_delete_timeoff_batch, wait_for_delete_timeoff_batch = rail.batch_execution(
            group_id='execute_delete_timeoff_batch',
            creation_task_id=create_delete_timeoff_batch.task_id
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "employee_id": '',
                "timeofftype": '',
                "startdate": '',
                "enddate": '',
                "hours": '',
                'action': '',
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            "No") >> get_min_max_dates_from_valid_records >> get_all_timeoff_bookings_for_min_max_dates >>\
            has_any_timeoff_for_min_max_dates

        has_any_timeoff_for_min_max_dates >> rail.Label(
            "Yes") >> create_time_off_collection

        has_any_timeoff_for_min_max_dates >> rail.Label(
            "No") >> finish

        create_time_off_collection >> query_records_not_in_feed_file >> has_any_timeoff_to_delete

        has_any_timeoff_to_delete >> rail.Label(
            "Yes") >> create_delete_timeoff_batch >> execute_delete_timeoff_batch >> wait_for_delete_timeoff_batch >> finish

        has_any_timeoff_to_delete >> rail.Label(
            "No") >> finish >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
