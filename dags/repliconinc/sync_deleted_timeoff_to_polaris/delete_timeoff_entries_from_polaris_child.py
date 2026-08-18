from datetime import datetime, timedelta
import rail
from repliconinc.sync_deleted_timeoff_to_polaris.utils import request_payload
from repliconinc.sync_deleted_timeoff_to_polaris.utils import custom_methods


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.delete_timeoff_entries_from_polaris_child,
        description=f"delete_timeoff_entries_from_polaris_child",
        company_key=config.company_key_polaris,
        replicon_conn_id=config.replicon_conn_id_polaris,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        if_request_useruri_not_present = rail.IfOperator(
            task_id='if_request_useruri_not_present',
            test='{{ dag_run.conf.useruri | is_falsy }}',
            yes_task="user_not_present",
            no_task="if_request_timeofftypeuri_not_present"
        )
    
        user_not_present = rail.FailOperator(
            task_id='user_not_present', 
            message="User not present in Replicon"
        )
        # Check if timeoff_uri is present
        if_request_timeofftypeuri_not_present = rail.IfOperator(
            task_id='if_request_timeofftypeuri_not_present',
            test='{{ dag_run.conf.timeofftypeuri | is_falsy }}',
            yes_task="timeofftypeuri_not_present",
            no_task="convert_decimal_to_seconds_task"
        )

        timeofftypeuri_not_present = rail.FailOperator(
            task_id='timeofftypeuri_not_present', 
            message="Time Off Type not present/disabled in Replicon"
        )
        
        convert_decimal_to_seconds_task = rail.PythonOperator(
        task_id="convert_decimal_to_seconds_task",
        python_callable=custom_methods.convert_decimal_to_seconds
        )
        
        get_time_off_details_for_user_and_date_range_1 = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range_1',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=request_payload.get_time_off_details_for_user_and_date_range_payload
        )
        
        if_uri_is_present = rail.IfOperator(
            task_id='if_uri_present',
            test=lambda: bool(rail.result("get_time_off_details_for_user_and_date_range_1") and
                              rail.result("get_time_off_details_for_user_and_date_range_1")[0].get("uri")),
            yes_task="if_timeofftype_name_equal",
            no_task="finish"
        )


        if_timeofftype_name_equal = rail.IfOperator(
            task_id='if_timeofftype_name_equal',
            test=lambda dag_run: (bool(rail.result("get_time_off_details_for_user_and_date_range_1")) and
                rail.result("get_time_off_details_for_user_and_date_range_1")[0].get("timeOffType", {}).get("name") is not None and
                rail.result("get_time_off_details_for_user_and_date_range_1")[0].get("timeOffType", {}).get("name")
                == (dag_run.conf.get("originalvalue"))
            ),
            yes_task="delete_time_off_1",
            no_task="finish"
        )
        
        delete_time_off = rail.RepliconServiceOperator(
            task_id='delete_time_off_1',
            replicon_conn_id=config.replicon_conn_id_polaris,
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=request_payload.delete_timeoff_payload_1
        )


        finish = rail.EmptyOperator(task_id='finish')
        
        if_request_useruri_not_present >> rail.Label("Yes") >> user_not_present
        if_request_useruri_not_present >> rail.Label("No")  >> if_request_timeofftypeuri_not_present 

        if_request_timeofftypeuri_not_present >> rail.Label("Yes") >> timeofftypeuri_not_present
        if_request_timeofftypeuri_not_present >> rail.Label("No")  >> convert_decimal_to_seconds_task

        convert_decimal_to_seconds_task >> get_time_off_details_for_user_and_date_range_1

        get_time_off_details_for_user_and_date_range_1 >> if_uri_is_present
        if_uri_is_present >> rail.Label("Yes") >> if_timeofftype_name_equal 
        if_uri_is_present >> rail.Label("No")  >> finish
        
        if_timeofftype_name_equal >> rail.Label("Yes") >> delete_time_off
        if_timeofftype_name_equal >> rail.Label("No")  >> finish


        return dag

rail.for_each_instance(create_child_dag)
    