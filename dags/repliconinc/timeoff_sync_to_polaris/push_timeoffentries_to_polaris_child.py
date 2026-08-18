from datetime import datetime, timedelta
import rail
from repliconinc.timeoff_sync_to_polaris.utils import request_payload
from repliconinc.timeoff_sync_to_polaris.utils import custom_methods
from repliconinc.timeoff_sync_to_polaris.tasks.create_timeoff_booking import get_create_timeoff

null = None
# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.push_timeoffentries_to_polaris,
        description=f"push_timeoffentries_to_polaris_child",
        company_key=config.company_key_polaris,
        replicon_conn_id=config.replicon_conn_id_polaris,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")
        
        # start of the code
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
            no_task="get_time_off_details_for_user_and_date_range_1"
        )

        timeofftypeuri_not_present = rail.FailOperator(
            task_id='timeofftypeuri_not_present', 
            message="Time Off Type not present/disabled in Replicon"
        )
        get_time_off_details_for_user_and_date_range_1 = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range_1',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=request_payload.get_time_off_details_for_user_and_date_range_payload
        )

        def _get_timeoff_result_item():
            res = rail.result("get_time_off_details_for_user_and_date_range_1")
            if isinstance(res, list):
                return res[0] if len(res) > 0 else {}
            return res or {}
        
        if_timeofftype_name_is_present = rail.IfOperator(
            task_id='if_timeofftype_name_present',
            test=lambda dag_run: ( _get_timeoff_result_item().get("timeOffType", {}).get("name") is not None and
                _get_timeoff_result_item().get("timeOffType", {}).get("name")
                != (dag_run.conf.get("timeofftype"))
            ),
            yes_task="delete_time_off_1",
            no_task="if_decimal_workdays_not_equal_to_timeoffdays"
        )
        
        delete_time_off_1 = rail.RepliconServiceOperator(
            task_id='delete_time_off_1',
            replicon_conn_id=config.replicon_conn_id_polaris,
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=request_payload.delete_timeoff_payload_1
        )

        # Display worddays_check
        if_decimal_workdays_not_equal_to_timeoffdays = rail.IfOperator(
            task_id='if_decimal_workdays_not_equal_to_timeoffdays',
            test=lambda dag_run: (
                float(_get_timeoff_result_item().get("totalDuration", {}).get("decimalWorkdays") or 0)
                != float(dag_run.conf.get("timeoffdays", 0.0))),
            yes_task="delete_time_off_1",
            no_task="finish"
        )
        
        if_timeoffdays_equal_to_one=rail.IfOperator(
            task_id='if_timeoffdays_equal_to_one',
            test=lambda dag_run: float(dag_run.conf["timeoffdays"]) == 1.0,
            yes_task="create_timeoff_booking_for_user_f",
            no_task="convert_decimal_to_seconds_task"
        )

        create_timeoff_booking_for_user_f = rail.EmptyOperator(task_id='create_timeoff_booking_for_user_f')
        create_timeoff_booking_type_f, approve_timeoff_booking_type_f = get_create_timeoff("F")
        
        convert_decimal_to_seconds_task = rail.PythonOperator(
        task_id="convert_decimal_to_seconds_task",
        python_callable=custom_methods.convert_decimal_to_seconds
        )
        
        create_timeoff_booking_for_user_h = rail.EmptyOperator(task_id='create_timeoff_booking_for_user_s')
        create_timeoff_booking_type_h, approve_timeoff_booking_type_h = get_create_timeoff("H")

        finish = rail.EmptyOperator(
            task_id='finish'
        )
        
        if_request_useruri_not_present >> rail.Label("Yes") >> user_not_present
        if_request_useruri_not_present >> rail.Label("No")  >> if_request_timeofftypeuri_not_present 

        if_request_timeofftypeuri_not_present >> rail.Label("Yes") >> timeofftypeuri_not_present
        if_request_timeofftypeuri_not_present >> rail.Label("No")  >> get_time_off_details_for_user_and_date_range_1

        get_time_off_details_for_user_and_date_range_1 >> if_timeofftype_name_is_present
        if_timeofftype_name_is_present >> rail.Label("Yes") >> delete_time_off_1 >> if_timeoffdays_equal_to_one
        if_timeofftype_name_is_present >> rail.Label("No")  >> if_decimal_workdays_not_equal_to_timeoffdays

        if_decimal_workdays_not_equal_to_timeoffdays >> rail.Label("Yes") >> delete_time_off_1 >> if_timeoffdays_equal_to_one
        if_decimal_workdays_not_equal_to_timeoffdays >> rail.Label("No")  >> finish

        if_timeoffdays_equal_to_one >> rail.Label("Yes") >> create_timeoff_booking_for_user_f >> create_timeoff_booking_type_f >> approve_timeoff_booking_type_f >> finish
        if_timeoffdays_equal_to_one >> rail.Label("No")  >> convert_decimal_to_seconds_task >> create_timeoff_booking_for_user_h >> create_timeoff_booking_type_h >> approve_timeoff_booking_type_h >> finish

        return dag

rail.for_each_instance(create_child_dag)
    