from datetime import timedelta
import uuid
import rail
from moodys.timeoff_import.utils import request_payload, response_filter
from airflow.models import Variable
null = None


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"moodys_time_data_process_each_record_child_{config.instance}",
        description=f"Moodys TimeSync Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                    config.can_run_batch_task_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='has_mandatory_fields'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='has_mandatory_fields',
            end_task='catch_and_log_errors',
        )

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.mandatory_fields_check,
            yes_task="search_user",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_madatory_fields_not_present',
            message='\
                {%- if dag_run.conf.Employee_ID | is_falsy -%} \
                    Employee ID is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Source_Time_Off_Booking_ID | is_falsy -%} \
                    Source timeoff Booking ID is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Entry_date | is_falsy -%} \
                    Entry date is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.End_Date | is_falsy -%} \
                    End date is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Time_Type__externalcode_ | is_falsy -%} \
                    Timeoff type is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Status | is_falsy -%} \
                    Status is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.No_of_Days_Booked | is_falsy -%} \
                    No of days booked is not present in payload, \
                {%- endif -%}',
            severity='Exception',
            properties={
                'countryid': "{{dag_run.conf.Country_ID}}",
                'employeeid': "{{dag_run.conf.Employee_ID}}",
                'sourcetimeoffid': "{{dag_run.conf.Source_Time_Off_Booking_ID}}",
                'entrydate': "{{dag_run.conf.Entry_date}}",
                'enddate': "{{dag_run.conf.End_Date}}",
                'timetypeexternalcode': "{{dag_run.conf.Time_Type__externalcode_}}",
                'duration': "{{dag_run.conf.Duration}}",
                'status': 'Exception',
            }
        )

        search_user = rail.RepliconServiceOperator(
            task_id="search_user",
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=request_payload.get_search_user_payload,
            data_handler=response_filter.get_user_data
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test=lambda: bool(rail.result('search_user')),
            yes_task="get_all_time_off_type",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            message="User not available",
            severity='Exception',
            properties={
                'countryid': "{{dag_run.conf.Country_ID}}",
                'employeeid': "{{dag_run.conf.Employee_ID}}",
                'sourcetimeoffid': "{{dag_run.conf.Source_Time_Off_Booking_ID}}",
                'entrydate': "{{dag_run.conf.Entry_date}}",
                'enddate': "{{dag_run.conf.End_Date}}",
                'timetypeexternalcode': "{{dag_run.conf.Time_Type__externalcode_}}",
                'duration': "{{dag_run.conf.Duration}}",
                'status': 'Exception',
            }
        )

        get_all_time_off_type = rail.RepliconServiceOperator(
            task_id='get_all_time_off_type',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            response_filter=response_filter.get_all_time_off_types
        )

        is_timeoff_available_in_replicon = rail.IfOperator(
            task_id="is_timeoff_available_in_replicon",
            test=lambda: bool(rail.result('get_all_time_off_type')),
            yes_task="get_all_assigned_time_off_type_for_user",
            no_task="log_timeoff_not_present"
        )

        log_timeoff_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_not_present',
            message="Time off not available in Replicon",
            severity='Exception',
            properties={
                'countryid': "{{dag_run.conf.Country_ID}}",
                'employeeid': "{{dag_run.conf.Employee_ID}}",
                'sourcetimeoffid': "{{dag_run.conf.Source_Time_Off_Booking_ID}}",
                'entrydate': "{{dag_run.conf.Entry_date}}",
                'enddate': "{{dag_run.conf.End_Date}}",
                'timetypeexternalcode': "{{dag_run.conf.Time_Type__externalcode_}}",
                'duration': "{{dag_run.conf.Duration}}",
                'status': 'Exception',
            }
        )

        get_all_assigned_time_off_type_for_user = rail.RepliconServiceOperator(
            task_id='get_all_assigned_time_off_type_for_user',
            endpoint='/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser',
            data=request_payload.get_all_assigned_time_off_type_for_user_payload,
            response_filter=response_filter.get_assigned_time_off_uris
        )

        put_time_off_type_for_user = rail.RepliconServiceOperator(
            task_id='put_time_off_type_for_user',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=request_payload.put_time_off_type_for_user_payload
        )

        get_hidden_oef_value = rail.RepliconServiceOperator(
            task_id='get_hidden_oef_value',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data=request_payload.get_hidden_oef_value_payload,
            response_filter=response_filter.get_hidden_oef_value
        )

        get_time_off_details_on_entryid = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_entryid",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_entryid,
            response_filter=response_filter.get_filtered_time_off_details_on_entryid
        )

        is_status_approved = rail.IfOperator(
            task_id='is_status_approved',
            test="{{dag_run.conf.Status != 'CANCELLED'}}",
            yes_task='is_timeoffentryid_available',
            no_task='is_timeoff_entryid_available'
        )

        is_timeoffentryid_available = rail.IfOperator(
            task_id='is_timeoffentryid_available',
            test=lambda: rail.result('get_time_off_details_on_entryid') != [],
            yes_task='delete_timeoff',
            no_task='create_time_off_draft'
        )

        is_timeoff_entryid_available = rail.IfOperator(
            task_id='is_timeoff_entryid_available',
            test=lambda: rail.result('get_time_off_details_on_entryid') != [],
            yes_task='delete_timeoff_booking',
            no_task='log_exception'
        )

        log_exception = rail.WriteLogOperator(
            task_id='log_exception',
            message="Time off booking cannot be deleted since the time off booking is not available in Replicon.",
            severity='Exception',
            properties={
                'countryid': "{{dag_run.conf.Country_ID}}",
                'employeeid': "{{dag_run.conf.Employee_ID}}",
                'sourcetimeoffid': "{{dag_run.conf.Source_Time_Off_Booking_ID}}",
                'entrydate': "{{dag_run.conf.Entry_date}}",
                'enddate': "{{dag_run.conf.End_Date}}",
                'timetypeexternalcode': "{{dag_run.conf.Time_Type__externalcode_}}",
                'duration': "{{dag_run.conf.Duration}}",
                'status': 'Exception',
            }
        )

        delete_timeoff_booking = rail.RepliconServiceOperator(
            task_id="delete_timeoff_booking",
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=lambda: {
                "timeOffUri": rail.result('get_time_off_details_on_entryid')[0]['timeoffuri']
            }
        )

        delete_timeoff = rail.RepliconServiceOperator(
            task_id="delete_timeoff",
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=lambda: {
                "timeOffUri": rail.result('get_time_off_details_on_entryid')[0]['timeoffuri']
            }
        )

        create_time_off_draft = rail.RepliconServiceOperator(
            task_id="create_time_off_draft",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data=request_payload.get_create_time_off_draft_payload,
        )

        put_timeoff_entry = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=request_payload.get_put_timeoff_entry_payload,
        )

        publish_time_off_draft = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data=request_payload.get_publish_time_off_draft_payload,
        )

        put_timeoff_entry_id_oef_value = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data=request_payload.get_put_timeoff_entry_id_oef_value_payload,
        )

        approve_timeoffbooking_for_user = rail.RepliconServiceOperator(
            task_id='approve_timeoffbooking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft').uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Replicon Integration"
            }
        )

        time_off_success = rail.WriteLogOperator(
            task_id='time_entry_success',
            message="Time off was successfully entried in replicon",
            severity='Success',
            properties={
                'countryid': "{{dag_run.conf.Country_ID}}",
                'employeeid': "{{dag_run.conf.Employee_ID}}",
                'sourcetimeoffid': "{{dag_run.conf.Source_Time_Off_Booking_ID}}",
                'entrydate': "{{dag_run.conf.Entry_date}}",
                'enddate': "{{dag_run.conf.End_Date}}",
                'timetypeexternalcode': "{{dag_run.conf.Time_Type__externalcode_}}",
                'duration': "{{dag_run.conf.Duration}}",
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message=lambda: request_payload.get_error_message(),
            properties={
                'countryid': "{{dag_run.conf.Country_ID}}",
                'employeeid': "{{dag_run.conf.Employee_ID}}",
                'sourcetimeoffid': "{{dag_run.conf.Source_Time_Off_Booking_ID}}",
                'entrydate': "{{dag_run.conf.Entry_date}}",
                'enddate': "{{dag_run.conf.End_Date}}",
                'timetypeexternalcode': "{{dag_run.conf.Time_Type__externalcode_}}",
                'duration': "{{dag_run.conf.Duration}}",
                'status': 'Exception',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> has_mandatory_fields

        has_mandatory_fields >> rail.Label(
            "No") >> log_madatory_fields_not_present >> catch_and_log_errors >> log_to_sumo

        has_mandatory_fields >> rail.Label("Yes") >> search_user >> is_user_present >> rail.Label("Yes") >> get_all_time_off_type\
            >> is_timeoff_available_in_replicon >> rail.Label("Yes") >> get_all_assigned_time_off_type_for_user >> put_time_off_type_for_user >> get_hidden_oef_value\
            >> get_time_off_details_on_entryid >> is_status_approved\
            >> rail.Label("Yes") >> is_timeoffentryid_available >> rail.Label("Yes") >> delete_timeoff >> create_time_off_draft\
            >> put_timeoff_entry >> publish_time_off_draft >> put_timeoff_entry_id_oef_value >> approve_timeoffbooking_for_user >> time_off_success

        is_user_present >> rail.Label(
            "No") >> log_user_not_present >> catch_and_log_errors >> log_to_sumo

        is_status_approved >> rail.Label(
            "No") >> is_timeoff_entryid_available >> rail.Label("Yes") >> delete_timeoff_booking >> time_off_success

        is_timeoff_available_in_replicon >> rail.Label(
            "No") >> log_timeoff_not_present >> catch_and_log_errors >> log_to_sumo

        is_timeoff_entryid_available >> rail.Label(
            "No") >> log_exception >> catch_and_log_errors >> log_to_sumo

        is_timeoffentryid_available >> rail.Label(
            "No") >> create_time_off_draft >> put_timeoff_entry >> publish_time_off_draft >> put_timeoff_entry_id_oef_value >> approve_timeoffbooking_for_user >> time_off_success >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
