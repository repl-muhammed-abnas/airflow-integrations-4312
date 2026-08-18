from datetime import timedelta, datetime as dt
from uuid import uuid4
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.disable_user_v2.utils.custom_methods import prepare_timeoff_uris_func, INPUT_DATE_FORMAT
from dxctechnology.workday_user_import_v1.disable_user_v2.utils import request_payload, data_handler
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.delete_future_entries_child_dag_id,
        description=f"dxctechnology workday user sync delete future time entries and timeoffs child {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.process_time_off_accrual_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_disable_user, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="calculate_deletion_date"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="calculate_deletion_date",
            end_task="catch_errors",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # STEP 1: Calculate the end date for deletion (user end date)
        calculate_deletion_date = rail.PythonOperator(
            task_id='calculate_deletion_date',
            python_callable=lambda dag_run: {
                "day": (dt.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT) + timedelta(days=1)).strftime("%d"),
                "month": (dt.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT) + timedelta(days=1)).strftime("%m"),
                "year": (dt.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT) + timedelta(days=1)).strftime("%Y")
            }
        )

        get_closure_date_oef = rail.RepliconServiceOperator(
            task_id = 'get_closure_date_oef',
            endpoint= '/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data= {
                    "bindingContextUri": "urn:replicon:object-type:user"
                },
            data_handler= lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', 'ClosureDate', 'uri', None)
        )

        update_closure_date_oef = rail.RepliconServiceOperator(
            task_id = 'update_closure_date_oef',
            endpoint= '/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "value": {
                    "definition": {
                    "uri": rail.result("get_closure_date_oef")
                    },
                    "textValue": (dt.now() + timedelta(days=7)).strftime('%m-%d-%Y')
                }
            }
        )

        # STEP 2: Get future time entries
        get_future_time_entries = rail.RepliconServiceOperator(
            task_id='get_future_time_entries',
            endpoint="/services/TimeEntryRevisionGroupListService1.svc/GetData",
            data= request_payload.get_time_entry_details_payload,
            data_handler= data_handler.get_time_entry_response_data
        )

        # STEP 3: Check if there are future time entries to process
        check_future_time_entries_exist = rail.IfOperator(
            task_id='check_future_time_entries_exist',
            test=lambda: len(rail.result('get_future_time_entries')) > 0,
            yes_task="get_unique_timesheets",
            no_task="check_should_delete_time_punches"
        )

        # STEP 4: Extract unique timesheet URIs from time entries
        get_unique_timesheets = rail.PythonOperator(
            task_id='get_unique_timesheets',
            python_callable=lambda: list({
                item['timesheetUri']: item['timesheetUri']
                for item in rail.result('get_future_time_entries')
                if item['timesheetUri'] is not None
            }.values())
        )

        # STEP 5: Check if we have timesheets to process
        check_timesheets_exist = rail.IfOperator(
            task_id='check_timesheets_exist',
            test=lambda: len(rail.result('get_unique_timesheets')) > 0,
            yes_task="get_timesheet_statuses",
            no_task="filter_approved_time_entries_to_reopen"
        )

        # STEP 6: Get status of all unique timesheets in bulk
        get_timesheet_statuses = rail.RepliconServiceOperator(
            task_id="get_timesheet_statuses",
            endpoint="/services/TimesheetService1.svc/BulkGetTimesheetDetails",
            data=lambda: {
                "timesheetUris": rail.result('get_unique_timesheets')
            },
            data_handler=lambda responses: [
                {
                    "timesheetUri": response['uri'],
                    "status": response['statusUri'],
                    "isOpen": response['statusUri'].split(':')[-1] in ['open', 'rejected'],
                    "unit_of_work_id": str(uuid4())
                }
                for response in responses
            ]
        )

        # STEP 7: Filter timesheets that need reopening (approved/submitted)
        filter_timesheets_to_reopen = rail.PythonOperator(
            task_id='filter_timesheets_to_reopen',
            python_callable=lambda: [
                {"timesheetUri": ts['timesheetUri'], "unit_of_work_id": ts['unit_of_work_id']}
                for ts in rail.result('get_timesheet_statuses')
                if not ts['isOpen']  # Not open means approved/submitted
            ]
        )

        # STEP 8: Check if any timesheets need reopening
        check_timesheets_need_reopening = rail.IfOperator(
            task_id='check_timesheets_need_reopening',
            test=lambda: len(rail.result('filter_timesheets_to_reopen')) > 0,
            yes_task="create_timesheet_reopen_batch",
            no_task="filter_approved_time_entries_to_reopen"
        )

        # STEP 9: Create timesheet reopen batch
        create_timesheet_reopen_batch = rail.RepliconServiceOperator(
            task_id="create_timesheet_reopen_batch",
            endpoint="/services/TimesheetApprovalService1.svc/CreateReopenBatch",
            data=lambda: {
                "timesheetUris": [
                    ts['timesheetUri']
                    for ts in rail.result('filter_timesheets_to_reopen')
                ],
                "comments": "Timesheet reopened by Integration for future entry deletion"
            }
        )

        # STEP 10: Execute and wait for timesheet reopen batch
        execute_timesheet_reopen_batch, wait_timesheet_reopen_batch = rail.batch_execution(
            group_id="timesheet_reopen_batch_execution",
            creation_task_id="create_timesheet_reopen_batch",
            replicon_conn_id=config.replicon_conn_id
        )

        # STEP 12: Filter approved/submitted time entries that need reopening
        filter_approved_time_entries_to_reopen = rail.PythonOperator(
            task_id='filter_approved_time_entries_to_reopen',
            python_callable=lambda: [
                entry['timeEntryUri']
                for entry in rail.result('get_future_time_entries')
                if entry['status']  in ['Approved', 'Waiting For Approval']
            ]
        )

        # STEP 13: Check if any approved time entries need reopening
        check_approved_time_entries_need_reopening = rail.IfOperator(
            task_id='check_approved_time_entries_need_reopening',
            test=lambda: len(rail.result('filter_approved_time_entries_to_reopen')) > 0,
            yes_task="create_time_entry_reopen_batch",
            no_task="prepare_time_entry_uris"
        )

        # STEP 14: Create time entry reopen batch (only for approved/submitted entries)
        create_time_entry_reopen_batch = rail.RepliconServiceOperator(
            task_id="create_time_entry_reopen_batch",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/CreateReopenBatch",
            data=lambda: {
                "timeEntryRevisionGroupUris": rail.result('filter_approved_time_entries_to_reopen'),
                "comments": "Approved time entries reopened by Integration for future entry cleanup"
            }
        )

        # STEP 15: Execute and wait for time entry reopen batch
        execute_time_entry_reopen_batch, wait_time_entry_reopen_batch = rail.batch_execution(
            group_id="time_entry_reopen_batch_execution",
            creation_task_id="create_time_entry_reopen_batch",
            replicon_conn_id=config.replicon_conn_id
        )

        # STEP 16: Extract time entry URIs for bulk deletion
        prepare_time_entry_uris = rail.PythonOperator(
            task_id='prepare_time_entry_uris',
            python_callable=lambda: [item['timeEntryUri'] for item in rail.result('get_future_time_entries')]
        )

        # STEP 17: Bulk delete time entries
        bulk_delete_time_entries = rail.RepliconServiceOperator(
            task_id="bulk_delete_time_entries",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/BulkDeleteTimeEntryRevisionGroups",
            data=lambda: {
                "timeEntryRevisionGroupUris": rail.result('prepare_time_entry_uris')
            }
        )
        
        # STEP 18: Check if time punch deletion should run (based on instance configuration)
        check_should_delete_time_punches = rail.IfOperator(
            task_id='check_should_delete_time_punches',
            test=lambda: config.instance not in ['sandbox'],
            yes_task="get_future_time_punches",
            no_task="get_future_timeoffs"
        )

        # STEP 19: Get all future time punches
        get_future_time_punches = rail.RepliconServiceOperator(
            task_id='get_future_time_punches',
            endpoint="/services/TimePunchListService1.svc/GetData",
            data=request_payload.get_time_punch_details_payload,
            data_handler=data_handler.get_time_punch_response_data
        )

        # STEP 20: Check if there are future time punches to process
        check_future_time_punches_exist = rail.IfOperator(
            task_id='check_future_time_punches_exist',
            test=lambda: len(rail.result('get_future_time_punches')) > 0,
            yes_task="bulk_delete_time_punches",
            no_task="get_future_timeoffs"
        )

        # STEP 21: Bulk delete time punches
        bulk_delete_time_punches = rail.RepliconServiceOperator(
            task_id="bulk_delete_time_punches",
            endpoint="/services/TimePunchService1.svc/BulkDelete",
            data=lambda: {
                "timePunchUris": [item['timePunchUri'] for item in rail.result('get_future_time_punches')]
            }
        )

        # STEP 22: Get all future time offs
        get_future_timeoffs = rail.RepliconServiceOperator(
            task_id='get_future_timeoffs',
            endpoint="/services/TimeOffListService1.svc/GetData",
            data= request_payload.get_timeoff_details_payload,
            data_handler=lambda response: [{
                "timeoffuris": item['cells'][0]['uri'],
                "startdate": item['cells'][1]['textValue'],
                "enddate": item['cells'][2]['textValue'],
                "timeofftype": item['cells'][3]['uri']
            } for item in response['rows']] if response['rows'] else []
        )

        # STEP 23: Check if there are any future time offs to delete
        check_future_timeoffs_exist = rail.IfOperator(
            task_id='check_future_timeoffs_exist',
            test=lambda: len(rail.result('get_future_timeoffs')) > 0,
            yes_task="prepare_timeoff_uris",
            no_task="get_current_timesheet"
        )

        # STEP 24: Prepare time off URIs for update and deletion
        prepare_timeoff_uris = rail.PythonOperator(
            task_id='prepare_timeoff_uris',
            python_callable=lambda: prepare_timeoff_uris_func(rail.result('get_future_timeoffs'), rail.result('calculate_deletion_date'))
        )

        # STEP 25: Check if there are timeoffs to update
        check_timeoffs_to_update = rail.IfOperator(
            task_id='check_timeoffs_to_update',
            test=lambda: len(rail.result('prepare_timeoff_uris')['update_timeoffs']) > 0,
            yes_task="create_timeoff_reopen_batch",
            no_task="check_timeoffs_to_delete"
        )

        # STEP 25.1: Create batch for reopening timeoffs that need updating
        create_timeoff_reopen_batch = rail.RepliconServiceOperator(
            task_id="create_timeoff_reopen_batch",
            endpoint="/services/TimeOffApprovalService1.svc/CreateReopenBatch",
            data=lambda: {
                "timeOffUris": [
                    timeoff['uri'] for timeoff in rail.result('prepare_timeoff_uris')['update_timeoffs']
                ],
                "comments": "Reopened by Disable User Integration for date range update"
            }
        )

        # STEP 25.2: Execute and wait for timeoff reopen batch
        execute_timeoff_reopen_batch, wait_timeoff_reopen_batch = rail.batch_execution(
            group_id="timeoff_reopen_batch_execution",
            creation_task_id="create_timeoff_reopen_batch",
            replicon_conn_id=config.replicon_conn_id
        )

        # STEP 25.3: Update each timeoff with new date range using PutAndSubmitTimeOff
        update_timeoffs_date_range = rail.RepliconServiceCallForEachItemOperator(
            task_id="update_timeoffs_date_range",
            items=lambda: rail.result('prepare_timeoff_uris')['update_timeoffs'],
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=lambda item, dag_run: {
                "timeOff": {
                     "target": {
                        "uri": item['uri'],
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "owner": {
                        "uri": dag_run.conf['user_uri'],
                    },
                    "timeOffType": {
                        "uri": item['timeofftype_uri'],
                    },
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": rail.parse_date(item['start_date'], "%d %B %Y"),
                            "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                        },
                        "timeOffEnd": {
                            "date": rail.parse_date(item['end_date'], "%d-%m-%Y"),
                            "relativeDuration": "urn:replicon:time-off-relative-duration:full-day"
                        }
                    },
                    "comments": f"Date range updated by Integration - truncated to {item['end_date']}"
                },
                "comments": "Updated by Disable User Integration",
                "unitOfWorkId": str(uuid4())
            }
        )

        # STEP 26: Check if there are timeoffs to delete
        check_timeoffs_to_delete = rail.IfOperator(
            task_id='check_timeoffs_to_delete',
            test=lambda: len(rail.result('prepare_timeoff_uris')['delete_timeoffs']) > 0,
            yes_task="create_timeoff_delete_batch",
            no_task="get_current_timesheet"
        )

        # STEP 27: Bulk delete time offs
        create_timeoff_delete_batch = rail.RepliconServiceOperator(
            task_id='create_timeoff_delete_batch',
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=lambda: {
                "timeOffUris": [timeoff['uri'] for timeoff in rail.result('prepare_timeoff_uris')['delete_timeoffs']]
            }
        )

        execute_timeoff_delete_batch, wait_timeoff_delete_batch = rail.batch_execution(
            group_id="timeoff_delete_batch_execution",
            creation_task_id="create_timeoff_delete_batch",
            replicon_conn_id=config.replicon_conn_id
        )

        # STEP 28: Get current timesheet for user's end date
        get_current_timesheet = rail.RepliconServiceOperator(
            task_id='get_current_timesheet',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "date": {
                    "day": (dt.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT)).strftime("%d"),
                    "month": (dt.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT)).strftime("%m"),
                    "year": (dt.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT)).strftime("%Y")
                },
                "timesheetGetOptionUri": None
            }
        )

        # STEP 29: Check if current timesheet exists
        check_current_timesheet_exists = rail.IfOperator(
            task_id='check_current_timesheet_exists',
            test=lambda: rail.result('get_current_timesheet') is not None and rail.result('get_current_timesheet').get('timesheet') is not None,
            yes_task="get_current_timesheet_status",
            no_task="catch_errors"
        )

        # STEP 30: Get current timesheet status
        get_current_timesheet_status = rail.RepliconServiceOperator(
            task_id='get_current_timesheet_status',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data=lambda:{
                "timesheetUri": rail.result('get_current_timesheet')['timesheet']['uri']
            },
            data_handler=lambda response: {
                "timesheetUri": response['uri'],
                "status": response['statusUri'],
                "isSubmittedOrApproved": response['statusUri'].split(':')[-1] in ['submitted', 'approved', 'waiting-for-approval']
            }
        )

        # STEP 31: Check if current timesheet needs submission
        check_current_timesheet_needs_submission = rail.IfOperator(
            task_id='check_current_timesheet_needs_submission',
            test=lambda: not rail.result('get_current_timesheet_status')['isSubmittedOrApproved'],
            yes_task="submit_current_timesheet",
            no_task="catch_errors"
        )

        # STEP 32: Submit current timesheet
        submit_current_timesheet = rail.RepliconServiceOperator(
            task_id='submit_current_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data=lambda:{
                "timesheetUri": rail.result('get_current_timesheet_status')['timesheetUri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet submitted by Integration after user termination",
            }
        )

        catch_errors = rail.PythonOperator(
            task_id="catch_errors",
            trigger_rule="one_failed",
            python_callable=lambda: rail.render_template(
                "Error in deleting future time entries and timeoffs for user: {{get_error_message()}}")
        )
        
        # Batch task setup
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_errors
        can_run_batch_task >> rail.Label("No") >> calculate_deletion_date

        # Main workflow - Sequential flow
        calculate_deletion_date >> get_closure_date_oef >> update_closure_date_oef >> get_future_time_entries >> check_future_time_entries_exist

        # Path 1: Process time entries
        check_future_time_entries_exist >> rail.Label("Yes") >> get_unique_timesheets >> check_timesheets_exist
        check_future_time_entries_exist >> rail.Label("No") >> check_should_delete_time_punches

        # Path 1a: Process timesheets (if they exist)
        check_timesheets_exist >> rail.Label("Yes") >> get_timesheet_statuses >> filter_timesheets_to_reopen >> check_timesheets_need_reopening
        check_timesheets_exist >> rail.Label("No") >> filter_approved_time_entries_to_reopen

        # Path 1a1: Reopen timesheets (if needed)
        check_timesheets_need_reopening >> rail.Label("Yes") >> create_timesheet_reopen_batch >> execute_timesheet_reopen_batch
        wait_timesheet_reopen_batch >> filter_approved_time_entries_to_reopen
        check_timesheets_need_reopening >> rail.Label("No") >> filter_approved_time_entries_to_reopen >> check_approved_time_entries_need_reopening

        # Path 2a: Reopen approved time entries (if needed)
        check_approved_time_entries_need_reopening >> rail.Label("Yes") >> create_time_entry_reopen_batch >> execute_time_entry_reopen_batch
        wait_time_entry_reopen_batch >> prepare_time_entry_uris
        check_approved_time_entries_need_reopening >> rail.Label("No") >> prepare_time_entry_uris >> bulk_delete_time_entries >> check_should_delete_time_punches

        # Path 3: Process time punches (conditional based on instance type)
        check_should_delete_time_punches >> rail.Label("Yes") >> get_future_time_punches >> check_future_time_punches_exist
        check_should_delete_time_punches >> rail.Label("No") >> get_future_timeoffs
        check_future_time_punches_exist >> rail.Label("Yes") >> bulk_delete_time_punches >> get_future_timeoffs
        check_future_time_punches_exist >> rail.Label("No") >> get_future_timeoffs >> check_future_timeoffs_exist

        # Path 4a: Process time offs (if they exist)
        check_future_timeoffs_exist >> rail.Label("Yes") >> prepare_timeoff_uris >> check_timeoffs_to_update
        check_future_timeoffs_exist >> rail.Label("No") >> get_current_timesheet
        
        # Path 4b: Update timeoffs (if any need updating)
        check_timeoffs_to_update >> rail.Label("Yes") >> create_timeoff_reopen_batch >> execute_timeoff_reopen_batch
        wait_timeoff_reopen_batch >> update_timeoffs_date_range >> check_timeoffs_to_delete
        check_timeoffs_to_update >> rail.Label("No") >> check_timeoffs_to_delete
        
        # Path 4c: Delete timeoffs (if any need deleting)
        check_timeoffs_to_delete >> rail.Label("Yes") >> create_timeoff_delete_batch >> execute_timeoff_delete_batch
        wait_timeoff_delete_batch >> get_current_timesheet
        check_timeoffs_to_delete >> rail.Label("No") >> get_current_timesheet

        # Path 5: Submit current timesheet (on user end date)
        get_current_timesheet >> check_current_timesheet_exists
        check_current_timesheet_exists >> rail.Label("Yes") >> get_current_timesheet_status >> check_current_timesheet_needs_submission
        check_current_timesheet_exists >> rail.Label("No") >> catch_errors
        check_current_timesheet_needs_submission >> rail.Label("Yes") >> submit_current_timesheet >> catch_errors
        check_current_timesheet_needs_submission >> rail.Label("No") >> catch_errors

    return dag


rail.for_each_instance(create_child_dag)