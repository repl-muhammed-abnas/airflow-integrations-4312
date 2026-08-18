from datetime import timedelta
from airflow.models import Variable
import rail

from capefoxcorporation.timesheet_sync.utils.custom_methods import (
    is_revert_required
)
from capefoxcorporation.timesheet_sync.utils.response_filters import (
    get_user_company,
    get_cost_center_uri,
    get_project_uris,
    get_task_uris,
    get_oef_tag_uris,
    get_timeoff_type_uris
)
from capefoxcorporation.timesheet_sync.utils.request_payload import (
    get_reversing_record,
    get_existing_timesheet_filter_payload,
    get_timesheet_import_payload,
    get_log_severity,
    get_log_properties,
    get_error_log_properties
)

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Capefoxcorporation timesheets sync from Replicon to Deltek Costpoint Master ({config.instance})',
        schedule_interval=None,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_costpoint_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log",
        )

        get_replicon_timesheet = rail.RepliconServiceOperator(
            task_id='get_replicon_timesheet',
            endpoint="/services/timesheetservice1.svc/GetTimesheetDetails",
            data={
                'timesheetUri': '{{ dag_run.conf.webhook.data.timesheet.uri }}'
            }
        )

        get_replicon_time_entries = rail.RepliconServiceOperator(
            task_id='get_replicon_time_entries',
            endpoint='/services/timeEntryrevisiongroupservice1.svc/GetTimeEntryRevisionGroupsForUserAndDateRange',
            data=lambda: {
                "user": {
                    "uri": rail.result('get_replicon_timesheet')['owner']['uri']
                },
                "dateRange": {
                    "startDate": rail.result('get_replicon_timesheet')['dateRange']['startDate'],
                    "endDate": rail.result('get_replicon_timesheet')['dateRange']['endDate']
                }
            }
        )

        get_replicon_pay_codes = rail.RepliconServiceOperator(
            task_id='get_replicon_pay_codes',
            endpoint='/services/PayCodeService1.svc/GetAllPayCodes',
        )

        get_replicon_user_details = rail.RepliconServiceOperator(
            task_id='get_replicon_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [{"uri": rail.result('get_replicon_timesheet')['owner']['uri']}],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        has_cost_center = rail.IfOperator(
            task_id='has_cost_center',
            test=lambda: get_cost_center_uri(rail.result('get_replicon_user_details')) is not None,
            yes_task='get_replicon_account_details',
            no_task='get_replicon_task_details'
        )

        get_account_details = rail.RepliconServiceOperator(
            task_id='get_replicon_account_details',
            endpoint='services/costcenterservice1.svc/GetCostCenterDetails',
            data=lambda: {
                "costCenterUri": get_cost_center_uri(rail.result('get_replicon_user_details'))
            }
        )

        get_replicon_task_details = rail.RepliconServiceOperator(
            task_id='get_replicon_task_details',
            endpoint="/services/taskservice1.svc/BulkGetTaskDetails",
            data=lambda: {
                "taskUris": get_task_uris(rail.result('get_replicon_time_entries'))
            }
        )

        get_replicon_project_details = rail.RepliconServiceOperator(
            task_id='get_replicon_project_details',
            endpoint='/services/projectservice1.svc/BulkGetProjectDetails3',
            data=lambda: {
                "projects": get_project_uris(rail.result('get_replicon_task_details'))
            }
        )

        get_oef_tag_details = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_oef_tag_details',
            items=lambda: get_oef_tag_uris(
                rail.result('get_replicon_time_entries'), config),
            endpoint="/services/ObjectExtensionTagService1.svc/GetObjectExtensionTagDetails",
            data={
                "objectExtensionTagUri": "{{ item }}"
            }
        )

        is_sync_time_off_bookings = rail.IfOperator(
            task_id='is_sync_time_off_bookings',
            test=lambda: getattr(config, 'is_sync_time_off_bookings', 'false').lower() == 'true',
            yes_task='get_replicon_timeoffs',
            no_task='get_existing_deltek_timesheet'
        )

        get_replicon_timeoffs = rail.RepliconServiceOperator(
            task_id='get_replicon_timeoffs',
            endpoint="/services/timesheetservice1.svc/GetAllOverlappingTimeOffForTimesheet2",
            data={
                'timesheetUri': '{{ dag_run.conf.webhook.data.timesheet.uri }}'
            }
        )

        get_replicon_time_off_type_details = rail.RepliconServiceOperator(
            task_id='get_replicon_time_off_type_details',
            endpoint="/services/timeoffservice1.svc/BulkGetTimeOffTypeDetails",
            data=lambda: {
                "timeOffTypeUris": get_timeoff_type_uris(rail.result('get_replicon_timeoffs'))
            }
        )

        get_existing_deltek_timesheet = rail.DeltekCostPointServiceOperator(
            task_id='get_existing_deltek_timesheet',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=get_existing_timesheet_filter_payload
        )

        is_timesheet_available = rail.IfOperator(
            task_id='is_timesheet_available',
            test=lambda: is_revert_required(
                rail.result('get_existing_deltek_timesheet')[0],
                lambda ts: get_reversing_record(ts, config)),
            yes_task='revert_existing_time',
            no_task='push_time_to_costpoint'
        )

        revert_existing_time = rail.DeltekCostPointServiceOperator(
            task_id='revert_existing_time',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda: get_reversing_record(
                rail.result('get_existing_deltek_timesheet')[0], config)
        )

        push_time_to_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='push_time_to_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda: get_timesheet_import_payload(config)
        )

        add_log = rail.WriteLogOperator(
            task_id='add_log',
            log="{{ result('create_log') }}",
            message="Timesheet sync completed",
            severity=get_log_severity,
            properties=get_log_properties
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            message="Failed to sync timesheet to Costpoint",
            severity="Error",
            properties=get_error_log_properties
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error

        can_run_batch_task >> rail.Label(
            'No') >> create_log

        create_log >> get_replicon_timesheet >> get_replicon_time_entries >> get_replicon_pay_codes >> \
            get_replicon_user_details >> has_cost_center
        has_cost_center >> rail.Label(
            'yes') >> get_account_details >> get_replicon_task_details
        has_cost_center >> rail.Label(
            'no') >> get_replicon_task_details
        get_replicon_task_details >> get_replicon_project_details >> \
            get_oef_tag_details >> is_sync_time_off_bookings
        is_sync_time_off_bookings >> rail.Label(
            'yes') >> get_replicon_timeoffs >> get_replicon_time_off_type_details >> get_existing_deltek_timesheet >> is_timesheet_available
        is_sync_time_off_bookings >> rail.Label(
            'no') >> get_existing_deltek_timesheet >> is_timesheet_available
        is_timesheet_available >> rail.Label(
            'yes') >> revert_existing_time >> push_time_to_costpoint >> add_log >> catch_and_log_error
        is_timesheet_available >> rail.Label(
            'no') >> push_time_to_costpoint >> add_log >> catch_and_log_error
        return dag


rail.for_each_instance(create_dag)
