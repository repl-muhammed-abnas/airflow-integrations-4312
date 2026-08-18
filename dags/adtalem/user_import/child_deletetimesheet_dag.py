from datetime import datetime, timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.response_filter import get_timesheet_uris, page_handler


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_deletetimesheet_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_delete_timesheet_v1.0_{config.instance}',
        description=f'To delete timesheet_V1.0 {config.instance}',
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
            no_task='get_currenttimesheet_uri'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_currenttimesheet_uri',
            end_task='dagrun_log_to_sumo',
        )

        def get_enddate_plus_1(end_date):
            enddate_plus1_datetime = datetime.strptime(
                end_date, "%d/%m/%Y") + timedelta(days=1)
            return {
                'year': enddate_plus1_datetime.year,
                'month': enddate_plus1_datetime.month,
                'day': enddate_plus1_datetime.day
            }
        get_currenttimesheet_uri = rail.RepliconServiceOperator(
            task_id='get_currenttimesheet_uri',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "date": get_enddate_plus_1(dag_run.conf['enddate'])
            },
            data_handler=lambda response: response.get(
                'timesheet', {}).get('uri', '') if response else ''
        )

        is_current_timesheet_present = rail.IfOperator(
            task_id='is_current_timesheet_present',
            test="{{ result('get_currenttimesheet_uri') | is_truthy }}",
            yes_task="get_currenttimesheet_details",
            no_task="get_timesheeturis_to_delete"
        )

        get_currenttimesheet_details = rail.RepliconServiceOperator(
            task_id='get_currenttimesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_currenttimesheet_uri') }}"
            }
        )

        get_timesheeturis_to_delete = rail.RepliconServicePageOperator(
            task_id='get_timesheeturis_to_delete',
            endpoint="/services/TimesheetListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:timesheet-list-column:timesheet",
                    "urn:replicon:timesheet-list-column:due-date"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:timesheet-list-filter:due-date"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "dateRange": {
                                    "startDate": get_enddate_plus_1(dag_run.conf['enddate'])
                                }
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uri": dag_run.conf['useruri']
                            }
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=get_timesheet_uris
        )

        is_timesheet_uris_to_delete = rail.IfOperator(
            task_id='is_timesheet_uris_to_delete',
            test="{{ result('get_timesheeturis_to_delete') | length > 0 }}",
            yes_task="create_timesheet_delete_batch",
            no_task="is_timesheeturi_present",
        )

        create_timesheet_delete_batch = rail.RepliconServiceOperator(
            task_id='create_timesheet_delete_batch',
            endpoint="/services/TimesheetService1.svc/CreateTimesheetDeleteBatch",
            data=lambda: {
                "timesheetUris": rail.result('get_timesheeturis_to_delete'),
                "deleteOptionUri": "urn:replicon:timesheet-delete-option:delete-overlapping-time-and-payable-time-entries"
            }
        )

        execute_timesheet_delete_batch = rail.RepliconServiceOperator(
            task_id='execute_timesheet_delete_batch',
            endpoint="/services/TimesheetService1.svc/ExecuteTimesheetDeleteBatch",
            data={
                "timesheetDeleteBatchUri": "{{ result('create_timesheet_delete_batch') }}"
            }
        )

        is_timesheeturi_present = rail.IfOperator(
            task_id='is_timesheeturi_present',
            test="{{ result('get_currenttimesheet_uri') | is_truthy and \
                dag_run.conf.effectivedate | sn | is_truthy }}",
            yes_task="is_effectivedate_equals_timesheet_startdate",
            no_task="dagrun_log_to_sumo",
        )

        def check_effectivedate_timesheetstartdate(effectivedate, timesheetstartdate_obj):
            effectivedate_datetime = datetime.strptime(
                effectivedate, '%m/%d/%Y')
            timesheetstartdate_datetime = datetime.strptime(
                f"{timesheetstartdate_obj['day']}/{timesheetstartdate_obj['month']}/{timesheetstartdate_obj['year']}",
                '%d/%m/%Y') if timesheetstartdate_obj else ''
            return effectivedate_datetime.date() == timesheetstartdate_datetime.date()
        is_effectivedate_equals_timesheet_startdate = rail.IfOperator(
            task_id='is_effectivedate_equals_timesheet_startdate',
            test=lambda dag_run: check_effectivedate_timesheetstartdate(
                dag_run.conf['effectivedate'], rail.result(
                    'get_currenttimesheet_details')['dateRange']['startDate']),
            yes_task="delete_current_timesheet",
            no_task="dagrun_log_to_sumo",
        )

        delete_current_timesheet = rail.RepliconServiceOperator(
            task_id='delete_current_timesheet',
            endpoint="/services/TimesheetService1.svc/Delete",
            data={
                "timesheetUri": "{{ result('get_currenttimesheet_uri') }}",
                "deleteOptionUri": "urn:replicon:timesheet-delete-option:delete-overlapping-time-and-payable-time-entries"
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
            'No') >> get_currenttimesheet_uri

        get_currenttimesheet_uri >> is_current_timesheet_present
        is_current_timesheet_present >> rail.Label(
            'Yes') >> get_currenttimesheet_details >> get_timesheeturis_to_delete
        is_current_timesheet_present >> rail.Label(
            'No') >> get_timesheeturis_to_delete >> is_timesheet_uris_to_delete
        is_timesheet_uris_to_delete >> rail.Label(
            'Yes') >> create_timesheet_delete_batch >> execute_timesheet_delete_batch >> is_timesheeturi_present
        is_timesheet_uris_to_delete >> rail.Label(
            'No') >> is_timesheeturi_present
        is_timesheeturi_present >> rail.Label(
            'Yes') >> is_effectivedate_equals_timesheet_startdate
        is_effectivedate_equals_timesheet_startdate >> rail.Label(
            'Yes') >> delete_current_timesheet >> dagrun_log_to_sumo
        is_effectivedate_equals_timesheet_startdate >> rail.Label(
            'No') >> dagrun_log_to_sumo
        is_timesheeturi_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_deletetimesheet_child_dag)
