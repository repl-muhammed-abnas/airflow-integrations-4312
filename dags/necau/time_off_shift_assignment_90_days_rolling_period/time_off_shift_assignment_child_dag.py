import itertools
import rail
from rail.lib.ecid import get_dagrun_ecid
from necau.time_off_shift_assignment_90_days_rolling_period.utils import python_callable_method
from necau.time_off_shift_assignment_90_days_rolling_period.utils import custom_method
from necau.time_off_shift_assignment_90_days_rolling_period.utils import request_payload
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'necau_time_off_shift_assignment_child_{config.instance}',
        description=f'NECAU - Timeoff Shift_Assignment_Child_v2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            return list(map(lambda row: {
                'name': row['cells'][2]['textValue'],
                'code': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'uri': row['cells'][0]['uri']
            }, flaten_rows))

        get_all_shift_details = rail.RepliconServicePageOperator(
            task_id="get_all_shift_details",
            endpoint="/services/ShiftListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 100000,
                "columnUris": [
                    "urn:replicon:shift-list-column:shift",
                    "urn:replicon:shift-list-column:code",
                    "urn:replicon:shift-list-column:name"
                ],
                "sort": [],
                "filterExpression": null
            },
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler
        )

        get_shift_schedule_summary = rail.RepliconServiceOperator(
            task_id='get_shift_schedule_summary',
            endpoint="/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary",
            data=request_payload.get_shift_summary_payload
        )

        get_shift_actions = rail.PythonOperator(
            task_id='get_shift_actions',
            python_callable=python_callable_method.get_shift_actions
        )

        has_shift_to_delete = rail.IfOperator(
            task_id='has_shift_to_delete',
            test=custom_method.has_shift_to_delete,
            yes_task='delete_shifts',
            no_task='has_shift_to_assign'
        )

        delete_shifts = rail.RepliconServiceOperator(
            task_id="delete_shifts",
            endpoint="/services/ShiftAssignmentService1.svc/BulkDelete",
            data=request_payload.get_assignment_uris_to_bulk_delete
        )

        has_shift_to_assign = rail.IfOperator(
            task_id='has_shift_to_assign',
            test=custom_method.has_shift_to_assign,
            yes_task='put_shift_assignment',
            no_task='add_log_entry'
        )

        put_shift_assignment = rail.RepliconServiceOperator(
            task_id='put_shift_assignment',
            endpoint="/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments",
            data=request_payload.get_bulk_assignment_request
        )

        add_log_entry = rail.WriteLogOperator(
            task_id="add_log_entry",
            items=lambda: rail.result('get_shift_actions')["records_status"],
            severity="Success",
            message="Successfully Completed",
            properties=custom_method.get_update_record_properties
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message()}}',
            properties=lambda dag_run: {
                'booking_date': f'{dag_run.conf["booking_start_date"]}-{dag_run.conf["booking_end_date"]}',
                'user_name': dag_run.conf['user_name'],
                'pattern': "NA",
                'status': 'Error',
                'reason': "Error",
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run)
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=lambda dag_run: {
                'booking_date ': f'{dag_run.conf["booking_start_date"]}-{dag_run.conf["booking_end_date"]}',
                'User_name': dag_run.conf['user_name']
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        get_all_shift_details >> get_shift_schedule_summary >> get_shift_actions >> \
            has_shift_to_delete
        has_shift_to_delete >> rail.Label(
            "Yes") >> delete_shifts >> has_shift_to_assign
        has_shift_to_delete >> rail.Label("No") >> has_shift_to_assign
        has_shift_to_assign >> rail.Label(
            "Yes") >> put_shift_assignment >> add_log_entry >> catch_and_log_errors >> log_to_sumo >> finish
        has_shift_to_assign >> rail.Label("No") >> add_log_entry

    return dag


rail.for_each_instance(create_dag)
