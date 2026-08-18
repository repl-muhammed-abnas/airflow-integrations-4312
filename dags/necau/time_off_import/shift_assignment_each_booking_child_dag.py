import itertools
from datetime import timedelta
import rail
from airflow.models import Variable
from necau.time_off_import.utils import python_callable_method
from necau.time_off_import.utils import custom_method
from necau.time_off_import.utils import request_payload
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'necau_shift_assignment_each_booking_child_{config.instance}',
        description=f'NECAU - Shift_Assignment_Each_Booking_v2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='get_processed_shift_info'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_processed_shift_info',
            end_task='finish',
        )

        get_processed_shift_info = rail.FilterLogEntriesOperator(
            task_id='get_processed_shift_info',
            log="{{ dag_run.conf.user_assignment_history }}",
            properties={"shift_md5": "{{dag_run.conf.shift_referance}}"}
        )

        has_shift_already_processed = rail.IfOperator(
            task_id="has_shift_already_processed",
            test=custom_method.get_processed_shift,
            yes_task='add_log_entry',
            no_task='add_new_shift_entry'
        )

        add_new_shift_entry = rail.WriteLogOperator(
            task_id='add_new_shift_entry',
            log="{{ dag_run.conf.user_assignment_history }}",
            severity="Success",
            properties=custom_method.get_referance_shift_info,
            message="Success"
        )

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

        shift_assignment_category = rail.PythonOperator(
            task_id='shift_assignment_category',
            python_callable=python_callable_method.get_assignment_category
        )

        has_shift_to_delete = rail.IfOperator(
            task_id='has_shift_to_delete',
            test=custom_method.has_shift_to_delete,
            yes_task='delete_shifts',
            no_task='dummy_operator_1'
        )

        delete_shifts = rail.RepliconServiceOperator(
            task_id="delete_shifts",
            endpoint="/services/ShiftAssignmentService1.svc/BulkDelete",
            data=request_payload.get_assignment_uris_to_delete
        )

        has_shift_to_assign = rail.IfOperator(
            task_id='has_shift_to_assign',
            test=custom_method.has_shift_to_assign,
            yes_task='put_shift_assignment',
            no_task='add_log_entry'
        )

        put_shift_assignment = rail.RepliconServiceOperator(
            task_id='put_shift_assignment',
            endpoint="/services/ShiftAssignmentService1.svc/PutShiftAssignment",
            data=request_payload.get_assignment_request
        )

        add_log_entry = rail.WriteLogOperator(
            task_id="add_log_entry",
            severity="Success",
            properties=lambda dag_run: {
                'User_name': dag_run.conf['user_name'],
                'shiftname': dag_run.conf['shift_name'],
                'status': 'Success',
                'reason': 'Shift assignment updated/added' if rail.result('put_shift_assignment') else 'No Change'
            },
            message="Successfully Approved",
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'User_name': dag_run.conf['user_name'],
                'shiftname': dag_run.conf['shift_name'],
                'status': 'Error',
                'reason': "Error"
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'shift_name': '{{ dag_run.conf.shift_name }}',
                'effective_date': '{{ dag_run.conf.effective_date }}',
                'pattern': '{{ dag_run.conf.pattern }}',
                'user_name': '{{ dag_run.conf.user_name }}'
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        dummy_operator_1 = rail.EmptyOperator(
            task_id='dummy_operator_1'
        )

        can_run_batch_task
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            "No") >> get_processed_shift_info >> has_shift_already_processed
        has_shift_already_processed >> rail.Label("Yes") >> add_log_entry
        has_shift_already_processed >> rail.Label(
            "No") >> add_new_shift_entry >> get_all_shift_details >> get_shift_schedule_summary >> \
            shift_assignment_category >> has_shift_to_delete
        has_shift_to_delete >> rail.Label(
            "Yes") >> delete_shifts >> dummy_operator_1 >> has_shift_to_assign
        has_shift_to_assign >> rail.Label(
            "Yes") >> put_shift_assignment >> add_log_entry >> catch_and_log_errors >> finish >> log_to_sumo
        has_shift_to_assign >> rail.Label("No") >> add_log_entry
        has_shift_to_delete >> rail.Label("No") >> dummy_operator_1

    return dag


rail.for_each_instance(create_dag)
