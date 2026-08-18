from datetime import datetime, timedelta
import itertools
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from necau.auto_shift_assignment.utils import python_callable_method
from necau.auto_shift_assignment.utils import request_payload
from necau.auto_shift_assignment.task.get_shift_summary import get_shift_summary
null = None

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'necau_auto_shift_assignment_child_{config.instance}',
        description=f'NECAU - auto shift assignment_Child_v3.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_child_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_child_log',
            end_task='catch_and_log_errors',
        )

        create_child_log = rail.CreateLogOperator(
            task_id="create_child_log"
        )

        get_shift_week_info = rail.PythonOperator(
            task_id='get_shift_week_info',
            python_callable=python_callable_method.get_shift_week_informations
        )

        def is_week2_less_current_week():
            shiftinformation = rail.result('get_shift_week_info')
            endweek2 = datetime.strptime(
                shiftinformation['endweek2'], '%Y%m%d')
            numberofdaystobesubstractedfrombeginningoftheweek2 = shiftinformation[
                'numberofdaystobesubstractedfrombeginningoftheweek2']
            todaystimeinmelbournetime = datetime.strptime(
                shiftinformation['todaystimeinmelbournetime'], '%Y%m%d')
            return endweek2 < (todaystimeinmelbournetime - timedelta(days=numberofdaystobesubstractedfrombeginningoftheweek2))

        is_endof_week2_less_current_week = rail.IfOperator(
            task_id='is_endof_week2_less_current_week',
            test=is_week2_less_current_week,
            yes_task='get_shift_day_diff',
            no_task='is_endof_current_week_firday'
        )

        get_shift_day_diff = rail.PythonOperator(
            task_id='get_shift_day_diff',
            python_callable=python_callable_method.get_weekly_iterations
        )

        def is_endof_week_friday(get_shift_day_diff_name, get_shift_week_info_name):
            get_shift_day_diff = rail.result(get_shift_day_diff_name)
            get_shift_week_info_name = rail.result(get_shift_week_info_name)
            return get_shift_day_diff and get_shift_day_diff['resultingvalue'] and get_shift_week_info_name['endofthecurrentweekfriday']

        is_endof_current_week_firday = rail.IfOperator(
            task_id='is_endof_current_week_firday',
            test=lambda: is_endof_week_friday(
                'get_shift_day_diff', 'get_shift_week_info'),
            yes_task='week1_informations',
            no_task='effective_date_week1_info'
        )

        week1_informations = rail.PythonOperator(
            task_id='week1_informations',
            python_callable=python_callable_method.get_week1_info
        )

        effective_date_week1_info = rail.PythonOperator(
            task_id='effective_date_week1_info',
            python_callable=python_callable_method.get_effective_date_week1_info
        )

        effective_date_toconsider_week2 = rail.PythonOperator(
            task_id='effective_date_toconsider_week2',
            python_callable=python_callable_method.get_week2_info
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

        def weeks_effective_date_present(task_info_by_week):
            return task_info_by_week and task_info_by_week['dates_in_week'] and len(task_info_by_week['dates_in_week'])

        has_effective_date_for_week1 = rail.IfOperator(
            task_id='has_effective_date_for_week1',
            test=lambda: weeks_effective_date_present(rail.result('week1_informations')) if rail.result(
                'week1_informations') else weeks_effective_date_present(rail.result('effective_date_week1_info')),
            yes_task='get_shift_schedule_summary_week1',
            no_task='has_effective_date_for_week2'
        )

        get_shift_schedule_summary_week1 = get_shift_summary(
            'week1')

        create_assigned_shift_details_week1 = rail.PythonOperator(
            task_id='create_assigned_shift_details_week1',
            python_callable=python_callable_method.get_assigned_shift_details,
            op_args=["week1"]
        )

        create_assigned_shift_details_week2 = rail.PythonOperator(
            task_id='create_assigned_shift_details_week2',
            python_callable=python_callable_method.get_assigned_shift_details,
            op_args=["week2"]
        )

        delete_shifts = rail.RepliconServiceOperator(
            task_id="delete_shifts",
            endpoint="/services/ShiftAssignmentService1.svc/BulkDelete",
            data=request_payload.get_assignment_uris_to_delete
        )

        has_effective_date_for_week2 = rail.IfOperator(
            task_id='has_effective_date_for_week2',
            test=lambda: weeks_effective_date_present(
                rail.result('effective_date_toconsider_week2')),
            yes_task='get_shift_schedule_summary_week2',
            no_task='has_shift_to_delete'
        )

        get_shift_schedule_summary_week2 = get_shift_summary(
            'week2')

        def is_shift_to_delete(delete_week1_shifts, delete_week2_shifts):
            assigned_shift_details_week1 = rail.result(delete_week1_shifts)
            assigned_shift_details_week2 = rail.result(delete_week2_shifts)
            return any(i and i['shifts_to_delete'] and len(i['shifts_to_delete']) > 0 for i in [assigned_shift_details_week1, assigned_shift_details_week2])

        has_shift_to_delete = rail.IfOperator(
            task_id='has_shift_to_delete',
            test=lambda: is_shift_to_delete(
                'create_assigned_shift_details_week1', 'create_assigned_shift_details_week2'),
            yes_task='delete_shifts',
            no_task='process_assignment'
        )

        def is_week_assignment_present(week_number):
            weekly_effective_dates = python_callable_method.get_weekly_effective_dates(
                week_number)
            return len(weekly_effective_dates) > 0

        process_assignment = rail.EmptyOperator(
            task_id='process_assignment'
        )

        has_week1_assignment = rail.IfOperator(
            task_id='has_week1_assignment',
            test=lambda: is_week_assignment_present('week1'),
            yes_task='shift_to_assign_week1',
            no_task='has_week2_assignment'
        )

        shift_to_assign_week1 = rail.PythonOperator(
            task_id='shift_to_assign_week1',
            python_callable=lambda dag_run: python_callable_method.get_shift_to_assign_for_week(
                dag_run, 'week1')
        )

        has_week2_assignment = rail.IfOperator(
            task_id='has_week2_assignment',
            test=lambda: is_week_assignment_present('week2'),
            yes_task='shift_to_assign_week2',
            no_task='has_data_to_assign_shift'
        )

        shift_to_assign_week2 = rail.PythonOperator(
            task_id='shift_to_assign_week2',
            python_callable=lambda dag_run: python_callable_method.get_shift_to_assign_for_week(
                dag_run, 'week2')
        )

        def is_week_assignment_replicon(week1_assigments, week2_assigments):
            week1_assigments_info = rail.result(week1_assigments)
            if week1_assigments_info and week1_assigments_info['shift_to_assign_week'] and len(week1_assigments_info['shift_to_assign_week']) > 0:
                return True

            week2_assigments_info = rail.result(week2_assigments)
            return week2_assigments_info and week2_assigments_info['shift_to_assign_week'] and len(week2_assigments_info['shift_to_assign_week']) > 0

        has_data_to_assign_shift = rail.IfOperator(
            task_id='has_data_to_assign_shift',
            test=lambda: is_week_assignment_replicon(
                'shift_to_assign_week1', 'shift_to_assign_week2'),
            yes_task='bulk_put_shift_assignment',
            no_task='add_log_entry'
        )

        bulk_put_shift_assignment = rail.RepliconServiceOperator(
            task_id="bulk_put_shift_assignment",
            endpoint="/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments",
            data=request_payload.get_put_shift_payload
        )

        add_log_entry = rail.WriteLogOperator(
            task_id="add_log_entry",
            log='{{ result("create_child_log") }}',
            severity="Success",
            properties=lambda dag_run: {
                'User_name': dag_run.conf['Loginname'],
                'shiftname': dag_run.conf['Shiftname'],
                'status': 'Success',
                'reason': 'Shift assignment updated/added' if rail.result('bulk_put_shift_assignment') else 'No Change',
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run)
            },
            message="Successfully Approved",
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_child_log") }}',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message()}}',
            properties=lambda dag_run: {
                'User_name': '{{ dag_run.conf.Loginname }}',
                'shiftname': '{{ dag_run.conf.Shiftname }}',
                'status': 'Error',
                'reason': "Error",
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run)
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'Loginname ': '{{ dag_run.conf.Loginname }}',
                'Shiftname': '{{ dag_run.conf.Loginname }}',
                'startdate': '{{ dag_run.conf.Startdate }}',
                'Wk1 pattern': '{{ dag_run.conf.Wk1pattern  }}',
                'Wk2 pattern': '{{ dag_run.conf.Wk2pattern }}',
                'username': '{{ dag_run.conf.Username }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_child_log

        create_child_log >> get_shift_week_info >> is_endof_week2_less_current_week

        is_endof_week2_less_current_week >> rail.Label(
            "Yes") >> get_shift_day_diff >> is_endof_current_week_firday
        is_endof_week2_less_current_week >> rail.Label(
            "No") >> is_endof_current_week_firday

        is_endof_current_week_firday >> rail.Label(
            "Yes") >> week1_informations >> get_all_shift_details
        is_endof_current_week_firday >> rail.Label(
            "No") >> effective_date_week1_info >> effective_date_toconsider_week2 >> get_all_shift_details

        get_all_shift_details >> has_effective_date_for_week1
        has_effective_date_for_week1 >> rail.Label(
            "Yes") >> get_shift_schedule_summary_week1 >> create_assigned_shift_details_week1 >> has_effective_date_for_week2
        has_effective_date_for_week1 >> rail.Label(
            "No") >> has_effective_date_for_week2

        has_effective_date_for_week2 >> rail.Label(
            "Yes") >> get_shift_schedule_summary_week2 >> create_assigned_shift_details_week2 >> has_shift_to_delete
        has_effective_date_for_week2 >> rail.Label(
            "No") >> has_shift_to_delete

        has_shift_to_delete >> rail.Label(
            "Yes") >> delete_shifts >> process_assignment
        has_shift_to_delete >> rail.Label(
            "No") >> process_assignment

        process_assignment >> has_week1_assignment

        has_week1_assignment >> rail.Label(
            "Yes") >> shift_to_assign_week1 >> has_week2_assignment
        has_week1_assignment >> rail.Label(
            "No") >> has_week2_assignment

        has_week2_assignment >> rail.Label(
            "Yes") >> shift_to_assign_week2 >> has_data_to_assign_shift
        has_week2_assignment >> rail.Label(
            "No") >> has_data_to_assign_shift

        has_data_to_assign_shift >> rail.Label(
            "Yes") >> bulk_put_shift_assignment >> add_log_entry
        has_data_to_assign_shift >> rail.Label(
            "No") >> add_log_entry

        add_log_entry >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
