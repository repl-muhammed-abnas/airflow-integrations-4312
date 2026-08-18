from datetime import datetime
import uuid
from nttdatabc.shift_automation.utils import python_callable_methods
from nttdatabc.shift_automation.utils import request_payload
from dateutil.relativedelta import relativedelta
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'nttdatabc_default_shift_assignment_per_user_child_{config.instance}',
        description=f'NTTData BC Default shift assignment per user child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_shift_schedule_summary_foruser = rail.RepliconServiceOperator(
            task_id='get_shift_schedule_summary_foruser',
            endpoint="/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary",
            data=request_payload.get_shift_schedule_summary_payload
        )

        create_assigned_shift_dates_collection = rail.CreateCollectionOperator(
            task_id='create_assigned_shift_dates_collection',
            source=python_callable_methods.get_assigned_shift_list,
            name='assigned_shift_dates_collection',
            columns=["date", "dateday", "month", "year", "week", "weekday", "shift"]
        )

        get_last_full_week_shifts = rail.CreateCollectionOperator(
            task_id='get_last_full_week_shifts',
            source=python_callable_methods.get_last_full_week_shifts,
            name='last_full_week_shifts',
            columns=["date", "dateday", "month", "year", "week", "weekday", "shift"]
        )

        is_full_week_shifts_exists = rail.IfOperator(
            task_id='is_full_week_shifts_exists',
            test='{{ result("get_last_full_week_shifts", "length") > 0 }}',
            yes_task='create_dates_to_consider_collection',
            no_task='log_exception'
        )

        log_exception = rail.WriteLogOperator(
            task_id='log_exception',
            message='Shifts for user not available in the last full week to assign',
            severity='Exception',
            properties=lambda dag_run: {
                'username': dag_run.conf['username'],
                'usertype': dag_run.conf['usertype'],
                'shiftstartdate': (datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d") + relativedelta(months=1, day=1)).strftime("%Y-%m-%d"),
                'shiftenddate': dag_run.conf['enddate'],
                'status': 'Exception',
                'description': 'Shifts for user not available in the last full week to assign'
            }
        )

        create_dates_to_consider_collection = rail.CreateCollectionOperator(
            task_id='create_dates_to_consider_collection',
            source=python_callable_methods.get_dates_to_consider_list,
            name='dates_to_consider_collection',
            columns=["seq", "date", "day", "dateday", "datemonth", "dateyear", "week"]
        )

        working_days_list = rail.QueryCollectionOperator(
            task_id='working_days_list',
            query='''SELECT * FROM dates_to_consider_collection WHERE (date NOT IN
                        (SELECT DISTINCT date FROM assigned_shift_dates_collection))
                        AND (day IN (SELECT DISTINCT weekday FROM last_full_week_shifts))'''
        )

        is_working_days_list_exists = rail.IfOperator(
            task_id='is_working_days_list_exists',
            test='{{ result("working_days_list", "length") > 0 }}',
            yes_task='create_shift_assignment_list',
            no_task='log_skipped_shift_already_assigned'
        )

        log_skipped_shift_already_assigned = rail.WriteLogOperator(
            task_id='log_skipped_shift_already_assigned',
            message='Shifts already assigned',
            severity='Skipped',
            properties=lambda dag_run: {
                'username': dag_run.conf['username'],
                'usertype': dag_run.conf['usertype'],
                'shiftstartdate': (datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d") + relativedelta(months=1, day=1)).strftime("%Y-%m-%d"),
                'shiftenddate': dag_run.conf['enddate'],
                'status': 'Skipped',
                'description': 'Shifts already assigned'
            }
        )

        create_shift_assignment_list = rail.PythonOperator(
            task_id='create_shift_assignment_list',
            python_callable=python_callable_methods.get_shift_assignment_list
        )

        bulk_put_shift_assignments = rail.RepliconServiceOperator(
            task_id='bulk_put_shift_assignments',
            endpoint='/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments',
            data=lambda: {
                "assignments": rail.result("create_shift_assignment_list"),
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        log_successfull = rail.WriteLogOperator(
            task_id='log_successfull',
            message='Shifts assigned successfully',
            severity=lambda dag_run: 'New User Shift Addition Success' if dag_run.conf["usertype"] == "New User" else 'Existing User Shift Addition Success',
            properties=lambda dag_run: {
                'username': dag_run.conf['username'],
                'usertype': dag_run.conf['usertype'],
                'shiftstartdate': (datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d") + relativedelta(months=1, day=1)).strftime("%Y-%m-%d"),
                'shiftenddate': dag_run.conf['enddate'],
                'status': 'New User Shift Addition Success' if dag_run.conf["usertype"] == "New User" else 'Existing User Shift Addition Success',
                'description': 'Shifts assigned successfully'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'username': dag_run.conf['username'],
                'usertype': dag_run.conf['usertype'],
                'shiftstartdate':  (datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d") + relativedelta(months=1, day=1)).strftime("%Y-%m-%d"),
                'shiftenddate': dag_run.conf['enddate'],
                'status': 'Error',
                'description': '{{ get_error_message() }}'
            }
        )

        get_shift_schedule_summary_foruser >> create_assigned_shift_dates_collection \
            >> get_last_full_week_shifts >> is_full_week_shifts_exists
        is_full_week_shifts_exists >> rail.Label("Yes") >> create_dates_to_consider_collection >> working_days_list >> is_working_days_list_exists
        is_full_week_shifts_exists >> rail.Label("No") >> log_exception >> catch_and_log_errors
        is_working_days_list_exists >> rail.Label("Yes") >> create_shift_assignment_list >> bulk_put_shift_assignments \
            >> log_successfull >> catch_and_log_errors
        is_working_days_list_exists >> rail.Label("No") >> log_skipped_shift_already_assigned >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
