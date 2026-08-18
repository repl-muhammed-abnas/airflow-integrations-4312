from itertools import chain
from wipro.auto_shift_assignment.holiday_shift_removal.utils import request_payload
import rail
def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.delete_holiday_shift_assignment_dag_id,
        description=f"Wipro Auto Shift Assignment Monthly Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child_1
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_shift_log = rail.CreateLogOperator(
            task_id="create_shift_log"
        )

        query_distinct_users_for_hoilday_calendar = rail.QueryCollectionOperator(
            task_id="query_distinct_users_for_hoilday_calendar",
            query="""SELECT * FROM query_enabled_users_data WHERE holiday_calendar_uri = '{{dag_run.conf.holiday_calendar_uri}}' """,
        )

        get_holidays_for_the_user = rail.RepliconServiceOperator(
            task_id="get_holidays_for_the_user",
            endpoint="/services/HolidayListService1.svc/GetData",
            data=lambda dag_run,config=config:request_payload.get_holiday_list_for_user(config,dag_run),
            data_handler= lambda response:list(map(lambda i: i["cells"][1]["dateValue"], response["rows"])) if response else None
        )

        any_holidays_for_next_three_months = rail.IfOperator(
            task_id="any_holidays_for_next_three_months",
            test=lambda:rail.result("get_holidays_for_the_user") and len(rail.result("get_holidays_for_the_user"))>0,
            yes_task="get_shifts_assigned_on_holiday",
            no_task="catch_and_log_errors"
        )

        get_shifts_assigned_on_holiday = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_shifts_assigned_on_holiday",
            items='{{result("get_holidays_for_the_user")|to_json}}',
            endpoint="/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary",
            data=request_payload.get_shifts_assigned_on_holiday,
            data_handler=lambda response: list(map(lambda i: i["assignmentUri"],response))
        )

        any_shift_assigned_on_holiday = rail.IfOperator(
            task_id="any_shift_assigned_on_holiday",
            test=lambda:list(chain.from_iterable(rail.result("get_shifts_assigned_on_holiday"))),
            yes_task="create_delete_batch",
            no_task="catch_and_log_errors"
        )

        create_delete_batch = rail.RepliconServiceOperator(
            task_id="create_delete_batch",
            endpoint="/services/ShiftAssignmentService1.svc/CreateDeleteBatch",
            data=lambda: {
                "shiftAssignmentUris": list(chain.from_iterable(rail.result("get_shifts_assigned_on_holiday")))
            }
        )

        execute_delete_batch, wait_for_delete_batch = rail.batch_execution(
            'execute_delete_batch', create_delete_batch.task_id)

        delete_shift_assignment_success = rail.WriteLogOperator(
            task_id='delete_shift_assignment_success',
            message="Deleted User Shift Assignments on Holidays",
            log='{{ result("create_shift_log") }}',
            items='{{result("query_distinct_users_for_hoilday_calendar")}}',
            severity='Success',
            properties=lambda item: {
                'username': item['user_name'],
                'employeeid': item['employee_id'],
                'status': 'Success',
                'country': item['country'],
                'schedule': item['schedule']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ result("create_shift_log") }}',
            severity='Error',
            items='{{result("query_distinct_users_for_hoilday_calendar")}}',
            message='{{ get_error_message() }}',
            properties=lambda item: {
                'username': item['user_name'],
                'employeeid': item['employee_id'],
                'status': 'Error',
                'country': item['country'],
                'schedule': item['schedule']
            },
        )

        create_shift_log >>\
        query_distinct_users_for_hoilday_calendar>>\
        get_holidays_for_the_user >>\
        any_holidays_for_next_three_months >> rail.Label("Yes") >> get_shifts_assigned_on_holiday>> any_shift_assigned_on_holiday >> rail.Label("Yes") >>\
        create_delete_batch >>\
        execute_delete_batch >> wait_for_delete_batch >> delete_shift_assignment_success >> catch_and_log_errors
        any_holidays_for_next_three_months >> rail.Label("No") >> catch_and_log_errors
        any_shift_assigned_on_holiday >> rail.Label("No") >> catch_and_log_errors
        return dag

rail.for_each_instance(create_airflow_child_dag)
