import rail

from pwcglobal.absense_data_pre_population_v3.utils import python_callable_method


def get_hours_quantity_check(caller):
    with rail.TaskGroup(group_id=f'hours_quantity_check_group_{caller}', prefix_group_id=False):

        is_hours_quantity_zero = rail.IfOperator(
            task_id=f'is_hours_quantity_zero_{caller}',
            test=lambda dag_run: bool(
                dag_run.conf["hoursquantityinseconds"] == 0),
            yes_task=f'log_time_entry_hours_zero_{caller}',
            no_task=f'end_of_hours_quantity_check_{caller}'
        )

        log_time_entry_hours_zero = rail.WriteLogOperator(
            task_id=f'log_time_entry_hours_zero_{caller}',
            log='{{ result("create_child_log") }}',
            message='Time entry {{ dag_run.conf.TimeEntryID }} not added since hours value received as 0',
            severity='Exception',
            properties=lambda: python_callable_method.get_log_properties(
                action='Add', status='Exception')
        )

        end_of_hours_quantity_check = rail.EmptyOperator(
            task_id=f'end_of_hours_quantity_check_{caller}'
        )

        is_hours_quantity_zero >> rail.Label(
            'Yes') >> log_time_entry_hours_zero
        is_hours_quantity_zero >> rail.Label(
            'No') >> end_of_hours_quantity_check

        return (is_hours_quantity_zero, log_time_entry_hours_zero, end_of_hours_quantity_check)
