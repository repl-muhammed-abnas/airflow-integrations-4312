import rail
from kmhavintegrationinc.custom_notification_for_task_milestone.utils import python_callable


def process_task_milestone(task_name, project_name,milestone_range,percentage,tenant_mail):
    with rail.TaskGroup(group_id=f'process_task_milestone_{milestone_range}', prefix_group_id=False):

        search_task_milestone_entry=rail.FilterLogEntriesOperator(
            task_id=f'search_task_milestone_entry_{milestone_range}',
            log= "{{ result('task_milestone_logger') }}",
            properties={
                'Task Name': "{{ result('foreach_item_in_task_milestone_do')['Task Name (Full Path)'] }}",
                'Project Name': "{{ result('foreach_item_in_task_milestone_do')['Project Name'] }}",
            }
        )

        is_current_range_same = rail.IfOperator(
            task_id= f'is_current_range_same_{milestone_range}',
            test= lambda: python_callable.test_milestone_range(rail.result(f'search_task_milestone_entry_{milestone_range}'),
                rail.result(f'search_task_milestone_entry_{milestone_range}','length'), rail.result('get_task_milestone_value')),
            yes_task= f'proceess_finish_{milestone_range}',
            no_task= f'send_milestone_mail_is_{milestone_range}'
        )

        send_milestone_mail=rail.EmailOperator(
            task_id=f'send_milestone_mail_is_{milestone_range}',
            to= tenant_mail,
            # pylint: disable=line-too-long
            subject=f'''High Importance! - Task "{task_name}" has reached {percentage} milestone | Project "{project_name}" ''',
            html_content= 'templates/email/milestone_mail.html',
        )

        remove_old_milestone_entry_log_table = rail.FilterLogEntriesOperator(
            task_id=f'remove_old_milestone_entry_log_table_{milestone_range}',
            log= "{{ result('task_milestone_logger') }}",
            properties={
                'Task Name': "{{ result('foreach_item_in_task_milestone_do')['Task Name (Full Path)'] }}",
                'Project Name': "{{ result('foreach_item_in_task_milestone_do')['Project Name'] }}",
            },
            remove_filtered_entries=True
        )

        add_new_entry_to_milestone_log_table=rail.WriteLogOperator(
            task_id=f"add_new_entry_to_milestone_log_table_{milestone_range}",
            log="{{ result('task_milestone_logger') }}",
            message="Add_Entry",
            properties={
                'Task Name':  "{{ result('foreach_item_in_task_milestone_do')['Task Name (Full Path)'] }}",
                'Project Name': "{{ result('foreach_item_in_task_milestone_do')['Project Name'] }}",
                'Milestone Range': milestone_range
            }
        )

        proceess_finish = rail.EmptyOperator(
            task_id =f'proceess_finish_{milestone_range}'
        )

        search_task_milestone_entry >> is_current_range_same
        is_current_range_same >> rail.Label("No") >> send_milestone_mail >> remove_old_milestone_entry_log_table >> add_new_entry_to_milestone_log_table
        is_current_range_same >> rail.Label('Yes') >> proceess_finish
        add_new_entry_to_milestone_log_table >> proceess_finish


        return search_task_milestone_entry, proceess_finish
