import rail


def log_exception_field_task(import_type):
    with rail.TaskGroup(group_id='log_exception_task', prefix_group_id=False) as log_exception_task:

        action_type = "created" if import_type == "Add" else "updated"

        has_exception_logs = rail.IfOperator(
            task_id='has_exception_logs',
            test=lambda: len(rail.result('get_exception_logs')) > 0,
            yes_task='write_exception_logs',
            no_task='write_success_log',
        )

        def get_project_exception_message():
            exception_logs = rail.result('get_exception_logs') + rail.result(
                'get_success_logs') if rail.result('get_success_logs') else rail.result('get_exception_logs')
            return f"Project {action_type} with exception: " + " | ".join([m for m in exception_logs if m is not None])

        def get_project_success_message():
            success_logs = rail.result('get_success_logs')
            return f"Project {action_type} successfully: " + " | ".join(
                [m for m in success_logs if m is not None]) if success_logs else f"Project {action_type} successfully"

        def get_log_status():
            if rail.result(
                    'get_success_logs') and 'Project start date was updated to a later date' in rail.result('get_success_logs'):
                return 'Warning'
            return 'Exception'

        write_exception_logs = rail.WriteLogOperator(
            task_id='write_exception_logs',
            log='{{ dag_run.conf.log }}',
            message=f"Project {action_type} with exception",
            severity='Exception',
            properties=lambda: {
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': "{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}",
                'Client Name|Client Code': 'nil',
                'Task Name|Task Code': 'nil',
                'status': get_log_status(),
                'details': get_project_exception_message(),
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': import_type
            }
        )

        write_success_log = rail.WriteLogOperator(
            task_id='write_success_log',
            log='{{ dag_run.conf.log }}',
            message=f"Project {action_type} successfully",
            severity='Success',
            properties=lambda: {
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': "{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}",
                'Client Name|Client Code': 'nil',
                'Task Name|Task Code': 'nil',
                'status': 'Success',
                'details': get_project_success_message(),
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': import_type
            }
        )

        dummy_exception_log_finish = rail.EmptyOperator(
            task_id='dummy_exception_log_finish'
        )

        has_exception_logs >> rail.Label(
            "Yes") >> write_exception_logs >> dummy_exception_log_finish

        has_exception_logs >> rail.Label(
            "No") >> write_success_log >> dummy_exception_log_finish

        return log_exception_task
