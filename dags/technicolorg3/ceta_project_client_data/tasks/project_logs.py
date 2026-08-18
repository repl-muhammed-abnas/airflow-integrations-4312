import rail
from technicolorg3.ceta_project_client_data.utils import python_callable_method

null = None


def get_project_logs(caller):
    with rail.TaskGroup(group_id=f'project_logs_group_{caller}', prefix_group_id=False) as project_logs_group:

        get_exception_messages = rail.PythonOperator(
            task_id=f'get_exception_messages_{caller}',
            python_callable=python_callable_method.get_exception_messages,
            op_args=[caller]
        )

        are_exceptions_present = rail.IfOperator(
            task_id=f'are_exceptions_present_{caller}',
            test=lambda: bool(rail.result(f'get_exception_messages_{caller}')),
            yes_task=f'log_with_exceptions_{caller}',
            no_task=f'log_successfull_{caller}'
        )

        log_with_exceptions = rail.WriteLogOperator(
            task_id=f'log_with_exceptions_{caller}',
            log='{{ result("client_project_logs_'+caller+'") }}',
            # pylint: disable=line-too-long
            message='The Client_Project transfer from CETA to Replicon with job reference "{{ dag_run_ecid() }}" has been completed with below exception(s). - {{result("get_exception_messages_'+caller+'")}}',
            properties={
                'db': '{{ dag_run.conf.millmpc }}',
                'client': '{{ dag_run.conf.clientname }}',
                'project': '{{ dag_run.conf.projectname }}',
                'status': 'Exception',
                'action': '\
                    {%- if "'+caller+'" == "add_project" -%} \
                         Add Project \
                    {%- else -%} \
                         Update Project\
                    {%- endif -%}',
                # pylint: disable=line-too-long
                'details': 'The Client_Project transfer from CETA to Replicon with job reference "{{ dag_run_ecid() }}" has been completed with below exception(s). - {{result("get_exception_messages_'+caller+'")}}',
                'reference': '{{ dag_run_ecid() }}',
                'exported': 'No'
            }
        )

        log_successfull = rail.WriteLogOperator(
            task_id=f'log_successfull_{caller}',
            log='{{ result("client_project_logs_'+caller+'") }}',
            message='The Client_Project transfer from CETA to Replicon with job reference "{{ dag_run_ecid() }}" has been completed successfully',
            properties={
                'db': '{{ dag_run.conf.millmpc }}',
                'client': '{{ dag_run.conf.clientname }}',
                'project': '{{ dag_run.conf.projectname }}',
                'status': 'Success',
                'action': '\
                    {%- if "'+caller+'" == "add_project" -%} \
                         Add Project \
                    {%- else -%} \
                         Update Project\
                    {%- endif -%}',
                # pylint: disable=line-too-long
                'details': 'The Client_Project transfer from CETA to Replicon with job reference "{{ dag_run_ecid() }}" has been completed successfully',
                'reference': '{{ dag_run_ecid() }}',
                'exported': 'No'
            }
        )

        get_exception_messages >> are_exceptions_present
        are_exceptions_present >> rail.Label(
            'Yes') >> log_with_exceptions
        are_exceptions_present >> rail.Label(
            'No') >> log_successfull

        return project_logs_group
