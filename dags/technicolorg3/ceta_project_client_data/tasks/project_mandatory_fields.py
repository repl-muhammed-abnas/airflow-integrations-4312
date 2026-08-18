import rail
from technicolorg3.ceta_project_client_data.utils import python_callable_method

null = None


def get_project_mandatory_fields(caller, config):
    with rail.TaskGroup(group_id=f'project_mandatory_fields_group_{caller}', prefix_group_id=False):

        client_project_logs = rail.CreateLogOperator(
            task_id=f'client_project_logs_{caller}',
            tenant_wide_name=f'{config.client_project_logs}',
            existing_log_mode='append',
        )

        project_message_to_log = rail.PythonOperator(
            task_id=f'project_message_to_log_{caller}',
            python_callable=python_callable_method.get_project_message_to_log
        )

        should_process_project_data = rail.IfOperator(
            task_id=f'should_process_project_data_{caller}',
            test=lambda: not bool(rail.result(
                f'project_message_to_log_{caller}')),
            yes_task=f'project_mandatory_fields_end_{caller}',
            no_task=f'log_project_fields_missing_{caller}'
        )

        log_project_fields_missing = rail.WriteLogOperator(
            task_id=f'log_project_fields_missing_{caller}',
            log="{{ result('client_project_logs_"+caller+"') }}",
            message="{{ result('project_message_to_log_"+caller+"') }}",
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
                'details': "{{ result('project_message_to_log_"+caller+"') }}",
                'reference': '{{ dag_run_ecid() }}',
                'exported': 'No'
            }
        )

        project_mandatory_fields_end = rail.EmptyOperator(
            task_id=f'project_mandatory_fields_end_{caller}'
        )

        client_project_logs >> project_message_to_log >> should_process_project_data

        should_process_project_data >> rail.Label(
            'Yes') >> project_mandatory_fields_end
        should_process_project_data >> rail.Label(
            'No') >> log_project_fields_missing

        return client_project_logs, project_mandatory_fields_end, log_project_fields_missing
