import rail

null = None


def get_project_catch_sumo_logs(caller, config):
    with rail.TaskGroup(group_id=f'project_catch_sumo_logs_group_{caller}', prefix_group_id=False):

        catch_and_log_errors = rail.WriteLogOperator(
            task_id=f'catch_and_log_errors_{caller}',
            log='{{ result("client_project_logs_'+caller+'") }}',
            trigger_rule='one_failed',
            severity='Error',
            message=config.error_template,
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
                'details': {config.error_template},
                'exported': 'No'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id=f'log_to_sumo_{caller}',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'name ': '{{ dag_run.conf.projectname }}',
                'code': '{{ dag_run.conf.projectcode }}',
                'db': '{{ dag_run.conf.millmpc }}',
                'status': "\
                    {%- if result('get_exception_messages_"+caller+"') | is_falsy -%} \
                         Success \
                    {%- else -%} \
                         Exception\
                    {%- endif -%}"
            }
        )

        catch_and_log_errors >> log_to_sumo

        return catch_and_log_errors, log_to_sumo
