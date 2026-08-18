from datetime import timedelta
from pendulum import now as pendulum_now
import rail


def create_disabled_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_userprofile_disable_child_{config.instance}",
        description=f'DXC_Fieldglass CWFUserProfiles_Disable_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint='services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.uri }}'
            }
        )

        def get_today_date():
            now = pendulum_now(tz=config.aus_timezone)
            return {
                'year': now.year,
                'month': now.month,
                'day': now.day
            }

        get_direct_reports = rail.RepliconServiceOperator(
            task_id='get_direct_reports',
            endpoint='/services/UserService1.svc/GetDirectReportsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['uri'],
                'asOfDate': get_today_date(),
                'userStatusOptionUri': 'urn:replicon:user-status-option:include-only-enabled-users'
            }
        )

        change_supervisor_of_reportees = rail.TriggerDagRunForEachItemOperator(
            task_id="change_supervisor_of_reportees",
            items="{{ result('get_direct_reports') | to_json }}",
            trigger_dag_id=f"macquarie_user_import_disable_users_update_supervisor_child_{config.instance}",
            conf=lambda item, dag_run:{
                "user_uri": item['uri'],
                "user_loginname": item['loginName'],
                "default_supervisor_uri": dag_run.conf.get('default_supervisor_uri'),
                "default_supervisor_effective_date": get_today_date()
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
                retries=0
        )

        wait_for_change_supervisor_of_reportees = rail.WaitForDagRunsSensor(
            task_id="wait_for_change_supervisor_of_reportees",
            dag_runs="{{result('change_supervisor_of_reportees')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_supervisor_errors_failures = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_supervisor_errors_failures',
            dag_runs="{{ result('change_supervisor_of_reportees') }}",
            dagrun_task_id='catch_and_log_error',
            flatten=True
        )

        gather_supervisor_errors_update_failures = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_supervisor_errors_update_failures',
            dag_runs="{{ result('change_supervisor_of_reportees') }}",
            dagrun_task_id='log_update_failed',
            flatten=True
        )

        has_any_failures = rail.IfOperator(
            task_id='has_any_failures',
            test="{{ result('gather_supervisor_errors_update_failures') | is_truthy or result('gather_supervisor_errors_failures') | is_truthy }}",
            yes_task='fail_disable_user_error',
            no_task='catch_disable_user_error'
        )

        fail_disable_user_error = rail.FailOperator(
            task_id='fail_disable_user_error',
            message='Errors noticed while updating supervisors for few users'
        )

        catch_disable_user_error = rail.PythonOperator(
            task_id='catch_disable_user_error',
            trigger_rule='one_failed',
            python_callable=lambda dag_run: {
                'user': dag_run.conf['user'],
                'useruri': dag_run.conf['uri'],
                'enddate': dag_run.conf['enddate'],
                'error': rail.result('disable_user', key='error')
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'user': '{{ dag_run.conf.user }}',
                'useruri': '{{ dag_run.conf.uri }}',
                'enddate': '{{ dag_run.conf.enddate }}',
                'error': '{{ get_error_message() }}'
            }
        )

        disable_user >> get_direct_reports >> change_supervisor_of_reportees >> wait_for_change_supervisor_of_reportees
        wait_for_change_supervisor_of_reportees >> gather_supervisor_errors_failures >> gather_supervisor_errors_update_failures >>\
            has_any_failures >> rail.Label("Yes") >> fail_disable_user_error >> catch_disable_user_error
        has_any_failures >> rail.Label(
            'On Error') >> catch_disable_user_error >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_disabled_user_child_dag)
