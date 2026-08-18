from datetime import timedelta
import rail
from airflow.models import Variable

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"daimlertrucks_custom_notification_send_email_notification_child_dag_{config.instance}",
        description=f"Daimlertrucks - Custom notification send email notification child dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=10
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        finish = rail.EmptyOperator(task_id='finish')

        can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='').lower() == 'true',
                yes_task='batch_task',
                no_task='has_supervisor'
            )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='has_supervisor',
            end_task='finish',
        )


        has_supervisor = rail.IfOperator(
            task_id='has_supervisor',
            test='{{ dag_run.conf["supervisoroftimesheetowner"] != "" }}',
            yes_task='get_supervisor_details',
            no_task='finish'
        )

        get_supervisor_details=rail.RepliconServiceOperator(
            task_id='get_supervisor_details',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data={"userUri": '{{ dag_run.conf["supervisoroftimesheetowneruri"] }}'},
            response_filter=lambda response: {"firstName" : response.json()['d']["firstName"], "emailAddress" : response.json()['d']["emailAddress"]}
        )

        send_email_to_supervisor = rail.EmailOperator(
            task_id = 'send_email_to_supervisor',
            to = '{{ result("get_supervisor_details")["emailAddress"]}}',
            bcc = config.internal_logs_email,
            subject = 'Replicon: Timesheets Pending Approval from Project Managers',
            html_content = "send_email_template.html"
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label('No') >> has_supervisor

        has_supervisor >> rail.Label('Yes') >> get_supervisor_details >> send_email_to_supervisor >> finish >> catch_and_log_errors

        has_supervisor >> rail.Label('No') >> finish >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
