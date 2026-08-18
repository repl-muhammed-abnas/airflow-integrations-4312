from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.request_payload import get_today_date


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_supervisordirectreport_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_supervisor_direct_reports_crv2.0_{config.instance}',
        description=f'Adtalem Supervisor direct reports Production CRV2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_timesheet_policysets'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_timesheet_policysets',
            end_task='dagrun_log_to_sumo',
        )

        get_timesheet_policysets = rail.RepliconServiceOperator(
            task_id='get_timesheet_policysets',
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:timesheet', '')
        )

        is_timesheet_policysets_present = rail.IfOperator(
            task_id='is_timesheet_policysets_present',
            test="{{ result('get_timesheet_policysets') | is_truthy }}",
            yes_task="update_supervisor",
            no_task="dagrun_log_to_sumo",
        )

        update_supervisor = rail.RepliconServiceOperator(
            task_id='update_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": dag_run.conf['supervisoruri'],
                "dateRange": {
                    "startDate": get_today_date()
                }
            }
        )

        send_mail_to_HR = rail.EmailOperator(
            task_id='send_mail_to_HR',
            to=config.hr_email,
            subject="{{ get_company_key() }} | Supervisor Assignment changed - {{ current_time('%m-%d-%Y') }}",
            html_content='templates/email/supervisor_direct_reports.html'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_timesheet_policysets >> is_timesheet_policysets_present

        is_timesheet_policysets_present >> rail.Label(
            'Yes') >> update_supervisor >> send_mail_to_HR >> dagrun_log_to_sumo

        is_timesheet_policysets_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_supervisordirectreport_child_dag)
