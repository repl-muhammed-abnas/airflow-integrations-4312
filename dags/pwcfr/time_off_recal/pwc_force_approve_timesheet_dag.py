from datetime import timedelta
import uuid
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_force_approve_timesheet_child_{config.instance}',
        description=f'Pwcfr_force_approve_timesheet_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='force_approve_timesheet'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='force_approve_timesheet',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        force_approve_timesheet = rail.RepliconServiceOperator(
            task_id='force_approve_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/ForceApprove",
            data=lambda dag_run: {
                "timesheetUri": dag_run.conf['timesheeturi_list']['timesheeturi'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Admin after Time Off Modification"
            }
        )

        log_success_entries = rail.WriteLogOperator(
            task_id='log_success_entries',
            log="{{dag_run.conf.pwctest_lookup_table}}",
            message="na",
            severity='Success',
            properties=lambda dag_run: {
                'loginname': dag_run.conf['timesheeturi_list']['loginname'],
                'timesheeturi': dag_run.conf['timesheeturi_list']['timesheeturi'],
                'status': "Success",
                'details': "NA",
                'jobid': "{{ dag_run_ecid() }}"

            }
        )

        log_error_entries = rail.WriteLogOperator(
            task_id='log_error_entries',
            log="{{dag_run.conf.pwctest_lookup_table}}",
            trigger_rule='one_failed',
            message='{{get_error_message()}}',
            severity='Error',
            properties=lambda dag_run: {
                'loginname': dag_run.conf['timesheeturi_list']['loginname'],
                'timesheeturi': dag_run.conf['timesheeturi_list']['timesheeturi'],
                'status': "Error",
                'details': "{{get_error_message()}}",
                'jobid': "{{ dag_run_ecid() }}",

            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> force_approve_timesheet
        force_approve_timesheet >> log_success_entries >> log_error_entries >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
