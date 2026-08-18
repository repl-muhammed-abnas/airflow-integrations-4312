from datetime import timedelta
import rail
from airflow.models import Variable

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"horizonmedia_timesheet_autosubmission_submit_timesheets_child_dag_{config.instance}",
        description=f"Horizon media - Timesheet auto submission Timesheets {config.instance}",
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
                no_task='attest_ts'
            )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='attest_ts',
            end_task='finish',
        )


        attest_ts = rail.RepliconServiceOperator(
            task_id='attest_ts',
            endpoint='/services/TimesheetService1.svc/UpdateTimesheetAttestationStatus',
            data={"timesheet": {
                    "uri": '{{ dag_run.conf["timesheet_uris"] }}'},
                    "attestationStatusUri": "urn:replicon:attestation-status:attested"
                }
        )

        submit_ts = rail.RepliconServiceOperator(
            task_id='submit_ts',
            endpoint='/services/TimesheetApprovalService1.svc/Submit2',
            data={"timesheetUri": '{{ dag_run.conf["timesheet_uris"] }}',
                    "unitOfWorkId": '{{ dag_run.conf["timesheet_uris"] }}',
                    "comments": "Submitted by automation"
                }
        )

        ts_submitted_success = rail.WriteLogOperator(
            task_id = "ts_submitted_success",
            severity="Success",
            message="Timesheet submitted. Message: ",
            properties= {
                'username': '{{ dag_run.conf["username"] }}',
                'timesheetperiod': '{{ dag_run.conf["timesheetperiod"] }}',
                'parentjobid': '{{ dag_run.conf["callerjobid"] }}',
                'jobid': "{{dag_run_ecid()}}",
                'status': 'Success',
                'details': "Timesheet successfully submitted"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'username': '{{ dag_run.conf["username"] }}',
                'timesheetperiod': '{{ dag_run.conf["timesheetperiod"] }}',
                'parentjobid': '{{ dag_run.conf["callerjobid"] }}',
                'jobid': "{{dag_run_ecid()}}",
                'status': 'Error',
                'details': "{{get_error_message()}}"
            },
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label('No') >> attest_ts

        attest_ts >> submit_ts >> ts_submitted_success >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
