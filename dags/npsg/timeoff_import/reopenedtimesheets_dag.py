
from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_0_{config.instance}',
        description=f'NPSG_timeoffimport_reopenedtimesheets V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_foreach_document_6_col2_ends_with_approved_7'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_foreach_document_6_col2_ends_with_approved_7',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        if_foreach_document_6_col2_ends_with_approved_7 = rail.IfOperator(
            task_id='if_foreach_document_6_col2_ends_with_approved_7',
            test=lambda dag_run: bool(
                dag_run.conf['status'].rsplit(':', 1)[-1] == 'approved'),
            yes_task="force_approve_8",
            no_task="if_foreach_document_6_col2_ends_with_waiting_9"
        )

        force_approve_8 = rail.RepliconServiceOperator(
            task_id='force_approve_8',
            endpoint="/services/TimesheetApprovalService1.svc/ForceApprove",
            data={
                "timesheetUri": "{{ dag_run.conf.timesheeturi }}",
                "unitOfWorkId": "ForceApprove_"+str(uuid.uuid4()),
                "comments": "ForceApproved by Replicon Integration"
            }
        )

        if_foreach_document_6_col2_ends_with_waiting_9 = rail.IfOperator(
            task_id='if_foreach_document_6_col2_ends_with_waiting_9',
            test=lambda dag_run: bool(
                dag_run.conf['status'].rsplit(':', 1)[-1] == 'waiting'),
            yes_task="submit2_10",
            no_task="finish",
        )

        submit2_10 = rail.RepliconServiceOperator(
            task_id='submit2_10',
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data={
                "timesheetUri": "{{ dag_run.conf.timesheeturi }}",
                "unitOfWorkId": "Submitted_"+str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> if_foreach_document_6_col2_ends_with_approved_7
        if_foreach_document_6_col2_ends_with_approved_7 >> rail.Label(
            'Yes') >> force_approve_8 >> if_foreach_document_6_col2_ends_with_waiting_9
        if_foreach_document_6_col2_ends_with_approved_7 >> rail.Label(
            'No') >> if_foreach_document_6_col2_ends_with_waiting_9
        if_foreach_document_6_col2_ends_with_waiting_9 >> rail.Label(
            'Yes') >> submit2_10 >> finish
        if_foreach_document_6_col2_ends_with_waiting_9 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
