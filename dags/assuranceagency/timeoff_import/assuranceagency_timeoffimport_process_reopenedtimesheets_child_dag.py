
from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'assuranceagency_timeoffimport_process_reopenedtimesheets_child_{config.instance}',
        description=f'Assuranceagency timeoffimport - Process Reopenedtimesheets - Child{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='assuranceagency_timeoffimport_reopenedtimesheets_search_entries_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='assuranceagency_timeoffimport_reopenedtimesheets_search_entries_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        assuranceagency_timeoffimport_reopenedtimesheets_search_entries_2=rail.FilterLogEntriesOperator(
            task_id='assuranceagency_timeoffimport_reopenedtimesheets_search_entries_2',
            log="{{dag_run.conf.reopenedtimesheetslookup}}",
            properties={
                'jobid': "{{dag_run.conf.callerjobid}}"
            }
        )

        if_search_entries_2_entries_greater_than_0_3=rail.IfOperator(
            task_id='if_search_entries_2_entries_greater_than_0_3',
            test='''{{ result('assuranceagency_timeoffimport_reopenedtimesheets_search_entries_2','length') > 0 }}''',
            yes_task="foreach_document_5",
            no_task="log_to_sumo",
        )

        foreach_document_5=rail.ForEachOperator(
            task_id='foreach_document_5',
            items="{{ result('assuranceagency_timeoffimport_reopenedtimesheets_search_entries_2')}}",
            start_task = 'if_foreach_document_5_col2_ends_with_approved_6',
            end_task = 'foreach_document_5_end'
        )

        if_foreach_document_5_col2_ends_with_approved_6=rail.IfOperator(
            task_id='if_foreach_document_5_col2_ends_with_approved_6',
            test='''{{ result('foreach_document_5').properties.status | ends_with('approved') }}''',
            yes_task="force_approve_7",
            no_task="if_foreach_document_5_col2_ends_with_waiting_8",
        )

        force_approve_7=rail.RepliconServiceOperator(
            task_id='force_approve_7',
            endpoint="/services/TimesheetApprovalService1.svc/ForceApprove",
            data=lambda: {
                "timesheetUri": rail.result('foreach_document_5')['properties']['timesheeturi'],
                "unitOfWorkId": "ForceApprove_" + str(uuid.uuid4()),
                "comments": "ForceApproved by Replicon Integration"
            }
        )

        if_foreach_document_5_col2_ends_with_waiting_8=rail.IfOperator(
            task_id='if_foreach_document_5_col2_ends_with_waiting_8',
            test='''{{ result('foreach_document_5').properties.status | ends_with('waiting') }}''',
            yes_task="submit2_9",
            no_task="foreach_document_5_end",
        )

        submit2_9=rail.RepliconServiceOperator(
            task_id='submit2_9',
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data=lambda:{
                "timesheetUri": rail.result('foreach_document_5')['properties']['timesheeturi'],
                "unitOfWorkId": "Submitted_" + str(uuid.uuid4()),
                "comments": "Submitted by Replicon Integration"
            }
        )

        foreach_document_5_end=rail.EmptyOperator(
            task_id='foreach_document_5_end',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule = 'all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> assuranceagency_timeoffimport_reopenedtimesheets_search_entries_2
        assuranceagency_timeoffimport_reopenedtimesheets_search_entries_2 >> if_search_entries_2_entries_greater_than_0_3
        if_search_entries_2_entries_greater_than_0_3 >> rail.Label('Yes') >> foreach_document_5 >> if_foreach_document_5_col2_ends_with_approved_6
        if_foreach_document_5_col2_ends_with_approved_6 >> rail.Label('Yes')  >> force_approve_7 >> if_foreach_document_5_col2_ends_with_waiting_8
        if_foreach_document_5_col2_ends_with_approved_6 >> rail.Label('No') >> if_foreach_document_5_col2_ends_with_waiting_8
        if_foreach_document_5_col2_ends_with_waiting_8 >> rail.Label('Yes')  >> submit2_9 >> foreach_document_5_end
        if_foreach_document_5_col2_ends_with_waiting_8 >> rail.Label('No') >> foreach_document_5_end
        foreach_document_5 >> foreach_document_5_end >> log_to_sumo
        if_search_entries_2_entries_greater_than_0_3 >> rail.Label('No') >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
