"""
Global Error Log DAG — generates error CSV and sends notification email.

Triggered by any entity sync child DAG on failure via TriggerDagRunOperator.
Receives error details in dag_run.conf:
  - entity_type        : the entity being synced (Lead, Opportunity, …)
  - entity_guid        : the D365 GUID of the entity that failed
  - entity_name        : name of the entity (from D365, may be empty if fetch failed)
  - action             : the action attempted (Sync)
  - status             : Error
  - details            : human-readable error description
  - jobid              : execution context ID of the child DAG run

Flow:
  view_conf (independent)
  create_log >> write_error >> filter_errors
    >> write_error_csv >> generate_download_link
    >> send_error_email
"""
# pylint: disable=line-too-long,pointless-statement,expression-not-assigned
from datetime import timedelta

import rail


def create_dag(config):
    """Create the error log DAG for a given instance."""

    with rail.create_airflow_dag(
        dag_id=config.error_log_dag_id,
        description='Generate error log CSV and send notification email',
        integration_type='generic',
        company_key=config.company_key,
        schedule_interval=None,
        max_active_runs=5,
        tags=['pim_d365', 'error_log', 'notification'],
        default_args={
            'execution_timeout': timedelta(minutes=10),
        },
    ) as dag:

        view_conf = rail.ViewDagRunConfOperator(
            task_id='view_conf'
        )

        create_log = rail.CreateLogOperator(task_id='create_log')

        write_error = rail.WriteLogOperator(
            task_id='write_error',
            log="{{ result('create_log') }}",
            severity='Error',
            message='{{ dag_run.conf.details }}',
            properties={
                'entity_type': '{{ dag_run.conf.entity_type }}',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': '{{ dag_run.conf.get("entity_name", "") }}',
                'action': '{{ dag_run.conf.action }}',
                'status': '{{ dag_run.conf.status }}',
                'details': '{{ dag_run.conf.details }}',
                'jobid': '{{ dag_run.conf.jobid }}',
            },
        )

        filter_errors = rail.FilterLogEntriesOperator(
            task_id='filter_errors',
            log="{{ result('create_log') }}",
            severity='Error',
        )

        write_error_csv = rail.WriteCSVFileOperator(
            task_id='write_error_csv',
            source="{{ result('filter_errors') }}",
            header=[
                'Entity Type',
                'Entity GUID',
                'Entity Name',
                'Action',
                'Status',
                'Details',
                'Jobid',
                'Timestamp',
            ],
            row=[
                "{{ item.properties | attr_or_default('entity_type', '') }}",
                "{{ item.properties | attr_or_default('entity_guid', '') }}",
                "{{ item.properties | attr_or_default('entity_name', '') }}",
                "{{ item.properties | attr_or_default('action', '') }}",
                "{{ item.properties | attr_or_default('status', '') }}",
                "{{ item.properties | attr_or_default('details', '') }}",
                "{{ item.properties | attr_or_default('jobid', '') }}",
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
            ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_error_csv') }}",
            output_file_name=(
                'SyncError_{{ dag_run.conf.entity_type }}'
                '_{{ ts_nodash }}.csv'
            ),
            expires_in_seconds=604800,
        )

        send_error_email = rail.EmailOperator(
            task_id='send_error_email',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject=(
                f'{config.company_key}'
                ' | {{ dag_run.conf.entity_type }}'
                ' Sync completed with errors'
                ' - {{ current_time_in_specified_tz() }}'
            ),
            html_content="templates/emails/sync_error_mail.html",
        )

        (
            create_log >> write_error >> filter_errors
            >> write_error_csv >> generate_download_link
            >> send_error_email
        )

        return dag


rail.for_each_instance(create_dag)
