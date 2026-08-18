"""
Cash Disbursement DAG for VP UKG Pro Journal Sync.
Fetches recent payroll from VP and triggers sync.
"""
from datetime import timedelta
import json
import rail
from vp_ukgpro_integration_v2.journal_sync.utils.python_callable_method import (
    build_cd_request_body,
    capture_cd_error,
    is_cd_already_exists_error
)
from vp_ukgpro_integration_v2.journal_sync.utils.config_helper import (
    extract_dynamic_config_from_dag_run
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """
    Create cash disbursement DAG to create journal payroll data.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_ukgpro_journal_sync_v2_cash_disbursement_{config.instance}',
        description=(
            'Create cash disbursement record in Vantagepoint'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_ukgpro', 'journal_sync', 'cash_disbursement'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        extract_dynamic_config = rail.PythonOperator(
            task_id='extract_dynamic_config',
            python_callable=lambda dag_run: extract_dynamic_config_from_dag_run(dag_run, config)
        )

        def read_artifact_data():
            """Read the cash disbursement data from artifact"""
            context = rail.get_current_context()
            artifact_name = context['dag_run'].conf['artifact_name']
            data = rail.read_artifact(artifact_name)
            return json.loads(data)

        read_cash_disbursement_data = rail.PythonOperator(
            task_id='read_cash_disbursement_data',
            python_callable=read_artifact_data
        )

        get_all_banks_from_vp = rail.VantagepointSettingsBankOperator(
            task_id='get_all_banks_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/BankCode/CFGBANKS',
            request_method='GET'
        )

        create_cash_disbursement = rail.VantagepointCustomOperator(
            task_id="create_cash_disbursement",
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/api/DataEntry/cdControl',
            request_method='POST',
            request_body=build_cd_request_body
        )

        is_batch_exist = rail.IfOperator(
            task_id='is_batch_exist',
            test=lambda: (
                rail.result('create_cash_disbursement')[0].get('Batch')
            ),
            yes_task='send_success_email',
            no_task='send_batch_not_found_email'
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to="{{ result('read_cash_disbursement_data')['notifierEmail'] }}",
            subject='Success | Vantagepoint Journal Sync',
            html_content="templates/success_mail.html"
        )

        send_batch_not_found_email = rail.EmailOperator(
            task_id='send_batch_not_found_email',
            to="{{ result('read_cash_disbursement_data')['notifierEmail'] }}",
            subject='Warning | Vantagepoint Journal Sync - Batch Not Created',
            html_content="templates/batch_not_found_mail.html"
        )

        check_cd_failure_reason = rail.IfOperator(
            task_id="check_cd_failure_reason",
            trigger_rule='one_failed',
            test=is_cd_already_exists_error,
            yes_task="send_already_exists_email",
            no_task="send_failure_email"
        )

        send_already_exists_email = rail.EmailOperator(
            task_id="send_already_exists_email",
            to="{{ result('read_cash_disbursement_data')['notifierEmail'] }}",
            subject="Error | Vantagepoint Journal Sync",
            html_content="templates/duplicate_mail.html"
        )

        send_failure_email = rail.EmailOperator(
            task_id="send_failure_email",
            to="{{ result('read_cash_disbursement_data')['notifierEmail'] }}",
            subject="Error | Vantagepoint Journal Sync",
            html_content="templates/failure_mail.html"
        )

        catch_cd_dag_error = rail.PythonOperator(
            task_id='catch_cd_dag_error',
            trigger_rule='all_done',
            python_callable=capture_cd_error,
            op_args=['{{ get_error_message() }}']
        )

        read_cash_disbursement_data >> get_all_banks_from_vp
        extract_dynamic_config >> read_cash_disbursement_data
        get_all_banks_from_vp >> create_cash_disbursement

        # Success path
        create_cash_disbursement >> is_batch_exist
        is_batch_exist >> rail.Label("Yes") >> send_success_email
        is_batch_exist >> rail.Label("No") >> send_batch_not_found_email

        # Failure path
        create_cash_disbursement >> check_cd_failure_reason
        (
            check_cd_failure_reason >> rail.Label("Yes")
            >> send_already_exists_email
        )
        check_cd_failure_reason >> rail.Label("No") >> send_failure_email

        send_success_email >> catch_cd_dag_error
        send_batch_not_found_email >> catch_cd_dag_error
        send_already_exists_email >> catch_cd_dag_error
        send_failure_email >> catch_cd_dag_error

        return dag


rail.for_each_instance(create_dag)
