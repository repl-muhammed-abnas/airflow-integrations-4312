from datetime import timedelta
import pendulum
import rail
from rail.lib.ecid import get_dagrun_ecid
from macquariegroup.clientimport.utils import python_callable_method, custom_methods

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'macquarie_ondemand_initiate_clientimport_{config.instance}',
        description=f'Macquarie On Demand | Initiate Client Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        log_current_time = rail.PythonOperator(
            task_id='log_current_time',
            python_callable=lambda: pendulum.now(
                config.timezone).strftime('%Y-%m-%d-%H%M%S')
        )

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id='list_sftp_files',
            paths=[config.input_filepath]
        )

        csv_files_list = rail.PythonOperator(
            task_id='csv_files_list',
            python_callable=python_callable_method.get_files_list,
            op_args=['list_sftp_files', config.input_filepath]
        )

        input_file_validations = rail.PythonOperator(
            task_id='input_file_validations',
            python_callable=python_callable_method.do_file_validations,
            op_args=['csv_files_list']
        )

        has_valid_files = rail.IfOperator(
            task_id="has_valid_files",
            test=lambda: rail.result('input_file_validations') == 'valid',
            yes_task='trigger_macquarie_process_clientimport_childasync',
            no_task='send_mail_for_invalid_files'
        )

        send_mail_for_invalid_files = rail.EmailOperator(
            task_id='send_mail_for_invalid_files',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Client Import - Import not processed {{ result('log_current_time') }}''',
            html_content='templates/email/invalid_files.html'
        )

        trigger_macquarie_process_clientimport_childasync = rail.TriggerDagRunOperator(
            task_id='trigger_macquarie_process_clientimport_childasync',
            retries=0,
            trigger_dag_id=f'macquarie_process_clientimport_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'time': rail.result("log_current_time"),
                'parentjobid': get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        wait_for_completion_trigger_macquarie_process_clientimport_childasync = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_macquarie_process_clientimport_childasync',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_macquarie_process_clientimport_childasync") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message=config.error_template,
            properties={
                'details': {config.error_template}
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=custom_methods.get_extra_info
        )

        log_current_time >> list_sftp_files >> csv_files_list >> input_file_validations >> has_valid_files
        has_valid_files >> rail.Label(
            'Yes') >> trigger_macquarie_process_clientimport_childasync >> \
            wait_for_completion_trigger_macquarie_process_clientimport_childasync >> finish
        has_valid_files >> rail.Label(
            'No') >> send_mail_for_invalid_files >> finish

        finish >> catch_and_log_errors >> log_dagrun_to_sumo
    return dag


rail.for_each_instance(create_dag)
