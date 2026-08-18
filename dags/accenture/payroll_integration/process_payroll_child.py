from datetime import timedelta
from airflow.models import Variable
import rail
from accenture.payroll_integration.utils.python_callable_methods import (
    get_compose_payroll_detail_row,
    get_file_name,
    export_started_payload,
    export_complete_payload,
    build_error_payload,
)


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_payroll_child_dag_id,
        description='Accenture ADP GV Payroll Export - child',
        integration_type='generic',
        company_key=config.company_key,
        replicon_conn_id=None,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dag_run_conf')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true'
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='mark_export_started',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='mark_export_started',
            end_task='build_error_status',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # --- Mark export started on the PayrollFile hub record ---
        mark_export_started = rail.VantagepointHubDataTablesOperator(
            task_id='mark_export_started',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            request_method='PUT',
            endpoint='{{ "/" + "' + config.vantagepoint_hub + '" + "/" + dag_run.conf.PayrollFileID }}',
            request_body=export_started_payload,
        )

        # --- Fetch header, details, footer from UDIC grids ---
        fetch_file_header = rail.VantagepointHubDataTablesOperator(
            task_id='fetch_file_header',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            request_method='GET',
            hub=config.vantagepoint_hub,
            hub_key='{{ dag_run.conf.PayrollFileID }}',
            associated_table=config.file_header_table,
        )

        fetch_file_details = rail.VantagepointHubDataTablesOperator(
            task_id='fetch_file_details',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            request_method='GET',
            hub=config.vantagepoint_hub,
            hub_key='{{ dag_run.conf.PayrollFileID }}',
            associated_table=config.file_details_table,
        )

        fetch_file_footer = rail.VantagepointHubDataTablesOperator(
            task_id='fetch_file_footer',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            request_method='GET',
            hub=config.vantagepoint_hub,
            hub_key='{{ dag_run.conf.PayrollFileID }}',
            associated_table=config.file_footer_table,
        )

        # --- Resolve output file name from webhook conf ---
        get_output_file_name = rail.PythonOperator(
            task_id='get_output_file_name',
            python_callable=get_file_name,
        )

        # --- Compose detail rows into CSV-style collection (applies decimal normalization) ---
        compose_payroll_details_csv = rail.WriteCSVFileOperator(
            task_id='compose_payroll_details_csv',
            source="{{ result('fetch_file_details') | tojson }}",
            header=[
                "RECTY", "CLIID", "INTCA", "ORDNO", "IOPER", "INFTY", "SUBTY", "BEGDA",
                "ENDDA", "OBJPS", "SPRPS", "SEQNR", "EXTRA", "LGART", "STDAZ", "BEGUZ",
                "ENDUZ", "BETRG", "WAERS", "ANZHL", "ZEINH", "VTKEN", "BWGRL", "AUFKZ",
                "ENDOF", "UFLD1", "UFLD2", "UFLD3", "KEYPR", "TRFGR", "TRFST", "PRAKN",
                "PRAKZ", "OTYPE", "PLANS", "VERSL", "EXBEL", "WTART", "TDLANGU", "TDSUBLA",
                "TDTYPE",
            ],
            row=get_compose_payroll_detail_row,
        )

        # --- Render G2 flat file from header/details/footer via template ---
        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file=config.payroll_template_file,
            dataset="{{ result('compose_payroll_details_csv') }}",
        )

        # --- PGP encrypt the file ---
        pgp_encrypt_file = rail.PGPEncryptionOperator(
            task_id='pgp_encrypt_file',
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id,
        )

        # --- Upload encrypted file to ADP SFTP ---
        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_sftp',
            sftp_conn_id=config.sftp_conn_id,
            content="{{ result('pgp_encrypt_file') }}",
            remote_filepath=config.sftp_remote_dir + '/'
            + "{{ result('get_output_file_name') }}.pgp",
        )

        # --- Mark export complete on the PayrollFile hub record ---
        mark_export_complete = rail.VantagepointHubDataTablesOperator(
            task_id='mark_export_complete',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            request_method='PUT',
            endpoint='{{ "/" + "' + config.vantagepoint_hub + '" + "/" + dag_run.conf.PayrollFileID }}',
            request_body=export_complete_payload,
        )

        # --- Error handling: only runs on failure ---
        build_error_status = rail.PythonOperator(
            task_id='build_error_status',
            trigger_rule='one_failed',
            python_callable=build_error_payload,
            op_args=['{{ get_error_message() }}'],
        )

        update_error_status = rail.VantagepointHubDataTablesOperator(
            task_id='update_error_status',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            request_method='PUT',
            endpoint='{{ "/" + "' + config.vantagepoint_hub + '" + "/" + dag_run.conf.PayrollFileID }}',
            request_body=lambda: rail.result('build_error_status'),
        )

        # --- Task dependencies ---
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> build_error_status
        can_run_batch_task >> rail.Label('No') >> mark_export_started

        # Success path
        mark_export_started >> get_output_file_name >> fetch_file_header >> fetch_file_details >> fetch_file_footer
        fetch_file_footer >> compose_payroll_details_csv >> create_document
        create_document >> pgp_encrypt_file >> upload_to_sftp >> mark_export_complete

        # Error path — only runs when an upstream task fails
        mark_export_complete >> build_error_status >> update_error_status

        return dag


rail.for_each_instance(create_dag)
