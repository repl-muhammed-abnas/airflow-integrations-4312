from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.psa_planned_leave.utils import response_filter
from dxctechnology.psa_planned_leave.utils import request_payload
from dxctechnology.psa_planned_leave.utils import python_callable_method

from dxctechnology.psa_planned_leave.tasks.generate_report_batch import report_batch


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_psa_planned_leave_master_{config.instance}',
        description='DXC_PSA_PLanned_Leave Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.schedule,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        get_export_data = rail.PythonOperator(
            task_id='get_export_data',
            python_callable=python_callable_method.get_export_data
        )

        get_all_psa_cost_centers = rail.RepliconServiceOperator(
            task_id="get_all_psa_cost_centers",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_all_psa_cost_centers,
            data_handler=response_filter.get_all_psa_cost_centers
        )

        get_australia_division_uris = rail.RepliconServiceOperator(
            task_id="get_australia_division_uris",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_australia_division_uris,
            data_handler=lambda response: response_filter.get_australia_division_uris(
                response, config.australia_company_codes)
        )

        load_report, fail_invalid_report_columns, empty_export_mail, create_report_collection = report_batch(
            config)

        query_unique_timeoff_uris = rail.QueryCollectionOperator(
            task_id='query_unique_timeoff_uris',
            name='uniquetimeoffuris',
            query="""SELECT DISTINCT TimeOffTypeUri FROM reportdatacollection"""
        )

        get_all_paycodes = rail.RepliconServiceOperator(
            task_id="get_all_paycodes",
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails",
            data=request_payload.get_all_paycodes,
            data_handler=response_filter.get_all_paycodes
        )

        get_paycode_codes = rail.RepliconServiceOperator(
            task_id="get_paycode_codes",
            endpoint="/services/PayCodeService1.svc/GetAllPayCodes",
            data_handler=response_filter.get_paycodes_codes
        )

        create_export_file = rail.WriteCSVFileOperator(
            task_id='create_export_file',
            source=lambda: rail.result('create_report_collection'),
            thread_pool_size=config.thread_pool_count,
            header=['EmployeeNumber', 'AbsenceType','LeaveDate', 'LeaveHours'],
            row=request_payload.translate_row,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
        )

        encrypt_export_file = rail.PGPEncryptionOperator(
            task_id="encrypt_export_file",
            source="{{ result('create_export_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        send_export_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='send_export_file_to_sftp',
            content="{{ result('encrypt_export_file') }}",
            remote_filepath=config.sftp_upload_path + '/' +
            "{{result('get_export_data').export_name}}" + '.csv.pgp',
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | PSA Planned Leave Export - Completed Successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/success_export.html",
            params={
                'sftp_upload_path': config.sftp_upload_path
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule="all_done",
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda: {
                "export_name": rail.result('get_export_data')['export_name'],
                "report_start_date":  rail.result('get_export_data')['report_start_date'],
                "no_of_records": rail.result('create_export_data_collection', key='length')
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        get_export_data >> get_all_psa_cost_centers >> get_australia_division_uris >> load_report
        create_report_collection >> query_unique_timeoff_uris >> get_all_paycodes >> get_paycode_codes >> create_export_file
        create_export_file >> encrypt_export_file >> send_export_file_to_sftp >> send_success_email >> log_to_sumo
        log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun
        empty_export_mail >> log_to_sumo
        fail_invalid_report_columns >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
