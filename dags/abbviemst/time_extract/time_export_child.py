from datetime import timedelta
from pendulum import now
import rail
import csv
from airflow.models import Variable
from abbviemst.time_extract.utils import request_payload
from abbviemst.time_extract.utils import response_filter
from abbviemst.time_extract.tasks.time_export_task import time_data_export
from abbviemst.time_extract.utils.python_callable import get_final_extract_data_row


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.time_extract_delta_child_dagid,
        description='Abbviemst Time Extract child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=request_payload.create_download_batch_payload,
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id,
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('" + create_download_batch.task_id + "') }}"
            },
            data_handler=lambda response: response['downloadUrl'],

        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('get_download_url') }}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('download_export') }}",
        )

        create_raw_timeexport_data_collection = rail.CreateCollectionOperator(
            task_id="create_raw_timeexport_data_collection",
            source="{{result('load_export')}}",
            name="raw_timeexport_data",
            columns={
                'Dept_code': 'dept_code',
                'Abb_emp_num': 'employee_id',
                'Benef_code': 'beneficiary_code',
                'Root_proj_code': 'root_project_code',
                'Protocol': 'protocol',
                'Reg_hrs': 'reg_hours',
                'Entry Date': 'entry_date',
                'Compass?': 'compass'
            }
        )

        has_any_timeexport_data = rail.IfOperator(
            task_id="has_any_timeexport_data",
            test="{{result('create_raw_timeexport_data_collection', 'length') > 0 }}",
            yes_task="process_time_export",
            no_task="catch_errors"
        )

        process_time_export = rail.EmptyOperator(
            task_id = "process_time_export"
        )

        time_export_start, mark_as_completed = time_data_export(
            group_id="time_export",
            generate_request=request_payload.get_create_time_data_export_batch_payload,
            get_export_name="{{ dag_run.conf.export_name }}",
            retries=0
        )

        query_blank_employee_id_records = rail.QueryCollectionOperator(
            task_id="query_blank_employee_id_records",
            query="""SELECT * FROM raw_timeexport_data rtd WHERE NULLIF(rtd.employee_id, '') IS NULL"""
        )

        query_employee_id_present_with_compass_y_records = rail.QueryCollectionOperator(
            task_id="query_employee_id_present_with_compass_y_records",
            query="""SELECT * FROM raw_timeexport_data rtd WHERE NULLIF(rtd.employee_id, '') IS NOT NULL AND LOWER(compass) = 'y'""",
            name = "validated_records"
        )

        is_employee_id_with_compass_y_present = rail.IfOperator(
            task_id="is_employee_id_with_compass_y_present",
            test="{{ result('query_employee_id_present_with_compass_y_records', 'length') > 0}}",
            yes_task="query_records_to_post",
            no_task="catch_errors"
        )

        query_records_to_post = rail.QueryCollectionOperator(
            task_id = "query_records_to_post",
            query="""SELECT * FROM validated_records
                """
        )

        modified_export_data = rail.DataAdaptorOperator(
            task_id='modified_export_data',
            source="{{result('query_records_to_post')}}",
            columns=['dept_code', 'employee_id', 'beneficiary_code', 'root_project_code',
                     'protocol', 'reg_hours', 'entry_date', 'compass', 'period_month', 'year'],
            data=lambda row: response_filter.translate_rows(row)
        )

        create_final_data_collection = rail.CreateCollectionOperator(
            task_id='create_final_data_collection',
            name='final_data',
            source="{{ result('modified_export_data') }}"
        )

        valid_extracted_data = rail.QueryCollectionOperator(
            task_id='valid_extracted_data',
            query="""SELECT dept_code,employee_id,beneficiary_code,root_project_code,protocol,SUM(reg_hours) as reg_hours,period_month,year FROM final_data GROUP BY employee_id, year"""
        )

        render_final_extract_data = rail.WriteCSVFileOperator(
            task_id='render_final_extract_data',
            source="{{ result('valid_extracted_data') }}",
            delimiter = "\t",
            header=None,
            row=get_final_extract_data_row,
            quoting=csv.QUOTE_ALL
        )
        
        upload_export_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_export_file_to_sftp',
            content="{{ result('render_final_extract_data') }}",
            remote_filepath=config.upload_filepath + '/{{ dag_run.conf.file_name }}.txt'
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export - Completed Successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_export_success.html",
            params={
                'sftp_upload_path': config.upload_filepath
            }
        )

        catch_errors = rail.PythonOperator(
            task_id = "catch_errors",
            trigger_rule = "one_failed",
            python_callable=lambda dag_run: {
                "dag_id": dag.dag_id,
                "run_id": dag_run.run_id,
                "error_message": rail.render_template("{{get_error_message()}}")
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )


        create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url >> download_export >> \
            load_export >> create_raw_timeexport_data_collection >> has_any_timeexport_data

        has_any_timeexport_data >> rail.Label("Yes") >> process_time_export >> time_export_start
        has_any_timeexport_data >> rail.Label("No") >> catch_errors

        mark_as_completed >> query_blank_employee_id_records >> query_employee_id_present_with_compass_y_records >> \
            is_employee_id_with_compass_y_present

        is_employee_id_with_compass_y_present >> rail.Label("Yes") >> query_records_to_post >> modified_export_data >> \
            create_final_data_collection >> valid_extracted_data >> render_final_extract_data >> upload_export_file_to_sftp >> \
                send_success_email >> catch_errors
        
        is_employee_id_with_compass_y_present >> rail.Label("No") >> catch_errors

        catch_errors >> finish

    return dag


rail.for_each_instance(create_main_dag)
