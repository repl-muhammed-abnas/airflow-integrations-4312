import rail
from mammoet.payroll_export_spain.utils.custom_methods import create_json_payload_callable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.payroll_export_post_export_dag_id,
        description="Mammoet Time Export Daily Master",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        query_records_to_post = rail.QueryCollectionOperator(
            task_id="query_records_to_post",
            query="""SELECT frd.employee_id as employeeid ,
                        frd.pay_code_name as paycodename,
                        frd.pay_code_code as paycode,
                        REPLACE(SUBSTR(frd.timesheet_period, 1, instr(frd.timesheet_period, ' - ') -1), '/', '-') as startdate,
                        REPLACE(SUBSTR(frd.timesheet_period, instr(frd.timesheet_period, ' - ') + LENGTH(' - ')), '/', '-') as enddate,
                        frd.pay_code_hours as hours
                    FROM final_raw_data frd
                    WHERE CAST (frd.record_id as int) BETWEEN {{dag_run.conf.record_start_index}} AND {{dag_run.conf.record_end_index}}"""
        )

        create_json_payload = rail.PythonOperator(
            task_id="create_json_payload",
            python_callable=create_json_payload_callable,
            op_args=[query_records_to_post.task_id]
        )

        create_csv_file = rail.WriteCSVFileOperator(
            task_id = "create_csv_file",
            source="{{result('query_records_to_post')}}",
            header=[
                'employeeid',
                'paycodename',
                'paycode',
                'startdate',
                'enddate',
                'hours'
            ],
            row= [
                "{{ item.employeeid }}",
                "{{ item.paycodename }}",
                "{{ item.paycode }}",
                "{{ item.startdate }}",
                "{{ item.enddate }}",
                "{{ item.hours }}"
            ]
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            content="{{result('create_csv_file')}}",
            remote_filepath=config.payroll_export_upload_input_filepath +
            '/{{dag_run.conf.export_name}}_{{dag_run.conf.index}}' + '.csv'
        )

        post_to_target = rail.HTTPUploadFileOperator(
            task_id='post_to_target',
            content_type='application/json',
            endpoint="/Replicon/SuccessFactors",
            # to be changed to config.http_conn_id for UAT
            http_conn_id=config.http_conn_id,
            content="{{ result('create_json_payload')}}",
            retries=0,
            headers={
                "Authorization": "Bearer {{dag_run.conf.access_token_to_use}}",
                "PayRollName": "{{dag_run.conf.export_name}}_{{dag_run.conf.index}}"
            },
            extra_options={
                'verify': False
            }
        )

        is_post_to_endpoint_failed = rail.IfOperator(
            task_id="is_post_to_endpoint_failed",
            trigger_rule="all_done",
            test="{{ get_task_state('post_to_target') | lower == 'failed' }}",
            yes_task="upload_to_backup_path",
            no_task="is_run_failed"
        )

        is_run_failed = rail.IfOperator(
            task_id="is_run_failed",
            test="{{ get_error_message() | is_truthy}}",
            yes_task="fail_dag_run"
        )

        fail_dag_run = rail.FailOperator(
            task_id="fail_dag_run",
            message="{{get_error_message()}}"
        )

        upload_to_backup_path = rail.SFTPUploadFileOperator(
            task_id="upload_to_backup_path",
            content="{{result('create_json_payload')}}",
            remote_filepath=config.payroll_export_upload_backup_filepath +
            '/{{dag_run.conf.export_name}}_{{dag_run.conf.index}}' + '.json'
        )

        send_posting_failed_email = rail.EmailOperator(
            task_id='send_posting_failed_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Payroll Export - {{dag_run.conf.payroll_location_name}} - Failed while posting to API endpoint - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_post_to_api_failed.html",
            params={
                'sftp_upload_path': config.payroll_export_upload_backup_filepath
            }
        )

        query_records_to_post >> create_json_payload >> create_csv_file >> upload_export_data_to_sftp >> post_to_target\
            >> is_post_to_endpoint_failed >> rail.Label("No") >> is_run_failed >> rail.Label("Yes") >> fail_dag_run
        is_post_to_endpoint_failed >> rail.Label(
            "Yes") >> upload_to_backup_path >> send_posting_failed_email

    return dag


rail.for_each_instance(create_main_dag)
