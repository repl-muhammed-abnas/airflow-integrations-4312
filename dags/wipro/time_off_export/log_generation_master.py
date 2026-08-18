from datetime import timedelta, datetime as dt
from pendulum import datetime
import rail
from wipro.time_off_export.utils.custom_methods import get_dagruns_to_process, do_format_logs

def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.log_master_dag_id,
        description=f'Wipro Time OFF Export Log Generation Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.log_generation_dag_interval,
        max_active_runs=config.master_max_active_run,
        start_date=datetime(2022, 1, 1)
    ) as dag:

        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=get_dagruns_to_process,
            op_args=[config.lookup_log_timestamp_var,
                     config.lookup_log_timestamp_hours,
                     config.child_dag_id]
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_timeoff_export_logs',
            no_task='delete_this_dagrun'
        )

        get_timeoff_export_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_timeoff_export_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='create_log',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=do_format_logs
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id='create_csv_log',
            source="{{result('format_logs')}}",
            header=[
                'EmployeeID',
                'WorkitemID',
                'BookingStartdate',
                'BookingEnddate',
                'Event',
                'Status',
                'Response',
                'ecid'
            ],
            row=[
                "{{item.properties.employee_id}}",
                "{{item.properties.work_item_id}}",
                "{{item.properties.booking_start_date}}",
                "{{item.properties.booking_end_date}}",
                "{{item.properties.event}}",
                "{{item.properties.status}}",
                "{{item.properties.response}}",
                "{{item.ecid}}"
            ],
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda: rail.get_company_key() + '_Logs_Timeoff_Export_' + dt.now().strftime('%m%d%YT%H%M%S') + '.csv'
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('create_csv_log')}}",
            output_file_name="{{ result('get_log_file_name') }}",
            expires_in_seconds=7*24*60*60
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | TimeOff Export - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + current_time("%m-%d-%Y-%H-%M-%S") }}',
            html_content="templates/emails/email_import_complete.html",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            'Yes') >> get_timeoff_export_logs >> format_logs >> create_csv_log >> get_log_file_name >>\
                generate_downloadable_link >> send_import_complete_email

        is_log_dagruns_present >> rail.Label(
            "No") >> delete_this_dagrun

    return dag

rail.for_each_instance(create_log_airflow_dag)
