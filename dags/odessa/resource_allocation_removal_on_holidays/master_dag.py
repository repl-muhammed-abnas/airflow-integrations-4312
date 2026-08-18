from datetime import timedelta
import rail
from odessa.resource_allocation_removal_on_holidays.utils import request_payload
from odessa.resource_allocation_removal_on_holidays.utils.python_callable import get_process_each_user_payload_dag_ids,do_format_logs
from airflow.models import Variable
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'odessa_resource_allocation_removal_on_holidays_master_{config.instance}',
        description=f'Odessa_resource_allocation_removal_on_holidays {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_data_report_name,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report_user_data',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document="{{result('run_report_user_data.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_data_list = rail.CreateCollectionOperator(
            task_id='create_user_data_list',
            source="{{ result('load_report_data')}}",
            name="userdata",
            columns={
                'User Email': 'usereemail',
                'Login Name': 'loginname',
                'useruri': 'useruri',
                'Holiday Calendar': 'holidaycalendar',
                'holidaycalendaruri': 'holidaycalendaruri',
            }
        )

        query_user_data_timesheet = rail.QueryCollectionOperator(
            task_id='query_user_data_timesheet',
            query="""SELECT DISTINCT holidaycalendar,holidaycalendaruri FROM userdata WHERE NULLIF('holidaycalendar', '') IS NOT NULL
            AND holidaycalendar IS NOT ''""",
        )

        has_holidays = rail.IfOperator(
            task_id='has_holidays',
            test='{{ result("query_user_data_timesheet") | length > 0 }}',
            yes_task='declare_list',
            no_task='finish',
        )

        declare_list = rail.SetVariableOperator(
            task_id='declare_list',
            append=False,
            name='holidaylist',
            value=[]
        )

        for_each_item_in_querylist_do = rail.ForEachOperator(
            task_id='for_each_item_in_querylist_do',
            items="{{result('query_user_data_timesheet')}}",
            start_task="get_holidays_in_date_range",
            end_task="for_each_item_in_querylist_do_end"
        )

        get_holidays_in_date_range = rail.RepliconServiceOperator(
            task_id='get_holidays_in_date_range',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidaysInDateRange",
            data=request_payload.get_holidays_in_data_range_payload

        )

        for_each_item_in_holidays_date_range = rail.ForEachOperator(
            task_id='for_each_item_in_holidays_date_range',
            items= lambda: rail.result('get_holidays_in_date_range'),
            start_task="insert_item_to_list",
            end_task="for_each_item_in_holidays_date_range_end"
        )

        insert_item_to_list = rail.SetVariableOperator(
            task_id='insert_item_to_list',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "holidayname": "{{ result('for_each_item_in_holidays_date_range').name }}",
                # pylint: disable=line-too-long
                "holidaydate": "{{result('for_each_item_in_holidays_date_range').date.day}}-{{result('for_each_item_in_holidays_date_range').date.month }}-{{ result('for_each_item_in_holidays_date_range').date.year }}",
                "holidayday": "{{ result('for_each_item_in_holidays_date_range').date.day }}",
                "holidaymonth": "{{ result('for_each_item_in_holidays_date_range').date.month }}",
                "holidayyear": "{{ result('for_each_item_in_holidays_date_range').date.year }}",
                "holidayuri": "{{ result('for_each_item_in_holidays_date_range').uri }}",
                "holidaycalendarname": "{{ result('for_each_item_in_querylist_do').holidaycalendar }}",
                "holidaycalendaruri": "{{ result('for_each_item_in_querylist_do').holidaycalendaruri }}"
            }
        )
        for_each_item_in_holidays_date_range_end = rail.EmptyOperator(
            task_id='for_each_item_in_holidays_date_range_end'
        )

        for_each_item_in_querylist_do_end = rail.EmptyOperator(
            task_id='for_each_item_in_querylist_do_end'
        )

        query_to_get_users = rail.QueryCollectionOperator(
            name= "finaluserdata",
            task_id='query_to_get_users',
            query="""SELECT DISTINCT loginname,useruri,holidaycalendaruri FROM userdata WHERE NULLIF('holidaycalendar', '') IS NOT NULL
            AND holidaycalendar IS NOT ''""",
        )

        has_users_to_process = rail.IfOperator(
            task_id='has_users_to_process',
            test='{{ result("query_to_get_users") | length > 0 }}',
            yes_task='query_final_user_data',
            no_task='finish',
        )

        query_final_user_data = rail.QueryCollectionOperator(
            task_id='query_final_user_data',
            query="""SELECT * FROM finaluserdata""",
        )

        process_each_user = rail.trigger_parallel_dagrun(
            task_id='process_each_user',
            items="{{result('query_final_user_data')}}",
            parallel_count=50,
            trigger_dag_id=f'odessa_remove_allocation_process_each_user_child_{config.instance}',
            conf=lambda item: {
                "loginname": item['loginname'],
                "useruri": item['useruri'],
                "holidaycalendaruri": item['holidaycalendaruri'],
                "list_item": rail.result("insert_item_to_list")
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=get_process_each_user_payload_dag_ids,
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_users_dag_ids") }}',
            dagrun_task_id='create_child_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )
        
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )

        has_logs = rail.IfOperator(
            task_id='has_logs',
            test='{{ result("format_logs") | length > 0 }}',
            yes_task='render_logs_csv',
            no_task='finish',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('format_logs') | to_json }}",
            header=['loginname', 'holidaycalendar', 'holidayname',
                    'holidaydate', 'status'],
            row=[
                '{{ item.loginname }}',
                '{{ item.holidaycalendar }}',
                '{{ item.holidayname }}',
                '{{ item.holidaydate }}',
                '{{ item.status }}'
            ]
        )

        upload_reference_s3_file = rail.S3UploadFileOperator(
            task_id='upload_reference_s3_file',
            aws_conn_id=config.aws_conn_id,
            source="{{ result('render_logs_csv') }}",
            bucket_name=lambda: Variable.get(config.bucket_name),
            key_name=config.log_file_path +
            'removeresourceallocation_{{current_time("%Y_%m_%d")}}'+'.csv',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='removeresourceallocation_ecid.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key()}} | Activity to remove project resource allocation on holidays is completed ',
            html_content="templates/email/complete_mail.html",
            params={
                'log_filepath': config.log_file_path,
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        get_report_details >> run_my_report_entry
        run_my_report_exit >> load_report_data >> create_user_data_list >> query_user_data_timesheet
        query_user_data_timesheet >> has_holidays
        has_holidays >> rail.Label("Yes") >> declare_list
        has_holidays >> rail.Label("No") >> finish >> log_to_sumo
        
        declare_list >> for_each_item_in_querylist_do >> get_holidays_in_date_range >>for_each_item_in_holidays_date_range
        for_each_item_in_holidays_date_range >> insert_item_to_list >> for_each_item_in_holidays_date_range_end
        for_each_item_in_holidays_date_range >> for_each_item_in_holidays_date_range_end >> for_each_item_in_querylist_do_end
        for_each_item_in_querylist_do >> for_each_item_in_querylist_do_end >> query_to_get_users
        query_to_get_users >> has_users_to_process
        
        has_users_to_process >> rail.Label("Yes") >> query_final_user_data >> process_each_user
        has_users_to_process >> rail.Label("No") >> finish >> log_to_sumo 
        
        process_each_user >> get_process_users_dag_ids >> gather_user_logs >> format_logs >> has_logs

        has_logs >> rail.Label("Yes") >> render_logs_csv
        has_logs >> rail.Label("No") >> finish >> log_to_sumo
        
        render_logs_csv >> upload_reference_s3_file >> generate_download_link
        generate_download_link >> send_import_complete_email >> finish    

    return dag

rail.for_each_instance(create_dag)
