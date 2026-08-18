import json
import itertools
from datetime import datetime, timedelta
from airflow.models import Variable
import rail
from tokamakenergy.timeoff_import.utils import request_payload, python_callable, response_filter

DATE_FORMAT = "%Y-%m-%d"

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master,
        description='Tokamak Timeoff Import Automation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_dag_interval,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        get_last_run_date = rail.PythonOperator(
            task_id='get_last_run_date',
            python_callable=lambda: python_callable.do_get_last_run_date(config)
        )

        def get_endpoint_detail():
            daterange_start = int(Variable.get(config.timeoff_start_daterange_var_name, default_var='31'))
            daterange_end = int(Variable.get(config.timeoff_end_daterange_var_name, default_var='365'))
            current_time = datetime.now()
            start = current_time - timedelta(days=daterange_start)
            end = current_time + timedelta(days=daterange_end)
            endpoint = f"/time_off/requests/?start={start.strftime(DATE_FORMAT)}&end={end.strftime(DATE_FORMAT)}"
            return endpoint

        get_endpoint = rail.PythonOperator(
            task_id='get_endpoint',
            python_callable=get_endpoint_detail
        )

        can_use_conf_payload = rail.IfOperator(
            task_id='can_use_conf_payload',
            test=lambda: Variable.get(
                config.can_use_conf_payload_var_name, default_var='false').lower() == 'true',
            yes_task='get_conf_payload',
            no_task='get_users_timeoff'
        )

        get_conf_payload = rail.PythonOperator(
            task_id='get_conf_payload',
            python_callable=lambda: json.dumps(rail.get_dag_run_conf())
        )

        #https://api.bamboohr.com/api/gateway.php/tokamakenergytest/v1/time_off/requests/?start=2023-08-26&end=2024-08-26
        get_users_timeoff = rail.BambooHROperator(
            task_id='get_users_timeoff',
            company_domain=config.company_domain,
            request_method='GET',
            endpoint="{{result('get_endpoint')}}",
            bamboohr_conn_id=config.bamboohr_conn_id
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        timeoff_data = rail.PythonOperator(
            task_id= "timeoff_data",
            python_callable= python_callable.get_timeoff_data,
            # show_return_value_in_logs= False
        )

        has_timeoff_data = rail.IfOperator(
            task_id='has_timeoff_data',
            test='''{{ result("timeoff_data") | length > 0 }}''',
            yes_task='get_booking_id_oef_value',
            no_task='finish'
        )

        get_booking_id_oef_value = rail.RepliconServiceOperator(
            task_id='get_booking_id_oef_value',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data=request_payload.get_booking_id_oef_value_payload,
            data_handler=response_filter.get_booking_id_oef_value
        )

        get_all_time_off_types_uris = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_uris',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_time_off_type_uris
        )

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_details',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=request_payload.get_timeoff_details_payload,
            data_handler=response_filter.get_filtered_timeoff_details
        )

        process_timeoff = rail.trigger_parallel_dagrun(
            task_id='process_timeoff',
            items="{{ result('timeoff_data') | to_json }}",
            parallel_count=config.parallel_dagrun_count_process_distict_projects,
            trigger_dag_id=config.process_timeoff_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: request_payload.get_conf(config.TIMEOFF_MAPPER_NAMES, item)
        )

        get_process_timeoff_dag_ids =rail.PythonOperator(
            task_id= 'get_process_timeoff_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_timeoff_{x+1}') if rail.result(
                    f'process_timeoff_{x+1}') else []), range(config.parallel_dagrun_count_process_distict_projects))))),
            show_return_value_in_logs= False
        )

        gather_timeoff_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_logs',
            dag_runs='{{ result("get_process_timeoff_dag_ids") }}',
            dagrun_task_id='create_process_timeoff_log',
            execution_timeout=timedelta(
                hours=config.gather_timeoff_logs_timeout_hours),
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=lambda: python_callable.do_format_logs(
                main_log=rail.result('create_log'),
                child_log=rail.result('gather_timeoff_logs'),
            )
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                "Employee ID",
                "Booking ID",
                "Start Date",
                "End Date",
                "Status",
                "Details",
                "Job id",
            ],
            row=[
                "{{ item.employee_id }}",
                "{{ item.booking_id }}",
                "{{ item.start_date }}",
                "{{ item.end_date }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.ecid }}",
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{get_company_key()}}_timeoff_sync_logs_{{current_time_in_specified_tz(fmt="%Y_%m_%d")}}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') > 0 -%}\
                "+config.alert_email+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() }} | Replicon timeoff sync{{" "}} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully - \
                {%- endif -%} \
                {{ current_time_in_specified_tz(fmt="%Y_%m_%d") }}',
            html_content="templates/emails/import_complete.html"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
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

        get_last_run_date >> get_endpoint >> can_use_conf_payload >> rail.Label("Yes") >> get_conf_payload >> create_log
        can_use_conf_payload >> rail.Label("No") >> get_users_timeoff >> create_log
        create_log >> timeoff_data >> has_timeoff_data >> rail.Label("Yes") >> get_booking_id_oef_value
        has_timeoff_data >> rail.Label("No") >> finish
        get_booking_id_oef_value >> get_all_time_off_types_uris >> get_timeoff_details
        get_timeoff_details >> process_timeoff >> get_process_timeoff_dag_ids >> gather_timeoff_logs >> \
        format_logs >> render_logs_csv >> generate_download_link >> send_completion_email >> finish
        finish >> log_to_sumo >> can_fail_dag >> rail.Label(
            'Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
