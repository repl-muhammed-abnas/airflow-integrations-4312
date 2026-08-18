import pendulum
from fujifilmdbtl.rehire_logic_after_1_year.utils import request_payload
import rail
from datetime import datetime, timedelta

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'Fujiflimdbtl | Rehire Logic | Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=1
    ) as dag:    
        

        create_log = rail.CreateLogOperator(
            task_id="create_log"
        )

        process_date_time =rail.PythonOperator(
            task_id='process_date_time',
            python_callable= lambda: pendulum.now(tz=config.time_zone).strftime("%B %d, %Y").replace(' 0', ' ')
        )

        get_report_details =rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name= config.report_name
        )

        check_report_is_present = rail.IfOperator(
            task_id="check_report_is_present",
            test='{{result("get_report_details") | is_truthy}}',
            yes_task="get_filter_uri",
            no_task="report_not_present"
        )

        report_not_present = rail.FailOperator(
            task_id="report_not_present",
            message="**Rehire Logic inititation Report** not present"
        )

        get_filter_uri = rail.PythonOperator(
            task_id='get_filter_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'UDFFilter_User11_Dateforrehirecalculation', 'uri', null)
        )

        
        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.get_run_report_payload
            )


        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_users_report_data',
            no_task= 'finish'
        )


        load_users_report_data = rail.LoadCSVFileOperator(
            task_id='load_users_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        users_report_data_collection = rail.CreateCollectionOperator(
            task_id='users_report_data_collection',
            source="{{ result('load_users_report_data') }}",
            columns={
                "User Name": "user_name",
                "Login Name": "login_name",
                "User Start Date": "user_start_date",
                "User Email": "user_email",
                "Date for rehire calculation": "date_for_rehire_calculation",
                "user uri": "useruri",
                "Full/Part Time": "fullparttime",
                "Regular/Temporary": "regular_temporary",
                "Adjusted Service Date": "adjusted_start_date"
            },
            name='inputdata'
        )

        query_valid_input_records = rail.QueryCollectionOperator(
            task_id='query_valid_input_records',
            query="""SELECT * from inputdata WHERE date_for_rehire_calculation=:process_date""",
            query_params={
                'process_date': "{{ result('process_date_time') }}"
            }
        )


        is_valid_records_present = rail.IfOperator(
            task_id="is_valid_records_present",
            test='{{result("query_valid_input_records") | length > 0}}',
            yes_task="trigger_timeoff_import_child",
            no_task="finish"
        )


        trigger_timeoff_import_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_import_child',
            retries=0,
            items='{{ result("query_valid_input_records") }}',
            trigger_dag_id=config.child_dag,
            conf = lambda item: {
                **item,
                "log": rail.result("create_log")
            },
            
        )

        wait_for_completion = rail.WaitForDagRunsSensor(
            task_id="wait_for_completion",
            dag_runs="{{result('trigger_timeoff_import_child')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('create_log') }}",
            header=['User', 'TimeOffType', 'Status', 'Details', 'Jobid'],
            row=[
                '{{ item.properties | attr_or_default("user", "") }}',
                '{{ item.properties | attr_or_default("timeofftype", "") }}',
                '{{ item.properties | attr_or_default("status", "") }}',
                '{{ item.properties | attr_or_default("details", "") }}',
                '{{ item.ecid }}'
            ]
        )


        filter_log_errors = rail.FilterLogEntriesOperator(
            task_id='filter_log_errors',
            log='{{ result("create_log") }}',
            severity='Error',
        )

        any_records_failed = rail.IfOperator(
            task_id='any_records_failed',
            test="{{ result('filter_log_errors', 'length') > 0 }}",
            yes_task='send_completion_error_mail'
        )

        send_completion_error_mail = rail.EmailOperator(
            task_id='send_completion_error_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Rehire Logic Initiation completed with errors',
            html_content="templates/email/completion_error_email.html",
            files=[
                ('RehireLogic_' + datetime.now().strftime("%m%d%Y%H%M%S")+'.csv', "{{ result('render_logs_csv') }}")]
        )

    
        finish=rail.EmptyOperator(
            task_id='finish',
        )

        
        create_log >> process_date_time >> get_report_details >> check_report_is_present >> rail.Label("Yes") >> get_filter_uri >> run_report_group_entry 
        run_report_group_exit >> is_report_failed >> rail.Label("Yes") >> fail_report_generation
        run_report_group_exit >> is_report_failed  >> rail.Label("No") >> report_has_data >> rail.Label("No") >> finish
        report_has_data >> rail.Label("Yes") >> load_users_report_data >> users_report_data_collection >> query_valid_input_records >> is_valid_records_present
        
        is_valid_records_present >> rail.Label("Yes") >> trigger_timeoff_import_child >> wait_for_completion >> render_logs_csv >> filter_log_errors >> any_records_failed

        any_records_failed >> rail.Label("Yes") >> send_completion_error_mail

        check_report_is_present >> rail.Label("No") >> report_not_present
        is_valid_records_present >> rail.Label("No") >> finish
    return dag

rail.for_each_instance(create_dag)

