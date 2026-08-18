from datetime import timedelta, datetime
import rail
from fujifilmdimatixg3.pto_timeoff_balance_update.utils import python_callable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdimatixg3_pto_timeoff_balance_update_master_{config.instance}',
        description=f'fujifilmdimatixg3_pto_timeoff_balance_update_master_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=15),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task="fail_bad_file_format",
        )

        fail_bad_file_format = rail.FailOperator(
            task_id = "fail_bad_file_format",
            message= "File format is not in CSV"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/{{ result('new_file_sensor') | file_name }}"
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id = "load_csv",
            document="{{ result('download_file') }}"
        )

        get_paid_time_off_uri = rail.RepliconServiceOperator(
            task_id = "get_paid_time_off_uri",
            endpoint = "services/TimeOffService1.svc/GetPageOfTimeOffTypesFilteredBySearch",
            data = python_callable.get_paid_timeoff_uri,
            data_handler= lambda response : rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Paid Time Off' , 'uri' , '')
        )

        logger_list = rail.CreateLogOperator(
            task_id = "logger_list",
        )

        get_starting_balance_set_to_script_uri = rail.RepliconServiceOperator(
            task_id = "get_starting_balance_set_to_script_uri",
            endpoint = "services/TimeOffBalanceEventScriptAdministrationService1.svc/GetActiveScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        process_each_empid = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_empid",
            items= "{{ result('load_csv')}}",
            trigger_dag_id= f'fujifilmdimatixg3_pto_timeoff_balance_update_child_{config.instance}',
            conf= {
                "employeeid" : "{{item.Employeeid}}",
                "effectivedate" : "{{item.Effectivedate}}",
                "balance" : "{{item.Balance}}",
                "pto_uri" : "{{ result('get_paid_time_off_uri') }}",
                "logger" : "{{ result('logger_list') }}",
                "starting_balance_set_to_script_uri": "{{ result('get_starting_balance_set_to_script_uri') }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_each_empid = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_empid',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_empid") }}'
        )

        write_log_file = rail.WriteCSVFileOperator(
            task_id="write_log_file",
            source=lambda: rail.result('logger_list'),
            header=['employeeid', 'status', 'ecid'],
            row=lambda item: [
                item['properties']['EmployeeID'],
                item['properties']['status'],
                item['ecid']
            ]
        )

        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            severity='Error',
        )

        any_records_failed = rail.IfOperator(
            task_id='any_records_failed',
            test="{{ result('filter_master_log', 'length') > 0 }}",
            yes_task='send_completion_error_mail',
            no_task='send_completion_mail'
        )

        download_fromaddress_file = rail.SFTPDownloadFileOperator(
            task_id='download_fromaddress_file',
            remote_filepath=config.fromaddress_filepath + "/{{ result('new_file_sensor') | file_name | replace('.csv', '.txt') }}"
        )

        load_fromaddress_csv = rail.LoadCSVFileOperator(
            task_id='load_fromaddress_csv',
            headers = None,
            document="{{ result('download_fromaddress_file') }}"
        )

        def get_from_data_func(from_address_data):
            if from_address_data:
                return from_address_data
            return ""

        check_from_data = rail.PythonOperator(
            task_id='check_from_data',
            python_callable = lambda: get_from_data_func(rail.read_artifact(rail.result("load_fromaddress_csv")))
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to="{{ result('check_from_data')}}",
            bcc=config.bcc_email,
            subject=f'Fujifilmdimatixinc |  PTO balance update has been processed successfully {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            html_content="templates/email/update_success.html",
            files=[
                    ("{{ dag_run_ecid() }}_ptobalanceupdate.csv", '{{result("write_log_file")}}')
                ]
        )

        send_completion_error_mail = rail.EmailOperator(
            task_id='send_completion_error_mail',
            to="{{ result('check_from_data')}}" + f", {config.tenant_email}",
            bcc=config.bcc_email,
            cc=config.cc_email,
            subject='Fujifilmdimatixinc |  PTO balance update has been completed with errors ' +(datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
            html_content="templates/email/update_with_errors.html",
            files=[
                    ("{{ dag_run_ecid() }}_ptobalanceupdate.csv", '{{result("write_log_file")}}')
                ]
        )

        new_file_sensor >> is_csv >> rail.Label("No") >> fail_bad_file_format
        is_csv >> rail.Label("Yes") >> download_file >> was_new_file_found >>  rail.Label("Yes") >> archive_file >> load_csv >> get_paid_time_off_uri >> \
        logger_list >> get_starting_balance_set_to_script_uri >> process_each_empid >> wait_for_process_each_empid >> write_log_file >> \
            download_fromaddress_file >> load_fromaddress_csv >> check_from_data >>\
            filter_master_log >> any_records_failed
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        any_records_failed >> rail.Label("Yes") >> send_completion_error_mail
        any_records_failed >> rail.Label("No") >> send_completion_mail

    return dag

rail.for_each_instance(create_dag)
