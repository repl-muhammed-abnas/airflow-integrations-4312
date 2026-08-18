import csv
from datetime import datetime as timedelta
import rail
from eisner_amper.time_and_timeoff_export_to_workday.utils import request_payload, response_filter


null = None
# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"eisner_amper_timeoff_export_child_{config.instance}",
        description=f"Eisner Amper Time off Export Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_timeoff_download_batch = rail.RepliconServiceOperator(
            task_id="create_timeoff_download_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            data=request_payload.get_timedata_download_batch_data
        )

        execute_timedata_batch, wait_fortimedata_batch = rail.batch_execution(
            'execute_payrun_batch', create_timeoff_download_batch.task_id)

        get_timeoff_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_timeoff_download_batch_result",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            data={
                "timeDataDownloadBatchUri": "{{ result('create_timeoff_download_batch') }}"}
        )

        download_timeoff_file = rail.HTTPDownloadFileOperator(
            task_id='download_timeoff_file',
            url="{{ result('get_timeoff_download_batch_result').downloadUrl }}",
        )
        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('download_timeoff_file') }}"
        )

        create_final_time_off_collection = rail.CreateCollectionOperator(
            task_id='create_final_time_off_collection',
            name='deltarawtimeoffdata',
            source="{{ result('load_export') }}",
            columns={
                'Employee ID': 'employeeid',
                'Entry Date': 'entrydate',
                'Project Code': 'projectcode',
                'Task Code': 'taskcode',
                'Hours': 'hours',
                'Time Entry Code': 'timeentrycode',
                'Project Profile': 'projectprofile',
                'Project Type': 'taskprojecttype',
                'Company Code Code': 'companycodecode',
                'Cost Center Code': 'costcentercode'
            }
        )

        query_finaltimeoff_records = rail.QueryCollectionOperator(
            task_id='query_finaltimeoff_records',
            query="""SELECT * FROM deltarawtimeoffdata WHERE (timeentrycode = '9100' OR timeentrycode = '9620'
            OR timeentrycode = '9745' OR timeentrycode = '9730')
            AND companycodecode LIKE 'US%' AND taskprojecttype= '01' AND
            (costcentercode != 'US01102100' AND costcentercode != 'US01201100'
            AND costcentercode != 'US01202100') """
        )

        has_distinct_data = rail.IfOperator(
            task_id='has_distinct_data',
            test="{{ result('query_finaltimeoff_records', 'length') > 0 }}",
            yes_task='compose_timeoff',
            no_task='send_no_data_mail'
        )

        compose_timeoff = rail.WriteCSVFileOperator(
            task_id='compose_timeoff',
            header=["Employee_ID", "Entry_Date", "Project_Code", "Task_Code",
                    "Hours", "Time_Entry_Code", "Md5"],
            source="{{ result('query_finaltimeoff_records') }}",
            row=request_payload.get_timeoff_data_csv_rows,
            thread_pool_size=config.thread_size
        )

        create_deltaformattedtimeoffdata_collection = rail.CreateCollectionOperator(
            task_id='create_deltaformattedtimeoffdata_collection',
            name='deltaformattedtimeoffdata',
            source="{{ result('compose_timeoff') }}"
        )

        get_all_us_company_codes = rail.RepliconServiceOperator(
            task_id="get_all_us_company_codes",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_all_us_company_codes,
            response_filter=response_filter.get_all_us_company_codes
        )

        get_all_us_cost_codes = rail.RepliconServiceOperator(
            task_id="get_all_us_cost_codes",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_all_us_cost_codes,
            response_filter=response_filter.get_all_us_cost_codes
        )

        create_time_download_batch = rail.RepliconServiceOperator(
            task_id="create_time_download_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            data=request_payload.get_time_download_batch_data
        )

        execute_time_batch, wait_fortime_batch = rail.batch_execution(
            'execute_timedata_batch', create_time_download_batch.task_id)

        get_time_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_time_download_batch_result",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            data={
                "timeDataDownloadBatchUri": "{{ result('create_time_download_batch') }}"}
        )

        download_time_file = rail.HTTPDownloadFileOperator(
            task_id='download_time_file',
            url="{{ result('get_time_download_batch_result').downloadUrl }}",
        )
        load_time_export = rail.LoadCSVFileOperator(
            task_id='load_time_export',
            document="{{ result('download_time_file') }}"
        )

        create_complete_draw_time_off_collection = rail.CreateCollectionOperator(
            task_id='create_complete_draw_time_off_collection',
            name='completedrawtimeoffdata',
            source="{{ result('load_time_export') }}",
            columns={
                'Employee ID': 'employeeid',
                'Entry Date': 'entrydate',
                'Project Code': 'projectcode',
                'Task Code': 'taskcode',
                'Hours': 'hours',
                'Time Entry Code': 'timeentrycode',
                'Project Profile': 'projectprofile',
                'Task Project Type': 'taskprojecttype',
            }
        )

        query_final_filter_records = rail.QueryCollectionOperator(
            task_id='query_final_filter_records',
            query="""SELECT * FROM completedrawtimeoffdata WHERE (timeentrycode="9100" OR timeentrycode="9620"
            OR timeentrycode="9745" OR timeentrycode="9730") AND projectprofile="YP04" AND taskprojecttype="01" """
        )

        compose_timedata = rail.WriteCSVFileOperator(
            task_id='compose_timedata',
            header=["Employee_ID", "Entry_Date", "Project_Code", "Task_Code",
                    "Hours", "Time_Entry_Code", "Md5"],
            source="{{ result('query_final_filter_records') }}",
            row=request_payload.get_timedata_data_csv_rows,
            thread_pool_size=config.thread_size
        )

        create_completedformattedtimeoffdata_collection = rail.CreateCollectionOperator(
            task_id='create_completedformattedtimeoffdata_collection',
            name='completedformattedtimeoffdata',
            source="{{ result('compose_timedata') }}"
        )

        query_completed_formatted_records = rail.QueryCollectionOperator(
            task_id='query_completed_formatted_records',
            query="""SELECT * FROM completedformattedtimeoffdata"""
        )

        query_no_completed_and_completed_filter_records = rail.QueryCollectionOperator(
            task_id='query_no_completed_and_completed_filter_records',
            query="""SELECT deltaformattedtimeoffdata.Employee_ID,deltaformattedtimeoffdata.Entry_Date,
            deltaformattedtimeoffdata.Project_Code,deltaformattedtimeoffdata.Task_Code,deltaformattedtimeoffdata.Hours,
            deltaformattedtimeoffdata.Time_Entry_Code,deltaformattedtimeoffdata.Md5 FROM deltaformattedtimeoffdata 
            WHERE deltaformattedtimeoffdata.Md5 NOT IN (SELECT DISTINCT completedformattedtimeoffdata.Md5 FROM 
            completedformattedtimeoffdata) UNION SELECT completedformattedtimeoffdata.Employee_ID,completedformattedtimeoffdata.Entry_Date,
            completedformattedtimeoffdata.Project_Code,completedformattedtimeoffdata.Task_Code,
            completedformattedtimeoffdata.Hours,completedformattedtimeoffdata.Time_Entry_Code,
            completedformattedtimeoffdata.Md5 FROM completedformattedtimeoffdata WHERE completedformattedtimeoffdata.Md5 
            IN (SELECT DISTINCT deltaformattedtimeoffdata.Md5 FROM deltaformattedtimeoffdata)"""
        )

        compose_timefinaldata = rail.WriteCSVFileOperator(
            task_id='compose_timefinaldata',
            source="{{ result('query_no_completed_and_completed_filter_records') }}",
            row=request_payload.get_final_data_csv_rows,
            header=None,
            quoting=csv.QUOTE_ALL,
            thread_pool_size=config.thread_size
        )

        upload_to_client_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_client_sftp',
            content="{{ result('compose_timefinaldata') }}",
            remote_filepath=config.client_timeoff_export_path +
            "{{ dag_run.conf['Twbname']}}" + '.csv',
            sftp_conn_id=config.sftp_conn_id
        )

        upload_to_internal_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_internal_sftp',
            content="{{ result('compose_timefinaldata') }}",
            remote_filepath=config.internal_timeoff_export_path +
            "{{ dag_run.conf['Twbname']}}" + '.csv',
            sftp_conn_id=config.sftp_conn_internal_id
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon time off extract for Workday- Completed Successfully ' + \
            (timedelta.now()).strftime("%Y%m%d%M%S"),
            html_content="template/timeoff_completion.html",
            params={
                'filepath': config.client_timeoff_export_path,
                'Created_time': (timedelta.now()).strftime("%Y%m%d%M%S")
            }
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon time off extract for Workday- No Data to Export  ' + \
            (timedelta.now()).strftime("%Y%m%d%M%S"),
            html_content="template/timeoff_no_data.html",
            params={
                'Created_time': (timedelta.now()).strftime("%Y%m%d%M%S")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
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

        create_timeoff_download_batch >> execute_timedata_batch >> wait_fortimedata_batch >> get_timeoff_download_batch_result\
            >> download_timeoff_file >> load_export >> create_final_time_off_collection >> query_finaltimeoff_records\
            >> has_distinct_data >> rail.Label("Yes") >> compose_timeoff >> create_deltaformattedtimeoffdata_collection\
            >> get_all_us_company_codes >> get_all_us_cost_codes >> create_time_download_batch >> execute_time_batch\
            >> wait_fortime_batch >> get_time_download_batch_result >> download_time_file >> load_time_export\
            >> create_complete_draw_time_off_collection >> query_final_filter_records >> compose_timedata\
            >> create_completedformattedtimeoffdata_collection >> query_completed_formatted_records\
            >> query_no_completed_and_completed_filter_records\
            >> rail.Label("Yes") >> compose_timefinaldata >> upload_to_client_sftp\
            >> upload_to_internal_sftp >> send_completion_mail\
            >> log_to_sumo >> can_fail_dag >> fail_dagrun

        has_distinct_data >> rail.Label("No") >> send_no_data_mail

    return dag


rail.for_each_instance(create_child_dag)
