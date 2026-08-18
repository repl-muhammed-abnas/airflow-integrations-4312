from datetime import datetime as dt, timedelta
from pendulum import datetime
import rail
import pytz

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"{config.instance}_previous_calendar_year_distance_data_extract_daily_report_for_Netherlands",
        description=f"Daily Distance Extract for Netherlands {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2022, 4, 1, tz=config.europe_timezone),
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda:  str(dt.now(pytz.timezone("Europe/Paris")).strftime("%Y%m%d%H%M%S"))
        )
        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda: "Yearly_Distance_Extract_"  +
            str(dt.now(pytz.timezone("Europe/Paris")).strftime("%Y%m%d%H%M%S")) + "_NLD.csv"
            )

        get_specific_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_specific_report_details',
            report_name=config.report_name,
        )

        def last_day_of_month(date):
            if date.month == 12:
                return date.replace(day=31)
            return dt.date(date.replace(month=date.month, day=1) - timedelta(days=1)
        )

        # pylint: disable=line-too-long
        process_previous_calendar_year_extract_upload_file = rail.TriggerDagRunForEachItemOperator(
            task_id='process_previous_calendar_year_extract_upload_file',
            retries=0,
            items=list(range(0, config.batch_size)),
            trigger_dag_id=f'{config.instance}_process_previous_calendar_year_extract_upload_file_Child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'index':  item,
                'batch_size': config.batch_size,
                'report_uri': rail.result('get_specific_report_details')['uri'],
                'report_filter_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_specific_report_details')["filterConfiguration"]["enabledFilters"], 'displayText', config.filter_name, 'uri'),
                'start_date':  str(dt(dt.today().year - 1, int(item * int(12/config.batch_size)) if(int(item * int(12/config.batch_size))) else 1, 1).strftime("%m/%d/%Y")),
                'end_date': str((last_day_of_month(dt(dt.today().year - 1, int((item + 1) * int(
                    12/config.batch_size)), 1)).strftime("%m/%d/%Y"))),
                'sequence_no': item + 1,
                'file_name': rail.result('get_file_name')
            })

        wait_to_process_previous_calendar_year_extract = rail.WaitForDagRunsSensor(
            task_id='wait_to_process_previous_calendar_year_extract',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_previous_calendar_year_extract_upload_file") }}',
        )
        get_data_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='get_data_from_child',
            dag_runs='{{ result("process_previous_calendar_year_extract_upload_file") }}',
            dagrun_task_id='report_payload_to_csv',
            flatten=True,
        )

        child_has_data = rail.IfOperator(
            task_id="child_has_data",
            test='{{result("get_data_from_child") | is_truthy }}',
            yes_task='both_export_data_is_available',
            no_task='finish_export'
        )
        def get_result():
            csv_list= list(rail.result('get_data_from_child'))
            if len(csv_list) ==2:
                return True
            return False
        both_export_data_is_available = rail.IfOperator(
            task_id="both_export_data_is_available",
            test=get_result,
            yes_task='dummy_collection_operator',
            no_task='create_single_collection'
        )
        create_single_collection = rail.CreateCollectionOperator(
            task_id="create_single_collection",
            name='report_data',
            source='{{result("get_data_from_child")[0]}}'
        )

        final_single_report_data = rail.QueryCollectionOperator(
            task_id="final_single_report_data",
            query="SELECT * FROM report_data WHERE NULLIF(TransactionDate, '') IS NOT NULL"
        )
        final_single_report_data_to_csv = rail.WriteCSVFileOperator2(
            task_id="final_single_report_data_to_csv",
            source="{{ result('final_single_report_data') }}",
            execution_timeout=timedelta(hours=6),
            header=["TransactionDate", "TimeEntryID", "PartyID", "ResourceGrade", "LegalEntityPartyID",
                    "WorkDayId", "TimesheetStartDate", "TimesheetEndDate", "Mileage", "ChargeCode", "WorkItemType"],
            row=['{{item.TransactionDate}}',
                 '{{item.TimeEntryID}}',
                 '{{item.PartyID}}',
                 '{{item.ResourceGrade}}',
                 '{{item.LegalEntityPartyID}}',
                 '{{item.WorkDayId}}',
                 '{{item.TimesheetStartDate}}',
                 '{{item.TimesheetEndDate}}',
                 '{{item.Mileage}}',
                 '{{item.ChargeCode}}',
                 '{{item.WorkItemType}}']
        )

        upload_export_single_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_single_data_to_sftp",
            content='{{result("final_single_report_data_to_csv")}}',
            execution_timeout=timedelta(minutes=60),
            remote_filepath=config.output_file_path + "{{result('get_file_name')}}"
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        dummy_collection_operator = rail.EmptyOperator(
            task_id='dummy_collection_operator'
        )

        create_export_data_collection_0 = rail.CreateCollectionOperator(
            task_id="create_export_data_collection_0",
            name='report_data_batch_I',
            source='{{result("get_data_from_child")[0]}}'
        )

        create_export_data_collection_1 = rail.CreateCollectionOperator(
            task_id="create_export_data_collection_1",
            name='report_data_batch_II',
            source='{{result("get_data_from_child")[1]}}'
        )

        combine_report_data = rail.QueryCollectionOperator(
            task_id="combine_report_data",
            # pylint: disable=line-too-long
            query="SELECT * FROM report_data_batch_I LEFT JOIN report_data_batch_II USING (TimeEntryID) UNION ALL SELECT * FROM report_data_batch_II LEFT JOIN report_data_batch_I USING (TimeEntryID)"
        )

        create_export_final_data_collection = rail.CreateCollectionOperator(
            task_id="create_export_final_data_collection",
            name='final_data',
            source='{{result("combine_report_data")}}'
        )

        final_report_data = rail.QueryCollectionOperator(
            task_id="final_report_data",
            query="SELECT * FROM final_data WHERE NULLIF(TransactionDate, '') IS NOT NULL"
        )

        final_data_to_csv = rail.WriteCSVFileOperator2(
            task_id="final_data_to_csv",
            source="{{ result('final_report_data') }}",
            header=["TransactionDate", "TimeEntryID", "PartyID", "ResourceGrade", "LegalEntityPartyID",
                    "WorkDayId", "TimesheetStartDate", "TimesheetEndDate", "Mileage", "ChargeCode", "WorkItemType"],
            row=['{{item.TransactionDate}}',
                 '{{item.TimeEntryID}}',
                 '{{item.PartyID}}',
                 '{{item.ResourceGrade}}',
                 '{{item.LegalEntityPartyID}}',
                 '{{item.WorkDayId}}',
                 '{{item.TimesheetStartDate}}',
                 '{{item.TimesheetEndDate}}',
                 '{{item.Mileage}}',
                 '{{item.ChargeCode}}',
                 '{{item.WorkItemType}}']
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            content='{{result("final_data_to_csv")}}',
            remote_filepath=config.output_file_path + "{{result('get_file_name')}}"
        )
        # pylint: disable=line-too-long
        process_start_time>> get_file_name>> get_specific_report_details >> process_previous_calendar_year_extract_upload_file \
        >> wait_to_process_previous_calendar_year_extract >> get_data_from_child >> child_has_data >> both_export_data_is_available
        child_has_data >> rail.Label("No") >> finish_export
        child_has_data >> rail.Label("Yes") >>both_export_data_is_available>> rail.Label("Yes")>> dummy_collection_operator
        both_export_data_is_available>> rail.Label("No")>> create_single_collection >> final_single_report_data >> final_single_report_data_to_csv
        final_single_report_data_to_csv >> upload_export_single_data_to_sftp
        dummy_collection_operator >> create_export_data_collection_0 >> combine_report_data
        dummy_collection_operator >> create_export_data_collection_1 >> combine_report_data
        combine_report_data >> create_export_final_data_collection>> final_report_data >>final_data_to_csv>> upload_export_data_to_sftp
    return dag

rail.for_each_instance(create_main_dag)
