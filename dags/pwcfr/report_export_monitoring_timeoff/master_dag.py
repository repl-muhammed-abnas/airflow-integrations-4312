from datetime import datetime
import pytz
from pendulum import datetime as dt
import rail

def create_main_dag(config):
    # pylint: disable=line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'pwcfr report export 4 monitoring TO airflow migration {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_connid,
        start_date=dt(2023,6,11,tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
            "sftp_connid": config.sftp_connid
        }
    ) as dag:

        report_columns= {
            'Employee ID':'employeeid','User Name':'username','Absence(s) approved in Workday Type':'absenceapprovedinworkdaytype',
            'Booking Start Date':'bookingstartdate','Booking End Date':'bookingenddate','Approval Status':'approvalstatus',
            'Absence(s) approved in Workday Days':'absenceapprovedinworkdayindays','Absence(s) approved in Workday Hrs':'absenceapprovedinworkdayhrs',
            'Absence(s) approved in Workday Comments':'absenceaprovedinworkdaycomments','Time-off Tracking':'timeofftracking',
            'Submitted On':'submittedon','Absence(s) approved in Workday Date':'absenceapprovedinworkdaydate',
            'Default Worktype Code':'defaultworktypecode','Default Worktype Name':'defaultworktypename',
            'Approval Comments':'approvalcomments','User Last Name':'userlastname','User First Name':'userfirstname','Approval Date':'approvaldate'
            }

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.report_name
        )

        def get_data():
            current_fiscal_year=int(datetime.now(pytz.timezone(config.time_zone)).strftime("%Y")) - 1 if int(datetime.now(
                pytz.timezone(config.time_zone)).strftime("%m")) <= 6 else int(datetime.now(pytz.timezone(config.time_zone)).strftime("%Y"))
            return{
                "daterangefilter_uri" : rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri', None),
                "current_fiscal_year_part1_start_date" : datetime.strptime("01/07/"+ str(current_fiscal_year), "%d/%m/%Y").strftime('%m/%d/%Y'),
                "current_fiscal_year_part1_end_date" : datetime.strptime("31/12/"+ str(current_fiscal_year), "%d/%m/%Y").strftime('%m/%d/%Y'),
                "current_fiscal_year_part2_start_date" : datetime.strptime("01/01/"+ str(current_fiscal_year+1), "%d/%m/%Y").strftime('%m/%d/%Y'),
                "current_fiscal_year_part2_end_date" : datetime.strptime("30/06/"+ str(current_fiscal_year+1), "%d/%m/%Y").strftime('%m/%d/%Y'),
                "next_fiscal_year_part1_start_date" : datetime.strptime("01/07/"+ str(current_fiscal_year+1), "%d/%m/%Y").strftime('%m/%d/%Y'),
                "next_fiscal_year_part1_end_date" : datetime.strptime("31/12/"+ str(current_fiscal_year+1), "%d/%m/%Y").strftime('%m/%d/%Y'),
                "next_fiscal_year_part2_start_date" : datetime.strptime("01/01/"+ str(current_fiscal_year+2), "%d/%m/%Y").strftime('%m/%d/%Y'),
                "next_fiscal_year_part2_end_date" : datetime.strptime("30/06/"+ str(current_fiscal_year+2), "%d/%m/%Y").strftime('%m/%d/%Y')
            }

        get_all_required_details = rail.PythonOperator(
            task_id='get_all_required_details',
            python_callable= get_data
        )

        #following process repeats for every half year data for a period of two fiscal years
        run_report_for_current_fiscal_year_part1 = rail.run_report2(
            group_id = "generate_report1",
            report_params={
                "reportParameters":[
                {
                    "reportUri": '{{ result("get_report_details").uri }}',
                    "filterValues": [
                                {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                               "value": ""
                               },
                               {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                                "value": "{{result('get_all_required_details')['current_fiscal_year_part1_start_date']}}"
                               },
                               {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                                "value": "{{result('get_all_required_details')['current_fiscal_year_part1_end_date']}}"
                               }
                           ],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
             ]
            },
            target='artifact',
        )

        if_payload1_contains_error = rail.IfOperator(
            task_id='if_payload1_contains_error',
            test="{{ (result('generate_report1.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task="stop_job1_with_error",
            no_task="load_current_fiscal_year_data_part1",
        )

        stop_job1_with_error = rail.FailOperator(
            task_id='stop_job1_with_error',
            message="{{(result('generate_report1.get_report_result')| load_json_artifact).reportGenerationResults[0].error}}"
        )

        load_current_fiscal_year_data_part1 = rail.LoadCSVFileOperator(
            task_id='load_current_fiscal_year_data_part1',
            document="{{ (result('generate_report1.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
            has_no_header=False,
        )

        create_current_fiscal_year_data_collection_part1 = rail.CreateCollectionOperator(
            task_id = "create_current_fiscal_year_data_collection_part1",
            source= "{{result('load_current_fiscal_year_data_part1')}}",
            name= "curr_fiscal_year_data_part1",
            columns = report_columns,
        )

        run_report_for_current_fiscal_year_part2 = rail.run_report2(
            group_id = "generate_report2",
            report_params={
                "reportParameters":[
                {
                    "reportUri": '{{ result("get_report_details").uri }}',
                    "filterValues": [
                                {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                               "value": ""
                               },
                               {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                                "value": "{{result('get_all_required_details')['current_fiscal_year_part2_start_date']}}"
                               },
                               {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                                "value": "{{result('get_all_required_details')['current_fiscal_year_part2_end_date']}}"
                               }
                           ],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
             ]
            },
            target='artifact',
        )

        if_payload2_contains_error = rail.IfOperator(
            task_id='if_payload2_contains_error',
            test="{{ (result('generate_report2.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task="stop_job2_with_error",
            no_task="load_current_fiscal_year_data_part2",
        )

        stop_job2_with_error = rail.FailOperator(
            task_id='stop_job2_with_error',
            message="{{(result('generate_report2.get_report_result')| load_json_artifact).reportGenerationResults[0].error}}"
        )

        load_current_fiscal_year_data_part2 = rail.LoadCSVFileOperator(
            task_id='load_current_fiscal_year_data_part2',
            document="{{ (result('generate_report2.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
            has_no_header=False,
        )

        create_current_fiscal_year_data_collection_part2 = rail.CreateCollectionOperator(
            task_id = "create_current_fiscal_year_data_collection_part2",
            source= "{{result('load_current_fiscal_year_data_part2')}}",
            name= "curr_fiscal_year_data_part2",
            columns = report_columns,
        )

        run_report_for_next_fiscal_year_part1 = rail.run_report2(
            group_id = "generate_report3",
            report_params={
                "reportParameters":[
                {
                    "reportUri": '{{ result("get_report_details").uri }}',
                    "filterValues": [
                                {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                               "value": ""
                               },
                               {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                                "value": "{{result('get_all_required_details')['next_fiscal_year_part1_start_date']}}"
                               },
                               {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                                "value": "{{result('get_all_required_details')['next_fiscal_year_part1_end_date']}}"
                               }
                           ],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
             ]
            },
            target='artifact',
        )

        if_payload3_contains_error = rail.IfOperator(
            task_id='if_payload3_contains_error',
            test="{{ (result('generate_report3.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task="stop_job3_with_error",
            no_task="load_next_fiscal_year_data_part1",
        )

        stop_job3_with_error = rail.FailOperator(
            task_id='stop_job3_with_error',
            message="{{(result('generate_report3.get_report_result')| load_json_artifact).reportGenerationResults[0].error}}"
        )

        load_next_fiscal_year_data_part1 = rail.LoadCSVFileOperator(
            task_id='load_next_fiscal_year_data_part1',
            document="{{ (result('generate_report3.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
            has_no_header=False,
        )

        create_next_fiscal_year_data_collection_part1 = rail.CreateCollectionOperator(
            task_id = "create_next_fiscal_year_data_collection_part1",
            source= "{{result('load_next_fiscal_year_data_part1')}}",
            name= "next_fiscal_year_data_part1",
            columns = report_columns
        )

        run_report_for_next_fiscal_year_part2 = rail.run_report2(
            group_id = "generate_report4",
            report_params={
                "reportParameters":[
                {
                    "reportUri": '{{ result("get_report_details").uri }}',
                    "filterValues": [
                                {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                               "value": ""
                               },
                               {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                                "value": "{{result('get_all_required_details')['next_fiscal_year_part2_start_date']}}"
                               },
                               {
                               "reportFilterUri": "{{result('get_all_required_details')['daterangefilter_uri']}}",
                                "value": "{{result('get_all_required_details')['next_fiscal_year_part2_end_date']}}"
                               }
                           ],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
             ]
            },
            target='artifact',
        )

        if_payload4_contains_error = rail.IfOperator(
            task_id='if_payload4_contains_error',
            test="{{ (result('generate_report4.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task="stop_job4_with_error",
            no_task="load_next_fiscal_year_data_part2",
        )

        stop_job4_with_error = rail.FailOperator(
            task_id='stop_job4_with_error',
            message="{{(result('generate_report4.get_report_result')| load_json_artifact).reportGenerationResults[0].error}}"
        )

        load_next_fiscal_year_data_part2 = rail.LoadCSVFileOperator(
            task_id='load_next_fiscal_year_data_part2',
            document="{{ (result('generate_report4.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
            has_no_header=False,
        )

        create_next_fiscal_year_data_collection_part2 = rail.CreateCollectionOperator(
            task_id = "create_next_fiscal_year_data_collection_part2",
            source= "{{result('load_next_fiscal_year_data_part2')}}",
            name= "next_fiscal_year_data_part2",
            columns = report_columns
        )

        #all 4 collections combine to to a single collection of two fiscal year data
        append_collections = rail.QueryCollectionOperator(
            task_id='append_collections',
            query="""SELECT * FROM curr_fiscal_year_data_part1 UNION ALL SELECT * FROM curr_fiscal_year_data_part2 UNION ALL SELECT * FROM next_fiscal_year_data_part1 UNION ALL SELECT * FROM next_fiscal_year_data_part2""",
            name="final_collection"
        )

        write_data_to_csv=rail.WriteCSVFileOperator(
            task_id='write_data_to_csv',
            source="{{result('append_collections')}}",
           header=['Employee ID','User Last Name','Absence(s) approved in Workday Type',
                   'Booking Start Date','Booking End Date','Approval Status','Absence(s) approved in Workday Days',
                   'Absence(s) approved in Workday Hrs','Absence(s) approved in Workday Comments','Time-off Tracking',
                   'Submitted On','Absence(s) approved in Workday Date','Default Worktype Code','Default Worktype Name',
                   'Approval Comments','User Last Name','User First Name','Approval Date'],
           row= lambda item:[
                item['employeeid'],
                item['username'],
                item['absenceapprovedinworkdaytype'],
                item['bookingstartdate'],
                item['bookingenddate'],
                item['approvalstatus'],
                item['absenceapprovedinworkdayindays'],
                item['absenceapprovedinworkdayhrs'],
                item['absenceaprovedinworkdaycomments'],
                item['timeofftracking'],
                item['submittedon'],
                item['absenceapprovedinworkdaydate'],
                item['defaultworktypecode'],
                item['defaultworktypename'],
                item['approvalcomments'],
                item['userlastname'],
                item['userfirstname'],
                item['approvaldate'],
            ],
        )

        upload_report_to_sftp = rail.SFTPUploadFileOperator(
            task_id = "upload_report_to_sftp",
            sftp_conn_id=config.sftp_connid,
            content='{{result("write_data_to_csv")}}',
            remote_filepath=config.sftp_file_export_path + '/TO_replicon' + datetime.now(pytz.timezone(config.time_zone)).strftime("%d%m%Y") + ".csv"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
            )

        fail_dagrun = rail.FailOperator(
                task_id="fail_dagrun",
                message='{{get_error_message()}}'
            )

        get_report_details >> get_all_required_details >> run_report_for_current_fiscal_year_part1 >> if_payload1_contains_error >>\
        rail.Label("Yes") >> stop_job1_with_error >> log_to_sumo
        if_payload1_contains_error >> rail.Label("No") >> load_current_fiscal_year_data_part1 >>\
        create_current_fiscal_year_data_collection_part1 >> run_report_for_current_fiscal_year_part2 >> if_payload2_contains_error >> rail.Label(
        "Yes") >> stop_job2_with_error >> log_to_sumo
        if_payload2_contains_error >> rail.Label("No") >> load_current_fiscal_year_data_part2 >>\
        create_current_fiscal_year_data_collection_part2 >> run_report_for_next_fiscal_year_part1 >> if_payload3_contains_error >> rail.Label(
        "Yes") >> stop_job3_with_error >> log_to_sumo
        if_payload3_contains_error >> rail.Label("No") >> load_next_fiscal_year_data_part1 >>\
        create_next_fiscal_year_data_collection_part1 >> run_report_for_next_fiscal_year_part2 >> if_payload4_contains_error >> rail.Label(
        "Yes") >> stop_job4_with_error >> log_to_sumo
        if_payload4_contains_error >> rail.Label("No") >> load_next_fiscal_year_data_part2 >>\
        create_next_fiscal_year_data_collection_part2 >> append_collections >>\
        write_data_to_csv >> upload_report_to_sftp >> log_to_sumo >> can_fail_dag >> fail_dagrun
    return dag

rail.for_each_instance(create_main_dag)
