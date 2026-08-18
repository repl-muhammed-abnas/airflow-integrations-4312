from datetime import timedelta
import hashlib
from airflow.models import Variable
import pendulum
import rail
from cbrefcg.user_billing_rate_update.utils import request_payload

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'cbrefcg_updating_users_billing_rate_assigned_to_project_master_{config.instance}',
        description=f'CBREFCGProduction_Updating_Users_BillingRate_Assigned_To_Project Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=pendulum.datetime(2022, 4, 1, tz=config.pacific_timezone),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_report_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_report_details',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.user_report_name,
        )

        load_users_data_from_report = rail.run_report2(
            group_id='load_users_data_from_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_user_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )


        user_report_has_data = rail.IfOperator(
            task_id='user_report_has_data',
            test='{{ result("load_users_data_from_report.get_report_result", "has_data") }}',
            yes_task='has_user_report_have_expected_columns',
            no_task='finish'
        )

        expected_user_report_columns = "Login Name,UserUri,User Default Billing Rate,currency"

        has_user_report_have_expected_columns= rail.IfOperator(
            task_id='has_user_report_have_expected_columns',
            # pylint: disable=consider-using-f-string line-too-long
            test="{{ result('load_users_data_from_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_user_report_columns,
            yes_task="load_user_report_data_to_csv",
            no_task="fail_dag",
        )

        fail_dag= rail.FailOperator(
            task_id='fail_dag',
            message='''Base report column order does not match'''
        )

        load_user_report_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_user_report_data_to_csv",
            document='{{ result("load_users_data_from_report.get_report_result").reportGenerationResults[0].payload }}',
            headers= ['Login Name','UserUri','User Default Billing Rate','currency']
        )

        compose_csv =rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source="{{ result('load_user_report_data_to_csv') }}",
            header=['Login Name',
                    'UserUri',
                    'User Default Billing Rate',
                    'currency',
                    'MD5'],
            row=lambda item:[
                item['Login Name'],
                item['UserUri'],
                item['User Default Billing Rate'],
                item['currency'],
                hashlib.md5((str(item['Login Name']) + '_' +
                             str(item['UserUri']) + '_' +
                             str(item['User Default Billing Rate']) + '_' +
                             str(item['currency'])).encode('utf-8')).hexdigest()],
        )

        active_user_billing_rate_collection = rail.CreateCollectionOperator(
            task_id='active_user_billing_rate_collection',
            source = "{{ result('compose_csv') }}",
            name = "activeusersbillingratedata",
            columns = {
                'Login Name':'LoginName',
                'UserUri':'UserUri',
                'User Default Billing Rate':'UserDefaultBillingRate',
                'currency':'currency',
                'MD5':'MD5'
            }
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.reference_file_path,
            sftp_conn_id= config.sftp_conn_id,
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}",
            headers=['Login Name','UserUri','User Default Billing Rate','currency', 'MD5']
        )

        reference_file_data_collection = rail.CreateCollectionOperator(
            task_id="reference_file_data_collection",
            source="{{result('parse_reference_file')}}",
            name= 'referencedata',
            columns = {
                'LoginName':'LoginName',
                'UserUri':'UserUri',
                'UserDefaultBillingRate':'UserDefaultBillingRate',
                'currency':'currency',
                'MD5':'MD5'
            }
        )

        query_final_payroll_collection = rail.QueryCollectionOperator(
            task_id = "query_final_payroll_collection",
            query="""SELECT * FROM activeusersbillingratedata where MD5 not in (SELECT DISTINCT MD5 from referencedata)""",
            name= "referencefinaldata"
        )

        has_query_data =rail.IfOperator(
            task_id='has_query_data',
            test="{{ result('query_final_payroll_collection', 'length') > 0 }}",
            yes_task="get_project_report_details",
            no_task="archive_reference_file",
        )

        get_project_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_project_report_details',
            report_name=config.project_report_name,
        )

        load_project_data_from_report = rail.run_report2(
            group_id='load_project_data_from_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_project_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        project_report_has_data = rail.IfOperator(
            task_id='project_report_has_data',
            test='{{ result("load_project_data_from_report.get_report_result", "has_data") }}',
            yes_task='has_project_report_have_expected_columns',
            no_task='finish'
        )

        # pylint: disable=line-too-long
        expected_project_report_columns = """Project Name,ProjectUri,Login Name,UserUri,Billing Rate Amount,User Default Billing Rate,currencyuri,Billing Rate Effective Date,daydiff"""

        has_project_report_have_expected_columns= rail.IfOperator(
            task_id='has_project_report_have_expected_columns',
            # pylint: disable=consider-using-f-string line-too-long
            test="{{ result('load_project_data_from_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_project_report_columns,
            yes_task="load_project_report_data_to_csv",
            no_task="fail_dag",
        )

        load_project_report_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_project_report_data_to_csv",
            document='{{ result("load_project_data_from_report.get_report_result").reportGenerationResults[0].payload }}',
            headers= ['Project Name','ProjectUri','Login Name','UserUri','Billing Rate Amount','User Default Billing Rate','currencyuri',
                      'Billing Rate Effective Date','daydiff']
        )

        create_project_data_collection = rail.CreateCollectionOperator(
            task_id='create_project_data_collection',
            source = "{{ result('load_project_report_data_to_csv') }}",
            name = "projectdata",
            columns = {
                'Project Name':'projectname',
                'ProjectUri':'projecturi',
                'Login Name':'loginname',
                'UserUri':'useruri',
                'Billing Rate Amount':'billingrateamount',
                'User Default Billing Rate':'userdefaultbillingrate',
                'currencyuri':'currencyuri',
                'Billing Rate Effective Date':'billingrateeffectivedate',
                'daydiff':'daydiff'
            }
        )

        query_project_data_collection=rail.QueryCollectionOperator(
            task_id='query_project_data_collection',
            query="""SELECT projectdata.projectname,projectdata.projecturi,projectdata.useruri,projectdata.billingrateamount,
                    projectdata.currencyuri, MIN( projectdata.daydiff), COUNT(*) FROM  projectdata GROUP BY  projectdata.projecturi,
                    projectdata.useruri """,
        )

        create_unique_project_collection = rail.CreateCollectionOperator(
            task_id='create_unique_project_collection',
            source = "{{ result('query_project_data_collection') }}",
            name = "uniqueprojectdata",
        )

        query_unique_project_collection=rail.QueryCollectionOperator(
            task_id='query_unique_project_collection',
            query="""SELECT projectdata.projecturi,projectdata.useruri,projectdata.daydiff,COUNT(*) FROM projectdata GROUP BY
                    projectdata.projecturi,projectdata.useruri ORDER BY  projectdata.daydiff""",
        )

        for_each_user_data = rail.ForEachOperator(
            task_id='for_each_user_data',
            items="{{ result('query_final_payroll_collection') }}",
            start_task = 'query_final_project_data',
            end_task = 'for_each_user_data_end'
        )

        query_final_project_data = rail.QueryCollectionOperator(
            task_id='query_final_project_data',
            query="""SELECT * FROM uniqueprojectdata WHERE uniqueprojectdata.useruri == '{{ result('for_each_user_data').UserUri }}' AND
                    uniqueprojectdata.billingrateamount != '{{ result('for_each_user_data').UserDefaultBillingRate }}'""",
        )

        is_final_project_data_present = rail.IfOperator(
            task_id='is_final_project_data_present',
            test="{{ result('query_final_project_data', 'length') > 0 }}",
            yes_task="process_user_billing_rate_assignment",
            no_task="for_each_user_data_end",
        )

        process_user_billing_rate_assignment = rail.TriggerDagRunOperator(
            task_id='process_user_billing_rate_assignment',
            retries=0,
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= request_payload.get_child_conf
        )

        for_each_user_data_end=rail.EmptyOperator(
            task_id='for_each_user_data_end',
        )

        archive_reference_file=rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            trigger_rule='all_done',
            existing_filename=config.reference_file_path,
            new_filename=config.archive_reference_file_path +
            "archived_reference_{{current_time('%Y-%m-%dT%H-%M-%S')}}.csv"
        )

        upload_new_reference_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file_to_sftp',
            content="{{ result('compose_csv') }}",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_file_path,
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        log_to_sumo= rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> get_user_report_details

        get_user_report_details >> load_users_data_from_report >> user_report_has_data

        user_report_has_data >> rail.Label(
            'Yes')  >> has_user_report_have_expected_columns

        user_report_has_data >> rail.Label(
            'No')  >> finish

        has_user_report_have_expected_columns >> rail.Label(
            'No')  >> fail_dag

        has_user_report_have_expected_columns >> rail.Label(
            'Yes') >> load_user_report_data_to_csv >> compose_csv >> active_user_billing_rate_collection >> download_reference_file >> \
                parse_reference_file >> reference_file_data_collection >> query_final_payroll_collection >> has_query_data

        has_query_data >> rail.Label(
            'Yes') >> get_project_report_details

        has_query_data >> rail.Label(
            'No') >> archive_reference_file

        get_project_report_details >> load_project_data_from_report >> project_report_has_data

        project_report_has_data >> rail.Label(
            "Yes") >> has_project_report_have_expected_columns

        project_report_has_data >> rail.Label(
            "No") >> finish

        has_project_report_have_expected_columns >> rail.Label(
            "Yes") >> load_project_report_data_to_csv >> create_project_data_collection >> query_project_data_collection >>\
                create_unique_project_collection >> query_unique_project_collection >> for_each_user_data

        has_project_report_have_expected_columns >> rail.Label(
            "No") >> fail_dag

        for_each_user_data >> query_final_project_data >> is_final_project_data_present

        is_final_project_data_present >> rail.Label(
            "Yes") >> process_user_billing_rate_assignment >> for_each_user_data_end

        is_final_project_data_present >> rail.Label(
            "No") >> for_each_user_data_end

        for_each_user_data >> for_each_user_data_end >> archive_reference_file >> upload_new_reference_file_to_sftp

        upload_new_reference_file_to_sftp >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
