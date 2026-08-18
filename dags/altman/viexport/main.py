import csv
from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'altman_viexport_master_{config.instance}',
        description=f'altman_viexport_master {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2023, 1, 1, tz=config.mountain_timezone),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        get_viexport_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_viexport_report_details',
            report_name=config.report_viexport,
        )

        run_viexport_report_group_entry, run_viexport_report_group_exit = rail.run_report(
            group_id='run_viexport_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_viexport_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        is_viexport_report_failed = rail.IfOperator(
            task_id="is_viexport_report_failed",
            test='{{result("run_viexport_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_dag",
            no_task="report_viexport_has_data"
        )

        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message='report extract failed'
        )

        report_viexport_has_data = rail.IfOperator(
            task_id="report_viexport_has_data",
            test="{{ result('run_viexport_report.get_report_result','has_data')}}",
            yes_task='load_report_data_viexport',
            no_task='finish'
        )

        load_report_data_viexport = rail.LoadCSVFileOperator(
            task_id='load_report_data_viexport',
            document="{{ result('run_viexport_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        get_userreport_details = rail.RepliconReportDetailsOperator(
            task_id='get_userreport_details',
            report_name=config.report_user,
        )

        run_user_report_group_entry, run_user_report_group_exit = rail.run_report(
            group_id='run_user_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_userreport_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        is_report_failed_user = rail.IfOperator(
            task_id="is_report_failed_user",
            test='{{result("run_user_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_dag",
            no_task="report_user_has_data"
        )

        report_user_has_data = rail.IfOperator(
            task_id="report_user_has_data",
            test="{{ result('run_user_report.get_report_result','has_data')}}",
            yes_task='load_report_data_user',
            no_task='finish'
        )

        load_report_data_user = rail.LoadCSVFileOperator(
            task_id='load_report_data_user',
            document="{{ result('run_user_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        all_user_reportdata = rail.PythonOperator(
            task_id="all_user_reportdata",
            python_callable=lambda: rail.load_all_records(
                rail.result('load_report_data_user'))
        )

        def get_rows(item):
            user_report_data = rail.result('all_user_reportdata')
            professionadvisor_fullname = rail.smartjoin_by_delim(list(map(lambda i: i['User First Name'] +
            "-" + i['User Last Name'], filter(lambda x: x['Employee ID'] == item['PA Employee ID'],
            user_report_data))), " ") if item["PA Employee ID"] else None
            return [
                item['Project Description'],
                item['Employee ID'],
                "*",
                item['Client Code'],
                item['Client Name'],
                item['Project Code'],
                item['Project Name'],
                item['Project Start Date'],
                item['Project End Date'],
                item['Actual Hours (Selected Dates)'],
                item['Hierarchy (Current)'],
                item['User First Name'],
                item['User Last Name'],
                item['User Email'],
                item['Location'],
                item['PA Employee ID'],
                professionadvisor_fullname.split(
                    '-', maxsplit=1)[0] if professionadvisor_fullname else None,
                professionadvisor_fullname.split(
                    '-')[1] if professionadvisor_fullname else None,
                item['User Start Date'],
                item['Hierarchy Effective Date'],
            ]

        create_final_report_data = rail.WriteCSVFileOperator(
            task_id="create_final_report_data",
            source="{{ result('load_report_data_viexport')}}",
            quoting=csv.QUOTE_ALL,
            header=["ProjectDescription", "EmployeeID", "EvaluatorID", "Client Code", "Client Name",
                    "Project Code", "Project Name", "Project Start Date", "Project End Date",
                    "Actual Hours (Selected Dates)", "Hierarchy", "Employee First name",
                    "Employee Last name", "Employee email address", "Employee Location",
                    "Professional advisor ID", "Professional Advisor First Name",
                    "Professional Advisor Last Name", "User Start Date", "Hierarchy Effective Date"],
            row=get_rows
        )

        upload_reportdata_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_reportdata_to_sftp',
            content="{{ result('create_final_report_data') }}",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="/Replicondata_"
            '{{ current_time_in_specified_tz("America/Denver","%Y-%m-%d") }}' + ".csv"
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} - Replicon data extract has uploaded to SFTP_{{ current_time("%d%m%Y%H%M%S") }}',
            html_content="templates/emails/success_email.html",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "exportfile_name":"/Replicondata_"
                '{{ current_time_in_specified_tz("America/Denver","%Y-%m-%d") }}' + ".csv"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_viexport_report_details >> run_viexport_report_group_entry
        run_viexport_report_group_exit >> is_viexport_report_failed

        is_viexport_report_failed >> rail.Label(
            "Yes") >> fail_dag

        is_viexport_report_failed >> rail.Label(
            "No") >> report_viexport_has_data

        report_viexport_has_data >> rail.Label(
            "Yes") >> load_report_data_viexport >> get_userreport_details

        report_viexport_has_data >> rail.Label(
            "No") >> finish

        get_userreport_details >> run_user_report_group_entry
        run_user_report_group_exit >> is_report_failed_user

        is_report_failed_user >> rail.Label(
            "Yes") >> fail_dag

        is_report_failed_user >> rail.Label(
            "No") >> report_user_has_data

        report_user_has_data >> rail.Label(
            "Yes") >> load_report_data_user >> all_user_reportdata >> create_final_report_data

        report_user_has_data >> rail.Label(
            "No") >> finish

        create_final_report_data >> upload_reportdata_to_sftp >> send_success_email >> log_to_sumo

        log_to_sumo >> finish

    return dag


rail.for_each_instance(create_main_dag)
