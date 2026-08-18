import rail

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcfr/otp_master_report_export/config.py"

def create_report_collection_for_export(report_name, task_suffix):
    with rail.TaskGroup(
        group_id=f"create_report_collection_for_export_{task_suffix}",
        prefix_group_id=False,
    ) as task_group:

        get_report_details_otp_report = rail.RepliconReportDetailsOperator(
            task_id=f"get_report_details_otp_{task_suffix}",
            report_name= report_name
        )

        enter_report_group, exit_report_group = rail.run_report(
            group_id=f"run_report_otp_report_{task_suffix}",
            report_params={
                "reportParameters": [
                    {
                        "reportUri": '{{ result("get_report_details_otp_'+task_suffix+'").uri }}',
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },

        )

        if_report_run_failed = rail.IfOperator(
            task_id = f"if_report_run_failed_{task_suffix}",
            test='{{result("run_report_otp_report_'+task_suffix+'.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task=f"failed_report_generation_{task_suffix}",
            no_task=f"load_report_data_csv_{task_suffix}"
        )

        failed_report_generation = rail.FailOperator(
            task_id=f"failed_report_generation_{task_suffix}",
            message='{{result("run_report_otp_report_'+task_suffix+'.get_report_result").reportGenerationResults[0].error}}'
        )

        load_report_data_csv = rail.LoadCSVFileOperator(
            task_id=f"load_report_data_csv_{task_suffix}",
            document='{{result("run_report_otp_report_'+task_suffix+'.get_report_result").reportGenerationResults[0].payload}}',
        )

        create_collection_for_otp_report = rail.CreateCollectionOperator(
            task_id = f"create_collection_for_otp_report_{task_suffix}",
            source='{{ result("load_report_data_csv_'+task_suffix+'") }}',
            columns={
                "OTP Name": "otpname",
                "OTP Code": "otpcode",
                "OTP Status": "otpstatus",
                "Time & Expense Entry Type": "timeandexpenseentrytype",
                "Project Profit Center": "projectprofilecenter",
            },
            name=f"otp_master_{task_suffix}"
        )

        get_report_details_otp_report >> enter_report_group >> exit_report_group >> \
        if_report_run_failed >> rail.Label("Yes") >> failed_report_generation
        if_report_run_failed >> rail.Label("No") >> load_report_data_csv >> create_collection_for_otp_report

    return task_group
