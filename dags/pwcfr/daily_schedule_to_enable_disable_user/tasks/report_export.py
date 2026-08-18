import rail
def run_user_status_report(report_name):
    with rail.TaskGroup(
        group_id=f"create_user_list_for_status_{report_name}",
        prefix_group_id=False,
    ) as task_group:
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id=f"get_report_details_{report_name}",
            report_name= report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id=f"run_report_for_{report_name}",
            report_params={
                "reportParameters": [
                    {
                        "reportUri": '{{ result("get_report_details_'+report_name+'").uri }}',
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        if_report_run_failed = rail.IfOperator(
            task_id = f"if_report_run_failed_{report_name}",
            test='{{result("run_report_for_'+report_name+'.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task=f"failed_report_generation_{report_name}",
            no_task=f"load_report_data_csv_{report_name}"
        )

        failed_report_generation = rail.FailOperator(
            task_id=f"failed_report_generation_{report_name}",
            message='{{result("run_report_for_'+report_name+'.get_report_result").reportGenerationResults[0].error}}'
        )

        load_report_data_csv = rail.LoadCSVFileOperator(
            task_id=f"load_report_data_csv_{report_name}",
            document='{{result("run_report_for_'+report_name+'.get_report_result").reportGenerationResults[0].payload}}',
        )

        create_collection_of_users = rail.CreateCollectionOperator(
            task_id=f"create_colletion_of_users_{report_name}",
            name=report_name,
            source='{{result("load_report_data_csv_'+report_name+'")}}',
            columns={
                    "User Name" : "username",
                    "User Email": "useremail",
                    "User Start Date": "userstartdate",
                    "User End Date": "userenddate",
                    "UserUri": "useruri",
                    "User Status": "userstatus",
                    "daydiff": "daydiff",
                    "today":"today",
                    "Schedule Name (Current)":"schedulename"
                }
        )

        get_report_details >> run_report_group_entry >> run_report_group_exit >>\
        if_report_run_failed >> rail.Label("Yes") >> failed_report_generation
        if_report_run_failed >> rail.Label("No") >> load_report_data_csv >> create_collection_of_users

        return task_group
