import rail
def create_user_report_collections(config, report_name, report_suffix):
    with rail.TaskGroup(
        group_id=f"create_user_report_collections_for_{report_suffix}",
        prefix_group_id=False
    ) as task_group:

        get_report_details = rail.RepliconReportDetailsOperator(
                task_id = f'get_report_details_{report_suffix}',
                report_name = report_name,
        )

        user_report_group_entry, user_report_group_exit = rail.run_report(
            group_id = f"run_report_for_{report_suffix}",
            replicon_conn_id=config.replicon_conn_id,
            report_params={
                    "reportParameters": [
                        {
                            "reportUri": '{{ result("get_report_details_'+report_suffix+'").uri }}',
                            "filterValues": [],
                            "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                        }
                    ]
                },
        )

        is_user_report_run_success = rail.IfOperator(
            task_id = f"is_report_run_success_{report_suffix}",
            test='{{result("run_report_for_'+report_suffix+'.get_report_result").reportGenerationResults[0].error | is_falsy}}',
            yes_task=f"load_user_report_data_csv_{report_suffix}",
            no_task=f"fail_dag_run_{report_suffix}",
        )

        fail_dag_run = rail.FailOperator(
            task_id = f"fail_dag_run_{report_suffix}",
            message='{{get_error_message()}}',
        )

        load_user_report_data_csv = rail.LoadCSVFileOperator(
            task_id=f"load_user_report_data_csv_{report_suffix}",
            document='{{result("run_report_for_'+report_suffix+'.get_report_result").reportGenerationResults[0].payload}}',
        )

        create_user_report_collection = rail.CreateCollectionOperator(
            task_id=f"create_user_report_collection_{report_suffix}",
            source='{{result("load_user_report_data_csv_'+report_suffix+'")}}',
            name=f"user_report_collection_{report_suffix}",
            columns={
                "Employee ID":"employeeid",
                "Schedule Name (Current)": "schedulename",
                "Login Name": "loginname"
            }
        )

        get_report_details >> user_report_group_entry >> user_report_group_exit >> \
        is_user_report_run_success >> rail.Label("Yes") >> load_user_report_data_csv >> create_user_report_collection
        is_user_report_run_success >> rail.Label("No") >> fail_dag_run
    return task_group
