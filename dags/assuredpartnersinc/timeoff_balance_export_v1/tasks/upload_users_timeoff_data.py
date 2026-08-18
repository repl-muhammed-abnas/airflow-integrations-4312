import rail

from assuredpartnersinc.timeoff_balance_export_v1.utils import custom_methods


def get_users_timeoff_data(status):
    with rail.TaskGroup(group_id=f'process_{status}_users_timeoff_data', prefix_group_id=False):

        users_report_has_data = rail.IfOperator(
            task_id=f'{status}_users_report_has_data',
            test="{{ result('run_"+status +
            "_users_report.get_report_result','has_data') }}",
            yes_task=f'{status}_users_report_has_no_error',
            no_task=f'{status}_user_timeoff_data_process_finish'
        )

        users_report_has_no_error = rail.IfOperator(
            task_id=f'{status}_users_report_has_no_error',
            test="{{ result('run_"+status +
            "_users_report.get_report_result').reportGenerationResults[0].error | is_falsy }}",
            yes_task=f'{status}_report_has_columns',
            no_task=f'{status}_user_timeoff_data_process_finish'
        )

        expected_report_columns = "Employee ID,Cpny Code,Time Off Type,Time Off Accrued,Time Off Taken,Time Off Balance,Units,Daily Hours"

        report_has_columns = rail.IfOperator(
            task_id=f'{status}_report_has_columns',
            # pylint: disable=consider-using-f-string line-too-long
            test="{{ result('run_"+status + \
            "_users_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s')}}" % expected_report_columns,
            yes_task=f'load_{status}_users_csv',
            no_task=f'{status}_user_timeoff_data_process_finish'
        )

        load_users_csv = rail.LoadCSVFileOperator(
            task_id=f'load_{status}_users_csv',
            document='{{ result("run_'+status +
            '_users_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        users_timeoff_collection = rail.CreateCollectionOperator(
            task_id=f'{status}_users_timeoff_collection',
            source='{{ result("load_'+status+'_users_csv") }}',
            columns={
                "Employee ID": "employeeid",
                "Cpny Code": "companycode",
                "Time Off Type": "timeofftype",
                "Time Off Accrued": "timeoffaccrued",
                "Time Off Taken": "timeofftaken",
                "Time Off Balance": "timeoffbalance",
                "Units": "units",
                "Daily Hours": "dailyhours"
            },
            name=f'{status}_users_timeoff_data',
        )

        query_company_exists = rail.QueryCollectionOperator(
            task_id=f'query_{status}_user_company_exists',
            query=f"SELECT * FROM {status}_users_timeoff_data WHERE NULLIF(companycode, '') IS NOT NULL"
        )

        filter_users_timeoff_data = rail.DataAdaptorOperator(
            task_id=f'filter_{status}_users_timeoff_data',
            source='{{ result("query_'+status+'_user_company_exists") }}',
            columns=["employeeid", "companycode", "timeofftype", "timeoffaccrued", "timeofftaken", "timeoffbalance",
                     "headercode", "ptocode", "initialtimeoffbalance", "timeoffbalanceupdated"],
            data=custom_methods.get_filtered_users_timeoff_data
        )

        get_filtered_users_timeoff_data = rail.CreateCollectionOperator(
            task_id=f'get_filtered_{status}_users_timeoff_data',
            source='{{ result("filter_'+status+'_users_timeoff_data") }}',
            name=f'processed_{status}_users_timeoff_data'
        )

        user_timeoff_data_process_finish = rail.EmptyOperator(
            task_id=f'{status}_user_timeoff_data_process_finish',
        )

        users_report_has_data >> rail.Label("Yes") >> users_report_has_no_error
        users_report_has_data >> rail.Label(
            "No") >> user_timeoff_data_process_finish

        users_report_has_no_error >> rail.Label("Yes") >> report_has_columns
        users_report_has_no_error >> rail.Label(
            "No") >> user_timeoff_data_process_finish

        report_has_columns >> rail.Label("Yes") >> load_users_csv
        report_has_columns >> rail.Label(
            "No") >> user_timeoff_data_process_finish
        load_users_csv >> users_timeoff_collection >> query_company_exists \
            >> filter_users_timeoff_data >> get_filtered_users_timeoff_data \
            >> user_timeoff_data_process_finish
        return users_report_has_data, user_timeoff_data_process_finish
