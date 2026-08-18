from datetime import timedelta, datetime
import rail
null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'centricbrands_disable_user_centricbrands_disable_users_with_end_date_master_{config.instance}',
        description=f'Centricbrands_disable_user_centricbrands_disable_users_with_end_date_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.disable_user_report_name,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report_user_data',
            report_params={
                "reportParameters": [
                    {
                     "reportUri": "{{ result('get_report_details').uri }}",
                     "filterValues": [],
                     "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        if_payload_has_nodata_present = rail.IfOperator(
            task_id='if_payload_has_nodata_present',
            test='{{result("run_report_user_data.get_report_result", "has_data")}}',
            yes_task="load_report_data",
            no_task="stop_job",
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document="{{result('run_report_user_data.get_report_result').reportGenerationResults[0].payload }}",
        )

        stop_job = rail.FailOperator(
            task_id='stop_job',
            message="Error fetching report data {{result('run_report_user_data.get_report_result').reportGenerationResults[0].error}}-{{result('run_report_user_data.get_report_result').reportGenerationResults[0].payload}}"
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            headers=["User Name", "Login Name",
                     "User Status", "User End Date", "useruri"],
            delimiter=',',
            document="{{ result('load_report_data') }}",
        )

        foreach_item_in_parse_csv_do = rail.ForEachOperator(
            task_id='foreach_item_in_parse_csv_do',
            items="{{ result('parse_csv')}}",
            start_task='if_foreach_item_in_parse_csv_do_has_columns',
            end_task='foreach_item_in_parse_csv_do_end'
        )

        if_foreach_item_in_parse_csv_do_has_columns = rail.IfOperator(
            task_id='if_foreach_item_in_parse_csv_do_has_columns',
            test=lambda: bool(rail.result('foreach_item_in_parse_csv_do')['User End Date']) and datetime.strptime(datetime.now().strftime('%B %d, %Y'), '%B %d, %Y') + timedelta(days=1) > (datetime.strptime(
                rail.result('foreach_item_in_parse_csv_do')['User End Date'], '%B %d, %Y')) and rail.result('foreach_item_in_parse_csv_do')['User Status'] == 'Enabled',
            yes_task="disable_login",
            no_task="on_error",
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_item_in_parse_csv_do')['useruri'] }}"
            }
        )

        accumulate_disabledusers_list_items = rail.SetVariableOperator(
            task_id='accumulate_disabledusers_list_items',
            name='disabledusers',
            append=True,
            value={
                "username": "{{ result('foreach_item_in_parse_csv_do')['User Name'] }}",
                "loginname": "{{ result('foreach_item_in_parse_csv_do')['Login Name'] }}",
                "userstatus": "{{ result('foreach_item_in_parse_csv_do')['User Status'] }}",
                "enddate": "{{ result('foreach_item_in_parse_csv_do')['User End Date'] }}",
                "uri": "{{ result('foreach_item_in_parse_csv_do')['useruri'] }}"
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        accumulate_failed_list_items = rail.SetVariableOperator(
            task_id='accumulate_failed_list_items',
            name='failedlist',
            append=True,
            value={
                "username": "{{ result('foreach_item_in_parse_csv_do')['User Name'] }}",
                "loginname": "{{ result('foreach_item_in_parse_csv_do')['Login Name'] }}",
                "userstatus": "{{ result('foreach_item_in_parse_csv_do')['User Status'] }}",
                "enddate": "{{ result('foreach_item_in_parse_csv_do')['User End Date'] }}",
                "uri": "{{ result('foreach_item_in_parse_csv_do')['useruri'] }}"
            }
        )

        foreach_item_in_parse_csv_do_end = rail.EmptyOperator(
            task_id='foreach_item_in_parse_csv_do_end'
        )

        if_accumulate_failed_list_items_greater_than = rail.IfOperator(
            task_id='if_accumulate_failed_list_items_greater_than',
            test="{{ result('accumulate_failed_list_items') | is_truthy and result('accumulate_failed_list_items') | length > 0 }}",
            yes_task="stop_job_with_error_message",
            no_task="catch",
        )

        stop_job_with_error_message = rail.FailOperator(
            task_id='stop_job_with_error_message',
            message='Error disabling users'
        )

        catch = rail.EmptyOperator(
            task_id='catch',
            trigger_rule='one_failed',
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

    get_report_details >> run_my_report_entry
    run_my_report_exit >> if_payload_has_nodata_present
    if_payload_has_nodata_present >> rail.Label('No') >> stop_job
    if_payload_has_nodata_present >> rail.Label(
        'Yes') >> load_report_data >> parse_csv >> foreach_item_in_parse_csv_do
    foreach_item_in_parse_csv_do >> if_foreach_item_in_parse_csv_do_has_columns
    if_foreach_item_in_parse_csv_do_has_columns >> rail.Label(
        'Yes') >> disable_login >> accumulate_disabledusers_list_items >> foreach_item_in_parse_csv_do_end
    if_foreach_item_in_parse_csv_do_has_columns >> rail.Label(
        'No') >> on_error >> accumulate_failed_list_items >> foreach_item_in_parse_csv_do_end
    foreach_item_in_parse_csv_do >> foreach_item_in_parse_csv_do_end >> if_accumulate_failed_list_items_greater_than
    if_accumulate_failed_list_items_greater_than >> rail.Label(
        'Yes') >> stop_job_with_error_message
    if_accumulate_failed_list_items_greater_than >> rail.Label(
        'No') >> catch >> finish

    return dag


rail.for_each_instance(create_dag)
