
from datetime import timedelta
import pendulum
from airflow.models import Variable
from ge_healthcare.ey_disable_users.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_disable_users_master_{config.instance}',
        description=f'GE Disable Users Final {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
        },
    ) as dag:

        can_trigger_tool = rail.IfOperator(
            task_id='can_trigger_tool',
            test=python_callable.check_for_trigger_day(config.time_zone),
            yes_task='can_run_batch_task',
            no_task='finish'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_enabled_employees'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_enabled_employees',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_enabled_employees = rail.RepliconServiceOperator(
            task_id='get_enabled_employees',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:end-date",
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        create_uri_list = rail.PythonOperator(
            task_id='create_uri_list',
            python_callable=python_callable.get_uri_list
        )

        declare_list_11 = rail.SetVariableOperator(
            task_id='declare_list_11',
            append=False,
            name='disable user status log ',
            value=[]
        )

        foreach_d_7_d_7_accumulate_list_items_10_10_12 = rail.ForEachOperator(
            task_id='foreach_d_7_d_7_accumulate_list_items_10_10_12',
            items=lambda:  rail.result('create_uri_list'),
            start_task='if_foreach_d_7_d_7_1_day_diff_equals_to_0_13',
            end_task='foreach_d_7_d_7_accumulate_list_items_10_10_12_end'
        )

        if_foreach_d_7_d_7_1_day_diff_equals_to_0_13 = rail.IfOperator(
            task_id='if_foreach_d_7_d_7_1_day_diff_equals_to_0_13',
            test='''{{ result('foreach_d_7_d_7_accumulate_list_items_10_10_12').day_diff == 0   or  result('foreach_d_7_d_7_accumulate_list_items_10_10_12').day_diff > 0 }}''',
            yes_task="disable_user_15",
            no_task="foreach_d_7_d_7_accumulate_list_items_10_10_12_end",
        )

        disable_user_15 = rail.RepliconServiceOperator(
            task_id='disable_user_15',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_d_7_d_7_accumulate_list_items_10_10_12').uri }}"
            }
        )

        if_user_disabled_successfully = rail.IfOperator(
            task_id='if_user_disabled_successfully',
            test='''{{ result('disable_user_15') | is_falsy }}''',
            yes_task="insert_to_list_16",
            no_task="insert_to_list_18",
        )

        insert_to_list_16 = rail.SetVariableOperator(
            task_id='insert_to_list_16',
            append=True,
            name='{{ result("declare_list_11").name }}',
            value={
                "username": "{{ result('foreach_d_7_d_7_accumulate_list_items_10_10_12').username }}",
                "uri": "{{ result('foreach_d_7_d_7_accumulate_list_items_10_10_12').uri }}",
                "ohrid": "{{ result('foreach_d_7_d_7_accumulate_list_items_10_10_12').OHRID }}",
                "status": "Success",
                "details": "User disabled successfully"
            }
        )

        insert_to_list_18 = rail.SetVariableOperator(
            task_id='insert_to_list_18',
            append=True,
            name='{{ result("declare_list_11").name }}',
            value={
                "username": "{{ result('foreach_d_7_d_7_accumulate_list_items_10_10_12').username }}",
                "uri": "{{ result('foreach_d_7_d_7_accumulate_list_items_10_10_12').uri }}",
                "ohrid": "{{ result('foreach_d_7_d_7_accumulate_list_items_10_10_12').OHRID }}",
                "status": "Error",
                "details": "Error occured while disabling"
            }
        )
        foreach_d_7_d_7_accumulate_list_items_10_10_12_end = rail.EmptyOperator(
            task_id='foreach_d_7_d_7_accumulate_list_items_10_10_12_end',
        )
        get_resource_list_data = rail.GetVariableOperator(
            task_id='get_resource_list_data',
            name='{{ result("declare_list_11").name }}'
        )
        if_first_username_present_19 = rail.IfOperator(
            task_id='if_first_username_present_19',
            test='''{{ result('get_resource_list_data').value | is_truthy }}''',
            yes_task="create_csv_file",
            no_task="send_mail_32",
        )

        create_csv_file = rail.WriteCSVFileOperator(
            task_id='create_csv_file',
            source=lambda: rail.result('get_resource_list_data').get('value'),
            header=['User Name',
                    'Uri',
                    'OHRID',
                    'Status',
                    'Details'],
            row=lambda item: {
                "column_0": item['username'],
                "column_1": item['uri'],
                "column_2": item['ohrid'],
                "column_3": item['status'],
                "column_4": item['details'],
            }.values(),
        )

        log_tobeusedinfilename_21 = rail.PythonOperator(
            task_id='log_tobeusedinfilename_21',
            python_callable=python_callable.get_date_format,
            op_args=[config.time_zone]
        )

        log_checkifthereis_error_22 = rail.PythonOperator(
            task_id='log_checkifthereis_error_22',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_resource_list_data')['value'], 'status', "Error")
        )

        log_subject_line_23 = rail.PythonOperator(
            task_id='log_subject_line_23',
            python_callable=lambda:  "completed with errors" if rail.result(
                'log_checkifthereis_error_22') else "completed successfully"
        )

        log_body_24 = rail.PythonOperator(
            task_id='log_body_24',
            python_callable=lambda:  "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if rail.result(
                'log_checkifthereis_error_22') else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_file')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )
        send_mail_with_cshare_30 = rail.EmailOperator(
            task_id='send_mail_with_cshare_30',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Disable user profiles {{ result('log_subject_line_23') }} - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The automation to disable users {{ result('log_subject_line_23') }} on {{ current_time() }}. Please find the below link to download the user import logs for reference. <br /> <br /><a href="{{result('generate_download_link')}}">Download log file</a> </p>
                        <p><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
                        {{ result('log_body_24') }}''',
            params=None,
        )

        send_mail_32 = rail.EmailOperator(
            task_id='send_mail_32',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Disable User Profiles - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Disable user job import is completed successfully and there was no user found to be disabled. </p>
                            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_trigger_tool >> rail.Label('Yes') >> can_run_batch_task
        can_trigger_tool >> rail.Label('No') >> finish
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_enabled_employees
        get_enabled_employees >> create_uri_list >> declare_list_11 >> foreach_d_7_d_7_accumulate_list_items_10_10_12 >> if_foreach_d_7_d_7_1_day_diff_equals_to_0_13
        if_foreach_d_7_d_7_1_day_diff_equals_to_0_13 >> rail.Label(
            'Yes') >> disable_user_15 >> if_user_disabled_successfully
        if_user_disabled_successfully >> rail.Label(
            'Yes') >> insert_to_list_16 >> foreach_d_7_d_7_accumulate_list_items_10_10_12_end
        if_user_disabled_successfully >> rail.Label(
            'No') >> insert_to_list_18 >> foreach_d_7_d_7_accumulate_list_items_10_10_12_end
        if_foreach_d_7_d_7_1_day_diff_equals_to_0_13 >> rail.Label(
            'No') >> foreach_d_7_d_7_accumulate_list_items_10_10_12_end
        foreach_d_7_d_7_accumulate_list_items_10_10_12 >> foreach_d_7_d_7_accumulate_list_items_10_10_12_end >> get_resource_list_data >> if_first_username_present_19
        if_first_username_present_19 >> rail.Label(
            'Yes') >> create_csv_file >> log_tobeusedinfilename_21 >> log_checkifthereis_error_22 >> log_subject_line_23 >> log_body_24 >> generate_download_link
        generate_download_link >> send_mail_with_cshare_30 >> finish >> log_to_sumo
        if_first_username_present_19 >> rail.Label(
            'No') >> send_mail_32 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
