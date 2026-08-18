
from datetime import timedelta
import pendulum
from airflow.models import Variable
from ge.ey_disable_users.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_disable_foreign_supervisor_master_{config.instance}',
        description=f'GE Disable Foreign Supervisor Final {config.instance}',
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
            no_task='get_all_employee_type_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_employee_type_details',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data=None
        )

        log_foreign_supervisoremployeetype_uri = rail.PythonOperator(
            task_id='log_foreign_supervisoremployeetype_uri',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_employee_type_details'), 'name', "Foreign Supervisor", 'uri')
        )

        get_all_foreign_supervisors = rail.RepliconServiceOperator(
            task_id='get_all_foreign_supervisors',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:user-list-filter:employee-type"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": "{{ result('log_foreign_supervisoremployeetype_uri') }}",
                                "uris": [],
                                "bool": null,
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
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
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
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        foreign_supervisor_uri_list = rail.PythonOperator(
            task_id='foreign_supervisor_uri_list',
            python_callable=python_callable.get_all_foreign_uri_list
        )

        declare_list_11 = rail.SetVariableOperator(
            task_id='declare_list_11',
            append=False,
            name='disable foreign supervisor status log',
            value=[]
        )

        foreach_d_9_d_9_accumulate_list_items_10_10_12 = rail.ForEachOperator(
            task_id='foreach_d_9_d_9_accumulate_list_items_10_10_12',
            items=lambda:  rail.result('foreign_supervisor_uri_list'),
            start_task='get_direct_reports_14',
            end_task='foreach_d_9_d_9_accumulate_list_items_10_10_12_end'
        )

        get_direct_reports_14 = rail.RepliconServiceOperator(
            task_id='get_direct_reports_14',
            endpoint="/services/UserService1.svc/GetDirectReportsForUser",
            data={
                "userUri": "{{ result('foreach_d_9_d_9_accumulate_list_items_10_10_12').uri }}",
                "asOfDate": null,
                "userStatusOptionUri": "urn:replicon:user-status-option:include-only-enabled-users"
            }
        )

        if_first_uri_not_blank = rail.IfOperator(
            task_id='if_first_uri_not_blank',
            test='''{{ result('get_direct_reports_14') | length > 0 }}''',
            yes_task="foreach_d_9_d_9_accumulate_list_items_10_10_12_end",
            no_task="disablelogin_16",
        )

        disablelogin_16 = rail.RepliconServiceOperator(
            task_id='disablelogin_16',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_d_9_d_9_accumulate_list_items_10_10_12').uri }}"
            }
        )

        if_user_disabled_successfully = rail.IfOperator(
            task_id='if_user_disabled_successfully',
            test='''{{ result('disablelogin_16') | is_falsy }}''',
            yes_task="insert_to_list_17",
            no_task="insert_to_list_19",
        )

        insert_to_list_17 = rail.SetVariableOperator(
            task_id='insert_to_list_17',
            append=True,
            name='{{ result("declare_list_11").name }}',
            value={
                "username": "{{ result('foreach_d_9_d_9_accumulate_list_items_10_10_12').username }}",
                "uri": "{{ result('foreach_d_9_d_9_accumulate_list_items_10_10_12').uri }}",
                "ohrid": "{{ result('foreach_d_9_d_9_accumulate_list_items_10_10_12').OHRID }}",
                "status": "Success",
                "details": "Foreign supervisor disabled successfully"
            }
        )

        insert_to_list_19 = rail.SetVariableOperator(
            task_id='insert_to_list_19',
            # trigger_rule='one_failed',
            append=True,
            name='{{ result("declare_list_11").name }}',
            value={
                "username": "{{ result('foreach_d_9_d_9_accumulate_list_items_10_10_12').username }}",
                "uri": "{{ result('foreach_d_9_d_9_accumulate_list_items_10_10_12').uri }}",
                "ohrid": "{{ result('foreach_d_9_d_9_accumulate_list_items_10_10_12').OHRID }}",
                "status": "Error",
                "details": "Error occured while disabling"
            }
        )

        foreach_d_9_d_9_accumulate_list_items_10_10_12_end = rail.EmptyOperator(
            task_id='foreach_d_9_d_9_accumulate_list_items_10_10_12_end',
        )
        get_resource_list_data = rail.GetVariableOperator(
            task_id='get_resource_list_data',
            name='{{ result("declare_list_11").name }}'
        )
        if_first_username_present_20 = rail.IfOperator(
            task_id='if_first_username_present_20',
            test='''{{ result('get_resource_list_data').value | is_truthy }}''',
            yes_task="create_csv_file",
            no_task="send_mail_33",
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

        log_filenametobeused_22 = rail.PythonOperator(
            task_id='log_filenametobeused_22',
            python_callable=python_callable.get_date_format,
            op_args=[config.time_zone]
        )

        log_checkifthereis_error_23 = rail.PythonOperator(
            task_id='log_checkifthereis_error_23',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_resource_list_data')['value'], 'status', "Error")
        )

        log_subject_line_24 = rail.PythonOperator(
            task_id='log_subject_line_24',
            python_callable=lambda:  "completed with errors" if rail.result(
                'log_checkifthereis_error_23') else "completed successfully"
        )

        log_body_25 = rail.PythonOperator(
            task_id='log_body_25',
            python_callable=lambda:  "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if rail.result(
                'log_checkifthereis_error_23') else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_file')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )
        send_mail_with_cshare_31 = rail.EmailOperator(
            task_id='send_mail_with_cshare_31',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Foreign Supervisor User Profile Disable {{ result('log_subject_line_24') }} - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The automation to disable foreign supervisors {{ result('log_subject_line_24') }} on {{ current_time() }}. Please find the below link to download the user import logs for reference. <br /> <br /><a href="{{result('generate_download_link')}}">Download log file</a> </p>
                        <p><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
                        {{ result('log_body_25') }} ''',
            params=None,
        )

        send_mail_33 = rail.EmailOperator(
            task_id='send_mail_33',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Foreign Supervisor User Profile Disable - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> There was no Foreign Supervisor record found matching the criteria to disable. </p>
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
        can_run_batch_task >> rail.Label('No') >> get_all_employee_type_details
        get_all_employee_type_details >> log_foreign_supervisoremployeetype_uri >> get_all_foreign_supervisors >> foreign_supervisor_uri_list
        foreign_supervisor_uri_list >> declare_list_11 >> foreach_d_9_d_9_accumulate_list_items_10_10_12 >> get_direct_reports_14 >> if_first_uri_not_blank
        if_first_uri_not_blank >> rail.Label(
            'No') >> disablelogin_16 >> if_user_disabled_successfully
        if_user_disabled_successfully >> rail.Label(
            'Yes') >> insert_to_list_17 >> foreach_d_9_d_9_accumulate_list_items_10_10_12_end
        if_user_disabled_successfully >> rail.Label(
            'No') >> insert_to_list_19 >> foreach_d_9_d_9_accumulate_list_items_10_10_12_end
        if_first_uri_not_blank >> rail.Label(
            'Yes') >> foreach_d_9_d_9_accumulate_list_items_10_10_12_end
        foreach_d_9_d_9_accumulate_list_items_10_10_12 >> foreach_d_9_d_9_accumulate_list_items_10_10_12_end >> get_resource_list_data >> if_first_username_present_20
        if_first_username_present_20 >> rail.Label(
            'Yes') >> create_csv_file >> log_filenametobeused_22 >> log_checkifthereis_error_23 >> log_subject_line_24 >> log_body_25 >> generate_download_link
        generate_download_link >> send_mail_with_cshare_31 >> finish
        if_first_username_present_20 >> rail.Label(
            'No') >> send_mail_33 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
