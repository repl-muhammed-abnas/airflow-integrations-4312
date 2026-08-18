
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'crjg3_normalized_report_export_master_{config.instance}',
        description=f'CRJ_Normalized Report Export{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=[rail.WebhookConf(
            bearer_token_var=f'crjg3_normalized_report_export_webook_{config.instance}_secret')],
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_runtime_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_runtime_4',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_runtime_4=rail.PythonOperator(
            task_id='log_runtime_4',
            python_callable= lambda:  datetime.now().strftime("%m/%d/%YT%H:%M:%S")
        )

        log_useruri_5=rail.PythonOperator(
            task_id='log_useruri_5',
            python_callable= lambda dag_run:  f"urn:replicon-tenant:{rail.get_tenant_slug()}:user:" + dag_run.conf['webhook']['data']['requestorid']
        )

        get_user_details_6=rail.RepliconServiceOperator(
            task_id='get_user_details_6',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data={
                "userUri": "{{result('log_useruri_5')}}"
            }
        )

        if_requestor_emailaddress_not_present=rail.IfOperator(
            task_id='if_requestor_emailaddress_not_present',
            test='''{{ result('get_user_details_6').emailAddress | is_falsy }}''',
            yes_task="finish",
            no_task="log_emailsids_forlogs_9",
        )

        log_emailsids_forlogs_9=rail.PythonOperator(
            task_id='log_emailsids_forlogs_9',
            python_callable= lambda dag_run: dag_run.conf['webhook']['data']['emailIds'] + "," + rail.result('get_user_details_6')['emailAddress'] if
                                dag_run.conf['webhook']['data']['emailIds'] else rail.result('get_user_details_6')['emailAddress']
        )

        if_payload_daterange_contains_null_10=rail.IfOperator(
            task_id='if_payload_daterange_contains_null_10',
            test=lambda dag_run: not dag_run.conf['webhook']['data']['dateRange'],
            yes_task="send_mail_incorrect_date_range",
            no_task="get_assigned_permission_sets_for_user2_13",
        )

        send_mail_incorrect_date_range=rail.EmailOperator(
            task_id='send_mail_incorrect_date_range',
            to="{{result('log_emailsids_forlogs_9')}}",
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Custom Actuals vs Normalized Report - No Data - {{ result('log_runtime_4') }} ''',
            html_content= '''templates/no_daterange_mail.html''',
        )

        get_assigned_permission_sets_for_user2_13=rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_13',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_useruri_5') }}"
            }
        )

        log_checkifrequiredpermissionisavailable_14=rail.PythonOperator(
            task_id='log_checkifrequiredpermissionisavailable_14',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_permission_sets_for_user2_13'),'policyUri','urn:replicon:policy:client-representation','permissionSet.name','') if rail.result(
                'get_assigned_permission_sets_for_user2_13')[0]['policyUri'] else null
        )

        if_log_checkifrequiredpermissionisavailable_14_blank_15=rail.IfOperator(
            task_id='if_log_checkifrequiredpermissionisavailable_14_blank_15',
            test='''{{ result('log_checkifrequiredpermissionisavailable_14') | is_falsy  or\
                result('log_checkifrequiredpermissionisavailable_14') != 'Custom Report Access' }}''',
            yes_task="finish",
            no_task="get_actual_vs_normalized_report_details",
        )

        get_actual_vs_normalized_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_actual_vs_normalized_report_details',
            report_name=config.actual_vs_normalized_report
        )

        if_log_actualsvs_normalized_report_baseereport1uri_18_blank_19=rail.IfOperator(
            task_id='if_log_actualsvs_normalized_report_baseereport1uri_18_blank_19',
            test='''{{ result('get_actual_vs_normalized_report_details') | is_falsy or result('get_actual_vs_normalized_report_details').uri | is_falsy }}''',
            yes_task="fail_with_error",
            no_task="if_columnconfiguration_doesnt_match",
        )

        fail_with_error=rail.FailOperator(
            task_id='fail_with_error',
            message='''**Payroll Report is not present in Replicon'''
        )

        if_columnconfiguration_doesnt_match=rail.IfOperator(
            task_id='if_columnconfiguration_doesnt_match',
            #pylint:disable = line-too-long
            test=lambda: ','.join([column['column']['displayText'] for column in rail.result(
                'get_actual_vs_normalized_report_details')['columnConfiguration']]) != 'User Name,Project Name,Project Code,Timesheet Period,Entry Date,Hours Worked,Week (Entry Date),Normalization Required?,Contract Type,Login Name,Employee Category,Time Type Name (Full Path),Time Type Code',
            yes_task="fail_cause_of_column_mismatch",
            no_task="impersonate_and_create_interactive_session_24",
        )

        fail_cause_of_column_mismatch=rail.FailOperator(
            task_id='fail_cause_of_column_mismatch',
            #pylint:disable = line-too-long
            message='''Column order mismatch in **Actuals vs Normalized Report - Base Report. Required column order is "User Name,Project Name,Project Code,Timesheet Period,Entry Date,Hours Worked,Week (Entry Date),Normalization Required?,Contract Type,Login Name,Employee Category"'''
        )

        def get_authtoken(res):
            data = res.json()['d']
            auth_token = list(
                filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
            tenant = list(
                filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
            return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}

        impersonate_and_create_interactive_session_24=rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session_24',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{ result('log_useruri_5') }}"
            },
            response_filter=get_authtoken
        )

        get_report_filter_uris=rail.PythonOperator(
            task_id='get_report_filter_uris',
            python_callable= lambda: {
                  'approvalstatusfilteruri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_actual_vs_normalized_report_details')['filterConfiguration']['enabledFilters'],'displayText','ApprovalStatusFilter','uri',''),
                  'timesheetperiodfilteruri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_actual_vs_normalized_report_details')['filterConfiguration']['enabledFilters'],'displayText','TimesheetPeriodFilter','uri',''),
                  'userfilteruri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_actual_vs_normalized_report_details')['filterConfiguration']['enabledFilters'],'displayText','UserFilter','uri','')
            }
        )

        if_payload_approvalid_present_33=rail.IfOperator(
            task_id='if_payload_approvalid_present_33',
            test='''{{ dag_run.conf.webhook.data.timesheetApprovalStatusIds | is_truthy }}''',
            yes_task="get_approvalstatus_filter_options",
            no_task="if_payload_userid_present_38",
        )

        def get_approval_status_filters(dag_run):
            approval_ids = dag_run.conf['webhook']['data']['timesheetApprovalStatusIds'].split(',')
            filter_uri = rail.result('get_report_filter_uris')['approvalstatusfilteruri']
            return [{
                'reportFilterUri': filter_uri,
                'value': 0 if id == 'Not Submitted' else ( 1 if id == 'Waiting for Approval' else ( 2 if id == 'Approved' else ( 3 if id == 'Rejected' else 4)))
            } for id in approval_ids]

        get_approvalstatus_filter_options=rail.PythonOperator(
            task_id='get_approvalstatus_filter_options',
            python_callable= get_approval_status_filters
        )

        if_payload_userid_present_38=rail.IfOperator(
            task_id='if_payload_userid_present_38',
            test='''{{ dag_run.conf.webhook.data.userIds | is_truthy }}''',
            yes_task="get_user_filter_options",
            no_task="get_timesheet_period_filters",
        )

        def get_user_filters(dag_run):
            user_filter_uri = rail.result('get_report_filter_uris')['userfilteruri']
            user_ids = dag_run.conf['webhook']['data']['userIds'].split(',')
            return [{
                'reportFilterUri': user_filter_uri,
                'value': user
            } for user in user_ids]

        get_user_filter_options=rail.PythonOperator(
            task_id='get_user_filter_options',
            python_callable= get_user_filters
        )

        def get_timesheet_filters(dag_run):
            timesheet_filteruri = rail.result('get_report_filter_uris')['timesheetperiodfilteruri']
            startdate = datetime.strptime(dag_run.conf['webhook']['data']['dateRange'].split('-')[0],'%m%d%Y').strftime('%m/%d/%Y')
            enddate = datetime.strptime(dag_run.conf['webhook']['data']['dateRange'].split('-')[-1],'%m%d%Y').strftime('%m/%d/%Y')
            return [
                {
                    'reportFilterUri': timesheet_filteruri,
                    'value': null
                },
                {
                    'reportFilterUri': timesheet_filteruri,
                    'value': startdate
                },
                {
                    'reportFilterUri': timesheet_filteruri,
                    'value': enddate
                }
            ]

        get_timesheet_period_filters = rail.PythonOperator(
            task_id = 'get_timesheet_period_filters',
            python_callable= get_timesheet_filters
        )

        log_reportfilterfor_custom_base_report1_43=rail.PythonOperator(
            task_id='log_reportfilterfor_custom_base_report1_43',
            python_callable= lambda: rail.result('get_timesheet_period_filters') + rail.result('get_approvalstatus_filter_options') +
                                rail.result('get_user_filter_options')
        )

        run_custom_report=rail.run_report2(
            group_id='run_custom_report',
            report_params=lambda:{
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_actual_vs_normalized_report_details')['uri'],
                        "filterValues": rail.result('log_reportfilterfor_custom_base_report1_43'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
        )

        trigger_child_custom_actuals_vs_normalized_report=rail.TriggerDagRunOperator(
            task_id='trigger_child_custom_actuals_vs_normalized_report',
            retries=0,
            trigger_dag_id=f'crjg3_normalized_report_export_custom_actuals_vs_normalized_report_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "datefield": datetime.strptime(dag_run.conf['webhook']['data']['dateRange'].split('-')[0],'%m%d%Y').strftime('%d/%m/%Y') + ' - ' +
                    datetime.strptime(dag_run.conf['webhook']['data']['dateRange'].split('-')[-1],'%m%d%Y').strftime('%d/%m/%Y'),
                "userid": dag_run.conf['webhook']['data']['userIds'],
                "username": rail.result('get_user_details_6')['firstName'],
                "emailid": rail.result('log_emailsids_forlogs_9'),
                "reportresult": rail.result('run_custom_report.get_report_result'),
                "requesterid": dag_run.conf['webhook']['data']['requestorid']
            }
        )

        wait_for_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_custom_actuals_vs_normalized_report") }}'
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_runtime_4
        log_runtime_4 >> log_useruri_5 >> get_user_details_6 >> if_requestor_emailaddress_not_present
        if_requestor_emailaddress_not_present >> rail.Label('Yes')  >> finish
        if_requestor_emailaddress_not_present >> rail.Label('No') >> log_emailsids_forlogs_9 >> if_payload_daterange_contains_null_10
        if_payload_daterange_contains_null_10 >> rail.Label('Yes')  >> send_mail_incorrect_date_range >> finish
        if_payload_daterange_contains_null_10 >> rail.Label('No') >> get_assigned_permission_sets_for_user2_13 >> log_checkifrequiredpermissionisavailable_14
        log_checkifrequiredpermissionisavailable_14 >> if_log_checkifrequiredpermissionisavailable_14_blank_15
        if_log_checkifrequiredpermissionisavailable_14_blank_15 >> rail.Label('Yes')  >> finish
        if_log_checkifrequiredpermissionisavailable_14_blank_15 >> rail.Label(
            'No') >> get_actual_vs_normalized_report_details >> if_log_actualsvs_normalized_report_baseereport1uri_18_blank_19
        if_log_actualsvs_normalized_report_baseereport1uri_18_blank_19 >> rail.Label('Yes')  >> fail_with_error >> finish
        if_log_actualsvs_normalized_report_baseereport1uri_18_blank_19 >> rail.Label('No') >> if_columnconfiguration_doesnt_match
        if_columnconfiguration_doesnt_match >> rail.Label('Yes')  >> fail_cause_of_column_mismatch >> finish
        if_columnconfiguration_doesnt_match >> rail.Label(
            'No') >> impersonate_and_create_interactive_session_24 >> get_report_filter_uris >> if_payload_approvalid_present_33
        if_payload_approvalid_present_33 >> rail.Label('Yes')  >> get_approvalstatus_filter_options >> if_payload_userid_present_38
        if_payload_approvalid_present_33 >> rail.Label('No') >> if_payload_userid_present_38
        if_payload_userid_present_38 >> rail.Label(
            'Yes') >> get_user_filter_options >> get_timesheet_period_filters >> log_reportfilterfor_custom_base_report1_43
        if_payload_userid_present_38 >> rail.Label(
            'No') >> get_timesheet_period_filters >> log_reportfilterfor_custom_base_report1_43 >> run_custom_report
        run_custom_report >> trigger_child_custom_actuals_vs_normalized_report >> wait_for_child >> finish

    return dag

rail.for_each_instance(create_dag)
