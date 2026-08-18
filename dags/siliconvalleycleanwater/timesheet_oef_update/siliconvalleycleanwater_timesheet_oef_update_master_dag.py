
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'siliconvalleycleanwater_timesheet_oef_update_master_{config.instance}',
        description=f'SiliconValleyCleanWater_Timesheet_OEF_Update_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        webhook_conf=[rail.WebhookConf(
            hmac_secret_var=f'siliconvalleycleanwater_timesheet_oef_update_webook_{config.instance}_secret')]
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_valid_webhookevent'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_valid_webhookevent',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        is_valid_webhookevent = rail.IfOperator(
            task_id = "is_valid_webhookevent",
            test = "{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] in ['TimesheetStatusChangedToWaiting']}}",
            yes_task="get_waiting_on_approver",
            no_task= "fail_invalid_webhookevent"
        )

        fail_invalid_webhookevent = rail.FailOperator(
            task_id = "fail_invalid_webhookevent",
            message= "Received invalid webhook trigger event: '{{dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type']}}'"
        )

        get_waiting_on_approver = rail.RepliconServiceOperator(
            task_id='get_waiting_on_approver',
            endpoint='/services/TimesheetApprovalService1.svc/GetCurrentlyWaitingOnApprovers',
            data=lambda dag_run: {
                "timesheetUri": dag_run.conf['webhook']['data']['timesheet']['uri']
            }
        )

        if_waiting_approver_not_repliconintegration=rail.IfOperator(
            task_id='if_waiting_approver_not_repliconintegration',
            test=lambda :  not bool(rail.find_first_by_attr_and_get_attr(rail.result('get_waiting_on_approver'),'displayText','replicon integration','uri','')),
            yes_task="finish",
            no_task="declare_logger_list",
        )

        declare_logger_list=rail.SetVariableOperator(
            task_id='declare_logger_list',
            append=False,
            name='Logger',
            value=[]
        )

        get_approval_status = rail.RepliconServiceOperator(
            task_id='get_approval_status',
            endpoint='/services/TimesheetApprovalService1.svc/GetTimesheetApprovalDetails2',
            data=lambda dag_run: {
                "timesheetUri": dag_run.conf['webhook']['data']['timesheet']['uri']
            }
        )

        get_timesheet_details=rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data=lambda dag_run: {
                "timesheetUri": dag_run.conf['webhook']['data']['timesheet']['uri']
            }
        )

        is_status_approved_or_waiting=rail.IfOperator(
            task_id='is_status_approved_or_waiting',
            test=lambda: bool(rail.result('get_approval_status')['approvalStatus']['displayText'] in ['Waiting for Approval','Approved']),
            yes_task="reopen_timesheet",
            no_task="get_timesheet_daterange",
        )

        reopen_timesheet=rail.RepliconServiceOperator(
            task_id='reopen_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": "{{ dag_run.conf.webhook.data.timesheet.uri}}",
                "unitOfWorkId": "Reopen_{{ dag_run_ecid() }}",
                "comments": "Reopened by Replicon Integration"
            }
        )

        def get_timesheetdaterange():
            daterange = rail.result('get_timesheet_details')['dateRange']
            startdate = daterange['startDate']
            enddate = daterange['endDate']
            return {
                "startdateday": startdate['day'],
                "startdatemonth": startdate['month'],
                "startdateyear": startdate['year'],
                "enddateday": enddate['day'],
                "enddatemonth": enddate['month'],
                "enddateyear": enddate['year']
            }

        get_timesheet_daterange=rail.PythonOperator(
            task_id='get_timesheet_daterange',
            python_callable= get_timesheetdaterange
        )

        def get_timesheet_period():
            timesheet_date_range = rail.result('get_timesheet_daterange')
            startdate = datetime.strptime(str(timesheet_date_range['startdateday'])+'/'+
                                    str(timesheet_date_range['startdatemonth'])+'/'+
                                    str(timesheet_date_range['startdateyear']),'%d/%m/%Y').strftime('%b %d, %Y')
            enddate = datetime.strptime(str(timesheet_date_range['enddateday'])+'/'+
                                    str(timesheet_date_range['enddatemonth'])+'/'+
                                    str(timesheet_date_range['enddateyear']),'%d/%m/%Y').strftime('%b %d, %Y')
            return startdate + ' - ' + enddate

        log_timesheet_period = rail.PythonOperator(
            task_id='log_timesheet_period',
            python_callable= get_timesheet_period
        )

        get_time_entries_for_user_and_date_range=rail.RepliconServiceOperator(
            task_id='get_time_entries_for_user_and_date_range',
            endpoint="/services/TimeEntryService3.svc/GetTimeEntriesForUserAndDateRange",
            data={
            "user": {
                "uri": " {{ result('get_timesheet_details').owner.uri }}",
                "loginName": null,
                "parameterCorrelationId": null
            },
            "dateRange": {
                "startDate": {
                "year": "{{ result('get_timesheet_daterange').startdateyear }}" ,
                "month": "{{ result('get_timesheet_daterange').startdatemonth }}",
                "day": "{{ result('get_timesheet_daterange').startdateday }}" 
                },
                "endDate": {
                "year": "{{ result('get_timesheet_daterange').enddateyear }}",
                "month": "{{ result('get_timesheet_daterange').enddatemonth }}",
                "day": "{{ result('get_timesheet_daterange').enddateday }}"
                },
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "asOf": null
            }
        )
        get_time_entries=rail.PythonOperator(
            task_id='get_time_entries',
            python_callable= lambda: list(map(lambda entry: {
                'rownumber': rail.find_first_by_attr_and_get_attr(entry['customMetadata'],'keyUri','urn:replicon:widget-ui-metadata-key:row-number',
                                'value.number',null),
                'taskuri': rail.find_first_by_attr_and_get_attr(entry['customMetadata'],'keyUri','urn:replicon:widget-ui-metadata-key:task',
                                'value.uri',null),
                'projecturi': rail.find_first_by_attr_and_get_attr(entry['customMetadata'],'keyUri','urn:replicon:time-entry-metadata-key:project',
                                'value.uri',null),
                'entrydate': str(entry['entryDate']['day']) + '-' + str(entry['entryDate']['month']) + '-' + str(entry['entryDate']['year']),
                'physicallocation': rail.find_first_by_attr_and_get_attr(entry['extensionFieldValues'],'definition.displayText','Physical Location',
                                        'tag.displayText',null),
                'process': rail.find_first_by_attr_and_get_attr(entry['extensionFieldValues'],'definition.displayText','Process','tag.displayText',null),
                'timeentryuri': entry['revision']['revisionUri'],
                'revisiongroupuri': entry['revisionGroupUri']
            },rail.result('get_time_entries_for_user_and_date_range')))
            )

        create_timeentries_list_in_collection = rail.CreateCollectionOperator(
            task_id='create_timeentries_list_in_collection',
            source = "{{ result('get_time_entries') | to_json }}",
            name = "timeentries",
        )

        query_entries_with_taskuri=rail.QueryCollectionOperator(
            task_id='query_entries_with_taskuri',
            query="""SELECT * FROM  timeentries WHERE  timeentries.taskuri IS NOT NULL""",
        )

        query_entries_with_projecturi=rail.QueryCollectionOperator(
            task_id='query_entries_with_projecturi',
            query="""SELECT * FROM  timeentries WHERE  timeentries.projecturi IS NOT NULL""",
        )

        create_project_for_task_list=rail.SetVariableOperator(
            task_id='create_project_for_task_list',
            append=False,
            name='projectfortask',
            value=[]
        )

        if_entries_with_taskuri_are_there=rail.IfOperator(
            task_id='if_entries_with_taskuri_are_there',
            test='''{{ result('query_entries_with_taskuri', 'length') > 0 }}''',
            yes_task="for_each_taskuri_entry",
            no_task="create_getprojectfortask_list_collection",
        )

        for_each_taskuri_entry=rail.ForEachOperator(
            task_id='for_each_taskuri_entry',
            items="{{ result('query_entries_with_taskuri') }}",
            start_task = 'if_projectfortask_list_doesnt_have_rownumber',
            end_task = 'for_each_taskuri_entry_end'
        )

        def if_rownumber_contains(rownumber):
            result = rail.get_dag_run_var('projectfortask')
            return bool(rail.find_first_by_attr_and_get_attr(result,'rownumber',rownumber,'rownumber',''))

        if_projectfortask_list_doesnt_have_rownumber=rail.IfOperator(
            task_id='if_projectfortask_list_doesnt_have_rownumber',
            test= lambda: not if_rownumber_contains(rail.result('for_each_taskuri_entry')['rownumber']),
            yes_task="get_task_details",
            no_task="for_each_taskuri_entry_end",
        )

        get_task_details=rail.RepliconServiceOperator(
            task_id='get_task_details',
            endpoint="/services/TaskService1.svc/GetTaskDetails",
            data={
                "taskUri": "{{ result('for_each_taskuri_entry').taskuri }}"
            }
        )

        insert_to_projectfortask_list=rail.SetVariableOperator(
            task_id='insert_to_projectfortask_list',
            append=True,
            name='{{ result("create_project_for_task_list").name }}',
            value={
                "rownumber": "{{ result('for_each_taskuri_entry').rownumber }}",
                "taskuri": "{{ result('for_each_taskuri_entry').taskuri }}",
                "projecturi": "{{ result('get_task_details').project.uri }}",
                "entrydate": "{{ result('for_each_taskuri_entry').entrydate }}",
                "physicallocation": "{{ result('for_each_taskuri_entry').physicalocation }}",
                "process": "{{ result('for_each_taskuri_entry').process }}",
                "timeentryuri": "{{ result('for_each_taskuri_entry').timeentryuri }}",
                "revisiongroupuri": "{{ result('for_each_taskuri_entry').revisiongroupuri }}"
            }
        )

        for_each_taskuri_entry_end=rail.EmptyOperator(
            task_id='for_each_taskuri_entry_end',
        )

        create_getprojectfortask_list_collection = rail.CreateCollectionOperator(
            task_id='create_getprojectfortask_list_collection',
            source = lambda: rail.get_dag_run_var('projectfortask') if rail.get_dag_run_var('projectfortask') else [],
            name = "getprojectfortaskcollection",
            columns={
                "rownumber",
                "taskuri",
                "projecturi",
                "entrydate" ,
                "physicallocation",
                "process",
                "timeentryuri",
                "revisiongroupuri",
            }
        )

        query_list_merged_data=rail.QueryCollectionOperator(
            task_id='query_list_merged_data',
            query="""SELECT * FROM  timeentries WHERE  timeentries.projecturi IS NOT NULL UNION SELECT * FROM  getprojectfortaskcollection""",
        )

        get_effective_user_group_membership=rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ result('get_timesheet_details').owner.uri }}",
                "dateRange": null
            }
        )

        if_division_present = rail.IfOperator(
            task_id = 'if_division_present',
            test=lambda: bool(rail.result('get_effective_user_group_membership')['divisions']),
            yes_task='get_division_details',
            no_task ='get_user_details'
        )

        get_division_details=rail.RepliconServiceOperator(
            task_id='get_division_details',
            endpoint="/services/DivisionService1.svc/GetDivisionDetails",
            data=lambda: {
                "divisionUri": rail.result('get_effective_user_group_membership')['divisions'][0]['division']['division']['uri'] if
                                rail.result('get_effective_user_group_membership')['divisions'] else null
            }
        )

        get_user_details=rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/UserService1.svc/GetUserDetails',
            data={
                "userUri": "{{ result('get_timesheet_details').owner.uri }}"
            }
        )

        log_expense_code_value=rail.PythonOperator(
            task_id='log_expense_code_value',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_user_details')['customFieldValues'],'customField.displayText','Expense Code','text','')
        )

        get_all_object_extension_field_projects=rail.RepliconServiceOperator(
            task_id='get_all_object_extension_field_projects',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:time-entry"
            }
        )

        get_client_details_for_user_choice_projects=rail.RepliconServiceOperator(
            task_id='get_client_details_for_user_choice_projects',
            endpoint="/services/TimeEntryRevisionGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "200",
                "columnUris": [
                    "urn:replicon:time-entry-revision-group-list-column:client",
                    "urn:replicon:time-entry-revision-group-list-column:time-entry-revision-group"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-entry-revision-group-list-filter:date-range"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                        "uri": null,
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": {
                            "startDate": {
                            "year": "{{ result('get_timesheet_daterange').startdateyear }}",
                            "month": "{{ result('get_timesheet_daterange').startdatemonth }}",
                            "day": "{{ result('get_timesheet_daterange').startdateday }}"
                            },
                            "endDate": {
                            "year": "{{ result('get_timesheet_daterange').enddateyear }}",
                            "month": "{{ result('get_timesheet_daterange').enddatemonth }}",
                            "day": "{{ result('get_timesheet_daterange').enddateday }}"
                            },
                            "relativeDateRangeUri": null,
                            "relativeDateRangeAsOfDate": null
                        },
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null
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
                        "filterDefinitionUri": "urn:replicon:time-entry-revision-group-list-filter:user"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                        "uri": "{{ result('get_user_details').uri}}",
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
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null
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

        get_client_data_for_user_choice=rail.PythonOperator(
            task_id='get_client_data_for_user_choice',
            python_callable= lambda: [ {
                'Clientname': row['cells'][0]['textValue'] if 'textValue' in list(row['cells'][0].keys()) else '',
                'Clienturi': row['cells'][0]['uri'] if 'uri' in list(row['cells'][0].keys()) else '',
                'timeentryrevisionuri': row['cells'][1]['uri'] if 'uri' in list(row['cells'][1].keys()) else ''
            } for row in rail.result('get_client_details_for_user_choice_projects')['rows']]
        )

        foreach_query_list_merged_data=rail.ForEachOperator(
            task_id='foreach_query_list_merged_data',
            items="{{ result('query_list_merged_data') }}",
            start_task = 'get_project_details',
            end_task = 'foreach_query_list_merged_data_end'
        )

        get_project_details=rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/GetProjectDetails',
            data={
                "projectUri": "{{ result('foreach_query_list_merged_data').projecturi}}"
            }
        )

        if_program_displaytext_equals_projects=rail.IfOperator(
            task_id='if_program_displaytext_equals_projects',
            test= lambda: bool(rail.result('get_project_details') and rail.result('get_project_details')['program']
                    and rail.result('get_project_details')['program']['displayText'] == 'Projects'),
            yes_task="if_code_format_incorrect_or_not_present",
            no_task="if_divisiondetails_code_not_present",
        )

        if_code_format_incorrect_or_not_present=rail.IfOperator(
            task_id='if_code_format_incorrect_or_not_present',
            test= lambda: bool(len(rail.result('get_project_details')['code'].split('-')) < 4 or
                               not rail.result('get_project_details')['code'].split('-')[1]),
            yes_task="insert_log_code_format_incorrect",
            no_task="if_divisiondetails_code_not_present",
        )

        insert_log_code_format_incorrect=rail.SetVariableOperator(
            task_id='insert_log_code_format_incorrect',
            append=True,
            name='{{ result("declare_logger_list").name }}',
            value=lambda: {
                "user": rail.result('get_timesheet_details')['owner']['displayText'],
                "timesheetperiod": rail.result('log_timesheet_period'),
                "entrydate": rail.result('foreach_query_list_merged_data')['entrydate'],
                "status": "Failed",
                "details": "The project code for the project -" +
                            rail.result('get_project_details')['client']['name'] + " is not present or not in correct format"
            }
        )

        if_divisiondetails_code_not_present=rail.IfOperator(
            task_id='if_divisiondetails_code_not_present',
            test=lambda: not bool( rail.result('get_division_details') and rail.result('get_division_details')['code'] ),
            yes_task="insert_log_code_not_present",
            no_task="if_physical_location_not_present",
        )

        insert_log_code_not_present=rail.SetVariableOperator(
            task_id='insert_log_code_not_present',
            append=True,
            name='{{ result("declare_logger_list").name }}',
            value=lambda: {
                "user": rail.result('get_timesheet_details')['owner']['displayText'],
                "timesheetperiod": rail.result('log_timesheet_period'),
                "entrydate": rail.result('foreach_query_list_merged_data')['entrydate'],
                "status": "Failed",
                "details": "The project code for the project -" +
                            rail.result('get_project_details')['client']['name'] + " is not present or not in correct format"
            }
        )

        if_physical_location_not_present=rail.IfOperator(
            task_id='if_physical_location_not_present',
            test= lambda:  (not rail.result('foreach_query_list_merged_data')['physicallocation']) or
                    len(rail.result('foreach_query_list_merged_data')['physicallocation'].split('-')) < 2 or
                    (not rail.result('foreach_query_list_merged_data')['physicallocation'].split('-')[-1]),
            yes_task="insert_log_process_not_present",
            no_task="if_process_not_present",
        )

        insert_log_process_not_present=rail.SetVariableOperator(
            task_id='insert_log_process_not_present',
            append=True,
            name='{{ result("declare_logger_list").name }}',
            value=lambda: {
                "user": rail.result('get_timesheet_details')['owner']['displayText'],
                "timesheetperiod": rail.result('log_timesheet_period'),
                "entrydate": rail.result('foreach_query_list_merged_data')['entrydate'],
                "status": "Failed",
                "details": "Process is not present or not in correct format for the project- " + rail.result('get_project_details')['client']['name']
            }
        )

        if_process_not_present=rail.IfOperator(
            task_id='if_process_not_present',
            test=lambda: (not rail.result('foreach_query_list_merged_data')['process']) or
                    len(rail.result('foreach_query_list_merged_data')['process'].split('-')) < 2 or
                    (not rail.result('foreach_query_list_merged_data')['process'].split('-')[-1]),
            yes_task="insert_process_not_present",
            no_task="if_expense_code_value_not_present",
        )

        insert_process_not_present=rail.SetVariableOperator(
            task_id='insert_process_not_present',
            append=True,
            name='{{ result("declare_logger_list").name }}',
            value=lambda: {
                "user": rail.result('get_timesheet_details')['owner']['displayText'],
                "timesheetperiod": rail.result('log_timesheet_period'),
                "entrydate": rail.result('foreach_query_list_merged_data')['entrydate'],
                "status": "Failed",
                "details": "Process is not present or not in correct format for the project- " + rail.result('get_project_details')['client']['name']
            }
        )

        if_expense_code_value_not_present=rail.IfOperator(
            task_id='if_expense_code_value_not_present',
            test='''{{ result('log_expense_code_value') | is_falsy }}''',
            yes_task="insert_log_expensecode_value_not_present",
            no_task="if_logger_list_empty",
        )

        insert_log_expensecode_value_not_present=rail.SetVariableOperator(
            task_id='insert_log_expensecode_value_not_present',
            append=True,
            name='{{ result("declare_logger_list").name }}',
            value=lambda: {
                "user": rail.result('get_timesheet_details')['owner']['displayText'],
                "timesheetperiod": rail.result('log_timesheet_period'),
                "entrydate": rail.result('foreach_query_list_merged_data')['entrydate'],
                "status": "Failed",
                "details": "Expense Code is not present for the user"
            }
        )

        if_logger_list_empty=rail.IfOperator(
            task_id='if_logger_list_empty',
            test=lambda: not(rail.get_dag_run_var('Logger')),
            yes_task="if_program_displaytext_equals_to_projects",
            no_task="foreach_query_list_merged_data_end",
        )

        if_program_displaytext_equals_to_projects=rail.IfOperator(
            task_id='if_program_displaytext_equals_to_projects',
            test= lambda: bool(rail.result('get_project_details') and rail.result('get_project_details')['program']
                    and rail.result('get_project_details')['program']['displayText'] == 'Projects'),
            yes_task="update_object_extension_field_value",
            no_task="if_program_displaytext_equals_to_workorders",
        )

        update_object_extension_field_value=rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data=lambda: {
            "objectUri": rail.result('foreach_query_list_merged_data')['timeentryuri'],
            "value": {
                "definition": {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_projects'),'name', "GL Account Number",'uri'),
                "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": str(rail.result('get_project_details')['code'].split('-')[1]).strip() + '-' +
                                str(rail.result('get_division_details')['code']).strip() +
                                '-' + str(rail.result('foreach_query_list_merged_data')['physicallocation'].split('-')[-1]).strip() + '-' +
                                str(rail.result('foreach_query_list_merged_data')['process'].split('-')[-1]).strip() + '-' +
                                str(rail.result('log_expense_code_value')).strip() + '-' +
                                str(rail.result('get_project_details')['code'].split('-')[-1]).strip(),
                "fileValue": null,
                "jsonValue": null
            }
            }
        )

        if_program_displaytext_equals_to_workorders=rail.IfOperator(
            task_id='if_program_displaytext_equals_to_workorders',
            test=lambda: bool(rail.result('get_project_details') and rail.result('get_project_details')['program']
                    and rail.result('get_project_details')['program']['displayText'] == 'Work Orders'),
            yes_task="if_clienturi_for_timeentryrevisionuri_present",
            no_task="insert_log_not_project_workorder",
        )

        if_clienturi_for_timeentryrevisionuri_present=rail.IfOperator(
            task_id='if_clienturi_for_timeentryrevisionuri_present',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_client_data_for_user_choice'),
                                'timeentryrevisionuri',rail.result('foreach_query_list_merged_data')['revisiongroupuri'],
                                'Clienturi','')),
            yes_task="get_client_details",
            no_task="insert_log_client_not_selected_for_workorder",
        )

        get_client_details=rail.RepliconServiceOperator(
            task_id='get_client_details',
            endpoint='/services/ClientService1.svc/GetClientDetails',
            data=lambda: {
                "clientUri": rail.find_first_by_attr_and_get_attr(rail.result('get_client_data_for_user_choice'),
                                'timeentryrevisionuri',rail.result('foreach_query_list_merged_data')['revisiongroupuri'],
                                'Clienturi')
            }
        )

        if_code_in_client_details_present=rail.IfOperator(
            task_id='if_code_in_client_details_present',
            test=lambda: bool(rail.result('get_client_details') and rail.result('get_client_details')['code']),
            yes_task="update_objectextension_fieldvalue",
            no_task="insert_log_client_code_not_present",
        )

        update_objectextension_fieldvalue=rail.RepliconServiceOperator(
            task_id='update_objectextension_fieldvalue',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data=lambda: {
            "objectUri": rail.result('foreach_query_list_merged_data')['timeentryuri'],
            "value": {
                "definition": {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_projects'),'name', "GL Account Number",'uri'),
                "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": str(rail.result('get_client_details')['code']).strip() + '-' + str(rail.result('get_division_details')['code']).strip() +
                                '-' + str(rail.result('foreach_query_list_merged_data')['physicallocation'].split('-')[-1]).strip() + '-' +
                                str(rail.result('foreach_query_list_merged_data')['process'].split('-')[-1]).strip() + '-' +
                                str(rail.result('log_expense_code_value')).strip(),
                "fileValue": null,
                "jsonValue": null
            }
            }
        )

        insert_log_client_code_not_present=rail.SetVariableOperator(
            task_id='insert_log_client_code_not_present',
            append=True,
            name='{{ result("declare_logger_list").name }}',
            value=lambda: {
                "user": rail.result('get_timesheet_details')['owner']['displayText'],
                "timesheetperiod": rail.result('log_timesheet_period'),
                "entrydate": rail.result('foreach_query_list_merged_data')['entrydate'],
                "status": "Failed",
                "details": "The Client code is not present for the project - " + rail.result('get_project_details')['client']['name']
            }
        )

        insert_log_client_not_selected_for_workorder=rail.SetVariableOperator(
            task_id='insert_log_client_not_selected_for_workorder',
            append=True,
            name='{{ result("declare_logger_list").name }}',
            value=lambda: {
                "user": rail.result('get_timesheet_details')['owner']['displayText'],
                "timesheetperiod": rail.result('log_timesheet_period'),
                "entrydate": rail.result('foreach_query_list_merged_data')['entrydate'],
                "status": "Failed",
                "details": "The Client is not selected for Work Order type project - " + rail.result('get_project_details')['client']['name']
            }
        )

        insert_log_not_project_workorder=rail.SetVariableOperator(
            task_id='insert_log_not_project_workorder',
            append=True,
            name='{{ result("declare_logger_list").name }}',
            value=lambda: {
                "user": rail.result('get_timesheet_details')['owner']['displayText'],
                "timesheetperiod": rail.result('log_timesheet_period'),
                "entrydate": rail.result('foreach_query_list_merged_data')['entrydate'],
                "status": "Failed",
                "details": "The Program assigned is not Project or Work Order for the project - " + rail.result('get_project_details')['client']['displayText']
            }
        )

        foreach_query_list_merged_data_end=rail.EmptyOperator(
            task_id='foreach_query_list_merged_data_end',
        )

        def get_headers(res):
            data = res.json()['d']
            auth_token = list(
                filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
            tenant = list(
                filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
            return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}

        impersonate_and_create_interactive_session=rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{result('get_waiting_on_approver')[0].uri}}"
            },
            response_filter = get_headers
        )

        if_logger_list_has_data=rail.IfOperator(
            task_id='if_logger_list_has_data',
            test=lambda: bool(rail.get_dag_run_var('Logger')),
            yes_task="get_logger_list_value",
            no_task="submit_timesheet",
        )

        def get_logger_list():
            result = rail.get_dag_run_var('Logger')
            return {
                'entrydate':result[0]['entrydate'],
                'status':result[0]['status'],
                'reason':result[0]['details']
            }

        get_logger_list_value = rail.PythonOperator(
            task_id = 'get_logger_list_value',
            python_callable=get_logger_list

        )

        send_mail=rail.EmailOperator(
            task_id='send_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''SiliconValleyCleanWater - Timesheet GL Account Number Update failed for - {{result('get_timesheet_details').owner.displayText}}''',
            html_content= '/templates/failure_mail.html',
        )

        submit_timesheet=rail.RepliconServiceOperator(
            task_id='submit_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data={
                "timesheetUri": "{{ dag_run.conf.webhook.data.timesheet.uri}}",
                "unitOfWorkId": "{{ current_time() }}",
                "comments": "Submitted by Replicon Integration after OEF Update",
                "changeReason": null
            },
            headers= lambda: rail.result('impersonate_and_create_interactive_session')
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> is_valid_webhookevent
        is_valid_webhookevent >> rail.Label('Yes') >> get_waiting_on_approver >> if_waiting_approver_not_repliconintegration
        is_valid_webhookevent >> rail.Label('No') >> fail_invalid_webhookevent >> finish
        if_waiting_approver_not_repliconintegration
        if_waiting_approver_not_repliconintegration >> rail.Label('Yes')  >> finish
        if_waiting_approver_not_repliconintegration >> rail.Label(
            'No') >> declare_logger_list >> get_approval_status >> get_timesheet_details >> is_status_approved_or_waiting
        is_status_approved_or_waiting >> rail.Label('Yes')  >> reopen_timesheet >> get_timesheet_daterange
        is_status_approved_or_waiting >> rail.Label(
            'No') >> get_timesheet_daterange >> log_timesheet_period >> get_time_entries_for_user_and_date_range >> get_time_entries
        get_time_entries >> create_timeentries_list_in_collection >> query_entries_with_taskuri >> query_entries_with_projecturi
        query_entries_with_projecturi >> create_project_for_task_list >> if_entries_with_taskuri_are_there
        if_entries_with_taskuri_are_there >> rail.Label('Yes')  >> for_each_taskuri_entry >> if_projectfortask_list_doesnt_have_rownumber
        if_projectfortask_list_doesnt_have_rownumber >> rail.Label('Yes')  >> get_task_details >> insert_to_projectfortask_list >> for_each_taskuri_entry_end
        if_projectfortask_list_doesnt_have_rownumber >> rail.Label('No') >> for_each_taskuri_entry_end
        for_each_taskuri_entry >> for_each_taskuri_entry_end >> create_getprojectfortask_list_collection
        if_entries_with_taskuri_are_there >> rail.Label(
            'No') >> create_getprojectfortask_list_collection >> query_list_merged_data >> get_effective_user_group_membership >> if_division_present
        if_division_present >> rail.Label('Yes') >> get_division_details
        if_division_present >> rail.Label('No') >> get_user_details
        get_division_details >> get_user_details >> log_expense_code_value >> get_all_object_extension_field_projects
        get_all_object_extension_field_projects >> get_client_details_for_user_choice_projects >> get_client_data_for_user_choice
        get_client_data_for_user_choice >> foreach_query_list_merged_data >> get_project_details >> if_program_displaytext_equals_projects
        if_program_displaytext_equals_projects >> rail.Label('Yes')  >> if_code_format_incorrect_or_not_present
        if_code_format_incorrect_or_not_present >> rail.Label('Yes')  >> insert_log_code_format_incorrect >> if_divisiondetails_code_not_present
        if_code_format_incorrect_or_not_present >> rail.Label('No') >> if_divisiondetails_code_not_present
        if_program_displaytext_equals_projects >> rail.Label('No') >> if_divisiondetails_code_not_present
        if_divisiondetails_code_not_present >> rail.Label('Yes')  >> insert_log_code_not_present >> if_physical_location_not_present
        if_divisiondetails_code_not_present >> rail.Label('No') >> if_physical_location_not_present
        if_physical_location_not_present >> rail.Label('Yes')  >> insert_log_process_not_present >> if_process_not_present
        if_physical_location_not_present >> rail.Label('No') >> if_process_not_present
        if_process_not_present >> rail.Label('Yes')  >> insert_process_not_present >> if_expense_code_value_not_present
        if_process_not_present >> rail.Label('No') >> if_expense_code_value_not_present
        if_expense_code_value_not_present >> rail.Label('Yes')  >> insert_log_expensecode_value_not_present >> if_logger_list_empty
        if_expense_code_value_not_present >> rail.Label('No') >> if_logger_list_empty
        if_logger_list_empty >> rail.Label('Yes')  >> if_program_displaytext_equals_to_projects
        if_program_displaytext_equals_to_projects >> rail.Label('Yes')  >> update_object_extension_field_value >> foreach_query_list_merged_data_end
        if_program_displaytext_equals_to_projects >> rail.Label('No') >> if_program_displaytext_equals_to_workorders
        if_program_displaytext_equals_to_workorders >> rail.Label('Yes')  >> if_clienturi_for_timeentryrevisionuri_present
        if_clienturi_for_timeentryrevisionuri_present >> rail.Label('Yes')  >> get_client_details >> if_code_in_client_details_present
        if_code_in_client_details_present >> rail.Label('Yes')  >> update_objectextension_fieldvalue >> foreach_query_list_merged_data_end
        if_code_in_client_details_present >> rail.Label('No') >> insert_log_client_code_not_present >> foreach_query_list_merged_data_end
        if_clienturi_for_timeentryrevisionuri_present >> rail.Label('No') >> insert_log_client_not_selected_for_workorder >> foreach_query_list_merged_data_end
        if_program_displaytext_equals_to_workorders >> rail.Label('No')  >> insert_log_not_project_workorder >> foreach_query_list_merged_data_end
        if_logger_list_empty >> rail.Label('No') >> foreach_query_list_merged_data_end
        foreach_query_list_merged_data >> foreach_query_list_merged_data_end >> impersonate_and_create_interactive_session >> if_logger_list_has_data
        if_logger_list_has_data >> rail.Label('Yes')  >> get_logger_list_value >> send_mail >> finish
        if_logger_list_has_data >> rail.Label('No') >> submit_timesheet >> finish

    return dag

rail.for_each_instance(create_dag)
