from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'wolverinepipeline_custom_paychex_master_{config.instance}',
        description=f'Wolverinepipeline_custom_paychex_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_activities'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_activities',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_activities = rail.RepliconServiceOperator(
            task_id='get_all_activities',
            endpoint="/services/ActivityService1.svc/GetAllActivities"
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id='get_all_reports',
            endpoint="/services/reportService1.svc/GetAllReports",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, "displayText", config.report_name, "uri")
        )

        get_report_details2 = rail.RepliconServiceOperator(
            task_id='get_report_details2',
            endpoint="/services/reportService1.svc/GetReportDetails2",
            data={
                "reportUri": "{{result('get_all_reports')}}"
            }
        )

        log_approval_status_filteruri = rail.PythonOperator(
            task_id='log_approval_status_filteruri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalStatusFilter', 'uri', '')
        )

        log_timesheet_period_filter_uri = rail.PythonOperator(
            task_id='log_timesheet_period_filter_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'TimesheetPeriodFilter', 'uri', '')
        )

        generate_report_group = rail.run_report2(
            group_id='generate_report_group',
            report_params=lambda dag_run: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_all_reports'),
                        "filterValues": [
                            {
                                "reportFilterUri": rail.result('log_approval_status_filteruri'),
                                "value": "2"
                            },
                            {
                                "reportFilterUri": rail.result('log_timesheet_period_filter_uri'),
                                "value": "LastTimesheetPeriod"
                            },
                            {
                                "reportFilterUri": rail.result('log_timesheet_period_filter_uri'),
                                "value": null
                            },
                            {
                                "reportFilterUri": rail.result('log_timesheet_period_filter_uri'),
                                "value": null
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        if_payload_has_data = rail.IfOperator(
            task_id='if_payload_has_data',
            test='''{{ result('generate_report_group.get_report_result','has_data')}}''',
            yes_task="load_csv",
            no_task="send_nodata_mail",
        )

        send_nodata_mail = rail.EmailOperator(
            task_id='send_nodata_mail',
            to="{{dag_run.conf.emailid}}",
            subject='''{{get_company_key()}} | Paychex Export - No data to export  - {{ current_time("%Y-%m-%eT%H:%M%S.%f") }} ''',
            html_content="templates/emails/nodata_mail.html"
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id="load_csv",
            document="{{result('generate_report_group.get_report_result').reportGenerationResults[0].payload}}",
        )

        create_collection_create_list_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv',
            source="{{ result('load_csv') }}",
            name="rawdata",
            columns={
                'Employee ID': 'employeeid',
                'Activity Code': 'activitycode',
                'Login Name': 'loginname',
                'Employee Type': 'employeetype',
                'Hours Worked': 'hoursworked',
                'Time Off Type': 'timeofftype',
                'Time Off Hrs': 'timeoffhours',
                'Entry Date': 'entrydate',
                'Activity Name': 'activityname',
                'PT - C': 'pt_c',
                'PT - E': 'pt_e',
                'PT - H': 'pt_h',
                'PT - NE': 'pt_ne',
                'PT - PT': 'pt_pt',
                'OT Meal Allocation': 'otmealallocation',
                'OT Meal': 'otmeal',
                'TimesheetPeriodUri': 'timesheeturi',
                'UserUri': 'useruri'
            }
        )

        query_to_sort_raw_data = rail.QueryCollectionOperator(
            task_id='query_to_sort_raw_data',
            query="""SELECT * FROM rawdata WHERE NULLIF(employeeid,'') IS NOT NULL AND NULLIF(loginname,'') IS NOT NULL ORDER BY rawdata.employeeid ASC, rawdata.hoursworked DESC """,
        )

        get_paycodelist = rail.RepliconServiceOperator(
            task_id='get_paycodelist',
            endpoint="/services/PayCodeListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:pay-code-list-column:name",
                    "urn:replicon:pay-code-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        for_each_in_rows_do = rail.ForEachOperator(
            task_id='for_each_in_rows_do',
            items=lambda: rail.result('get_paycodelist')['rows'] if rail.result(
                'get_paycodelist') else [],
            start_task='accumulate_paycode_list',
            end_task='for_each_in_rows_do_end'
        )

        accumulate_paycode_list = rail.SetVariableOperator(
            task_id='accumulate_paycode_list',
            name='PayCode Codes',
            append=True,
            value=lambda: {
                "paycodename": rail.result('for_each_in_rows_do')['cells'][0]['textValue'],
                "paycodecode": rail.result('for_each_in_rows_do')['cells'][1]['textValue']
            }
        )

        for_each_in_rows_do_end = rail.EmptyOperator(
            task_id='for_each_in_rows_do_end',
        )

        get_employeetypedetails = rail.RepliconServiceOperator(
            task_id='get_employeetypedetails',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails"
        )

        for_each_in_list_do = rail.ForEachOperator(
            task_id='for_each_in_list_do',
            items=lambda: rail.result('get_employeetypedetails') if rail.result(
                'get_employeetypedetails') else [],
            start_task='accumulate_employeetype_list',
            end_task='for_each_in_list_do_end'
        )

        accumulate_employeetype_list = rail.SetVariableOperator(
            task_id='accumulate_employeetype_list',
            name='EmployeeTypeDetails',
            append=True,
            value=lambda: {
                "employeetype": rail.result('for_each_in_list_do')['name'],

                "otmealallocation": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'OT Meal Allocation', 'text', ''),

                "otmealpaycode": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'OT Meal Pay Code', 'text', ''),

                "personaltimelevel3_paid": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Personal Time Level 3 (Paid)', 'text', ''),

                "personaltimelevel2_unpaid": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Personal Time Level 2 (Unpaid)', 'text', ''),

                "personaltimelevel1_paid": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Personal Time Level 1 (Paid)', 'text', ''),

                "shorttermdisability_1_2_pay_workrelated": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Short Term Disability (1/2 Pay, Work Related)', 'text', ''),

                "shorttermdisability_1_2_pay_non_workrelated": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Short Term Disability (1/2 Pay, Non-Work Related)', 'text', ''),

                "shorttermdisability_fullpay_workrelated": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Short Term Disability (Full Pay, Work Related)', 'text', ''),

                "electiveholiday_notimeoff_additionalpay": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Elective Holiday : (No Time Off - Additional Pay)', 'text', ''),

                "vacation": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Vacation', 'text', ''),

                "shorttermdisability_fullpay_non_workrelated": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Short Term Disability (Full Pay, Non-Work Related)', 'text', ''),

                "electiveholiday": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Elective Holiday', 'text', ''),

                "companyholiday": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Company Holiday', 'text', ''),

                "electiveholiday_notworked_includeinsalary": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Elective Holiday : (Not Worked - Include in Salary)', 'text', ''),

                "companyholiday_notworked_includeinsalary": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Company Holiday : (Not Worked - Included in Salary)', 'text', ''),

                "companyholiday_notscheduled_notworked_additionalpay": rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_list_do')['customFields'], 'customField.displayText', 'Company Holiday : (Not Scheduled Not Worked - Additional Pay)', 'text', '')
            }
        )

        for_each_in_list_do_end = rail.EmptyOperator(
            task_id='for_each_in_list_do_end',
        )

        get_all_objects = rail.RepliconServiceOperator(
            task_id='get_all_objects',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:time-entry"
            }
        )

        for_each_in_list = rail.ForEachOperator(
            task_id='for_each_in_list',
            items=lambda: rail.result('get_all_objects') if rail.result(
                'get_all_objects') else [],
            start_task='if_name_startswith_PT',
            end_task='for_each_in_list_end'
        )

        if_name_startswith_PT = rail.IfOperator(
            task_id='if_name_startswith_PT',
            test='''{{ result('for_each_in_list').name | starts_with('PT -')}}''',
            yes_task="get_object_definition_details",
            no_task="for_each_in_list_end",
        )

        get_object_definition_details = rail.RepliconServiceOperator(
            task_id='get_object_definition_details',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{result('for_each_in_list').uri}}"
            }
        )

        for_each_item_in_list_do = rail.ForEachOperator(
            task_id='for_each_item_in_list_do',
            items=lambda: rail.result('get_object_definition_details')['tags'] if rail.result(
                'get_object_definition_details') else [],
            start_task='if_isenabled_true',
            end_task='for_each_item_in_list_do_end'
        )

        if_isenabled_true = rail.IfOperator(
            task_id='if_isenabled_true',
            test="{{ result('for_each_item_in_list_do').isEnabled | is_truthy}}",
            yes_task="accumulate_object_list",
            no_task="for_each_item_in_list_do_end",
        )

        accumulate_object_list = rail.SetVariableOperator(
            task_id='accumulate_object_list',
            name='PT - Codes',
            append=True,
            value=lambda: {
                "PT**": rail.result('get_object_definition_details')['name'],
                "dropdownoption": rail.result('for_each_item_in_list_do')['name'],
                "dropdowncode": rail.result('for_each_item_in_list_do')['code'] if rail.result('for_each_item_in_list_do')['code'] else ""
            }
        )

        for_each_item_in_list_do_end = rail.EmptyOperator(
            task_id='for_each_item_in_list_do_end'
        )

        for_each_in_list_end = rail.EmptyOperator(
            task_id='for_each_in_list_end'
        )

        reportdata_lookuptable = rail.CreateLogOperator(
            task_id='reportdata_lookuptable'
        )

        for_each_item_in_query_do = rail.ForEachOperator(
            task_id='for_each_item_in_query_do',
            items="{{result('query_to_sort_raw_data')}}",
            start_task='log_required_employeetypedetails',
            end_task='for_each_item_in_query_do_end'
        )

        log_required_employeetypedetails = rail.PythonOperator(
            task_id='log_required_employeetypedetails',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_employeetypedetails'), 'name', rail.result('for_each_item_in_query_do')['employeetype'], 'customFields', '')
        )

        if_timetype_not_present = rail.IfOperator(
            task_id='if_timetype_not_present',
            test='''{{ result('for_each_item_in_query_do').timeofftype | is_falsy}}''',
            yes_task="if_hoursworked_is_present",
            no_task="if_timetype__present",
        )

        if_hoursworked_is_present = rail.IfOperator(
            task_id='if_hoursworked_is_present',
            test=lambda: float(rail.result('for_each_item_in_query_do')[
                               'hoursworked']) > 0,
            yes_task="add_entry_to_list",
            no_task="if_otmeal_is_present",
        )

        def payrate_data():
            record = rail.result('accumulate_object_list')['value'] if rail.result(
                'accumulate_object_list') else []
            data = rail.result('for_each_item_in_query_do')
            ddcode = (list(filter(lambda x: x['PT**'] == 'PT - C' and x['dropdownoption'] == data['pt_c'], record))) if data['pt_c'] else ((list(filter(lambda x: x['PT**'] == 'PT - E' and x['dropdownoption'] == data['pt_e'], record))) if data['pt_e'] else ((list(filter(lambda x: x['PT**'] == 'PT - H' and x['dropdownoption']
                                                                                                                                                                                                                                                                              == data['pt_h'], record))) if data['pt_h'] else ((list(filter(lambda x: x['PT**'] == 'PT - NE' and x['dropdownoption'] == data['pt_ne'], record))) if data['pt_ne'] else ((list(filter(lambda x: x['PT**'] == 'PT - PT' and x['dropdownoption'] == data['pt_pt'], record))) if data['pt_pt'] else ""))))
            return ddcode[0]['dropdowncode']

        add_entry_to_list = rail.WriteLogOperator(
            task_id='add_entry_to_list',
            log="{{result('reportdata_lookuptable')}}",
            severity='',
            message='na',
            properties=lambda: {
                'Empid': rail.result('for_each_item_in_query_do')['employeeid'],

                'Paycode': (rail.find_first_by_attr_and_get_attr(rail.result('accumulate_paycode_list')['value'], 'paycodename', rail.result('for_each_item_in_query_do')['pt_c'], 'paycodecode', 'nil')) if rail.result('for_each_item_in_query_do')['pt_c'] else ((rail.find_first_by_attr_and_get_attr(rail.result('accumulate_paycode_list')['value'], 'paycodename', rail.result('for_each_item_in_query_do')['pt_e'], 'paycodecode', 'nil')) if rail.result('for_each_item_in_query_do')['pt_e'] else ((rail.find_first_by_attr_and_get_attr(rail.result('accumulate_paycode_list')['value'], 'paycodename', rail.result('for_each_item_in_query_do')['pt_h'], 'paycodecode', 'nil')) if rail.result('for_each_item_in_query_do')['pt_h'] else ((rail.find_first_by_attr_and_get_attr(rail.result('accumulate_paycode_list')['value'], 'paycodename', rail.result('for_each_item_in_query_do')['pt_ne'], 'paycodecode', 'nil')) if rail.result('for_each_item_in_query_do')['pt_ne'] else ((rail.find_first_by_attr_and_get_attr(rail.result('accumulate_paycode_list')['value'], 'paycodename', rail.result('for_each_item_in_query_do')['pt_pt'], 'paycodecode', 'nil')) if rail.result('for_each_item_in_query_do')['pt_pt'] else '')))),

                'Activitycode': rail.result('for_each_item_in_query_do')['activitycode'] if rail.result('for_each_item_in_query_do')['activitycode'] else "",
                'Loginname': rail.result('for_each_item_in_query_do')['loginname'],
                'Employeetype': rail.result('for_each_item_in_query_do')['employeetype'],
                'Hours': rail.result('for_each_item_in_query_do')['hoursworked'],
                'Hourstype': 'workhours',
                'Amount': '',

                'Payrate': payrate_data(),

                'Timesheeturi': rail.result('for_each_item_in_query_do')['timesheeturi'],
                'Timeofftype': '',
                'Entrydate': rail.result('for_each_item_in_query_do')['entrydate'],
                'Activityname': rail.result('for_each_item_in_query_do')['activityname'] if rail.result('for_each_item_in_query_do')['activityname'] else "",
                'Otmeal': '',
                'jobid': rail.render_template("{{ dag_run_ecid() }}")
            },
        )

        if_otmeal_is_present = rail.IfOperator(
            task_id='if_otmeal_is_present',
            test="{{result('for_each_item_in_query_do').otmeal | is_truthy}}",
            yes_task="add_entry_for_otmeal",
            no_task="if_timetype__present",
        )

        add_entry_for_otmeal = rail.WriteLogOperator(
            task_id='add_entry_for_otmeal',
            log="{{result('reportdata_lookuptable')}}",
            severity='',
            message='na',
            properties=lambda: {
                'Empid': rail.result('for_each_item_in_query_do')['employeeid'],

                'Paycode': rail.find_first_by_attr_and_get_attr(rail.result('log_required_employeetypedetails'), 'customField.displayText', 'OT Meal Pay Code', 'text', 'nil') if rail.find_first_by_attr_and_get_attr(rail.result('log_required_employeetypedetails'), 'customField.displayText', 'OT Meal Pay Code', 'text', 'nil') else "",

                'Activitycode': "",
                'Loginname': rail.result('for_each_item_in_query_do')['loginname'],
                'Employeetype': rail.result('for_each_item_in_query_do')['employeetype'],
                'Hours': "",
                'Hourstype': 'otmeal',
                'Amount': float((rail.find_first_by_attr_and_get_attr(rail.result('log_required_employeetypedetails'), 'customField.displayText', 'OT Meal Allocation', 'text', ''))) * float(rail.result('for_each_item_in_query_do')['otmeal']) if rail.find_first_by_attr_and_get_attr(rail.result('log_required_employeetypedetails'), 'customField.displayText', 'OT Meal Allocation', 'text', '') else "",

                'Payrate': payrate_data(),

                'Timesheeturi': rail.result('for_each_item_in_query_do')['timesheeturi'],
                'Timeofftype': "",
                'Entrydate': rail.result('for_each_item_in_query_do')['entrydate'],
                'Activityname': "",
                'Otmeal': rail.result('for_each_item_in_query_do')['otmeal'],
                'jobid': rail.render_template("{{ dag_run_ecid() }}")
            },
        )

        if_timetype__present = rail.IfOperator(
            task_id='if_timetype__present',
            test='''{{ result('for_each_item_in_query_do').timeofftype | is_truthy}}''',
            yes_task="if_timetype_has_data_present",
            no_task="for_each_item_in_query_do_end",
        )

        if_timetype_has_data_present = rail.IfOperator(
            task_id='if_timetype_has_data_present',
            test=lambda: rail.result('for_each_item_in_query_do')['timeofftype'] == 'Company Holiday' or rail.result(
                'for_each_item_in_query_do')['timeofftype'] == 'Elective Holiday',
            yes_task="get_timeoff_details",
            no_task="add_entry_for_timetype",
        )

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_details',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda: {
                "userUri": rail.result('for_each_item_in_query_do')['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(rail.result('for_each_item_in_query_do')['entrydate'], "%b %d, %Y").year,
                        "month": datetime.strptime(rail.result('for_each_item_in_query_do')['entrydate'], "%b %d, %Y").month,
                        "day": datetime.strptime(rail.result('for_each_item_in_query_do')['entrydate'], "%b %d, %Y").day,
                    },
                    "endDate": {
                        "year": datetime.strptime(rail.result('for_each_item_in_query_do')['entrydate'], "%b %d, %Y").year,
                        "month": datetime.strptime(rail.result('for_each_item_in_query_do')['entrydate'], "%b %d, %Y").month,
                        "day": datetime.strptime(rail.result('for_each_item_in_query_do')['entrydate'], "%b %d, %Y").day,
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        for_each_in_data_do = rail.ForEachOperator(
            task_id='for_each_in_data_do',
            items="{{result('get_timeoff_details') | to_json}}",
            start_task='accumulate_timeoff_per_user',
            end_task='for_each_in_data_do_end'
        )

        accumulate_timeoff_per_user = rail.SetVariableOperator(
            task_id='accumulate_timeoff_per_user',
            name='timeoffs_perday_peruser',
            append=True,
            value=lambda: {
                'empid': rail.result('for_each_item_in_query_do')['employeeid'],
                'loginname': rail.result('for_each_in_data_do')['owner']['loginName'],
                'timeofftype': rail.result('for_each_in_data_do')['timeOffType']['name'],
                'startdate': str(rail.result('for_each_in_data_do')['startDateDetails']['date']['month']) + "/" + str(rail.result('for_each_in_data_do')['startDateDetails']['date']['day']) + "/" + str(rail.result('for_each_in_data_do')['startDateDetails']['date']['year']),

                'enddate': str(rail.result('for_each_in_data_do')['endDateDetails']['date']['month']) + "/" + str(rail.result('for_each_in_data_do')['endDateDetails']['date']['day']) + "/" + str(rail.result('for_each_in_data_do')['endDateDetails']['date']['year']),

                'hours': rail.result('for_each_in_data_do')['startDateDetails']['totalDuration']['calendarDayDuration']['hours'],
                'minutes': str(round(float(float(rail.result('for_each_in_data_do')['startDateDetails']['totalDuration']['calendarDayDuration']['minutes']) / 60), 2)),

                'durationinhours': (str(float(rail.result('for_each_in_data_do')['startDateDetails']['totalDuration']['calendarDayDuration']['hours']) + round(float(float(rail.result('for_each_in_data_do')['startDateDetails']['totalDuration']['calendarDayDuration']['minutes']) / 60), 2)) + "0") if len(str(round(float(float(rail.result('for_each_in_data_do')['startDateDetails']['totalDuration']['calendarDayDuration']['minutes']) / 60), 2)).split(".")[1]) == 1 else str((float(rail.result('for_each_in_data_do')['startDateDetails']['totalDuration']['calendarDayDuration']['hours']) + round(float(float(rail.result('for_each_in_data_do')['startDateDetails']['totalDuration']['calendarDayDuration']['minutes']) / 60), 2))),

                'controlleroption': rail.find_first_by_attr_and_get_attr(rail.result('for_each_in_data_do')['extensionFieldValues'], 'definition.displayText', str(rail.result('for_each_item_in_query_do')['timeofftype']) + ' - ' + 'Controller Options', 'tag.displayText', ''),

                'useruri': rail.result('for_each_in_data_do')['owner']['uri'],
                'timeoffuri': rail.result('for_each_in_data_do')['timeOffType']['uri'],
            },
        )

        for_each_in_data_do_end = rail.EmptyOperator(
            task_id='for_each_in_data_do_end'
        )
        for_each_item_in_query_do_end = rail.EmptyOperator(
            task_id='for_each_item_in_query_do_end'
        )

        def get_dropdowndata():
            records = rail.result('accumulate_timeoff_per_user')['value'] if rail.result(
                'accumulate_timeoff_per_user') and rail.result('accumulate_timeoff_per_user')['value'] else None
            for data in records:
                if data['loginname'] == rail.result('for_each_item_in_query_do')['loginname'] and data['timeofftype'] == rail.result('for_each_item_in_query_do')['timeofftype'].strip() and data['startdate'] == datetime.strptime(rail.result('for_each_item_in_query_do')['entrydate'], "%b %d, %Y").strftime("%m/%d/%Y") and data['durationinhours'] == rail.result('for_each_item_in_query_do')['timeoffhours']:
                    return data['controlleroption']
            return None

        log_dropdown_oef_value = rail.PythonOperator(
            task_id='log_dropdown_oef_value',
            python_callable=get_dropdowndata
        )

        log_selected_dropdown = rail.PythonOperator(
            task_id='log_selected_dropdown',
            python_callable=lambda: " : " +
            str(rail.result('log_dropdown_oef_value')) if rail.result(
                'log_dropdown_oef_value') else null
        )

        final_value_in_lookuptable = rail.PythonOperator(
            task_id='final_value_in_lookuptable',
            python_callable=lambda: rail.result('for_each_item_in_query_do')[
                'timeofftype'] + str(rail.result('log_selected_dropdown') if rail.result('log_selected_dropdown') else '')
        )

        accumulate_timeoff_details = rail.SetVariableOperator(
            task_id='accumulate_timeoff_details',
            name='timeoffdetails',
            append=True,
            value=lambda: {
                "timeofftypename": rail.result('get_timeoff_details'),
                "oefselected": rail.result('log_dropdown_oef_value'),
                "lookupvalue": (rail.result('for_each_item_in_query_do')['timeofftype'] + str(rail.result('log_selected_dropdown')) if rail.result('log_selected_dropdown') else ''),
                "payrate": rail.find_first_by_attr_and_get_attr(rail.result('log_required_employeetypedetails'), 'customField.displayText', rail.result('final_value_in_lookuptable'), 'text', ''),
                "employeetype": rail.result('for_each_item_in_query_do')['employeetype'],
                "empid": rail.result('for_each_item_in_query_do')['employeeid']
            }
        )

        def get_payrate():
            record_data = rail.result('log_required_employeetypedetails') if rail.result(
                'log_required_employeetypedetails') else null
            for item in record_data:
                if item['customField']['displayText'] == rail.result('final_value_in_lookuptable'):
                    return item['text']
            return ''

        add_entry_for_timeoff = rail.WriteLogOperator(
            task_id='add_entry_for_timeofff',
            log="{{result('reportdata_lookuptable')}}",
            severity='',
            message='na',
            properties=lambda: {
                'Empid': rail.result('for_each_item_in_query_do')['employeeid'],

                'Paycode': rail.find_first_by_attr_and_get_attr(rail.result('accumulate_paycode_list')['value'], 'paycodename', rail.result('for_each_item_in_query_do')['timeofftype'], 'paycodecode', 'nil') if (rail.find_first_by_attr_and_get_attr(rail.result('accumulate_paycode_list')['value'], 'paycodename', rail.result('for_each_item_in_query_do')['timeofftype'], 'paycodecode', 'nil')) else "",

                'Activitycode': "",
                'Loginname': rail.result('for_each_item_in_query_do')['loginname'],
                'Employeetype': rail.result('for_each_item_in_query_do')['employeetype'],
                'Hours': rail.result('for_each_item_in_query_do')['timeoffhours'],
                'Hourstype': 'timeoffhours',
                'Amount': "",

                'Payrate': get_payrate(),

                'Timesheeturi': rail.result('for_each_item_in_query_do')['timesheeturi'],
                'Timeofftype': rail.result('for_each_item_in_query_do')['timeofftype'],
                'Entrydate': rail.result('for_each_item_in_query_do')['entrydate'],
                'Activityname': rail.result('for_each_item_in_query_do')['activityname'],
                'Otmeal': "",
                'jobid': rail.render_template("{{ dag_run_ecid() }}")
            },
        )

        add_entry_for_timetype = rail.WriteLogOperator(
            task_id='add_entry_for_timetype',
            log="{{result('reportdata_lookuptable')}}",
            severity='',
            message='na',
            properties=lambda: {
                'Empid': rail.result('for_each_item_in_query_do')['employeeid'],

                'Paycode': rail.find_first_by_attr_and_get_attr(rail.result('accumulate_paycode_list')['value'], 'paycodename', rail.result('for_each_item_in_query_do')['timeofftype'], 'paycodecode', 'nil') if rail.find_first_by_attr_and_get_attr(rail.result('accumulate_paycode_list')['value'], 'paycodename', rail.result('for_each_item_in_query_do')['timeofftype'], 'paycodecode', 'nil') else "",

                'Activitycode': "",
                'Loginname': rail.result('for_each_item_in_query_do')['loginname'],
                'Employeetype': rail.result('for_each_item_in_query_do')['employeetype'],
                'Hours': rail.result('for_each_item_in_query_do')['timeoffhours'],
                'Hourstype': 'timeoffhours',
                'Amount': "",

                'Payrate': rail.find_first_by_attr_and_get_attr(rail.result('log_required_employeetypedetails'), 'customField.displayText', rail.result('for_each_item_in_query_do')['timeofftype'], 'text', '') if rail.find_first_by_attr_and_get_attr(rail.result('log_required_employeetypedetails'), 'customField.displayText', rail.result('for_each_item_in_query_do')['timeofftype'], 'text', '') else "",

                'Timesheeturi': rail.result('for_each_item_in_query_do')['timesheeturi'],
                'Timeofftype': rail.result('for_each_item_in_query_do')['timeofftype'],
                'Entrydate': rail.result('for_each_item_in_query_do')['entrydate'],
                'Activityname': rail.result('for_each_item_in_query_do')['activityname'],
                'Otmeal': "",
                'jobid': rail.render_template("{{ dag_run_ecid() }}"),
            },
        )

        search_entries_in_lookup_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_lookup_table',
            log="{{result('reportdata_lookuptable')}}",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
            }
        )

        custom_paychex_lookuptable = rail.CreateLogOperator(
            task_id='custom_paychex_lookuptable'
        )

        if_entry_present = rail.IfOperator(
            task_id='if_entry_present',
            test="{{result('search_entries_in_lookup_table','length') > 0 }}",
            yes_task="render_logs_csv",
            no_task="send_no_data_export_mail",
        )

        send_no_data_export_mail = rail.EmailOperator(
            task_id='send_no_data_export_mail',
            to="{{dag_run.conf.emailid}}",
            subject='''{{get_company_key()}} |  Paychex Export - No data to export  - {{ current_time("%Y-%m-%eT%H:%M%S.%f") }} ''',
            html_content="templates/emails/no_data_to_export_mail.html"
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('reportdata_lookuptable') }}",
            header=['empid', 'paycode', 'activitycode', 'loginname', 'employeetype', 'hours', 'hourstype',
                    'amount', 'payrate', 'timesheeturi', 'timeofftype', 'entrydate', 'activityname', 'otmeal'],
            row=[
                '{{ item.properties.Empid}}',
                '{{ item.properties.Paycode}}',
                '{{ item.properties.Activitycode}}',
                '{{ item.properties.Loginname}}',
                '{{ item.properties.Employeetype}}',
                '{{ item.properties.Hours}}',
                '{{ item.properties.Hourstype}}',
                '{{ item.properties.Amount}}',
                '{{ item.properties.Payrate}}',
                '{{ item.properties.Timesheeturi}}',
                '{{ item.properties.Timeofftype}}',
                '{{ item.properties.Entrydate}}',
                '{{ item.properties.Activityname}}',
                '{{ item.properties.Otmeal}}',
            ],
        )

        create_reportdata_collection = rail.CreateCollectionOperator(
            task_id='create_reportdata_collection',
            source="{{ result('render_logs_csv') }}",
            name="reportdata",
            columns={
                'empid': 'empid', 'paycode': 'paycode', 'activitycode': 'activitycode', 'loginname': 'loginname', 'employeetype': 'employeetype', 'hours': 'hours', 'hourstype': 'hourstype', 'amount': 'amount', 'payrate': 'payrate', 'timesheeturi': 'timesheeturi', 'timeofftype': 'timeofftype', 'entrydate': 'entrydate', 'activityname': 'activityname', 'otmeal': 'otmeal'
            }
        )

        query_distinct_timesheets = rail.QueryCollectionOperator(
            task_id='query_distinct_timesheets',
            query="""SELECT DISTINCT reportdata.timesheeturi FROM reportdata WHERE NULLIF(loginname,'') IS NOT NULL AND NULLIF(employeetype,'') IS NOT NULL""",
        )

        process_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child',
            items="{{result('query_distinct_timesheets')}}",
            trigger_dag_id=f'wolverinepipeline_custom_paychex_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run, **context: {
                "timesheeturi": item['timesheeturi'],
                "Islastitem": "yes" if (int(context['index']) + 1) == rail.result('query_distinct_timesheets', 'length') else "no",
                'jobid': rail.render_template("{{ dag_run_ecid() }}"),
                'emailid': dag_run.conf['emailid'],
                'custom_paychex_lookuptable': rail.result('custom_paychex_lookuptable')
            },
            retries=0
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_all_activities
        get_all_activities >> get_all_reports >> get_report_details2 >> log_approval_status_filteruri
        log_approval_status_filteruri >> log_timesheet_period_filter_uri >> generate_report_group
        generate_report_group >> if_payload_has_data >> rail.Label(
            'Yes') >> load_csv >> create_collection_create_list_from_csv >> query_to_sort_raw_data
        query_to_sort_raw_data >> get_paycodelist >> for_each_in_rows_do
        for_each_in_rows_do >> accumulate_paycode_list >> for_each_in_rows_do_end >> get_employeetypedetails
        get_employeetypedetails >> for_each_in_list_do >> accumulate_employeetype_list
        accumulate_employeetype_list >> for_each_in_list_do_end
        for_each_in_list_do >> for_each_in_list_do_end >> get_all_objects >> for_each_in_list
        for_each_in_list >> if_name_startswith_PT >> rail.Label(
            'Yes') >> get_object_definition_details >> for_each_item_in_list_do
        for_each_item_in_list_do >> if_isenabled_true >> rail.Label(
            'Yes') >> accumulate_object_list >> for_each_item_in_list_do_end >> for_each_in_list_end >> reportdata_lookuptable
        if_isenabled_true >> rail.Label(
            'No') >> for_each_item_in_list_do_end
        reportdata_lookuptable >> for_each_item_in_query_do
        for_each_item_in_query_do >> log_required_employeetypedetails >> if_timetype_not_present
        if_timetype_not_present >> rail.Label(
            'Yes') >> if_hoursworked_is_present
        if_hoursworked_is_present >> rail.Label(
            'Yes') >> add_entry_to_list >> if_otmeal_is_present
        if_hoursworked_is_present >> rail.Label('No') >> if_otmeal_is_present
        if_otmeal_is_present >> rail.Label(
            'Yes') >> add_entry_for_otmeal >> if_timetype__present
        if_otmeal_is_present >> rail.Label('No') >> if_timetype__present
        if_timetype__present >> rail.Label(
            'Yes') >> if_timetype_has_data_present
        if_timetype_has_data_present >> rail.Label(
            'Yes') >> get_timeoff_details >> for_each_in_data_do
        for_each_in_data_do >> accumulate_timeoff_per_user >> for_each_in_data_do_end
        for_each_in_data_do_end >> log_dropdown_oef_value >> log_selected_dropdown
        log_selected_dropdown >> final_value_in_lookuptable >> accumulate_timeoff_details
        accumulate_timeoff_details >> add_entry_for_timeoff >> for_each_item_in_query_do_end
        for_each_item_in_query_do_end >> search_entries_in_lookup_table >> custom_paychex_lookuptable >> if_entry_present
        if_entry_present >> rail.Label(
            'Yes') >> render_logs_csv >> create_reportdata_collection >> query_distinct_timesheets
        query_distinct_timesheets >> process_child >> log_to_sumo
        if_entry_present >> rail.Label(
            'No') >> send_no_data_export_mail >> log_to_sumo
        for_each_in_data_do >> for_each_in_data_do_end
        if_timetype_has_data_present >> rail.Label(
            'No') >> add_entry_for_timetype >> for_each_item_in_query_do_end
        if_timetype__present >> rail.Label(
            'No') >> for_each_item_in_query_do_end
        if_timetype_not_present >> rail.Label('No') >> if_timetype__present
        for_each_item_in_query_do >> for_each_item_in_query_do_end
        for_each_item_in_list_do >> for_each_item_in_list_do_end
        if_name_startswith_PT >> rail.Label(
            'No') >> for_each_in_list_end
        for_each_in_list >> for_each_in_list_end
        for_each_in_rows_do >> for_each_in_rows_do_end
        if_payload_has_data >> rail.Label(
            'No') >> send_nodata_mail >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
