from datetime import datetime, timedelta
from airflow.models import Variable
import rail
# pylint:disable=undefined-loop-variable
# pylint:disable=inconsistent-return-statements
# pylint:disable=too-many-arguments
# pylint:disable=too-many-nested-blocks
# pylint:disable=too-many-statements
# Dummy
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_timesheet_sync_{config.instance}',
        description=f'deltek_costpoint_timesheet_sync_poc_{config.instance}',
        schedule_interval=None,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=[
            rail.WebhookConf(hmac_secret_var=config.cp_rep_webhook_secret)
        ],
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_replicon_timesheet'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_replicon_timesheet',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_existing_deltek_timesheet = rail.DeltekCostPointServiceOperator(
            task_id='get_existing_deltek_timesheet',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda: {
                "filter": {
                    "id": "replicon_exp_ldmtime",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMTIME_TSHDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "EMPL_ID",
                                                "relation": "=",
                                                "value": rail.result('get_replicon_user_details')[0]['userDetails']['employeeId']
                                            },
                                            {
                                                "name": "TS_DT",
                                                "relation": "=",
                                                "value": get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate'])
                                            }

                                        ]
                                    }
                                ],
                                "children": [
                                    {
                                        "rsWhere": {
                                            "rsId": "LDMTIME_TSLN",
                                                    "conditions": [],
                                                    "children": []
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        )

        get_replicon_timesheet = rail.RepliconServiceOperator(
            task_id='get_replicon_timesheet',
            endpoint="/services/timesheetservice1.svc/GetTimesheetDetails",
            data={
                'timesheetUri': '{{ dag_run.conf.webhook.data.timesheet.uri }}'
            }
        )

        get_replicon_time_entries = rail.RepliconServiceOperator(
            task_id='get_replicon_time_entries',
            endpoint='/services/timeEntryrevisiongroupservice1.svc/GetTimeEntryRevisionGroupsForUserAndDateRange',
            data=lambda: {
                "user": {
                    "uri": rail.result('get_replicon_timesheet')['owner']['uri']
                },
                "dateRange": {
                    "startDate": rail.result('get_replicon_timesheet')['dateRange']['startDate'],
                    "endDate": rail.result('get_replicon_timesheet')['dateRange']['endDate']
                }
            }
        )

        get_replicon_user_details = rail.RepliconServiceOperator(
            task_id='get_replicon_user_details',
            endpoint="/services/importservice1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [{"uri": rail.result('get_replicon_timesheet')['owner']['uri']}]
            }
        )

        get_replicon_task_details = rail.RepliconServiceOperator(
            task_id='get_replicon_task_details',
            endpoint="/services/taskservice1.svc/BulkGetTaskDetails",
            data=lambda: {
                "taskUris": get_task_uris(rail.result('get_replicon_time_entries'))
            }
        )

        get_replicon_project_details = rail.RepliconServiceOperator(
            task_id='get_replicon_project_details',
            endpoint='/services/projectservice1.svc/BulkGetProjectDetails3',
            data=lambda: {
                "projects": get_project_uris(rail.result('get_replicon_task_details'))
            }
        )

        get_division_details = rail.RepliconServiceOperator(
            task_id='get_division_details',
            endpoint='/services/divisionservice1.svc/BulkGetDivisionDetails',
            data=lambda: {
                "divisionUris": get_division_uris(rail.result('get_replicon_project_details'))
            }
        )

        get_replicon_billing_rate_details = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_replicon_billing_rate_details',
            items=lambda: get_billing_rate_uris(
                rail.result('get_replicon_time_entries')),
            endpoint="/services/BillingRateService1.svc/GetCompanyBillingRateDetails",
            data={
                "companyBillingRateUri": "{{ item }}",
                "asOfDate": null
            }
        )

        get_oef_tag_details = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_oef_tag_details',
            items=lambda: get_oef_tag_uris(
                rail.result('get_replicon_time_entries')),
            endpoint="/services/ObjectExtensionTagService1.svc/GetObjectExtensionTagDetails",
            data={
                "objectExtensionTagUri": "{{ item }}"
            }
        )

        push_time_to_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='push_time_to_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda:
            {
                "document": {
                    "id": "replicon_imp_ldmtime",
                    "rows": [
                        {
                            "row": {
                                "rsId": "LDMTIME_TSHDR",
                                "tranType": "INSERT",
                                "data": {
                                    "EMPL_ID": rail.result('get_replicon_user_details')[0]['userDetails']['employeeId'],
                                    "FY_CD": get_financial_year(rail.result('get_replicon_timesheet')),
                                    "OTH_HRS": get_other_hours(rail.result('get_replicon_time_entries'), rail.result('get_replicon_pay_codes'),
                                                               rail.result('get_oef_tag_details')),
                                    "PD_NO": get_period_number(rail.result('get_replicon_timesheet')),
                                    "REG_HRS": get_reg_hours(rail.result('get_replicon_time_entries'), rail.result('get_replicon_pay_codes'),
                                                             rail.result('get_oef_tag_details')),
                                    "SUB_PD_NO": 1,
                                    "S_TS_TYPE_CD": "R",
                                    "TH___AUTO_ADJ_PCT_RT": 1,
                                    "TS_DT": get_timesheet_date(rail.result('get_replicon_timesheet')),
                                    "TS_HDR_SEQ_NO": get_timesheet_header_seq(rail.result('get_existing_deltek_timesheet')[0])
                                },
                                "children": get_children(rail.result('get_replicon_time_entries'), rail.result('get_replicon_task_details'),
                                                         rail.result('get_replicon_pay_codes'), rail.result(
                                                             'get_replicon_billing_rate_details'),
                                                         rail.result('get_replicon_account_details'), rail.result(
                                                             'get_replicon_project_details'),
                                                         rail.result('get_division_details'), rail.result('get_oef_tag_details'))
                            }
                        }
                    ]
                }
            }
        )

        is_timesheet_available = rail.IfOperator(
            task_id='is_timesheet_available',
            test=lambda: is_revert_required(
                rail.result('get_existing_deltek_timesheet')[0]),
            yes_task='revert_existing_time',
            no_task='push_time_to_costpoint'
        )

        revert_existing_time = rail.DeltekCostPointServiceOperator(
            task_id='revert_existing_time',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda: get_reversing_record(
                rail.result('get_existing_deltek_timesheet')[0])
        )

        get_replicon_pay_codes = rail.RepliconServiceOperator(
            task_id='get_replicon_pay_codes',
            endpoint='/services/PayCodeService1.svc/GetAllPayCodes',
        )

        get_account_details = rail.RepliconServiceOperator(
            task_id='get_replicon_account_details',
            endpoint='services/costcenterservice1.svc/GetCostCenterDetails',
            data=lambda: {
                "costCenterUri": rail.result('get_replicon_user_details')[0]['costCenterSchedule'][-1]['costCenter']['uri']
            }
        )

        is_export_successful = rail.IfOperator(
            task_id='is_export_successful',
            test=lambda: rail.result('push_time_to_costpoint')[0][
                'MethodResponse']['Severity'] < 3,
            yes_task='catch_error',
            no_task='export_error'
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: 'Error:' +
            rail.render_template("{{get_error_message()}}")
        )

        def get_user_company(userDetails):
            company = rail.find_first_by_attr_and_get_attr(
                userDetails['extensionFieldValues'], 'definition.displayText', 'Company', 'textValue')
            return [company]

        def is_revert_required(existingTimesheet):
            if len(existingTimesheet['document']['rows']) > 0:
                reversingRecord = get_reversing_record(existingTimesheet)
                if reversingRecord is not None:
                    return True
            return False

        def get_export_message():
            timesheet_info = rail.result('get_replicon_timesheet')
            costpoint_response = rail.result('push_time_to_costpoint')
            if timesheet_info:
                if costpoint_response:
                    return rail.render_template('''Time sync failed for user "{{ result('get_replicon_timesheet').owner.loginName }}" \
                        and timesheet startdate "{{ result('get_replicon_timesheet').dateRange.startDate.month }}/{{ result('get_replicon_timesheet').dateRange.startDate.day }}\
                            /{{ result('get_replicon_timesheet').dateRange.startDate.year }}" message from api "{{ result('push_time_to_costpoint') }}"''')
                return rail.render_template('''Time sync failed for "{{ result('get_replicon_timesheet').owner.loginName }}" \
                    timesheet startdate "{{ result('get_replicon_timesheet').dateRange.startDate.month }}/{{ result('get_replicon_timesheet').dateRange.startDate.day }}\
                            /{{ result('get_replicon_timesheet').dateRange.startDate.year }}"''')
            return rail.render_template('''Time sync failed for the timesheet "{{ dag_run.conf.webhook.data.timesheet.uri }}"''')

        export_error = rail.PythonOperator(
            task_id="export_error",
            python_callable=get_export_message
        )

        send_error = rail.EmailOperator(
            task_id='send_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint Timesheet Sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong>
            <br /> <br />Hello, <br /> <br /> {{ result('export_error') }}
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        def get_reversing_record(costpointTimesheet):
            if costpointTimesheet:
                for row in costpointTimesheet['document']['rows']:
                    if row['row']['data']['S_TS_TYPE_CD'] == 'R' \
                            and not is_data_reversed(row, costpointTimesheet['document']['rows']):
                        return change_to_reversed(row)
            return None

        def change_to_reversed(row):
            return {
                "document": {
                    "id": "replicon_imp_ldmtime",
                    "rows": [
                        {
                            "row": {
                                "rsId": "LDMTIME_TSHDR",
                                "tranType": "INSERT",
                                "data": {
                                    "EMPL_ID": row['row']['data']['EMPL_ID'],
                                    "FY_CD": row['row']['data']['FY_CD'],
                                    "OTH_HRS": -1.0 * row['row']['data']["OTH_HRS"],
                                    "PD_NO": row['row']['data']['PD_NO'],
                                    "REG_HRS": -1.0 * row['row']['data']["REG_HRS"],
                                    "SUB_PD_NO": row['row']['data']['SUB_PD_NO'],
                                    "S_TS_TYPE_CD": 'C',
                                    "REFERENCE_SEQ_NO": row['row']['data']['TS_HDR_SEQ_NO'],
                                    "REFERENCE_TS_TYPE_CD": "R",
                                    "TH___CORRECTING_REF_DT": row['row']['data']['TS_DT'],
                                    "TH___AUTO_ADJ_PCT_RT": row['row']['data']['TH___AUTO_ADJ_PCT_RT'],
                                    "TS_DT": row['row']['data']['TS_DT'],
                                    "TS_HDR_SEQ_NO": row['row']['data']['TS_HDR_SEQ_NO']
                                },
                                "children": get_revert_children(row['row']['children'])
                            }
                        }
                    ]
                }
            }

        def get_revert_children(children):
            revertChildren = []
            for child in children:
                if child['row']['rsId'] == 'LDMTIME_TSLN':
                    if child['row']['data']['TS_LN___S_TS_LN_TYPE_CD'] == config.mo_line_type:
                        revertChildren.append(
                            {
                                "row": {
                                    "rsId": "LDMTIME_TSLN",
                                    "tranType": "INSERT",
                                    "data": get_project_data(child),
                                    "children": [
                                        {
                                            "row": {
                                                "rsId": "LDMTIME_TSLNMO",
                                                "tranType": "INSERT",
                                                "data": {
                                                    "MO_ID": child['row']['children'][0]['row']['data']['MO_ID'],
                                                    "MO_OPER_SEQ_NO": child['row']['children'][0]['row']['data']['MO_OPER_SEQ_NO'],
                                                    "MO_OPER_STEP_NO": child['row']['children'][0]['row']['data']['MO_OPER_STEP_NO'],
                                                    "S_ACTIVITY_TYPE": child['row']['children'][0]['row']['data']['S_ACTIVITY_TYPE'],
                                                    "WC_ID": child['row']['children'][0]['row']['data']['WC_ID']
                                                }
                                            }
                                        }
                                    ]
                                }
                            })
                    else:
                        revertChildren.append(
                            {
                                "row": {
                                    "rsId": "LDMTIME_TSLN",
                                    "tranType": "INSERT",
                                    "data": get_project_data(child)
                                }
                            })
            return revertChildren

        def get_project_data(child):
            data = child['row']['data']
            return {
                "ACCT_ID": data['ACCT_ID'],
                "BILL_LAB_CAT_CD": data.get('BILL_LAB_CAT_CD'),
                "GENL_LAB_CAT_CD": data.get('GENL_LAB_CAT_CD'),
                "ORG_ID": data['ORG_ID'],
                "PAY_TYPE": data['PAY_TYPE'],
                "PROJ_ID": data['PROJ_ID'],
                "TS_LN___CHG_HRS": -1.0 * data['TS_LN___CHG_HRS'],
                "TS_LN___LAB_LOC_CD": data.get('TS_LN___LAB_LOC_CD'),
                "TS_LN___S_TS_LN_TYPE_CD": data['TS_LN___S_TS_LN_TYPE_CD'],
                "TS_LN___WORK_COMP_CD": data.get('TS_LN___WORK_COMP_CD')
            }

        def is_data_reversed(row, rows):
            for item in rows:
                if item['row']['data']['FY_CD'] == row['row']['data']['FY_CD'] \
                        and item['row']['data']['PD_NO'] == row['row']['data']['PD_NO']\
                        and item['row']['data']['SUB_PD_NO'] == row['row']['data']['SUB_PD_NO'] \
                        and item['row']['data']['TS_HDR_SEQ_NO'] == row['row']['data']['TS_HDR_SEQ_NO'] \
                        and item['row']['data']['S_TS_TYPE_CD'] != 'R':
                    return item['row']['data']['FY_CD'] == row['row']['data']['FY_CD'] \
                        and item['row']['data']['PD_NO'] == row['row']['data']['PD_NO']\
                        and item['row']['data']['SUB_PD_NO'] == row['row']['data']['SUB_PD_NO'] \
                        and item['row']['data']['TS_HDR_SEQ_NO'] == row['row']['data']['TS_HDR_SEQ_NO']\
                        and item['row']['data']['S_TS_TYPE_CD'] != row['row']['data']['S_TS_TYPE_CD'] and \
                        (item['row']['data']['S_TS_TYPE_CD'] ==
                         'RV' or item['row']['data']['S_TS_TYPE_CD'] == 'C')
            return False

        def get_project_uris(tasks):
            projectUris = []
            projects = []
            if tasks:
                for task in tasks:
                    projectUri = task['project']['uri']
                    if projectUri not in projectUris:
                        projects.append({"uri": projectUri})
                        projectUris.append(projectUri)
            return projects

        def get_division_uris(projects):
            divisions = []
            if projects:
                for project in projects:
                    division = project['projectDetails']['division']
                    if division and division['uri'] and division['uri'] not in divisions:
                        divisions.append(division['uri'])
            return divisions

        def get_timesheet_header_seq(existingTimesheet):
            if not existingTimesheet or \
                not existingTimesheet['document'] \
                    or len(existingTimesheet['document']['rows']) == 0:
                return 1
            seq_no = get_max_header_seq(existingTimesheet) + 1
            return seq_no

        def get_max_header_seq(existingTimesheet):
            seq_no = 1
            for row in existingTimesheet['document']['rows']:
                if row['row']['data']['TS_HDR_SEQ_NO'] > seq_no:
                    seq_no = row['row']['data']['TS_HDR_SEQ_NO']
            return seq_no

        def get_period_number(timesheet):
            return timesheet['dateRange']['endDate']['month']

        def get_financial_year(timesheet):
            return timesheet['dateRange']['endDate']['year']

        def get_task_uris(entries):
            taskUris = []
            for entry in entries:
                task_uri = get_task_uri(entry)
                if task_uri and task_uri not in taskUris:
                    taskUris.append(task_uri)
            return taskUris

        def get_oef_tag_uris(entries):
            tagUris = []
            payTypeOefName = Variable.get(
                config.pay_type_oef_var_name, default_var='Pay Type')
            if payTypeOefName:
                for entry in entries:
                    tagUri = rail.find_first_by_attr_and_get_attr(
                        entry['extensionFieldValues'], 'definition.displayText', payTypeOefName, 'tag.uri')
                    if tagUri and tagUri not in tagUris:
                        tagUris.append(tagUri)
            return tagUris

        def get_billing_rate_uris(entries):
            billingRateUris = []
            for entry in entries:
                billingRateUri = rail.find_first_by_attr_and_get_attr(
                    entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:billing-rate', 'value.uri')
                if billingRateUri and billingRateUri not in billingRateUris:
                    billingRateUris.append(billingRateUri)
            return billingRateUris

        def get_timesheet_date(timesheet):
            return get_formatted_date(timesheet['dateRange']['endDate'])

        def get_children(entries, taskDetails, payCodes, billingRates, accountDetails, projects, divisions, oefTags):
            childRows = []
            projectEntries = {}
            if Variable.get(config.group_by_project_var_name, default_var='false') == '1':
                for entry in entries:
                    if is_project_allocation_type(entry):
                        projectId = get_project_id(entry, taskDetails)
                        plc = get_billing_labor_category(entry, billingRates)
                        payType = get_pay_type(entry, payCodes, oefTags)
                        key = projectId + "_" + plc + "_" + payType

                        if key in projectEntries:
                            projectEntries[key].append(entry)
                        else:
                            projectEntries[key] = [entry]
                for key, projectEntry in projectEntries.items():
                    mo_project = is_project_mo(
                        projectEntry[0], taskDetails, projects)
                    if (mo_project is True):
                        ch_rows = get_grouped_project_timeentries(
                            taskDetails, payCodes, billingRates, accountDetails, projects, divisions, oefTags, projectEntry)
                        childRows.append(ch_rows)
                    else:
                        childRows.append({
                            "row": {
                                "rsId": "LDMTIME_TSLN",
                                "tranType": "INSERT",
                                "data": {
                                    "ACCT_ID": get_account_id(accountDetails),
                                    "BILL_LAB_CAT_CD": get_billing_labor_category(projectEntry[0], billingRates),
                                    "ORG_ID": get_org_id(projectEntry[0], taskDetails, projects, divisions),
                                    "PAY_TYPE": get_pay_type(projectEntry[0], payCodes, oefTags),
                                    "PROJ_ID": get_project_id(projectEntry[0], taskDetails),
                                    "TS_LN___CHG_HRS": get_total_hours(projectEntry, payCodes),
                                    "TS_LN___S_TS_LN_TYPE_CD": get_line_type_code(projectEntry[0]),
                                }
                            }
                        })
            else:
                for entry in entries:
                    if is_project_allocation_type(entry):
                        if get_total_hours([entry], payCodes) != 0:
                            mo_project = is_project_mo(
                                entry, taskDetails, projects)
                            if (mo_project is True):
                                get_timesheet_mo_line_item(
                                    childRows, entry, accountDetails, payCodes, oefTags, taskDetails, projects, divisions)
                            else:
                                childRows.append(
                                    {
                                        "row": {
                                            "rsId": "LDMTIME_TSLN",
                                            "tranType": "INSERT",
                                            "data": {
                                                "ACCT_ID": get_account_id(accountDetails),
                                                "BILL_LAB_CAT_CD": get_billing_labor_category(entry, billingRates),
                                                "ORG_ID": get_org_id(entry, taskDetails, projects, divisions),
                                                "PAY_TYPE": get_pay_type(entry, payCodes, oefTags),
                                                "PROJ_ID": get_project_id(entry, taskDetails),
                                                "TS_LN_DT": get_line_date(entry),
                                                "TS_LN___CHG_HRS": get_total_hours([entry], payCodes),
                                                "TS_LN___S_TS_LN_TYPE_CD": get_line_type_code(entry),
                                                "TS_LN___NOTES": get_comments(entry)
                                            }
                                        }
                                    }
                                )
            return childRows

        def get_grouped_project_timeentries(taskDetails, payCodes, billingRates, accountDetails, projects, divisions, oefTags, projectEntry):
            mo_info = get_mo_details(taskDetails, projectEntry[0])
            return {
                "row": {
                    "rsId": "LDMTIME_TSLN",
                    "tranType": "INSERT",
                    "data": {
                        "ACCT_ID": get_account_id(accountDetails),
                        "BILL_LAB_CAT_CD": get_billing_labor_category(projectEntry[0], billingRates),
                        "ORG_ID": get_org_id(projectEntry[0], taskDetails, projects, divisions),
                        "PAY_TYPE": get_pay_type(projectEntry[0], payCodes, oefTags),
                        "PROJ_ID": get_mo_project_id(projects, taskDetails, projectEntry[0]),
                        "TS_LN___CHG_HRS": get_total_hours(projectEntry, payCodes),
                        "TS_LN___S_TS_LN_TYPE_CD": get_line_type_code(projectEntry[0]),
                    }
                },
                "children": [
                    {
                        "row": {
                            "rsId": "LDMTIME_TSLNMO",
                            "tranType": "INSERT",
                            "data": {
                                "MO_ID": mo_info['mo_id'] if mo_info else "",
                                "MO_OPER_SEQ_NO": mo_info['seq'] if mo_info else "",
                                "MO_OPER_STEP_NO": mo_info['step'] if mo_info else "",
                                "S_ACTIVITY_TYPE": mo_info['activity_type'] if mo_info else ""
                            }
                        }
                    }
                ]
            }

        def is_project_mo(entry, taskDetails, project_details):
            project_uri = get_project_uri(entry, taskDetails)
            if project_uri:
                project_info = list(
                    filter(lambda x: x['projectDetails']['uri'] == project_uri, project_details))
                if project_info:
                    custom_field_info = project_info[0]['projectDetails']["customFields"]
                    mo_project_flag = rail.find_first_by_attr_and_get_attr(
                        custom_field_info, 'customField.displayText', config.proj_mo_project_flag, 'text')
                    return True if mo_project_flag and mo_project_flag.lower() == 'yes' else False
            return False

        def get_project_uri(entry, taskDetails):
            task_uri = get_task_uri(entry)
            task_info = list(
                filter(lambda x: x['uri'] == task_uri, taskDetails))
            project_uri = task_info[0]['project']['uri'] if task_info else None
            return project_uri

        def get_mo_project_id(project_details, taskDetails, entry):
            task_uri = get_task_uri(entry)
            task_info = list(
                filter(lambda x: x['uri'] == task_uri, taskDetails))
            project_uri = task_info[0]['project']['uri'] if task_info else None
            if project_uri:
                project_info = list(
                    filter(lambda x: x['projectDetails']['uri'] == project_uri, project_details))
                custom_field_info = project_info[0]['projectDetails']["customFields"]
                referance_project = rail.find_first_by_attr_and_get_attr(
                    custom_field_info, 'customField.displayText', config.proj_referance_project_id, 'text')
                return referance_project
            return ""

        def get_mo_details(taskDetails, entry):
            activity_type = rail.find_first_by_attr_and_get_attr(
                entry['extensionFieldValues'], 'definition.displayText', config.activity_type, 'tag.displayText')
            workcenter = rail.find_first_by_attr_and_get_attr(
                entry['extensionFieldValues'], 'definition.displayText', config.work_center, 'tag.displayText')
            task_uri = get_task_uri(entry)
            for task in taskDetails:
                if task['uri'] == task_uri:
                    return {
                        "mo_id": task['project']['code'],
                        "seq": task['parent']['task']['code'],
                        "step": task['code'],
                        "activity_type": activity_type,
                        "workcenter": workcenter
                    }
            return None

        def get_timesheet_mo_line_item(childRows, entry, accountDetails, payCodes, oefTags, taskDetails, projects, divisions):
            mo_info = get_mo_details(taskDetails, entry)
            mo_line_item = {
                "row": {
                    "rsId": "LDMTIME_TSLN",
                    "tranType": "INSERT",
                    "data": {
                        "ACCT_ID": get_account_id(accountDetails),
                        "BILL_LAB_CAT_CD": null,
                        "ORG_ID": get_org_id(entry, taskDetails, projects, divisions),
                        "PAY_TYPE": get_pay_type(entry, payCodes, oefTags),
                        "PROJ_ID": get_mo_project_id(projects, taskDetails, entry),
                        "TS_LN_DT": get_line_date(entry),
                        "TS_LN___CHG_HRS": get_total_hours([entry], payCodes),
                        "TS_LN___S_TS_LN_TYPE_CD": config.mo_line_type,
                        "TS_LN___NOTES": get_comments(entry)
                    },
                    "children": [
                        {
                            "row": {
                                "rsId": "LDMTIME_TSLNMO",
                                "tranType": "INSERT",
                                "data": {
                                    "MO_ID": mo_info['mo_id'] if mo_info else "",
                                    "MO_OPER_SEQ_NO": mo_info['seq'] if mo_info else "",
                                    "MO_OPER_STEP_NO": mo_info['step'] if mo_info else "",
                                    "S_ACTIVITY_TYPE": mo_info['activity_type'] if mo_info else ""
                                }
                            }
                        }
                    ]
                }
            }

            childRows.append(mo_line_item)

        def get_comments(entry):
            if entry and entry['customMetadata']:
                comment = rail.find_first_by_attr_and_get_attr(
                    entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:comments', 'value.text')
                return comment[0:254] if comment else ''
            return ''

        def is_project_allocation_type(entry):
            if entry and entry['timeAllocationTypeUris'] \
                    and 'urn:replicon:time-allocation-type:project' in entry['timeAllocationTypeUris']:
                return True
            return False

        def get_line_type_code(entry):
            return config.line_type if entry else None

        def get_line_date(entry):
            return get_formatted_date(entry['entryDate'])

        def get_formatted_date(dateObject):
            return f"{dateObject['year']}-{str(dateObject['month']).zfill(2)}-{str(dateObject['day']).zfill(2)}T00:00:00"

        def get_project_id(entry, taskDetails):
            taskuri = get_task_uri(entry)
            if taskuri:
                for task in taskDetails:
                    if task['uri'] == taskuri:
                        return task['code']
            return ""

        def get_project_name(entry, taskDetails):
            taskuri = get_task_uri(entry)
            if taskuri:
                for task in taskDetails:
                    if task['uri'] == taskuri:
                        return task['project']['name']
            return ""

        def get_task_uri(entry):
            return rail.find_first_by_attr_and_get_attr(
                entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:task', 'value.uri')

        def get_billing_labor_category(entry, billingRates):
            billingRateUri = rail.find_first_by_attr_and_get_attr(
                entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:billing-rate', 'value.uri')
            if billingRateUri:
                return rail.find_first_by_attr_and_get_attr(
                    billingRates, 'uri', billingRateUri, 'description')

        def get_org_id(entry, tasks, projects, divisions):
            if entry:
                taskUri = get_task_uri(entry)
                if taskUri:
                    projectUri = rail.find_first_by_attr_and_get_attr(
                        tasks, 'uri', taskUri, 'project.uri')
                    if projectUri:
                        divisionUri = rail.find_first_by_attr_and_get_attr(
                            projects, 'projectDetails.uri', projectUri, 'projectDetails.division.uri')
                        if divisionUri:
                            return rail.find_first_by_attr_and_get_attr(
                                divisions, 'uri', divisionUri, 'code')

        def get_account_id(costCenterDetails):
            if costCenterDetails:
                return costCenterDetails['code']

        def get_pay_type(entry, allPaycodes, oefTags):
            paycode = get_pay_code(entry, allPaycodes, oefTags)
            return paycode['code'] if paycode and paycode['code'] else config.regular_pay_type

        def get_pay_code(entry, allPaycodes, oefTags):
            if entry:
                payCodeUri = rail.find_first_by_attr_and_get_attr(
                    entry['customMetadata'], 'keyUri', 'urn:replicon:object-type-uri:pay-code', 'value.uri')
                if payCodeUri:
                    return rail.find_first_by_attr_and_get_attr(
                        allPaycodes, 'uri', payCodeUri)
                # no matching paycode
                payTypeOefName = Variable.get(
                    config.pay_type_oef_var_name, default_var='Pay Type')
                tagUri = rail.find_first_by_attr_and_get_attr(
                    entry['extensionFieldValues'], 'definition.displayText', payTypeOefName, 'tag.uri')
                if tagUri:
                    oefTag = rail.find_first_by_attr_and_get_attr(
                        oefTags, 'uri', tagUri)
                    if oefTag:
                        return {
                            "code": oefTag['code'],
                            "multiplier": oefTag['description'] if oefTag['description'] else 1.0
                        }
            return None

        def get_reg_hours(time_entries, allPayCodes, oefTags):
            totalHours = 0.0
            if allPayCodes:
                for entry in time_entries:
                    if is_project_allocation_type(entry) and entry and entry['interval'] \
                            and is_reg_paycode(entry, allPayCodes, oefTags):
                        totalHours += get_hours(entry['interval'])
            return totalHours

        def get_total_hours(time_entries, allPayCodes):
            totalHours = 0.0
            if allPayCodes:
                for entry in time_entries:
                    if is_project_allocation_type(entry) and entry and entry['interval']:
                        totalHours += get_hours(entry['interval'])
            return totalHours

        def get_other_hours(time_entries, allPayCodes, oefTags):
            totalHours = 0.0
            for entry in time_entries:
                if is_project_allocation_type(entry) and entry and entry['interval'] \
                        and entry['interval'] and not is_reg_paycode(entry, allPayCodes, oefTags):
                    totalHours += get_hours(entry['interval'])
            return totalHours

        def is_reg_paycode(entry, allPayCodes, oefTags):
            payCode = get_pay_code(entry, allPayCodes, oefTags)
            if payCode:
                if float(payCode['multiplier']) == 1.0:
                    return True
                return False
            return True

        def get_hours(hoursObject):
            if hoursObject['hours']:
                return hoursObject['hours']['hours'] + hoursObject['hours']['minutes']/60.00 + hoursObject['hours']['seconds']/3600.00

            timePair = hoursObject['timePair']
            if timePair and timePair['startTime'] and timePair['endTime']:
                endDate = datetime(
                    0, 0, 0, timePair['endTime']['hour'], timePair['endTime']['minute'], timePair['endTime']['second'])
                startDate = datetime(
                    0, 0, 0, timePair['startTime']['hour'], timePair['startTime']['minute'], timePair['startTime']['second'])
                if startDate > endDate:
                    endDate = endDate + timedelta(days=1)
                diff = endDate - startDate
                return diff.total_seconds()/3600.00

        timesync_error = rail.PythonOperator(
            task_id="timesync_error",
            python_callable=get_export_message
        )

        send_unexpected_error = rail.EmailOperator(
            task_id='send_unexpected_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint Timesheet Sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong>
            <br /> <br />Hello, <br /> <br /> {{ result('timesync_error') }}
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_replicon_timesheet

        get_replicon_timesheet >> get_replicon_time_entries >> get_replicon_pay_codes >> \
            get_replicon_user_details >> get_account_details >> get_replicon_task_details >> get_replicon_project_details >> \
            get_division_details >> get_replicon_billing_rate_details >> get_oef_tag_details >> \
            get_existing_deltek_timesheet >> is_timesheet_available
        is_timesheet_available >> rail.Label(
            'yes') >> revert_existing_time >> push_time_to_costpoint >> is_export_successful
        is_timesheet_available >> rail.Label(
            'no') >> push_time_to_costpoint >> is_export_successful
        is_export_successful >> rail.Label(
            'no') >> export_error >> send_error >> catch_error >> timesync_error >> send_unexpected_error >> log_to_sumo
        is_export_successful >> rail.Label('yes') >> catch_error
        return dag


rail.for_each_instance(create_dag)
