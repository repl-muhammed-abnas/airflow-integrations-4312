import datetime
import rail
from airflow.models import Variable
from dxctechnology.c1_leanstaffing_assignment_v1.currency_map import CURRENCY_MAP

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_leanstaffing_assignment_v1/config.py

# pylint: disable=too-many-statements


def create_dag(config):
    dag_id_postfix = f'_{config.instance}_v1'
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_leanstaffassignment{dag_id_postfix}',
        description=f'DXC C1 Leanstaffing Assignment {config.instance} v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval="0 */2 * * *",
        start_date=datetime.datetime(2022, 1, 1),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        get_webhook_log = rail.CreateLogOperator(
            task_id="get_webhook_log",
            tenant_wide_name=config.get_webhook_log_name,
            existing_log_mode="truncate",
        )

        has_any_data = rail.HasDataOperator(
            task_id="has_any_data",
            source="{{ result('get_webhook_log', 'truncated_data') }}",
            yes_task='write_csv_backup',
            no_task='delete_this_dagrun'
        )

        write_csv_backup = rail.WriteCSVFileOperator(
            task_id="write_csv_backup",
            source="{{ result('get_webhook_log', 'truncated_data') }}",
            header=[
                'execution-correlation-id',
                'project-uri',
                'project-name',
                'event-type',
                'event-time',
                'user-uri'],
            row=['{{ item.ecid }}', '{{ item.properties.project_uri }}', '{{ item.properties.project_name }}', '{{ item.message }}',
                 '{{ item.properties.event_time }}', '{{ item.properties.user_uri }}'],
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        archive_input_webhooks = rail.SFTPUploadFileOperator(
            task_id='archive_input_webhooks',
            content="{{ result('write_csv_backup') }}",
            sftp_conn_id=config.secondary_sftp_conn_id,
            remote_filepath=config.archive_filepath +
            '/{{ ecid() | replace(":", "-") }}_webhookevents.csv',
        )

        create_events_collection = rail.CreateCollectionOperator(
            task_id='create_events_collection',
            source="{{ result('write_csv_backup') }}",
        )

        query_project_users = rail.QueryCollectionOperator(
            task_id='query_project_users',
            query='SELECT DISTINCT project_uri, min(project_name) as project_name, user_uri FROM create_events_collection WHERE ' +
            'event_type IN ("ProjectTeamMemberBillingRateAssociationsModified", "ProjectTeamMemberAssignmentDatesModified") GROUP BY project_uri, user_uri',
        )

        query_project_team = rail.QueryCollectionOperator(
            task_id='query_project_team',
            query='''SELECT project_uri, min(project_name) as project_name FROM create_events_collection
                    WHERE event_type = "ProjectTeamModified" GROUP BY project_uri''',
        )

        query_earliest_event_time = rail.QueryCollectionOperator(
            task_id='query_earliest_event_time',
            mode='single-row',
            query='SELECT min(event_time) as earliest FROM create_events_collection',
        )

        get_team_changes = rail.TriggerDagRunForEachItemOperator(
            task_id='get_team_changes',
            trigger_dag_id=f'dxctechnology_c1_leanstaffassignment_team_changes{dag_id_postfix}',
            items="{{ result('query_project_team') }}",
            execution_timeout=datetime.timedelta(days=14),
            conf={
                'project_uri': '{{ item.project_uri }}',
                # pylint: disable=line-too-long
                'get_changes_since': "{{ (macros.datetime.fromisoformat(result('query_earliest_event_time').earliest) + macros.timedelta(minutes=-2)).isoformat() }}",
            }
        )

        wait_for_get_team_changes = rail.WaitForDagRunsSensor(
            task_id='wait_for_get_team_changes',
            execution_timeout=datetime.timedelta(days=14),
            dag_runs='{{ result("get_team_changes") }}'
        )

        gather_added_team_members = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_added_team_members',
            dag_runs="{{ result('get_team_changes') }}",
            dagrun_task_id='added_users',
            flatten=True,
        )

        gather_removed_team_members = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_removed_team_members',
            dag_runs="{{ result('get_team_changes') }}",
            dagrun_task_id='map_removed_users',
            flatten=True,
        )

        create_added_members_collection = rail.CreateCollectionOperator(
            task_id='create_added_members_collection',
            source="{{ result('gather_added_team_members') | tojson }}",
            columns=["project_uri", "user_uri"],
        )

        create_removed_members_collection = rail.CreateCollectionOperator(
            task_id='create_removed_members_collection',
            source="{{ result('gather_removed_team_members') | tojson }}",
            columns=['projectname', 'masterwbs', 'internalsapobjectid', 'employeeid', 'username',
                     'labortype', 'currency', 'datechangeflag', 'projecturi', 'companycode', 'projectstartdate',
                     'projectenddate', 'projectype', 'taskassignment_billingratechangedate', 'useruri',
                     'usercompanycode', 'assignmentstartdate', 'assignmentenddate']
        )

        query_eligible_users = rail.QueryCollectionOperator(
            task_id='query_eligible_users',
            query='''SELECT project_uri, user_uri from query_project_users
                        UNION
                        SELECT project_uri, user_uri from create_added_members_collection
                        ''',
        )

        get_team_assignments_extract_details = rail.RepliconReportDetailsOperator(
            task_id='get_team_assignments_extract_details',
            report_name=config.extract_report_name,
        )

        # pylint: disable=consider-using-f-string
        filter_uri_expr = "{{ (result('get_team_assignments_extract_details').filterConfiguration.enabledFilters | " + \
            "filter_by_attr('displayText', 'equals', '%s') | first()).uri }}" % config.report_filter_name

        report_group_entry, report_group_exit = rail.run_report(
            group_id='run_team_assignments_extract',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_team_assignments_extract_details').uri }}",
                        "filterValues": [
                            # these empty filters are here to work around a bug in
                            # the core services (remove them any you get 500s,
                            # *shrug*)
                            {"reportFilterUri": filter_uri_expr, "value": None},
                            {
                                "reportFilterUri": filter_uri_expr,
                                # pylint: disable=line-too-long
                                "value": "{{ (macros.datetime.fromisoformat(result('query_earliest_event_time').earliest)).strftime('%m/%d/%Y') }}"
                            },
                            {"reportFilterUri": filter_uri_expr, "value": None},
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_team_assignments_extract.get_report_result', 'has_data') }}",
            yes_task='report_has_expected_columns',
            no_task='fail_no_report_data',
        )

        fail_no_report_data = rail.FailOperator(
            task_id="fail_no_report_data",
            message="No data in the base report",
        )

        expected_report_columns = 'WBS / SO Name,"Master WBS (SO, WO)",Custom Internal SAP Object ID,Employee_id,User Name,' + \
            'Labor Type Name,WBS / SO Invoice Currency,Date Change Flag,ProjectUri,WBS / SO Company Code (Full Path),' + \
            'WBS / SO Start Date,WBS / SO End Date,Project Type,Taskassignment_billingratechangedate,' + \
            'UserUri,Company Code (Current),Assignment Start Date,Assignment End Date'
        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string line-too-long
            test="{{ result('run_team_assignments_extract.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            no_task='fail_bad_report_columns',
            yes_task='load_report_data',
        )

        fail_bad_report_columns = rail.FailOperator(
            task_id="fail_bad_report_columns",
            message="Base report column does not match",
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_team_assignments_extract.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            source="{{ result('load_report_data') }}",
            columns={
                'WBS / SO Name': 'projectname',
                'Master WBS (SO, WO)': 'masterwbs',
                'Custom Internal SAP Object ID': 'internalsapobjectid',
                'Employee_id': 'employeeid',
                'User Name': 'username',
                'Labor Type Name': 'labortype',
                'WBS / SO Invoice Currency': 'currency',
                'Date Change Flag': 'datechangeflag',
                'ProjectUri': 'projecturi',
                'WBS / SO Company Code (Full Path)': 'companycode',
                'WBS / SO Start Date': 'projectstartdate',
                'WBS / SO End Date': 'projectenddate',
                'Project Type': 'projectype',
                'Taskassignment_billingratechangedate': 'taskassignment_billingratechangedate',
                'UserUri': 'useruri',
                'Company Code (Current)': 'usercompanycode',
                'Assignment Start Date': 'assignmentstartdate',
                'Assignment End Date': 'assignmentenddate',
            }
        )

        query_eligible_projects = rail.QueryCollectionOperator(
            task_id='query_eligible_projects',
            query='''SELECT * from create_report_collection
                    WHERE EXISTS
                        (SELECT 1 FROM create_events_collection where project_uri = create_report_collection.projecturi)''',
        )

        query_specific_project_users = rail.QueryCollectionOperator(
            task_id='query_specific_project_users',
            query='''SELECT ep.*, NULL as Indicator FROM query_eligible_projects ep
                    INNER JOIN query_eligible_users eu ON eu.project_uri = ep.projecturi AND eu.user_uri = ep.useruri
                    UNION
                    SELECT *, 'D' as Indicator FROM create_removed_members_collection''',
        )

        has_any_project_users = rail.IfOperator(
            task_id="has_any_project_users",
            test="{{ result('query_specific_project_users', 'length') > 0 }}",
            yes_task='can_send_result',
        )

        can_send_result = rail.IfOperator(
            task_id="can_send_result",
            test=lambda: Variable.get(config.can_send_result_var).lower() == 'true',
            yes_task='convert_to_target_format',
        )

        def convert_data_to_target_format(data):
            if not data:
                return None

            def get_currency_for_code(code):
                entry = CURRENCY_MAP.get(code, {'check': False})
                return entry['currency'] if entry['check'] else None

            date_format = "%d %B %Y"

            def format_date(date):
                return datetime.datetime.strptime(date, date_format).strftime('%Y%m%d')

            def get_end_date(end_date):
                today_date = datetime.datetime.utcnow()
                if data.get('Indicator') == 'D':
                    if datetime.datetime.strptime(end_date, date_format) > today_date:
                        return (today_date + datetime.timedelta(days=14)).strftime(date_format)
                    return end_date
                return end_date if data.get('assignmentstartdate') and data.get('assignmentenddate') else data.get('projectenddate')

            ret = {
                'WBSElement': data['projectname'] if 'WBS' in data['masterwbs'] else None,
                'ServiceOrder': data['projectname'] if 'SO' in data['masterwbs'] else None,
                'AssignmentObjectType': {"WBS": "P", "SO": "O"}.get(data['masterwbs']),
                'ObjectID': data['internalsapobjectid'],
                'PersonnelNumber': data['employeeid'],
                'StartDate': format_date(data['assignmentstartdate']
                                         if data['assignmentstartdate'] and data['assignmentenddate'] else data['projectstartdate']),
                'EndDate': format_date(
                    get_end_date(data['assignmentenddate'] if data['assignmentenddate'] else data['projectenddate'])),
                'CurrencyKey': get_currency_for_code(data['usercompanycode']),
                'CATSRelevant': 'X',
                'ForecastingRelevant': 'X',
                'LaborType': data['labortype'].split('|')[0] if '|' in data['labortype'] else None,
                'BillableNonBillable': None if '|' not in data['labortype'] or 'Non-Billable' in data['labortype'].split('|')[1] else 'X',
                'StaffAssignmentRole': None,
                'DateChangeFlag': data['datechangeflag'],
                'Indicator': None if not data['Indicator'] else data['Indicator'],
            }
            return {k: v if v is not None else '' for k, v in ret.items()}

        convert_to_target_format = rail.DataAdaptorOperator(
            task_id='convert_to_target_format',
            source="{{ result('query_specific_project_users') }}",
            columns=['WBSElement', 'ServiceOrder', 'AssignmentObjectType', 'ObjectID', 'PersonnelNumber', 'StartDate', 'EndDate', 'CurrencyKey',
                     'CATSRelevant', 'ForecastingRelevant', 'LaborType', 'BillableNonBillable', 'StaffAssignmentRole', 'DateChangeFlag', 'Indicator'],
            data=convert_data_to_target_format,
        )

        write_sftp_output_filename = rail.RenderTemplateOperator(
            task_id='write_sftp_output_filename',
            target='result',
            template=config.output_filepath +
            '/{{ ecid() | replace(":", "-") }}_LeanstaffAssignment.xml'
        )

        write_csv_file = rail.WriteCSVFileOperator(
            task_id='write_csv_file',
            source="{{ result('convert_to_target_format') }}",
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content="{{ result('write_csv_file') }}",
            remote_filepath="{{ result('write_sftp_output_filename') | replace('.xml', '.csv') }}",
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id='write_xml_file',
            target='artifact',
            template_file='output_template.xml',
            dataset="{{ result('convert_to_target_format') }}",
        )

        upload_xml_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_xml_to_sftp',
            content="{{ result('write_xml_file') }}",
            remote_filepath="{{ result('write_sftp_output_filename') }}",
        )

        post_to_http_endpoint = rail.TriggerDagRunForEachItemOperator(
            task_id='post_to_http_endpoint',
            items="{{ result('convert_to_target_format') }}",
            execution_timeout=datetime.timedelta(days=14),
            trigger_dag_id=f'dxctechnology_c1_leanstaffassignment_post_output{dag_id_postfix}',
            batch_size=config.post_batch_size
        )

        wait_for_post_to_http_endpoint = rail.WaitForDagRunsSensor(
            task_id='wait_for_post_to_http_endpoint',
            execution_timeout=datetime.timedelta(days=14),
            dag_runs='{{ result("post_to_http_endpoint") }}'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id='send_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | C1 LeanstaffAssignment extract from Replicon completed - {{ current_time() }}',
            html_content="templates/email/email_export_complete.html",
            params={
                "http_conn_id": config.http_conn_id,
            }
        )

        get_webhook_log >> has_any_data >> rail.Label('Yes') >> write_csv_backup >> [
            create_events_collection, archive_input_webhooks, get_team_assignments_extract_details]
        create_events_collection >> [
            query_project_users,
            query_project_team,
            query_earliest_event_time]
        [query_project_team, query_earliest_event_time] >> get_team_changes >> wait_for_get_team_changes >> \
            [gather_added_team_members, gather_removed_team_members] >> create_added_members_collection >> \
            create_removed_members_collection >> query_eligible_users >> query_specific_project_users
        query_project_users >> query_eligible_users
        get_team_assignments_extract_details >> report_group_entry
        report_group_exit >> report_has_data >> rail.Label('Yes') >> report_has_expected_columns >> rail.Label('Yes') >> \
            load_report_data >> create_report_collection >> \
            query_eligible_projects >> query_specific_project_users >> has_any_project_users >> can_send_result
        can_send_result >> rail.Label("Yes") >> convert_to_target_format >> post_to_http_endpoint >> \
            wait_for_post_to_http_endpoint >> send_export_complete_email
        convert_to_target_format >> write_csv_file >> upload_csv_to_sftp >> send_export_complete_email
        convert_to_target_format >> write_xml_file >> upload_xml_to_sftp >> send_export_complete_email
        convert_to_target_format >> write_sftp_output_filename >> [
            upload_csv_to_sftp, upload_xml_to_sftp]
        report_has_data >> rail.Label('No') >> fail_no_report_data
        report_has_expected_columns >> rail.Label(
            'No') >> fail_bad_report_columns
        has_any_data >> rail.Label('No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_dag)
