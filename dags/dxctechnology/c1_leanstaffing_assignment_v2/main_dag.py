import datetime
from datetime import timedelta
import logging
import rail
from airflow.models import Variable
from dxctechnology.c1_leanstaffing_assignment_v2.currency_map import CURRENCY_MAP
from dxctechnology.c1_leanstaffing_assignment_v2.utils import python_method

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_leanstaffing_assignment_v2/config.py

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.export_master_dag_id,
        description=f'DXC C1 Leanstaffing Assignment {config.instance}',
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
            yes_task='can_run_batch_task',
            no_task='delete_this_dagrun'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='write_csv_backup'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='write_csv_backup',
            end_task='finish',
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

        # OPTIMIZATION: Combined query to reduce collection scans
        query_project_users = rail.QueryCollectionOperator(
            task_id='query_project_users',
            query='''SELECT DISTINCT project_uri, min(project_name) as project_name, user_uri
                    FROM create_events_collection
                    WHERE event_type IN ("ProjectTeamMemberBillingRateAssociationsModified", "ProjectTeamMemberAssignmentDatesModified")
                    GROUP BY project_uri, user_uri''',
        )

        query_project_team = rail.QueryCollectionOperator(
            task_id='query_project_team',
            query='''SELECT project_uri, min(project_name) as project_name
                    FROM create_events_collection
                    WHERE event_type = "ProjectTeamModified"
                    GROUP BY project_uri''',
        )

        # OPTIMIZATION: Single scan for earliest event time
        query_earliest_event_time = rail.QueryCollectionOperator(
            task_id='query_earliest_event_time',
            mode='single-row',
            query='SELECT min(event_time) as earliest FROM create_events_collection',
        )

        # ------------------------------------------------------------------
        # Export-side bulk validation (gated, trial only).
        # When enabled, the webhook processor logged events on a fast path
        # without per-event validation, so this block validates the distinct
        # event projects in bulk and writes the tracking UDF for eligible
        # projects. Eligibility is enforced in two places that together cover
        # both export paths:
        #   - additions: query_eligible_projects intersects the report with the
        #     gate-aware eligible-projects collection (built below);
        #   - deletions: get_team_changes is restricted to eligible projects.
        # When disabled, the eligible collection equals all event projects, so
        # query_eligible_projects is identical to the original behaviour and the
        # whole validation branch is skipped.
        # ------------------------------------------------------------------
        # Always computed (cheap, no API) - used by bulk validation and as the
        # gate-off fallback for the eligible-projects collection.
        query_distinct_event_projects = rail.QueryCollectionOperator(
            task_id='query_distinct_event_projects',
            query='SELECT DISTINCT project_uri FROM create_events_collection '
                  'WHERE NULLIF(project_uri, "") IS NOT NULL',
        )

        should_bulk_validate = rail.IfOperator(
            task_id='should_bulk_validate',
            test=lambda: (Variable.get(
                config.export_bulk_validation_var_name, default_var='false').lower() == 'true')
            if config.export_bulk_validation_var_name else False,
            yes_task='bulk_get_event_projects',
            no_task='get_team_changes',
        )

        bulk_get_event_projects = rail.RepliconServiceOperator(
            task_id='bulk_get_event_projects',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda: {"projects": [
                {"uri": row['project_uri']} for row in rail.load_all_records(rail.result('query_distinct_event_projects'))]},
            response_filter=lambda resp: [
                (item or {}).get('projectDetails') for item in resp.json()['d']],
        )

        get_distinct_division_uris = rail.PythonOperator(
            task_id='get_distinct_division_uris',
            python_callable=lambda: [
                {'uri': uri} for uri in {
                    pd['division']['uri']
                    for pd in rail.result('bulk_get_event_projects')
                    if pd and pd.get('division') and pd['division'].get('uri')
                }
            ],
        )

        bulk_get_divisions = rail.RepliconServiceCallForEachItemOperator(
            task_id='bulk_get_divisions',
            endpoint='/services/DivisionService1.svc/GetDivisionDetails',
            items=lambda: rail.result('get_distinct_division_uris'),
            data=lambda item: {"divisionUri": item['uri']},
        )

        def do_build_eligible_project_uris():
            projects = rail.result('bulk_get_event_projects')
            division_uris = rail.result('get_distinct_division_uris')
            division_results = rail.result('bulk_get_divisions') or []
            division_code_by_uri = {}
            for div_item, div_detail in zip(division_uris, division_results):
                if div_detail:
                    division_code_by_uri[div_item['uri']] = div_detail.get('code')
            return python_method.compute_eligible_project_uris(projects, division_code_by_uri)

        build_eligible_project_uris = rail.PythonOperator(
            task_id='build_eligible_project_uris',
            python_callable=do_build_eligible_project_uris,
        )

        get_udf_field = rail.RepliconServiceOperator(
            task_id='get_udf_field',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:project"},
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', 'Taskassignment_billingratechangedate'),
        )

        def build_udf_date_value():
            now = datetime.datetime.now(datetime.timezone.utc)
            return {'year': now.year, 'month': now.month, 'day': now.day}

        write_udf_for_eligible_projects = rail.RepliconServiceCallForEachItemOperator(
            task_id='write_udf_for_eligible_projects',
            endpoint='/services/CustomFieldService1.svc/UpdateDateValue',
            items=lambda: rail.result('build_eligible_project_uris'),
            data=lambda item: {
                'objectUri': item['project_uri'],
                'customFieldUri': rail.result('get_udf_field')['uri'],
                'value': build_udf_date_value(),
            },
        )

        # When bulk validation is enabled the webhook log contains events for all
        # projects (the processor skipped per-event validation), so the team-change
        # (deletion) path must be restricted to eligible projects too - otherwise
        # removed members on non-eligible projects would be exported as deletions.
        # The report/UDF filter only protects the additions path. When the gate is
        # disabled this returns query_project_team verbatim (identical behaviour).
        def get_team_change_project_items():
            team_projects = list(rail.load_all_records(rail.result('query_project_team')))
            if not config.export_bulk_validation_var_name:
                return team_projects
            if Variable.get(config.export_bulk_validation_var_name,
                            default_var='false').lower() != 'true':
                return team_projects
            # build_eligible_project_uris is a PythonOperator -> its result is the
            # actual list (no load_all_records needed).
            eligible_uris = {row['project_uri'] for row in rail.result('build_eligible_project_uris')}
            return [p for p in team_projects if p.get('project_uri') in eligible_uris]

        # OPTIMIZATION: Batch multiple projects per child DAG to reduce overhead
        get_team_changes = rail.TriggerDagRunForEachItemOperator(
            task_id='get_team_changes',
            trigger_dag_id=config.export_get_team_changes_child_dag_id,
            items=get_team_change_project_items,
            execution_timeout=datetime.timedelta(days=14),
            batch_size=config.team_changes_batch_size,  # Process multiple projects per child DAG
            conf={
                'projects': '{{ items | tojson }}',  # Pass batch of projects
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

        # Gate-aware set of projects eligible for export. When bulk validation is
        # ON this is the strictly-validated set from build_eligible_project_uris
        # (so a project that flipped non-eligible mid-day cannot leak in via a
        # stale tracking UDF). When OFF it is every distinct event project, which
        # makes query_eligible_projects below identical to the original
        # (report INTERSECT events) behaviour.
        def get_eligible_projects_for_filter():
            gate_on = bool(config.export_bulk_validation_var_name) and Variable.get(
                config.export_bulk_validation_var_name, default_var='false').lower() == 'true'
            if gate_on:
                # PythonOperator result is the actual list.
                return rail.result('build_eligible_project_uris')
            # QueryCollection result must be loaded into row dicts.
            return list(rail.load_all_records(rail.result('query_distinct_event_projects')))

        create_eligible_projects_collection = rail.CreateCollectionOperator(
            task_id='create_eligible_projects_collection',
            source=get_eligible_projects_for_filter,
            columns=['project_uri'],
        )

        query_eligible_projects = rail.QueryCollectionOperator(
            task_id='query_eligible_projects',
            query='''SELECT * from create_report_collection
                    WHERE EXISTS
                        (SELECT 1 FROM create_eligible_projects_collection where project_uri = create_report_collection.projecturi)''',
        )

        # OPTIMIZATION: Optimized query with better join strategy
        query_specific_project_users = rail.QueryCollectionOperator(
            task_id='query_specific_project_users',
            query='''SELECT ep.*, NULL as Indicator
                    FROM query_eligible_projects ep
                    INNER JOIN query_eligible_users eu
                        ON eu.project_uri = ep.projecturi AND eu.user_uri = ep.useruri
                    UNION
                    SELECT *, 'D' as Indicator
                    FROM create_removed_members_collection''',
        )

        has_any_project_users = rail.IfOperator(
            task_id="has_any_project_users",
            test="{{ result('query_specific_project_users', 'length') > 0 }}",
            yes_task='can_send_result',
            no_task='finish'
        )

        can_send_result = rail.IfOperator(
            task_id="can_send_result",
            test=lambda: Variable.get(config.can_send_result_var).lower() == 'true',
            yes_task='convert_to_target_format',
            no_task='finish'
        )

        # OPTIMIZATION: Pre-compile date operations and currency lookups
        # Create currency lookup with O(1) access
        CURRENCY_LOOKUP = {k: v['currency'] if v['check'] else None for k, v in CURRENCY_MAP.items()}

        def convert_data_to_target_format(data):
            if not data:
                return None

            # OPTIMIZATION: Direct currency lookup instead of function call
            currency = CURRENCY_LOOKUP.get(data.get('usercompanycode'))

            date_format = "%d %B %Y"
            today_date = datetime.datetime.now(datetime.timezone.utc)

            # OPTIMIZATION: Inline date operations to reduce function call overhead
            def format_date(date):
                if not date:
                    return None
                return datetime.datetime.strptime(date, date_format).strftime('%Y%m%d')

            def parse_date(date_str):
                # Return timezone-aware datetime for proper comparison
                return datetime.datetime.strptime(date_str, date_format).replace(tzinfo=datetime.timezone.utc)

            # V2.2 LOGIC - Complex Date Validation
            def validate_and_adjust_dates(assignment_start, assignment_end, project_start, project_end):
                """
                Implements the V2.2 7-scenario date validation table
                Returns: (should_export, adjusted_start_date, adjusted_end_date)
                """
                # Scenario 1: WBS status is 'In Progress' - then only export if assignment dates are valid
                
                # Parse dates for comparison
                # Use project dates if no assignment dates
                if not assignment_start:
                    assignment_start = project_start
                if not assignment_end:
                    if project_end:
                        assignment_end = project_end

                assign_start_dt = parse_date(assignment_start)
                assign_end_dt = parse_date(assignment_end)
                proj_start_dt = parse_date(project_start)
                proj_end_dt = parse_date(project_end) if project_end else parse_date(assignment_end)

                
                # Scenario 4: Assignment completely after WBS end date
                if assign_start_dt > proj_end_dt:
                    return False, None, None
                
                # Scenario 5: Assignment completely before WBS start date  
                if assign_end_dt < proj_start_dt:
                    return False, None, None
                
                # Scenario 2: Assignment within WBS dates
                if assign_start_dt >= proj_start_dt and assign_end_dt <= proj_end_dt:
                    return True, assignment_start, assignment_end
                
                # Scenario 6: Assignment start < WBS start, end > WBS start (but before WBS end)
                if assign_start_dt < proj_start_dt and assign_end_dt >= proj_start_dt and assign_end_dt <= proj_end_dt:
                    return True, project_start, assignment_end
                
                # Scenario 3: Assignment end > WBS end (but start before WBS end)
                if assign_start_dt >= proj_start_dt and assign_start_dt <= proj_end_dt and assign_end_dt > proj_end_dt:
                    return True, assignment_start, project_end
                
                # Scenario 7: Assignment spans beyond both WBS dates
                if assign_start_dt < proj_start_dt and assign_end_dt > proj_end_dt:
                    return True, project_start, project_end
                
                # Default case
                return True, assignment_start, assignment_end

            # Apply V2.2 validation
            should_export, validated_start, validated_end = validate_and_adjust_dates(
                data.get('assignmentstartdate'),
                data.get('assignmentenddate'), 
                data.get('projectstartdate'),
                data.get('projectenddate')
            )
            
            # If validation fails, return None to exclude from export
            if not should_export:
                return None

            # Handle deleted records (existing V1.9 logic)
            def get_end_date(end_date):
                if data.get('Indicator') == 'D':
                    if parse_date(end_date) > today_date:
                        return (today_date + datetime.timedelta(days=14)).strftime(date_format)
                    return end_date
                return end_date if data.get('assignmentstartdate') and data.get('assignmentenddate') else data.get('projectenddate')

            # Build the final record
            ret = {
                'WBSElement': data['projectname'] if 'WBS' in data['masterwbs'] else None,
                'ServiceOrder': data['projectname'] if 'SO' in data['masterwbs'] else None,
                'AssignmentObjectType': {"WBS": "P", "SO": "O"}.get(data['masterwbs']),
                'ObjectID': data['internalsapobjectid'],
                'PersonnelNumber': data['employeeid'],
                'StartDate': format_date(validated_start if data['assignmentstartdate'] and data['assignmentenddate'] else data['projectstartdate']),
                'EndDate': format_date(get_end_date(validated_end if data['assignmentenddate'] else data['projectenddate'])),
                'CurrencyKey': currency,  # Using pre-computed currency lookup
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
            trigger_dag_id=config.export_post_to_api_endpoint_child_dag_id,
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

        finish = rail.EmptyOperator(
            task_id = "finish"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> write_csv_backup

        # SERIALIZED EXECUTION: All tasks run sequentially
        get_webhook_log >> has_any_data >> rail.Label('Yes') >> can_run_batch_task
        write_csv_backup >> create_events_collection >> archive_input_webhooks >> \
            query_project_users >> query_project_team >> query_earliest_event_time >> \
            query_distinct_event_projects >> should_bulk_validate

        # Gated bulk-validation branch: validate the distinct event projects and
        # write the tracking UDF for eligible ones before the report runs. Both
        # branches reconverge at get_team_changes.
        should_bulk_validate >> rail.Label('Yes') >> \
            bulk_get_event_projects >> get_distinct_division_uris >> bulk_get_divisions >> \
            build_eligible_project_uris >> get_udf_field >> write_udf_for_eligible_projects >> \
            get_team_changes
        should_bulk_validate >> rail.Label('No') >> get_team_changes

        get_team_changes >> wait_for_get_team_changes >> \
            gather_added_team_members >> gather_removed_team_members >> \
            create_added_members_collection >> create_removed_members_collection >> \
            query_eligible_users >> get_team_assignments_extract_details >> \
            report_group_entry

        report_group_exit >> report_has_data >> rail.Label('Yes') >> report_has_expected_columns >> rail.Label('Yes') >> \
            load_report_data >> create_report_collection >> create_eligible_projects_collection >> \
            query_eligible_projects >> query_specific_project_users >> has_any_project_users >> rail.Label('Yes') >> can_send_result
        has_any_project_users >> rail.Label('No') >> finish

        # SERIALIZED OUTPUT: Generate and upload files sequentially
        can_send_result >> rail.Label("Yes") >> convert_to_target_format >> \
            write_sftp_output_filename >> write_csv_file >> upload_csv_to_sftp >> \
            write_xml_file >> upload_xml_to_sftp >> \
            post_to_http_endpoint >> wait_for_post_to_http_endpoint >> \
            send_export_complete_email >> finish
        report_has_data >> rail.Label('No') >> fail_no_report_data >> finish
        report_has_expected_columns >> rail.Label(
            'No') >> fail_bad_report_columns >> finish
        has_any_data >> rail.Label('No') >> delete_this_dagrun

        can_send_result >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_dag)
