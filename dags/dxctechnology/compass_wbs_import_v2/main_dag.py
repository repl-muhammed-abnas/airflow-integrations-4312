# pylint: disable=too-many-statements
from datetime import timedelta
import rail
from dxctechnology.compass_wbs_import_v2.tasks.get_active_project_prereqs import get_active_project_prereqs

#config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/compass_wbs_import_v2/config.py

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dagid,
        description='COMPASS_WBS_Automation Master - SFTP',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
        )

        is_xml = rail.IfOperator(
            task_id='is_xml',
            test='{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon project sync for Compass WBS - Incorrect File Format - {{ current_time() }}',
            html_content="templates/emails/email_bad_file_format.html",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/{{ ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('download_file') }}",
            xsd_document='./dags/dxctechnology/compass_wbs_import_v2/xml_schema/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='create_project_collection',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon project sync for Compass WBS - Blank Payload - {{ current_time() }}',
            html_content="templates/emails/email_blank_payload.html",
        )

        create_project_collection = rail.CreateCollectionOperator(
            task_id='create_project_collection',
            name ='inputdata',
            source="{{ result('parse_xml') | xpath('Records') }}",
        )

        query_inactive_projects = rail.QueryCollectionOperator(
            task_id='query_inactive_projects',
            query="SELECT * FROM inputdata WHERE LOWER(Status) = 'inactive'"
        )

        process_inactive_projects = rail.trigger_parallel_dagrun(
            task_id='process_inactive_projects',
            items="{{ result('query_inactive_projects') }}",
            trigger_dag_id=config.inactive_project_dagid,
            parallel_count=config.trigger_parallel_dagrun_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_active_projects = rail.QueryCollectionOperator(
            task_id='query_active_projects',
            query="SELECT * FROM inputdata WHERE LOWER(Status) = 'active'"
        )

        has_any_active_projects = rail.IfOperator(
            task_id='has_any_active_projects',
            test='{{ result("query_active_projects", "length") > 0 }}',
            no_task="generate_output_log",
            yes_task="active_projects_prereqs",
        )

        active_project_prereqs_group_entry, active_project_prereqs_group_exit = get_active_project_prereqs()

        query_unique_programs_from_payload = rail.QueryCollectionOperator(
            task_id='query_unique_programs_from_payload',
            query="""SELECT DISTINCT Project,ProjectDescription
                        FROM inputdata
                        WHERE NULLIF(Project, '') IS NOT NULL
                        """
        )

        has_programs = rail.IfOperator(
            task_id='has_programs',
            test='{{ result("query_unique_programs_from_payload", "length") > 0 }}',
            no_task="finish_client_program_processing",
            yes_task="dummy_process_programs",
        )

        dummy_process_programs = rail.EmptyOperator(
            task_id='dummy_process_programs'
        )

        process_programs = rail.trigger_parallel_dagrun(
            task_id='process_programs',
            parallel_count=config.trigger_parallel_dagrun_count,
            items="{{ result('query_unique_programs_from_payload') }}",
            trigger_dag_id=config.process_program_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'program_name': f"{item['Project']}-{item['ProjectDescription']}" if item['ProjectDescription'] else f"{item['Project']}-",
            }
        )

        query_unique_clients_from_payload = rail.QueryCollectionOperator(
            task_id='query_unique_clients_from_payload',
            query="""SELECT ClientName,ClientID
                        FROM inputdata
                        WHERE NULLIF(ClientID, '') IS NOT NULL
                        GROUP BY ClientID
                        """
        )

        has_clients = rail.IfOperator(
            task_id='has_clients',
            test='{{ result("query_unique_clients_from_payload", "length") > 0 }}',
            no_task="finish_client_program_processing",
            yes_task="dummy_process_clients",
        )

        dummy_process_clients = rail.EmptyOperator(
            task_id='dummy_process_clients'
        )

        process_clients = rail.trigger_parallel_dagrun(
            task_id='process_clients',
            parallel_count=config.trigger_parallel_dagrun_count,
            items="{{ result('query_unique_clients_from_payload') }}",
            trigger_dag_id=config.process_client_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= lambda item: {
                'client':  item['ClientID'] ,
                'client_code':  item['ClientName'] if item['ClientName'] else None
            }
        )

        finish_client_program_processing =rail.EmptyOperator(
           task_id= 'finish_client_program_processing'
        )

        # pylint: disable=line-too-long
        process_active_projects = rail.trigger_parallel_dagrun(
            task_id='process_active_projects',
            items="{{ result('query_active_projects') }}",
            parallel_count=config.trigger_parallel_dagrun_count,
            trigger_dag_id=config.active_project_dagid,
            conf={
                'wbs': '{{ item.WBS }}',
                'description': '{{ item.WBSDescription | sn }}',
                'status': '{{ "In Progress" if item.Status == "ACTIVE" else "Completed" if item.Status == "INACTIVE" else "" }}',
                'companycode': '{{ result("get_divisions_company_codes") | filter_by_attr("name", "equals", item.CompanyCode) | first_or_default | attr_or_default("uri", "") }}',
                'payloadcompanycode': '{{ item.CompanyCode }}',
                'personresponsible1': '{{ item.PersonResponsible1 | sn }}',
                'personresponsible2': '{{ item.PersonResponsible2 | sn }}',
                'adminpermissionuri': '{{ result("get_permission_sets") | filter_by_attr("name", "equals", "Project Team Assignment Data Import") | first_or_default | attr_or_default("uri", "") }}',
                'projectmanagerpermissionuri': '{{ result("get_permission_sets") | filter_by_attr("name", "equals", "Limited WBS Manager") | first_or_default | attr_or_default("uri", "") }}',
                'enduserpermissionuri': '{{ result("get_permission_sets") | filter_by_attr("name", "equals", "Manager") | first_or_default | attr_or_default("uri", "") }}',
                'employeetyperestrictiongroups': '{{ result("get_employee_types") | map_to_attr({ "name": "name", "uri": "uri" }) | to_json }}',
                'startdate': '{{ item.ProjectStart | sn}}',
                'enddate': '{{ item.ProjectEnd | sn}}',
                'timetrackingattributeuri': '{{ result("get_project_oefs").timetrackingrequired }}',
                'compassprojecttypeuri': '{{ result("get_project_oefs").compassprojecttype }}',
                'globalwbsindicatoruri': '{{ result("get_project_oefs").globalflag }}',
                'iwowbsindicatoruri': '{{ result("get_project_oefs").iwoindicator }}',
                'wbstypeuri': '{{ result("get_project_oefs").wbstype }}',
                'timetrackingattribute': '{{ result("get_timetrackingrequired_values") | filter_by_attr("name", "equals", item.TimeTrackingRequiredAttribute) | first_or_default | attr_or_default("uri", "") }}',
                'compassprojecttype': '{{ result("get_compassprojecttype_values") | filter_by_attr("name", "equals", item.ProjectType) | first_or_default | attr_or_default("uri", "") }}',
                'globalwbsindicator': '{{ result("get_globalflag_values") | filter_by_attr("name", "equals", item.GlobalWBSIndicator) | first_or_default | attr_or_default("uri", "") }}',
                'iwowbsindicator': '{{ result("get_iwoindicator_values") | filter_by_attr("name", "equals", item.IWOWBSIndicator) | first_or_default | attr_or_default("uri", "") }}',
                'projectteamassignment': '{{ result("get_divisions_company_codes") | filter_by_attr("name", "equals", "IWO") | first_or_default | attr_or_default("uri") if item.IWOWBSIndicator == "X" else \
                    result("get_divisions_company_codes") | filter_by_attr("name", "equals", "COMPASS") | first_or_default | attr_or_default("uri") if item.GlobalWBSIndicator == "X" else \
                    result("get_divisions_company_codes") | filter_by_attr("name", "equals", item.CompanyCode) | first_or_default | attr_or_default("parenturi", "") }}',
                'organizationunituri': '{{ result("get_departments") | filter_by_attr("displayText", "equals", "DXC") | first_or_default | attr_or_default("uri", "") }}',
                'salesforceopportunityiduri': '{{ result("get_project_oefs").salesforceopportunityid | sn }}',
                'salesforceopportunitynameuri': '{{ result("get_project_oefs").salesforceopportunityname | sn }}',
                'client': '{{ item.ClientID | sn }}',
                'salesforceopportunityname': '{{ item.OpportunityName | sn}}',
                'salesforceopportunityid': '{{ item.OpportunityID | sn}}',
                'program': '{{item.Project + "-" + item.ProjectDescription if  item.ProjectDescription and item.Project  else item.Project + "-" if item.Project  else None}}',
                'programid': '{{ item.Project | sn}}',
                'parentwbsfilteruri':'{{result("get_all_filter_definitions")}}',
                'parentwbscolumnuri': '{{result("get_all_columns")}}',
                'gsapcompanycodes': "{{ result('get_gsap_company_codes') }}",
                'psaprojectteamassignmenturi': '{{ result("get_divisions_company_codes") | filter_by_attr("name", "equals", "PSA") | first_or_default | attr_or_default("uri") }}',
                'psa_x_flaguri': '{{ result("get_psaflag_values") | filter_by_attr("name", "equals", "X") | first_or_default | attr_or_default("uri", "") }}',
                'psaflagdefinitionuri': '{{ result("get_project_oefs").psaflag | sn }}',
                'projectofferinggroupvalue': '{{ item.ProjectOfferingGroup | sn }}',
                'projectofferinggroupuri' : '{{ result("get_project_oefs").projectofferinggroup | sn }}',
                'projectofferinggroup': '{{ result("get_projectofferinggroup_values") | filter_by_attr("name", "equals", item.ProjectOfferingGroup) | first_or_default | attr_or_default("uri", "") }}',
                'wbsofferinggroupvalue': '{{ item.WBSOfferingGroup | sn }}',
                'wbsofferinggroupuri': '{{ result("get_project_oefs").wbsofferinggroup | sn }}',
                'wbsofferinggroup': '{{ result("get_wbsofferinggroup_values") | filter_by_attr("name", "equals", item.WBSOfferingGroup) | first_or_default | attr_or_default("uri", "") }}',
                'serviceofferingid': '{{ item.ServiceOfferingID | sn }}',
                'serviceofferingiddefinitionuri': '{{ result("get_project_oefs").serviceofferingid | sn }}',
                },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

        get_success_projects = rail.FilterLogEntriesOperator(
            task_id='get_success_projects',
            properties={'status': 'Success'}
        )

        get_errored_projects = rail.FilterLogEntriesOperator(
            task_id='get_errored_projects',
            properties={'status': 'Error'}
        )

        get_exception_projects = rail.FilterLogEntriesOperator(
            task_id='get_exception_projects',
            properties={'status': 'Exception'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{ get_master_log() | load_all_records | length }}',
                'Function: COMPASS WBS Master inbound',
                '',
                ''],
            row=[
                '{{ item.properties | attr_or_default("projectname", "")  }}',
                '{{ item.properties | attr_or_default("projectcode", "") }}',
                '{{ item.properties | attr_or_default("status", "") }}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
            footer=['Number of Records Processed Successfully: {{ result("get_success_projects", key="length") }}',
                    'Number of Records Processed with Exception: {{ result("get_exception_projects", key="length") }}',
                    'Number of Records with Error: {{ result("get_errored_projects", key="length") }}', '', ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_projects', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon project sync for Compass WBS " }} \
                {%- if result("get_errored_projects", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_projects", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="templates/emails/email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        new_file_sensor >> is_xml >> rail.Label(
            'Yes') >> download_file >> parse_xml >> has_data >> rail.Label('Yes') >> create_project_collection
        create_project_collection >> query_unique_programs_from_payload >>  has_programs

        has_programs >> rail.Label("No") >>finish_client_program_processing
        has_programs >> rail.Label(
                "Yes") >> dummy_process_programs >> process_programs >> finish_client_program_processing
        create_project_collection >> query_unique_clients_from_payload >>  has_clients

        has_clients >> rail.Label("No") >> finish_client_program_processing
        has_clients >> rail.Label(
                "Yes") >> dummy_process_clients >> process_clients >> finish_client_program_processing
        finish_client_program_processing >> [
            query_active_projects, query_inactive_projects]
        query_active_projects >> has_any_active_projects >> rail.Label(
            'Yes') >> active_project_prereqs_group_entry
        has_any_active_projects >> rail.Label("No") >> generate_output_log >> [
            get_success_projects, get_errored_projects, get_exception_projects] >> render_logs_csv
        query_inactive_projects >> process_inactive_projects >> generate_output_log
        active_project_prereqs_group_exit >> process_active_projects >> generate_output_log
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        is_xml >> rail.Label("No") >> send_bad_file_format_email
        has_data >> rail.Label("No") >> send_blank_payload_email
        # was_new_file_found has trigger_rule = 'all_done', so it will execute whenever download_file is done, regardless of whether it
        # succeeded, failed, or was skipped
        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
    return dag

rail.for_each_instance(create_main_airflow_dag)
