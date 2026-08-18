from datetime import timedelta
import json
import rail
from odessa.project_team_update_v2.utils import python_callable_method
from odessa.project_team_update_v2.utils import request_payload
from odessa.project_team_update_v2.utils import response_filter
from odessa.project_team_update_v2.utils import custom_method

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'odessa_jira_import_child_process_jira_data_v2_{config.instance}',
        description=f'odessa jira import child process jira data V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.second_master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Simple conditional flow - base page vs API call
        is_base_page = rail.IfOperator(
            task_id='is_base_page',
            test='{{ dag_run.conf.is_base_page == "True" }}',
            yes_task='use_base_response',
            no_task='fetch_and_check_next_page'
        )

        # Base page: Use pre-loaded data from master DAG
        use_base_response = rail.PythonOperator(
            task_id='use_base_response',
            python_callable=lambda dag_run: rail.load_json_artifact(dag_run.conf['base_response'])["issues"]
        )

        # Non-base page: Make HTTP call and check for next page
        fetch_and_check_next_page = rail.SimpleHttpOperator(
            task_id='fetch_and_check_next_page',
            method='POST',
            endpoint='rest/api/3/search/jql',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                "jql": 'Customer != null AND Wing != null AND "Sync to Replicon" = "Yes" AND ("Is it available in Replicon" = null OR "Is it available in Replicon" != "Yes") AND updated >= -1h',
                "maxResults": 1000,
                "fields": ["*all"],
                "fieldsByKeys": False,
                "nextPageToken": '{{ dag_run.conf.next_page_token }}'
            }),
            http_conn_id=config.http_conn_id,
            response_filter=lambda response: response.json(),
            dag=dag,
        )

        # Check if we need to trigger next page (only for non-base pages)
        has_next_page = rail.IfOperator(
            task_id='has_next_page',
            test=lambda: str(rail.result("fetch_and_check_next_page")['isLast']).lower() != "true",
            yes_task='trigger_next_page',
            no_task='extract_issues_from_response'
        )

        trigger_next_page = rail.TriggerDagRunOperator(
            task_id='trigger_next_page',
            trigger_dag_id=f'odessa_jira_import_child_process_jira_data_v2_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                'page_number': int(dag_run.conf['page_number']) + 1,
                'next_page_token': rail.result("fetch_and_check_next_page")["nextPageToken"],
                'is_base_page': 'False',
                'base_response': None
            }
        )

        # Extract issues from HTTP response for processing
        extract_issues_from_response = rail.PythonOperator(
            task_id='extract_issues_from_response',
            python_callable=lambda: rail.result("fetch_and_check_next_page")["issues"]
        )

        # Get the issues data from whichever path was taken
        jira_sync_data = rail.PythonOperator(
            task_id='jira_sync_data',
            python_callable=lambda dag_run: (
                rail.result("use_base_response") if dag_run.conf["is_base_page"] == "True"
                else rail.result("extract_issues_from_response")
            )
        )

        map_to_issue_schema = rail.DataAdaptorOperator(
            task_id="map_to_issue_schema",
            source='{{ result("jira_sync_data") | to_json}}',
            columns=['key', 'summary', 'customer', 'wing', 'task_type', 'parent_jira', 'epic_id'],
            data=custom_method.convert_input_data_to_task_data,
        )

        compose_csv = rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source="{{ result('map_to_issue_schema') }}",
            header=[
                'key',
                'ecid'],
            row=[
                '{{ item.key }}',
                '{{ dag_run_ecid() }}',
            ],
        )

        upload_file = rail.SFTPUploadFileOperator(
            task_id='upload_file',
            remote_filepath=config.filepath +"/jiralist_page{{ dag_run.conf.page_number }}_{{ dag_run_ecid() | replace(':', '-')}}.csv",
            content="{{ result('compose_csv') }}",
        )

        jira_list_collection = rail.CreateCollectionOperator(
            task_id='jira_list_collection',
            source="{{result('map_to_issue_schema')}}",
            name='jiraupdatedata{{dag_run.conf.page_number}}',
        )

        query_jira_projects = rail.QueryCollectionOperator(
            task_id='query_jira_projects',
            query="""SELECT * FROM jiraupdatedata{{dag_run.conf.page_number}}""",
            name='queryjiralist{{dag_run.conf.page_number}}'
        )

        has_any_data = rail.IfOperator(
            task_id='has_any_data',
            test="{{result('query_jira_projects','length') > 0}}",
            no_task="log_to_sumo",
            yes_task='jira_list'
        )

        jira_list = rail.QueryCollectionOperator(
            task_id='jira_list',
            query="""SELECT DISTINCT customer as customer FROM queryjiralist{{dag_run.conf.page_number}}""",
            name= 'jiracustomerlist{{dag_run.conf.page_number}}',
        )

        get_all_uniq_projects_based_on_customer = rail.TriggerDagRunForEachItemOperator(
            task_id='get_all_uniq_projects_based_on_customer',
            trigger_dag_id=f'odessa_jira_import_child_get_project_data_v2_{config.instance}',
            items="{{ result('jira_list') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'customer': '{{ item.customer }}',
                'page_number': '{{ dag_run.conf.page_number}}'
            }
        )

        wait_for_get_all_uniq_projects_based_on_customer = rail.WaitForDagRunsSensor(
            task_id='wait_for_get_all_uniq_projects_based_on_customer',
            dag_runs='{{ result("get_all_uniq_projects_based_on_customer") }}',
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        gather_time_and_materials_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_and_materials_data',
            dag_runs="{{ result('get_all_uniq_projects_based_on_customer') }}",
            dagrun_task_id='add_time_and_materials_data',
            flatten=True,
        )

        gather_fixed_bid_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_fixed_bid_data',
            dag_runs="{{ result('get_all_uniq_projects_based_on_customer') }}",
            dagrun_task_id='add_fixed_bid_items_data',
            flatten=True,
        )

        final_project_data = rail.PythonOperator(
            task_id='final_project_data',
            python_callable=lambda: [*rail.result('gather_time_and_materials_data'),*rail.result('gather_fixed_bid_data')]
        )

        variable_list_has_data = rail.IfOperator(
            task_id='variable_list_has_data',
            test="{{ result('final_project_data') | is_truthy }}",
            yes_task="get_all_task_custom_fields",
            no_task='log_to_sumo'
        )

        get_all_task_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_task_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                "objectUri": "urn:replicon:object-type:task"
            },
            response_filter=response_filter.get_task_custom_field
        )

        get_all_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_project_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                "objectUri": "urn:replicon:object-type:project"
            },
            response_filter=response_filter.get_project_custom_field
        )

        unique_project_list = rail.PythonOperator(
            task_id='unique_project_list',
            python_callable=lambda: python_callable_method.get_unique_project_list(
                rail.result("final_project_data"))
        )

        get_all_project_data = rail.RepliconServiceOperator(
            task_id='get_all_project_data',
            endpoint='services/ProjectListService1.svc/GetData',
            data=request_payload.get_all_project_data,
            response_filter=response_filter.project_list
        )

        final_compose_data = rail.DataAdaptorOperator(
            task_id='final_compose_data',
            source='{{ result("final_project_data") | to_json}}',
            columns=['Client', 'Clienturi', 'Projectname', 'Key', 'Summary', 'Customer', 'Wing', 'Billingtype', 'Repliconprojectname',
                     'Repliconprojecturi', 'Repliconprojectstatus', 'Repliconprojectstartdate', 'Repliconprojectenddate',
                     'Issuetype', 'Parentjira', 'Epicid'],
            data=python_callable_method.final_data_list
        )

        upload_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_sftp',
            remote_filepath=config.filepath +"/jirasync_{{ dag_run_ecid() | replace(':', '-')}}.csv",
            content="{{ result('final_compose_data') }}",
        )

        jira_and_project_list_collection = rail.CreateCollectionOperator(
            task_id='jira_and_project_list_collection',
            source='{{result("final_compose_data")}}',
            name='jiraandprojectdata{{dag_run.conf.page_number}}',
        )

        projects_not_present_in_replicon_query = rail.QueryCollectionOperator(
            task_id='projects_not_present_in_replicon_query',
            query="""SELECT * FROM jiraandprojectdata{{dag_run.conf.page_number}}
                    WHERE NULLIF(Repliconprojectstatus, '') IS NULL
                    """,
            name="projectsnotinrepliconquery{{dag_run.conf.page_number}}"
        )

        has_any_projects_not_present_in_replicon_query = rail.IfOperator(
            task_id='has_any_projects_not_present_in_replicon_query',
            test="{{ result('projects_not_present_in_replicon_query','length') > 0 }}",
            yes_task='get_all_new_unique_projects',
            no_task='query_all_non_archieved_projects_in_replicon'
        )

        get_all_new_unique_projects = rail.QueryCollectionOperator(
            task_id='get_all_new_unique_projects',
            query="""SELECT DISTINCT Projectname as Projectname FROM jiraandprojectdata{{dag_run.conf.page_number}}
            WHERE NULLIF(Repliconprojectstatus, '') IS NULL AND NULLIF(Projectname, '') IS NOT NULL
                    """,
            name="getallnewuniqueprojects{{dag_run.conf.page_number}}"
        )

        for_each_operator_for_new_project = rail.ForEachOperator(
            task_id= 'for_each_operator_for_new_project',
            items="{{ result('get_all_new_unique_projects') }}",
            start_task= 'get_all_jira_for_specified_project',
            end_task= 'for_each_operator_end'
        )

        get_all_jira_for_specified_project = rail.QueryCollectionOperator(
            task_id='get_all_jira_for_specified_project',
            query="""SELECT * FROM jiraandprojectdata{{dag_run.conf.page_number}}
                    WHERE Projectname == :Projectname
                    """,
            query_params={
                "Projectname": "{{ result('for_each_operator_for_new_project').Projectname }}",
            },
            name='getjirataskdetails{{dag_run.conf.page_number}}'
        )

        process_jira_for_new_projects = rail.TriggerDagRunForEachItemOperator(
            task_id='process_jira_for_new_projects',
            trigger_dag_id=f'odessa_jira_import_child_create_project_v2_{config.instance}',
            items="{{ result('get_all_jira_for_specified_project') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'Lasttimeentrydate_URI': "{{ result('get_all_task_custom_fields')[0] }}",
                'createdby_task_URI': "{{ result('get_all_task_custom_fields')[1] }}",
                'createdby_project': "{{ result('get_all_project_custom_fields') }}",
                'issuetype_uri': "{{ result('get_all_task_custom_fields')[2] }}",
                'parentid_uri': "{{ result('get_all_task_custom_fields')[3] }}",
                'epiclink_uri': "{{ result('get_all_task_custom_fields')[4] }}",
                'epicid_uri': "{{ result('get_all_task_custom_fields')[5] }}",
                'epicsummary_uri': "{{ result('get_all_task_custom_fields')[6] }}",
                'Projectname': "{{ item.Projectname}}",
                'end_date': config.end_date,
                'Key': "{{ item.Key }}",
                'Billingtype': "{{ item.Billingtype }}",
                'Summary': "{{ item.Summary}}",
                'Issuetype': "{{ item.Issuetype}}",
                'Parentjira': "{{ item.Parentjira}}",
                'Epicid': "{{ item.Epicid}}",
                'Clienturi': '{{ item.Clienturi }}'
            }
        )

        for_each_operator_end = rail.EmptyOperator(
            task_id = 'for_each_operator_end'
        )

        query_all_non_archieved_projects_in_replicon = rail.QueryCollectionOperator(
            task_id='query_all_non_archieved_projects_in_replicon',
            query="""SELECT * FROM jiraandprojectdata{{dag_run.conf.page_number}}
                    WHERE Repliconprojectstatus!="Archived" AND NULLIF(Repliconprojectstatus,'') IS NOT NULL
                    """,
            name="getuniquenewprojects{{dag_run.conf.page_number}}"
        )

        process_jira_for_existing_projects = rail.TriggerDagRunForEachItemOperator(
            task_id='process_jira_for_existing_projects',
            trigger_dag_id=f'odessa_jira_import_child_update_project_v2_{config.instance}',
            items="{{ result('query_all_non_archieved_projects_in_replicon') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'Lasttimeentrydate_URI': "{{ result('get_all_task_custom_fields')[0] }}",
                'createdby_task_URI': "{{ result('get_all_task_custom_fields')[1] }}",
                'issuetype_uri': "{{ result('get_all_task_custom_fields')[2] }}",
                'parentid_uri': "{{ result('get_all_task_custom_fields')[3] }}",
                'epiclink_uri': "{{ result('get_all_task_custom_fields')[4] }}",
                'epicid_uri': "{{ result('get_all_task_custom_fields')[5] }}",
                'epicsummary_uri': "{{ result('get_all_task_custom_fields')[6] }}",
                'Projectname': "{{ item.Projectname}}",
                'Repliconprojecturi': "{{ item.Repliconprojecturi }}",
                'Key': "{{ item.Key }}",
                'Billingtype': "{{ item.Billingtype }}",
                'Summary': "{{ item.Summary}}",
                'Repliconprojectstartdate': "{{ item.Repliconprojectstartdate }}",
                'end_date': config.end_date,
                'Issuetype': "{{ item.Issuetype}}",
                'Parentjira': "{{ item.Parentjira}}",
                'Epicid': "{{ item.Epicid}}"
            }
        )

        is_allowed_update_custom_field_in_jira= rail.IfOperator(
            task_id= 'is_allowed_update_custom_field_in_jira',
            test= config.is_update_custom_field_in_jira,
            yes_task= 'update_custom_field_in_jira',
            no_task='get_synced_issues_log'
        )

        update_custom_field_in_jira = rail.TriggerDagRunForEachItemOperator(
            task_id='update_custom_field_in_jira',
            trigger_dag_id=f'odessa_jira_import_child_update_custom_field_v2_{config.instance}',
            items="{{ result('jira_sync_data') | to_json }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'issuekey': "{{ item.key }}",
            }
        )

        get_synced_issues_log = rail.CreateLogOperator(
            task_id="get_synced_issues_log",
            tenant_wide_name="odessa_jira_synced_issues",
            existing_log_mode="append",
        )

        write_synced_issues_log = rail.WriteLogOperator(
            task_id="write_synced_issues_log",
            log="{{ result('get_synced_issues_log') }}",
            items= '{{ result("query_jira_projects") }}',
            message= "Odessa processed issues",
            properties={
                'Issuekey': "{{ item.key }}",
                'Masterid': "{{ dag_run_ecid() }}",
                'syncDate': "{{ current_time('%Y/%m/%d') }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'No of jira': '{{ result("query_jira_projects","length") }}'
            }
        )

        # Simple conditional flow
        is_base_page >> rail.Label("Yes") >> use_base_response >> jira_sync_data
        is_base_page >> rail.Label("No") >> fetch_and_check_next_page >> has_next_page

        # For non-base pages, check if we need to trigger next page
        has_next_page >> rail.Label("Yes") >> trigger_next_page >> extract_issues_from_response >> jira_sync_data
        has_next_page >> rail.Label("No") >> extract_issues_from_response

        # Continue with current page processing
        jira_sync_data >> map_to_issue_schema >> compose_csv >> upload_file >> jira_list_collection >> query_jira_projects >> has_any_data

        has_any_data >> rail.Label("No") >> log_to_sumo

        has_any_data >> rail.Label(
            "Yes") >> jira_list >> get_all_uniq_projects_based_on_customer >> wait_for_get_all_uniq_projects_based_on_customer

        wait_for_get_all_uniq_projects_based_on_customer >> gather_time_and_materials_data

        gather_time_and_materials_data >> gather_fixed_bid_data >> final_project_data >> variable_list_has_data

        variable_list_has_data >> rail.Label(
            "Yes") >> get_all_task_custom_fields >> get_all_project_custom_fields >> unique_project_list

        variable_list_has_data >> rail.Label("No") >> log_to_sumo

        unique_project_list >> get_all_project_data >> final_compose_data >> upload_file_to_sftp >> jira_and_project_list_collection

        jira_and_project_list_collection >> projects_not_present_in_replicon_query >> has_any_projects_not_present_in_replicon_query

        has_any_projects_not_present_in_replicon_query >> rail.Label(
            "Yes") >> get_all_new_unique_projects >> for_each_operator_for_new_project >> get_all_jira_for_specified_project >> \
                process_jira_for_new_projects >> for_each_operator_end

        for_each_operator_for_new_project >> for_each_operator_end >> query_all_non_archieved_projects_in_replicon

        has_any_projects_not_present_in_replicon_query >> rail.Label(
            "No") >> query_all_non_archieved_projects_in_replicon

        query_all_non_archieved_projects_in_replicon >> process_jira_for_existing_projects >> is_allowed_update_custom_field_in_jira >> rail.Label(
            "Yes") >> update_custom_field_in_jira >> get_synced_issues_log

        is_allowed_update_custom_field_in_jira >> rail.Label(
            "No") >> get_synced_issues_log >> write_synced_issues_log >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
