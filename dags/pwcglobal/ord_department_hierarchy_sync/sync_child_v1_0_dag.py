from datetime import timedelta, datetime
import json
from airflow.models import Variable
from pwcglobal.ord_department_hierarchy_sync.utils import response_filter, custom_methods, python_callable_method
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'pwc_ord_department_group_hierarchy_sync_child_v10_{config.instance}',
        description=f'PwC | ORD Department Group Hierarchy Sync Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        ord_level = "{{ dag_run.conf.ordlevel1 }}"
        ord_id = "{{ dag_run.conf.id }}"

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='pwc_ord_structure_from_variable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='pwc_ord_structure_from_variable',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        pwc_ord_structure_from_variable = rail.PythonOperator(
            task_id='pwc_ord_structure_from_variable',
            python_callable=python_callable_method.get_pwc_ord_structure_from_variable,
            op_args=[ord_level, ord_id, config.instance]
        )

        is_ord_structure_value_present_in_variable = rail.IfOperator(
            task_id='is_ord_structure_value_present_in_variable',
            test=lambda: rail.result('pwc_ord_structure_from_variable'),
            yes_task='parse_json',
            no_task='get_all_ord_structure_from_mft_3'
        )

        get_all_ord_structure_from_mft_3 = rail.SimpleHttpOperator(
            task_id='get_all_ord_structure_from_mft_3',
            method='GET',
            endpoint=config.endpoint,
            http_conn_id=config.http_conn_id,
            headers={
                "Accept": 'application/json',
                "Accept-Charset": 'utf-8',
                "apikey": "{{ var.value." + config.apikey + " }}",
                "apikeysecret": "{{ var.value." + config.apikeysecret + " }}",
                "Proxy-Authorization": "Basic {{ var.value." + config.proxy_token_var + " }}",
                "Authorization": "Basic {{ var.value." + config.token_var + " }}"
            },
            data={
                "since": "{{ dag_run.conf.datemodified }}",
                "filter": "ShareNode1Id=\"{{ dag_run.conf.id }}\""
            },
            extra_options={
                'verify': False
            },
        )

        parse_json = rail.PythonOperator(
            task_id='parse_json',
            python_callable=lambda: json.loads(
                rail.result('get_all_ord_structure_from_mft_3')) if rail.result('get_all_ord_structure_from_mft_3') else json.loads(
                rail.result('pwc_ord_structure_from_variable'))
        )

        create_ord_department_sync_logs = rail.CreateLogOperator(
            task_id='create_ord_department_sync_logs'
        )

        if_first_naturalkeyname_blank_6 = rail.IfOperator(
            task_id='if_first_naturalkeyname_blank_6',
            test=lambda: bool(len(rail.result('parse_json')) == 0 or rail.result(
                'parse_json')[0]['NaturalKeyName'] is null),
            yes_task="send_mail_send_no_records_email_7",
            no_task="create_ord_department_sync_logs",
        )

        send_mail_send_no_records_email_7 = rail.EmailOperator(
            task_id='send_mail_send_no_records_email_7',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            # pylint: disable=line-too-long
            subject='''{{ get_company_key() }}| Replicon Department group sync for ORD Automation - {{ dag_run.conf.ordlevel1 }} - No Records to Process - {{ dag_run.conf.jobcreatedtime }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /><br />Hello, <br /><br />The Replicon department group sync for ORD automation is completed successfully on {{ dag_run.conf.jobcreatedtime }}.<br /><br />There are no records to process from date "{{ dag_run.conf.datemodified }}" for territory "{{ dag_run.conf.ordlevel1 }}".</p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        ordlevlel1_uri_path = rail.EmptyOperator(
            task_id='ordlevlel1_uri_path'
        )

        if_request_ordlevel1uri_blank_11 = rail.IfOperator(
            task_id='if_request_ordlevel1uri_blank_11',
            test=lambda dag_run: dag_run.conf['ordlevel1uri'] is null,
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012",
            no_task="log_level1_uri_15",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012',
            retries=0,
            items=[0],
            trigger_dag_id=f'pwc_ord_department_group_hierarchy_sync_create_v10_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "name": "{{ dag_run.conf.ordlevel1 }}",
                "level": "1",
                "fullpath": "PwC/{{ dag_run.conf.ordlevel1 }}",
                "parenturi": "{{ dag_run.conf.rooturi }}",
                "code": null,
                "existing_dep_uri": null
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012") }}'
        )

        gather_departmentgroupuri_from_create = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_departmentgroupuri_from_create',
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012") }}',
            dagrun_task_id='create_department_group_or_apply_modification_6',
            flatten=True
        )

        log_department_group_created_success = rail.WriteLogOperator(
            task_id='log_department_group_created_success',
            log="{{ result('create_ord_department_sync_logs') }}",
            message='Created Successfully',
            properties={
                "name": "{{ dag_run.conf.ordlevel1 }}",
                "level": "1",
                "fullpath": "PwC/{{ dag_run.conf.ordlevel1 }}",
                "status": "Success",
                "details": "Created Successfully",
            }
        )

        gather_ord_department_logs_from_child_v1_012 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_ord_department_logs_from_child_v1_012',
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012") }}',
            dagrun_task_id='create_ord_department_sync_child_logs',
            flatten=True
        )

        log_level1_uri_15 = rail.PythonOperator(
            task_id='log_level1_uri_15',
            python_callable=lambda dag_run: dag_run.conf['ordlevel1uri'] if dag_run.conf['ordlevel1uri'] else rail.result(
                'gather_departmentgroupuri_from_create')[0]['uri']
        )

        get_child_hierarchy_databasedon_level1name_16 = rail.RepliconServiceOperator(
            task_id='get_child_hierarchy_databasedon_level1name_16',
            endpoint="/services/DepartmentGroupListService1.svc/GetHierarchyData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:effectively-enabled"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
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
                            "text": "{{ dag_run.conf.ordlevel1 }}",
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
                "hierarchyListDataOptionUris": [
                    "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
                ]
            },
            response_filter=response_filter.get_department_list_details
        )

        create_csv_lines_input_data_20 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_input_data_20',
            source="{{ result('parse_json') | to_json }}",
            header=['fullpath',
                    'sharenode1name',
                    'sharenode2name',
                    'sharenode3name',
                    'sharenode4name',
                    'sharenode5name',
                    'sharenode6name',
                    'sharenode6code',
                    'length',
                    'costcentername'],
            row=custom_methods.get_csv_rows_20
        )

        load_csv_create_list_from_csv_21 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_21",
            document="{{ result('create_csv_lines_input_data_20') }}",
        )

        create_collection_create_list_from_csv_21 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_21',
            source="{{ result('load_csv_create_list_from_csv_21') }}",
            name="rawdata",
            columns={
                'fullpath': 'fullpath',
                'sharenode1name': 'sharenode1name',
                'sharenode2name': 'sharenode2name',
                'sharenode3name': 'sharenode3name',
                'sharenode4name': 'sharenode4name',
                'sharenode5name': 'sharenode5name',
                'sharenode6name': 'sharenode6name',
                'sharenode6code': 'sharenode6code',
                'length': 'length',
                'costcentername': 'costcentername'
            }
        )

        create_csv_lines_level2_data_22 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_level2_data_22',
            source="{{ result('parse_json') | to_json }}",
            header=['fullpath',
                    'sharenode1name',
                    'sharenode2name',
                    'sharenode2fullpath',
                    'sharenode2uri',
                    'status'],
            row=custom_methods.get_csv_rows_22
        )

        load_csv_create_list_from_csv_23 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_23",
            document="{{ result('create_csv_lines_level2_data_22') }}",
        )

        create_collection_create_list_from_csv_23 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_23',
            source="{{ result('load_csv_create_list_from_csv_23') }}",
            name="level2data",
            columns={
                'fullpath': 'fullpath',
                'sharenode1name': 'parent',
                'sharenode2name': 'name',
                'sharenode2fullpath': 'level2fullpath',
                'sharenode2uri': 'uri',
                'status': 'status'
            }
        )

        query_list_get_all_distinct_level2_24 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level2_24',
            query="""SELECT DISTINCT name, level2fullpath, uri FROM level2data WHERE uri = '' or uri IS NULL""",
        )

        if_query_list_get_all_distinct_level2_24_rows_greater_than_0_25 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level2_24_rows_greater_than_0_25',
            test='''{{ result('query_list_get_all_distinct_level2_24', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_028",
            no_task="query_list_get_all_distinct_level2wherestatusisdisabled_31",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_028 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_028',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level2_24') }}",
            trigger_dag_id=f'pwc_ord_department_group_hierarchy_sync_create_v10_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "name": "{{ item.name }}",
                "level": "2",
                "fullpath": "{{ item.level2fullpath }}",
                "parenturi": "{{ result('log_level1_uri_15') }}",
                "code": null,
                "existing_dep_uri": null
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_028 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_028',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_028") }}'
        )

        log_department_group_created_level2_success = rail.WriteLogOperator(
            task_id='log_department_group_created_level2_success',
            log="{{ result('create_ord_department_sync_logs') }}",
            items="{{ result('query_list_get_all_distinct_level2_24') }}",
            message='Created Successfully',
            properties={
                "name": "{{ item.name }}",
                "level": "2",
                "fullpath": "{{ item.level2fullpath }}",
                "status": "Success",
                "details": "Created Successfully",
            }
        )

        gather_ord_department_logs_from_child_v1_028 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_ord_department_logs_from_child_v1_028',
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_028") }}',
            dagrun_task_id='create_ord_department_sync_child_logs',
            flatten=True
        )

        query_list_get_all_distinct_level2wherestatusisdisabled_31 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level2wherestatusisdisabled_31',
            query="""SELECT DISTINCT name, level2fullpath, uri, status FROM level2data WHERE NULLIF(uri,'') IS NOT NULL AND status='False'"""
        )

        if_query_list_get_all_distinct_level2wherestatusisdisabled_31_rows_greater_than_0_32 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level2wherestatusisdisabled_31_rows_greater_than_0_32',
            test='''{{ result('query_list_get_all_distinct_level2wherestatusisdisabled_31', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_035",
            no_task="get_child_hierarchy_databasedon_level1uri_32",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_035 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_035',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level2wherestatusisdisabled_31') }}",
            trigger_dag_id=f'pwcglobal_ord_department_hierarchy_sync_new_pwc_ord_department_group_hierarchy_sync_enable_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "uri": "{{ item.uri }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_035 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_035',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_035") }}'
        )

        get_child_hierarchy_databasedon_level1uri_32 = rail.RepliconServiceOperator(
            task_id='get_child_hierarchy_databasedon_level1uri_32',
            endpoint="/services/DepartmentGroupListService1.svc/GetHierarchyData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:effectively-enabled"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
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
                            "text": "{{ dag_run.conf.ordlevel1 }}",
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
                "hierarchyListDataOptionUris": [
                    "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
                ]
            },
            response_filter=response_filter.get_department_list_details
        )

        create_csv_lines_level3_data_36 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_level3_data_36',
            source="{{ result('parse_json') | to_json }}",
            header=['fullpath',
                    'parentname',
                    'parentfullpath',
                    'name',
                    'levelfullpath',
                    'uri',
                    'status'],
            row=custom_methods.get_csv_rows_36
        )

        load_csv_create_list_from_csv_37 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_37",
            document="{{ result('create_csv_lines_level3_data_36') }}",
        )

        create_collection_create_list_from_csv_37 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_37',
            source="{{ result('load_csv_create_list_from_csv_37') }}",
            name="level3data",
            columns={
                'fullpath': 'fullpath',
                'parentname': 'parent',
                'parentfullpath': 'parentfullpath',
                'name': 'name',
                'levelfullpath': 'levelfullpath',
                'uri': 'uri',
                'status': 'status'
            }
        )

        query_list_get_all_distinct_level3_38 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level3_38',
            query="""SELECT DISTINCT parent, parentfullpath, name, levelfullpath, uri FROM level3data WHERE level3data.uri = '' or level3data.uri IS NULL""",
        )

        if_query_list_get_all_distinct_level3_38_rows_greater_than_0_39 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level3_38_rows_greater_than_0_39',
            test='''{{ result('query_list_get_all_distinct_level3_38', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_042",
            no_task="query_list_get_all_distinct_level3wherestatusisdisabled_52",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_042 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_042',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level3_38') }}",
            trigger_dag_id=f'pwc_ord_department_group_hierarchy_sync_create_v10_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "name": item['name'],
                "level": "3",
                "fullpath": item['levelfullpath'],
                "parenturi": custom_methods.get_parenturi(rail.result('get_child_hierarchy_databasedon_level1uri_32'),
                                                          item['parent'], item['parentfullpath']),
                "code": null,
                "existing_dep_uri": null
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_042 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_042',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_042") }}'
        )

        log_department_group_created_level3_success = rail.WriteLogOperator(
            task_id='log_department_group_created_level3_success',
            log="{{ result('create_ord_department_sync_logs') }}",
            items="{{ result('query_list_get_all_distinct_level3_38') }}",
            message='Created Successfully',
            properties={
                "name": "{{ item.name }}",
                "level": "3",
                "fullpath": "{{ item.levelfullpath }}",
                "status": "Success",
                "details": "Created Successfully",
            }
        )

        gather_ord_department_logs_from_child_v1_042 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_ord_department_logs_from_child_v1_042',
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_042") }}',
            dagrun_task_id='create_ord_department_sync_child_logs',
            flatten=True
        )

        query_list_get_all_distinct_level3wherestatusisdisabled_52 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level3wherestatusisdisabled_52',
            # pylint: disable=line-too-long
            query="""SELECT DISTINCT parent, parentfullpath, name, levelfullpath, uri, status FROM level3data WHERE NULLIF(uri,'') IS NOT NULL AND status='False'"""
        )

        if_query_list_get_all_distinct_level3wherestatusisdisabled_52_rows_greater_than_0_53 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level3wherestatusisdisabled_52_rows_greater_than_0_53',
            test='''{{ result('query_list_get_all_distinct_level3wherestatusisdisabled_52', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_056",
            no_task="get_child_hierarchy_databasedon_level1uri_46",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_056 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_056',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level3wherestatusisdisabled_52') }}",
            trigger_dag_id=f'pwcglobal_ord_department_hierarchy_sync_new_pwc_ord_department_group_hierarchy_sync_enable_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "uri": "{{ item.uri }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_056 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_056',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_056") }}'
        )

        get_child_hierarchy_databasedon_level1uri_46 = rail.RepliconServiceOperator(
            task_id='get_child_hierarchy_databasedon_level1uri_46',
            endpoint="/services/DepartmentGroupListService1.svc/GetHierarchyData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:effectively-enabled"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
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
                            "text": "{{ dag_run.conf.ordlevel1 }}",
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
                "hierarchyListDataOptionUris": [
                    "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
                ]
            },
            response_filter=response_filter.get_department_list_details
        )

        create_csv_lines_level4_data_50 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_level4_data_50',
            source="{{ result('parse_json') | to_json }}",
            header=['fullpath',
                    'parentname',
                    'parentfullpath',
                    'name',
                    'levelfullpath',
                    'uri',
                    'status'],
            row=custom_methods.get_csv_rows_50
        )

        load_csv_create_list_from_csv_51 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_51",
            document="{{ result('create_csv_lines_level4_data_50') }}",
        )

        create_collection_create_list_from_csv_51 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_51',
            source="{{ result('load_csv_create_list_from_csv_51') }}",
            name="level4data",
            columns={
                'fullpath': 'fullpath',
                'parentname': 'parent',
                'parentfullpath': 'parentfullpath',
                'name': 'name',
                'levelfullpath': 'levelfullpath',
                'uri': 'uri',
                'status': 'status'
            }
        )

        query_list_get_all_distinct_level4_52 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level4_52',
            query="""SELECT DISTINCT parent, parentfullpath, name, levelfullpath, uri FROM level4data WHERE uri = '' or uri IS NULL""",
        )

        if_query_list_get_all_distinct_level4_52_rows_greater_than_0_53 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level4_52_rows_greater_than_0_53',
            test='''{{ result('query_list_get_all_distinct_level4_52', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_056",
            no_task="query_list_get_all_distinct_level4wherestatusisdisabled_73",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_056 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_056',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level4_52') }}",
            trigger_dag_id=f'pwc_ord_department_group_hierarchy_sync_create_v10_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "name": item['name'],
                "level": "4",
                "fullpath": item['levelfullpath'],
                "parenturi": custom_methods.get_parenturi(rail.result('get_child_hierarchy_databasedon_level1uri_46'),
                                                          item['parent'], item['parentfullpath']),
                "code": null,
                "existing_dep_uri": null
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_056 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_056',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_056") }}'
        )

        log_department_group_created_level4_success = rail.WriteLogOperator(
            task_id='log_department_group_created_level4_success',
            log="{{ result('create_ord_department_sync_logs') }}",
            items="{{ result('query_list_get_all_distinct_level4_52') }}",
            message='Created Successfully',
            properties={
                "name": "{{ item.name }}",
                "level": "4",
                "fullpath": "{{ item.levelfullpath }}",
                "status": "Success",
                "details": "Created Successfully",
            }
        )

        gather_ord_department_logs_from_child_v1_056 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_ord_department_logs_from_child_v1_056',
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_056") }}',
            dagrun_task_id='create_ord_department_sync_child_logs',
            flatten=True
        )

        query_list_get_all_distinct_level4wherestatusisdisabled_73 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level4wherestatusisdisabled_73',
            # pylint: disable=line-too-long
            query="""SELECT DISTINCT parent, parentfullpath, name, levelfullpath, uri, status FROM level4data WHERE  NULLIF(uri,'') IS NOT NULL AND level4data.status='False'"""
        )

        if_query_list_get_all_distinct_level4wherestatusisdisabled_73_rows_greater_than_0_74 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level4wherestatusisdisabled_73_rows_greater_than_0_74',
            test='''{{ result('query_list_get_all_distinct_level4wherestatusisdisabled_73', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_077",
            no_task="get_child_hierarchy_databasedon_level1uri_60",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_077 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_077',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level4wherestatusisdisabled_73') }}",
            trigger_dag_id=f'pwcglobal_ord_department_hierarchy_sync_new_pwc_ord_department_group_hierarchy_sync_enable_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "uri": "{{ item.uri }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_077 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_077',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_077") }}'
        )

        get_child_hierarchy_databasedon_level1uri_60 = rail.RepliconServiceOperator(
            task_id='get_child_hierarchy_databasedon_level1uri_60',
            endpoint="/services/DepartmentGroupListService1.svc/GetHierarchyData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:effectively-enabled"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
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
                            "text": "{{ dag_run.conf.ordlevel1 }}",
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
                "hierarchyListDataOptionUris": [
                    "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
                ]
            },
            response_filter=response_filter.get_department_list_details
        )

        create_csv_lines_level5_data_64 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_level5_data_64',
            source="{{ result('parse_json') | to_json }}",
            header=['fullpath',
                    'parentname',
                    'parentfullpath',
                    'name',
                    'levelfullpath',
                    'uri',
                    'status'],
            row=custom_methods.get_csv_rows_64
        )

        load_csv_create_list_from_csv_65 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_65",
            document="{{ result('create_csv_lines_level5_data_64') }}",
        )

        create_collection_create_list_from_csv_65 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_65',
            source="{{ result('load_csv_create_list_from_csv_65') }}",
            name="level5data",
            columns={
                'fullpath': 'fullpath',
                'parentname': 'parent',
                'parentfullpath': 'parentfullpath',
                'name': 'name',
                'levelfullpath': 'levelfullpath',
                'uri': 'uri',
                'status': 'status'
            }
        )

        query_list_get_all_distinct_level5_66 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level5_66',
            query="""SELECT DISTINCT parent, parentfullpath, name, levelfullpath, uri FROM level5data WHERE uri = '' or uri IS NULL""",
        )

        if_query_list_get_all_distinct_level5_66_rows_greater_than_0_67 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level5_66_rows_greater_than_0_67',
            test='''{{ result('query_list_get_all_distinct_level5_66', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_070",
            no_task="query_list_get_all_distinct_level5wherestatusisdisabled_94",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_070 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_070',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level5_66') }}",
            trigger_dag_id=f'pwc_ord_department_group_hierarchy_sync_create_v10_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "name": item['name'],
                "level": "5",
                "fullpath": item['levelfullpath'],
                "parenturi": custom_methods.get_parenturi(rail.result('get_child_hierarchy_databasedon_level1uri_60'),
                                                          item['parent'], item['parentfullpath']),
                "code": null,
                "existing_dep_uri": null
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_070 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_070',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_070") }}'
        )

        log_department_group_created_level5_success = rail.WriteLogOperator(
            task_id='log_department_group_created_level5_success',
            log="{{ result('create_ord_department_sync_logs') }}",
            items="{{ result('query_list_get_all_distinct_level5_66') }}",
            message='Created Successfully',
            properties={
                "name": "{{ item.name }}",
                "level": "5",
                "fullpath": "{{ item.levelfullpath }}",
                "status": "Success",
                "details": "Created Successfully",
            }
        )

        gather_ord_department_logs_from_child_v1_070 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_ord_department_logs_from_child_v1_070',
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_070") }}',
            dagrun_task_id='create_ord_department_sync_child_logs',
            flatten=True
        )

        query_list_get_all_distinct_level5wherestatusisdisabled_94 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level5wherestatusisdisabled_94',
            # pylint: disable=line-too-long
            query="""SELECT DISTINCT parent, parentfullpath, name, levelfullpath, uri, status FROM level5data WHERE NULLIF(uri,'') IS NOT NULL AND status='False'""",
        )

        if_query_list_get_all_distinct_level5wherestatusisdisabled_94_rows_greater_than_0_95 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level5wherestatusisdisabled_94_rows_greater_than_0_95',
            test='''{{ result('query_list_get_all_distinct_level5wherestatusisdisabled_94', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_098",
            no_task="get_child_hierarchy_databasedon_level1uri_74",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_098 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_098',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level5wherestatusisdisabled_94') }}",
            trigger_dag_id=f'pwcglobal_ord_department_hierarchy_sync_new_pwc_ord_department_group_hierarchy_sync_enable_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "uri": "{{ item.uri }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_098 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_098',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_098") }}'
        )

        get_child_hierarchy_databasedon_level1uri_74 = rail.RepliconServiceOperator(
            task_id='get_child_hierarchy_databasedon_level1uri_74',
            endpoint="/services/DepartmentGroupListService1.svc/GetHierarchyData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:effectively-enabled",
                    "urn:replicon:department-group-list-column:code"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
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
                            "text": "{{ dag_run.conf.ordlevel1 }}",
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
                "hierarchyListDataOptionUris": [
                    "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
                ]
            },
            response_filter=response_filter.get_department_list_details_with_code
        )

        create_csv_lines_level6_data_78 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_level6_data_78',
            source="{{ result('parse_json') | to_json }}",
            header=['fullpath',
                    'parentname',
                    'parentfullpath',
                    'name',
                    'levelfullpath',
                    'uri',
                    'code',
                    'codeinreplicon',
                    'status'],
            row=custom_methods.get_csv_rows_78
        )

        load_csv_create_list_from_csv_79 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_79",
            document="{{ result('create_csv_lines_level6_data_78') }}",
        )

        create_collection_create_list_from_csv_79 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_79',
            source="{{ result('load_csv_create_list_from_csv_79') }}",
            name="level6data",
            columns={
                'fullpath': 'fullpath',
                'parentname': 'parent',
                'parentfullpath': 'parentfullpath',
                'name': 'name',
                'levelfullpath': 'levelfullpath',
                'uri': 'uri',
                'code': 'code',
                'codefromreplicon': 'codefromreplicon',
                'status': 'status'
            }
        )

        query_list_get_all_distinct_level6_uri_not_present_80 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level6_uri_not_present_80',
            # pylint: disable=line-too-long
            query="""SELECT DISTINCT parent, parentfullpath, name, levelfullpath, uri, code, status FROM level6data WHERE NULLIF(uri,'') IS NULL""",
        )

        if_query_list_get_all_distinct_level6_80_rows_greater_than_0_81 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level6_80_rows_greater_than_0_81',
            test='''{{ result('query_list_get_all_distinct_level6_uri_not_present_80', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_086",
            no_task="query_list_get_all_distinct_level6_uri_present_80",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_086 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_086',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level6_uri_not_present_80') }}",
            trigger_dag_id=f'pwc_ord_department_group_hierarchy_sync_create_v10_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "name": item['name'],
                "level": "6",
                "fullpath": item['levelfullpath'],
                "parenturi": custom_methods.get_parenturi(rail.result('get_child_hierarchy_databasedon_level1uri_74'),
                                                          item['parent'], item['parentfullpath']),
                "code": item['code'],
                "existing_dep_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_child_hierarchy_databasedon_level1uri_74'), 'code',
                                                                         item['code'], 'uri')
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_086 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_086',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_086") }}'
        )

        log_department_group_created_level6_success = rail.WriteLogOperator(
            task_id='log_department_group_created_level6_success',
            log="{{ result('create_ord_department_sync_logs') }}",
            items="{{ result('query_list_get_all_distinct_level6_uri_not_present_80') }}",
            message='Created Successfully',
            properties={
                "name": "{{ item.name }}",
                "level": "6",
                "fullpath": "{{ item.levelfullpath }}",
                "status": "Success",
                "details": "Created Successfully",
            }
        )

        gather_ord_department_logs_from_child_v1_086 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_ord_department_logs_from_child_v1_086',
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_086") }}',
            dagrun_task_id='create_ord_department_sync_child_logs',
            flatten=True
        )

        query_list_get_all_distinct_level6_uri_present_80 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level6_uri_present_80',
            # pylint: disable=line-too-long
            query="""SELECT DISTINCT parent, parentfullpath, name, levelfullpath, uri, code, status FROM level6data WHERE NULLIF(uri,'') IS NOT NULL AND status='False'""",
        )

        if_query_list_get_all_distinct_level6_uri_present_greater_than_0_81 = rail.IfOperator(
            task_id='if_query_list_get_all_distinct_level6_uri_present_greater_than_0_81',
            test='''{{ result('query_list_get_all_distinct_level6_uri_present_80', 'length') > 0 }}''',
            yes_task="trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_0117",
            no_task="query_list_get_all_distinct_level6_92",
        )

        trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_0117 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_0117',
            retries=0,
            items="{{ result('query_list_get_all_distinct_level6_uri_present_80') }}",
            trigger_dag_id=f'pwcglobal_ord_department_hierarchy_sync_new_pwc_ord_department_group_hierarchy_sync_enable_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "uri": item['uri'],
                "code": item['code'],
                "name": item['name'],
                "level": "6",
                "existing_dep_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_child_hierarchy_databasedon_level1uri_74'), 'code',
                                                                         item['code'], 'uri')
            }
        )

        wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_0117 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_0117',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_0117") }}'
        )

        log_department_group_enabled_level6_success = rail.WriteLogOperator(
            task_id='log_department_group_enabled_level6_success',
            log="{{ result('create_ord_department_sync_logs') }}",
            items="{{ result('query_list_get_all_distinct_level6_uri_present_80') }}",
            message='Enabled Successfully',
            properties={
                "name": "{{ item.name }}",
                "level": "6",
                "fullpath": "{{ item.levelfullpath }}",
                "status": "Success",
                "details": "Enabled Successfully",
            }
        )

        query_list_get_all_distinct_level6_92 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_distinct_level6_92',
            # pylint: disable=line-too-long
            query="""SELECT DISTINCT parent, parentfullpath, name, levelfullpath, uri, code FROM level6data WHERE  NULLIF(uri,'') IS NOT NULL AND NULLIF(codefromreplicon,'') IS NULL""",
        )

        update_departmentgroup_code_level1uri_95 = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_departmentgroup_code_level1uri_95',
            items="{{ result('query_list_get_all_distinct_level6_92') }}",
            endpoint="/services/DepartmentGroupService1.svc/UpdateCode",
            data=lambda item: {
                "departmentGroupUri": item['uri'] if item['uri'] else null,
                "code": item['code']
            }
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_method.do_format_logs
        )

        if_first_name_present_98 = rail.IfOperator(
            task_id='if_first_name_present_98',
            test='{{ result("format_logs") | length > 0 }}',
            yes_task="create_csv_lines_final_logfile_100",
            no_task="send_mail_send_no_records_email_106",
        )

        create_csv_lines_final_logfile_100 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_final_logfile_100',
            source='{{ result("format_logs") | to_json }}',
            header=['Territory',
                    'Name',
                    'Level',
                    'Full Path',
                    'Status',
                    'Details',
                    'JobID'],
            row=['{{ dag_run.conf.ordlevel1 }}',
                 '{{ item.name }}',
                 '{{ item.level }}',
                 '{{ item.fullpath }}',
                 '{{ item.status }}',
                 '{{ item.details }}',
                 '{{ item.jobid }}']
        )

        def get_log_file_name(dag_run):
            datetime_obj = datetime.strptime(
                dag_run.conf['jobcreatedtime'], "%Y-%m-%dT%H:%M:%S.%f%z")
            formatted_timestamp = datetime_obj.strftime('%d%m%YT%H%M%S')
            return "ORD_Logs_" + formatted_timestamp + "_" + dag_run.conf["ordlevel1"] + ".csv"

        create_log_file_name = rail.PythonOperator(
            task_id='create_log_file_name',
            python_callable=get_log_file_name
        )

        upload_upload_logsto_m_f_t_102 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_logsto_m_f_t_102',
            content="{{ result('create_csv_lines_final_logfile_100') }}",
            remote_filepath=config.log_filepath +
            '/{{ result("create_log_file_name") }}'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_final_logfile_100') }}",
            output_file_name='{{ result("create_log_file_name") }}',
            expires_in_seconds=7*24*60*60,
        )

        send_mail_with_cshare_send_completion_email_104 = rail.EmailOperator(
            task_id='send_mail_with_cshare_send_completion_email_104',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            # pylint: disable=line-too-long
            subject='''{{ get_company_key() }}| Replicon Department group sync for ORD Automation - {{ dag_run.conf.ordlevel1 }} - Completed Successfully - {{ dag_run.conf.jobcreatedtime }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /><br />Hello, <br /><br />The Replicon department group sync for ORD automation is completed successfully on {{ dag_run.conf.jobcreatedtime }}.<br /><br />Below is the summary for the execution.</p>Please find the below link to download the user import logs for reference. <br /> <br /><a href="{{ result('generate_download_link') }}">Download log file</a></p>
            <ul>
            <li>Log file path: {{params.log_file_path}} </li>
            <li>Log file: {{ result("create_log_file_name") }}</li>
            <li>Date Considered: {{ dag_run.conf.datemodified }} </li>
            <li>Territory: {{ dag_run.conf.ordlevel1 }} </li>
            <li>Total ORD Hirearchy available: {{ result('parse_json')| length }} </li>
            <li>Level 2 created: {{ result('query_list_get_all_distinct_level2_24', 'length') }} </li>
            <li>Level 3 created: {{ result('query_list_get_all_distinct_level3_38', 'length') }} </li>
            <li>Level 4 created: {{ result('query_list_get_all_distinct_level4_52', 'length') }} </li>
            <li>Level 5 created: {{ result('query_list_get_all_distinct_level5_66', 'length') }} </li>
            <li>Level 6 created: {{ result('query_list_get_all_distinct_level6_uri_not_present_80', 'length') }} </li>
            </ul>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={'log_file_path': config.log_filepath},
        )

        send_mail_send_no_records_email_106 = rail.EmailOperator(
            task_id='send_mail_send_no_records_email_106',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            # pylint: disable=line-too-long
            subject='''{{ get_company_key() }}| Replicon Department group sync for ORD Automation - {{ dag_run.conf.ordlevel1 }} - No Records to Process - {{ dag_run.conf.jobcreatedtime }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /><br />Hello, <br /><br />The Replicon department group sync for ORD automation is completed successfully on {{ dag_run.conf.jobcreatedtime }}.<br /><br />There are no records to process from date "{{ dag_run.conf.datemodified }}" for territory "{{ dag_run.conf.ordlevel1 }}".</p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        is_get_all_ord_structure_from_mft_3_task_failed = rail.IfOperator(
            task_id="is_get_all_ord_structure_from_mft_3_task_failed",
            trigger_rule = "one_failed",
            test= lambda : rail.result('get_all_ord_structure_from_mft_3',"error") == "AirflowException('404:')",
            yes_task="send_404_failure_mail",
            no_task="is_get_all_ord_structure_from_mft_3_task_failed_with_504"
        )

        send_404_failure_mail = rail.EmailOperator(
            task_id='send_404_failure_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            # pylint: disable=line-too-long
            subject='''{{ get_company_key() }} | Replicon Department group sync for ORD Automation - {{ dag_run.conf.ordlevel1 }} - Failed to Get ORD Data - {{ dag_run.conf.jobcreatedtime }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /><br />Hello, <br /><br />The Replicon department group sync for ORD automation is failed on {{ dag_run.conf.jobcreatedtime }}.<br /><br />The Integration failed to get ORD Data for territory {{ dag_run.conf.ordlevel1 }}.</p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        is_get_all_ord_structure_from_mft_3_task_failed_with_504 = rail.IfOperator(
            task_id="is_get_all_ord_structure_from_mft_3_task_failed_with_504",
            trigger_rule = "one_failed",
            test= lambda : rail.result('get_all_ord_structure_from_mft_3',"error") == "AirflowException('504:')" or rail.result('get_all_ord_structure_from_mft_3',"error") == "AirflowException('500:')",
            yes_task="send_504_failure_mail",
            no_task="fail_dag"
        )

        send_504_failure_mail = rail.EmailOperator(
            task_id='send_504_failure_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            # pylint: disable=line-too-long
            subject='''{{ get_company_key() }} | Replicon Department group sync for ORD Automation - {{ dag_run.conf.ordlevel1 }} - Failed due to {{ "504 Time-Out" if result("get_all_ord_structure_from_mft_3","error")== "AirflowException('504:')" else "500 Server" }} Error ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /><br />Hello, <br /><br />The Replicon department group sync for ORD automation is failed on {{ dag_run.conf.jobcreatedtime }}.<br /><br />The Integration failed after 3 retry with {{ "504 Time-Out" if result("get_all_ord_structure_from_mft_3","error")== "AirflowException('504:')" else "500 Server" }} Error.</p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message="{{ get_error_message() }}"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> pwc_ord_structure_from_variable >> is_ord_structure_value_present_in_variable

        is_ord_structure_value_present_in_variable >> rail.Label(
            'Yes') >> parse_json
        is_ord_structure_value_present_in_variable >> rail.Label(
            'No') >> get_all_ord_structure_from_mft_3 >> parse_json
        parse_json >> if_first_naturalkeyname_blank_6
        if_first_naturalkeyname_blank_6 >> rail.Label(
            'Yes') >> send_mail_send_no_records_email_7 >> finish
        if_first_naturalkeyname_blank_6 >> rail.Label(
            'No') >> create_ord_department_sync_logs >> ordlevlel1_uri_path >> if_request_ordlevel1uri_blank_11
        if_request_ordlevel1uri_blank_11 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_012 \
            >> gather_departmentgroupuri_from_create >> log_department_group_created_success \
            >> gather_ord_department_logs_from_child_v1_012 >> log_level1_uri_15
        if_request_ordlevel1uri_blank_11 >> rail.Label('No') >> log_level1_uri_15 >> get_child_hierarchy_databasedon_level1name_16 \
            >> create_csv_lines_input_data_20 >> load_csv_create_list_from_csv_21 \
            >> create_collection_create_list_from_csv_21 >> create_csv_lines_level2_data_22 >> load_csv_create_list_from_csv_23 \
            >> create_collection_create_list_from_csv_23 >> query_list_get_all_distinct_level2_24 \
            >> if_query_list_get_all_distinct_level2_24_rows_greater_than_0_25
        if_query_list_get_all_distinct_level2_24_rows_greater_than_0_25 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_028 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_028 \
            >> log_department_group_created_level2_success >> gather_ord_department_logs_from_child_v1_028 \
            >> query_list_get_all_distinct_level2wherestatusisdisabled_31
        if_query_list_get_all_distinct_level2_24_rows_greater_than_0_25 >> rail.Label(
            'No') >> query_list_get_all_distinct_level2wherestatusisdisabled_31 \
            >> if_query_list_get_all_distinct_level2wherestatusisdisabled_31_rows_greater_than_0_32
        if_query_list_get_all_distinct_level2wherestatusisdisabled_31_rows_greater_than_0_32 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_035 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_035 \
            >> get_child_hierarchy_databasedon_level1uri_32
        if_query_list_get_all_distinct_level2wherestatusisdisabled_31_rows_greater_than_0_32 >> rail.Label(
            'No') >> get_child_hierarchy_databasedon_level1uri_32 >> create_csv_lines_level3_data_36 \
            >> load_csv_create_list_from_csv_37 >> create_collection_create_list_from_csv_37 \
            >> query_list_get_all_distinct_level3_38 >> if_query_list_get_all_distinct_level3_38_rows_greater_than_0_39
        if_query_list_get_all_distinct_level3_38_rows_greater_than_0_39 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_042 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_042 \
            >> log_department_group_created_level3_success >> gather_ord_department_logs_from_child_v1_042 \
            >> query_list_get_all_distinct_level3wherestatusisdisabled_52
        if_query_list_get_all_distinct_level3_38_rows_greater_than_0_39 >> rail.Label(
            'No') >> query_list_get_all_distinct_level3wherestatusisdisabled_52 \
            >> if_query_list_get_all_distinct_level3wherestatusisdisabled_52_rows_greater_than_0_53
        if_query_list_get_all_distinct_level3wherestatusisdisabled_52_rows_greater_than_0_53 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_056 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_056 \
            >> get_child_hierarchy_databasedon_level1uri_46
        if_query_list_get_all_distinct_level3wherestatusisdisabled_52_rows_greater_than_0_53 >> rail.Label(
            'No') >> get_child_hierarchy_databasedon_level1uri_46 >> create_csv_lines_level4_data_50 \
            >> load_csv_create_list_from_csv_51 >> create_collection_create_list_from_csv_51 \
            >> query_list_get_all_distinct_level4_52 >> if_query_list_get_all_distinct_level4_52_rows_greater_than_0_53
        if_query_list_get_all_distinct_level4_52_rows_greater_than_0_53 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_056 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_056 \
            >> log_department_group_created_level4_success >> gather_ord_department_logs_from_child_v1_056 \
            >> query_list_get_all_distinct_level4wherestatusisdisabled_73
        if_query_list_get_all_distinct_level4_52_rows_greater_than_0_53 >> rail.Label(
            'No') >> query_list_get_all_distinct_level4wherestatusisdisabled_73 \
            >> if_query_list_get_all_distinct_level4wherestatusisdisabled_73_rows_greater_than_0_74
        if_query_list_get_all_distinct_level4wherestatusisdisabled_73_rows_greater_than_0_74 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_077 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_077 \
            >> get_child_hierarchy_databasedon_level1uri_60
        if_query_list_get_all_distinct_level4wherestatusisdisabled_73_rows_greater_than_0_74 >> rail.Label(
            'No') >> get_child_hierarchy_databasedon_level1uri_60 >> create_csv_lines_level5_data_64 \
            >> load_csv_create_list_from_csv_65 >> create_collection_create_list_from_csv_65 \
            >> query_list_get_all_distinct_level5_66 >> if_query_list_get_all_distinct_level5_66_rows_greater_than_0_67
        if_query_list_get_all_distinct_level5_66_rows_greater_than_0_67 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_070 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_070 \
            >> log_department_group_created_level5_success >> gather_ord_department_logs_from_child_v1_070 \
            >> query_list_get_all_distinct_level5wherestatusisdisabled_94
        if_query_list_get_all_distinct_level5_66_rows_greater_than_0_67 >> rail.Label(
            'No') >> query_list_get_all_distinct_level5wherestatusisdisabled_94 \
            >> if_query_list_get_all_distinct_level5wherestatusisdisabled_94_rows_greater_than_0_95
        if_query_list_get_all_distinct_level5wherestatusisdisabled_94_rows_greater_than_0_95 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_098 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_098 \
            >> get_child_hierarchy_databasedon_level1uri_74
        if_query_list_get_all_distinct_level5wherestatusisdisabled_94_rows_greater_than_0_95 >> rail.Label(
            'No') >> get_child_hierarchy_databasedon_level1uri_74 >> create_csv_lines_level6_data_78 \
            >> load_csv_create_list_from_csv_79 >> create_collection_create_list_from_csv_79 \
            >> query_list_get_all_distinct_level6_uri_not_present_80 >> if_query_list_get_all_distinct_level6_80_rows_greater_than_0_81
        if_query_list_get_all_distinct_level6_80_rows_greater_than_0_81 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_086 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_create_v1_086 \
            >> log_department_group_created_level6_success >> gather_ord_department_logs_from_child_v1_086 \
            >> query_list_get_all_distinct_level6_uri_present_80 >> if_query_list_get_all_distinct_level6_uri_present_greater_than_0_81
        if_query_list_get_all_distinct_level6_80_rows_greater_than_0_81 >> rail.Label(
            'No') >> query_list_get_all_distinct_level6_uri_present_80
        if_query_list_get_all_distinct_level6_uri_present_greater_than_0_81 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_0117 \
            >> wait_for_completion_trigger_dag_run_live_pwc_ord_department_group_hierarchy_sync_enable_v1_0117 \
            >> log_department_group_enabled_level6_success >> query_list_get_all_distinct_level6_92
        if_query_list_get_all_distinct_level6_uri_present_greater_than_0_81 >> rail.Label(
            'No') >> query_list_get_all_distinct_level6_92 >> update_departmentgroup_code_level1uri_95 \
            >> format_logs >> if_first_name_present_98
        if_first_name_present_98 >> rail.Label(
            'Yes') >> create_csv_lines_final_logfile_100 >> create_log_file_name \
            >> upload_upload_logsto_m_f_t_102 >> generate_download_link >> send_mail_with_cshare_send_completion_email_104 >> finish
        if_first_name_present_98 >> rail.Label(
            'No') >> send_mail_send_no_records_email_106 >> finish

        finish >> log_to_sumo

        log_to_sumo >> is_get_all_ord_structure_from_mft_3_task_failed >> rail.Label('Yes') >> send_404_failure_mail
        is_get_all_ord_structure_from_mft_3_task_failed >> rail.Label('No') >> \
        is_get_all_ord_structure_from_mft_3_task_failed_with_504 >> rail.Label('Yes') >> send_504_failure_mail
        is_get_all_ord_structure_from_mft_3_task_failed_with_504 >> rail.Label('No') >> fail_dag

    return dag


rail.for_each_instance(create_dag)
