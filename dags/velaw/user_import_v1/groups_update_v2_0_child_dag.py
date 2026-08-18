
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.groups_update_child_dag_id,
        description=f'VelawG3 Child_groups update V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='velawg3_groups_table_truncate_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='velawg3_groups_table_truncate_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        velawg3_groups_table_truncate_3 = rail.CreateLogOperator(
            task_id='velawg3_groups_table_truncate_3',
        )

        parse_csv_5 = rail.LoadCSVFileOperator(
            task_id='parse_csv_5',
            document="{{ dag_run.conf.filepath }}",
            encoding="ISO-8859-1"
        )

        def get_csv_rows(item):
            row_data = [
                item['FIRST_NAME'],
                item['LAST_NAME'],
                item['EMAIL'],
                item['EMPLOYEE_ID'],
                item['START_DATE'],
                item['END_DATE'],
                item['JOB_CODE'],
                item['JOB_TITLE'],
                item['FLSA_STATUS'],
                item['ASSIGNMENT_CATEGORY'],
                item['COUNTRY_ISO_CODE'],
                item['PERSON_TYPE'],
                item['LEGAL_EMPLOYER'],
                item['LOGIN_NAME'],
                item['SUPERVISOR_LOGIN_NAME'],
                item['IS_LOGIN_ENABLED'],
                item['DEPARTMENT_NAME'].strip(
                ) if item['DEPARTMENT_NAME'] else null,
                item['DEPARTMENT_CODE'].strip(
                ) if item['DEPARTMENT_CODE'] else null,
                item['EMPLOYEE_TYPE'].strip() if item['EMPLOYEE_TYPE'] else null,
                item['LOCATION'].strip() if item['LOCATION'] else null,
                item['JOB_FAMILIES'].strip() if item['JOB_FAMILIES'] else null,
                item['PAY_TYPE'].strip() if item['PAY_TYPE'] else null,
                item['PAY_RATES_AMOUNT'],
                item['PAY_RATES_CURRENCY'],
                item['DEFAULT_BILLING_RATE_AMOUNT'],
                item['DEFAULT_BILLING_RATE_CURRENCY'],
                item['HOURLY_COST_AMOUNT'],
                item['HOURLY_COST_CURRENCY'],
                ("Vinson & Elkins | " + item['LEGAL_EMPLOYER'] +
                 " | " + item['DEPARTMENT_NAME']).split(" | ")
            ]
            return row_data

        create_csv_lines_6 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_6',
            source="{{ result('parse_csv_5') }}",
            header=['firstname',
                    'lastname',
                    'email',
                    'employeeid',
                    'startdate',
                    'enddate',
                    'jobcode',
                    'jobtitle',
                    'flsastatus',
                    'assignmentcategory',
                    'countryisocode',
                    'persontype',
                    'legalemployer',
                    'loginname',
                    'supervisorloginname',
                    'isloginenabled',
                    'departmentname',
                    'departmentcode',
                    'employeetype',
                    'location',
                    'jobfamilies',
                    'paytype',
                    'payratesamount',
                    'payratescurrency',
                    'defaultbillingrateamount',
                    'defaultbillingratecurrency',
                    'hourlycostamount',
                    'hourlycostcurrency',
                    'department'],
            row=get_csv_rows
        )

        load_csv_create_list_from_csv_7 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_7",
            document="{{ result('create_csv_lines_6') }}",
        )

        create_collection_create_list_from_csv_7 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_7',
            source="{{ result('load_csv_create_list_from_csv_7') }}",
            name="rawcollectiondata"
        )

        get_cost_center_details_11 = rail.RepliconServiceOperator(
            task_id='get_cost_center_details_11',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:full-path",
                    "urn:replicon:cost-center-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        invoke_custom_ruby_code_14 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_14',
            python_callable=lambda: list(map(lambda x: {
                "name": x['cells'][0]['textValue'],
                "uri": x['cells'][0]['uri'],
                "fullpath": "/".join(list(filter(lambda x: x, map(lambda y: y['textValue'], x['cells'][1]['cellCollection'])))),
                "length": len(x['cells'][1]['cellCollection']),
                "status": x['cells'][2]['textValue']
            }, rail.result('get_cost_center_details_11')['rows']))
        )

        create_list_15 = rail.CreateCollectionOperator(
            task_id='create_list_15',
            source="{{ result('invoke_custom_ruby_code_14') | to_json }}",
            name="costcenterdata",
        )

        query_list_get_distinct_cost_center_16 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_cost_center_16',
            query="""SELECT DISTINCT jobfamilies FROM rawcollectiondata WHERE NULLIF(jobfamilies,'') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')"""
        )

        create_costcenterlist_17 = rail.CreateCollectionOperator(
            task_id='create_costcenterlist_17',
            source="{{ result('query_list_get_distinct_cost_center_16') }}",
            name="costcenterlist",
            columns={
                'jobfamilies': 'costcenter'
            }
        )

        query_list_getallcostcentersnotin_replicon_18 = rail.QueryCollectionOperator(
            task_id='query_list_getallcostcentersnotin_replicon_18',
            query="""SELECT * FROM costcenterlist WHERE LOWER(costcenter) NOT IN (SELECT DISTINCT LOWER(name) FROM costcenterdata)""",
        )

        query_list_get_allcostcenterspresentinfeedfileandisdisabledin_replicon_19 = rail.QueryCollectionOperator(
            task_id='query_list_get_allcostcenterspresentinfeedfileandisdisabledin_replicon_19',
            query="""SELECT * FROM costcenterdata WHERE (LOWER(name) IN (SELECT DISTINCT LOWER(costcenter) FROM costcenterlist) AND status='False')""",
        )

        trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_022 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_022',
            retries=0,
            items="{{ result('query_list_get_allcostcenterspresentinfeedfileandisdisabledin_replicon_19') }}",
            trigger_dag_id=config.cost_center_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "costcenter": "{{ item.costcenter }}",
                "type": "costCenter",
                "uri": "{{ item.uri }}"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_022 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_022',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_022") }}'
        )

        if_query_list_getallcostcentersnotin_replicon_18_rows_greater_than_0_25 = rail.IfOperator(
            task_id='if_query_list_getallcostcentersnotin_replicon_18_rows_greater_than_0_25',
            test='''{{ result('query_list_getallcostcentersnotin_replicon_18', 'length') > 0 }}''',
            yes_task="trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_029",
            no_task="get_department_group_details_36",
        )

        trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_029 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_029',
            retries=0,
            items="{{ result('query_list_getallcostcentersnotin_replicon_18') }}",
            trigger_dag_id=config.cost_center_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "costcenter": "{{ item.costcenter }}"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_029 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_029',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_029") }}'
        )

        accumulate_list_items_31 = rail.GatherResultsFromDagRunsOperator(
            task_id='accumulate_list_items_31',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_029') }}",
            dagrun_task_id='catch_group_error',
            flatten=True
        )

        get_department_group_details_36 = rail.RepliconServiceOperator(
            task_id='get_department_group_details_36',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        invoke_custom_ruby_code_39 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_39',
            python_callable=lambda: list(map(lambda row: {
                "name": row['cells'][0]['textValue'],
                "uri": row['cells'][0]['uri'],
                "fullpath": "|".join(list(filter(lambda row: row, map(lambda y: y['textValue'], row['cells'][1]['cellCollection'])))),
                "length": len(row['cells'][1]['cellCollection']),
                "status": row['cells'][2]['textValue']
            }, rail.result('get_department_group_details_36')['rows']))
        )

        create_list_40 = rail.CreateCollectionOperator(
            task_id='create_list_40',
            source="{{ result('invoke_custom_ruby_code_39') | to_json }}",
            name="departmentgroupdata",
        )

        query_list_get_distinct_department_groupfrom_input_41 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_department_groupfrom_input_41',
            query="""SELECT DISTINCT legalemployer, departmentname, department, departmentcode FROM rawcollectiondata WHERE (NULLIF(departmentname,'') IS NOT NULL OR  NULLIF(legalemployer,'') IS NOT NULL)  AND (countryisocode='US' OR countryisocode='GB')"""
        )

        create_list_42 = rail.CreateCollectionOperator(
            task_id='create_list_42',
            source="{{ result('query_list_get_distinct_department_groupfrom_input_41') }}",
            name="departmentlist",
            columns={
                'legalemployer': 'level2',
                'departmentname': 'level3',
                'department': 'fullpath',
                'departmentcode': 'code'
            }
        )

        query_list_getall_departmentsnotin_replicon_43 = rail.QueryCollectionOperator(
            task_id='query_list_getall_departmentsnotin_replicon_43',
            query="""SELECT * FROM departmentlist WHERE LOWER(fullpath) NOT IN (SELECT DISTINCT LOWER(fullpath) FROM departmentgroupdata)""",
        )

        query_list_getall_departmentspresentinfeedfileanddisabledin_replicon_44 = rail.QueryCollectionOperator(
            task_id='query_list_getall_departmentspresentinfeedfileanddisabledin_replicon_44',
            query="""SELECT * FROM departmentgroupdata WHERE (LOWER(fullpath) IN (SELECT DISTINCT LOWER(fullpath) FROM departmentlist) AND status='False')""",
        )

        trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_department_47 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_department_47',
            retries=0,
            items="{{ result('query_list_getall_departmentspresentinfeedfileanddisabledin_replicon_44') }}",
            trigger_dag_id=config.cost_center_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "costcenter": "{{ item.department }}",
                "type": "departmentGroup",
                "uri": "{{ item.uri }}"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_department_47 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_department_47',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_department_47") }}'
        )

        if_query_list_getall_departmentsnotin_replicon_43_rows_greater_than_0_50 = rail.IfOperator(
            task_id='if_query_list_getall_departmentsnotin_replicon_43_rows_greater_than_0_50',
            test='''{{ result('query_list_getall_departmentsnotin_replicon_43', 'length') > 0 }}''',
            yes_task="velawg3_groups_table_add_entry_52",
            no_task="get_location_details_66",
        )

        velawg3_groups_table_add_entry_52 = rail.WriteLogOperator(
            task_id='velawg3_groups_table_add_entry_52',
            log="{{ result('velawg3_groups_table_truncate_3') }}",
            items="{{ result('invoke_custom_ruby_code_39') | to_json }}",
            message="na",
            severity="fixme",
            properties={
                "name": "{{ item.name }}",
                "uri": "{{ item.uri }}",
                "fullpath": "{{ item.fullpath }}",
                "type": "department"
            }
        )

        log_parent_group_uri_53 = rail.PythonOperator(
            task_id='log_parent_group_uri_53',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'invoke_custom_ruby_code_39'), 'fullpath', 'Vinson & Elkins', 'uri') if rail.result('invoke_custom_ruby_code_39') else null
        )

        if_log_parent_group_uri_53_blank_54 = rail.IfOperator(
            task_id='if_log_parent_group_uri_53_blank_54',
            test='''{{ result('log_parent_group_uri_53') | is_falsy }}''',
            yes_task="log_to_sumo",
            no_task="if_foreach_query_list_getall_departmentsnotin_replicon_43_56_fullpath_present_57",
        )

        if_foreach_query_list_getall_departmentsnotin_replicon_43_56_fullpath_present_57 = rail.IfOperator(
            task_id='if_foreach_query_list_getall_departmentsnotin_replicon_43_56_fullpath_present_57',
            test='''{{ result('query_list_getall_departmentsnotin_replicon_43', 'length') > 0 }}''',
            yes_task="trigger_dag_run_velaw_user_import_velawg3_child_department_add_v2_059",
            no_task="get_location_details_66",
        )

        trigger_dag_run_velaw_user_import_velawg3_child_department_add_v2_059 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_department_add_v2_059',
            retries=0,
            items="{{ result('query_list_getall_departmentsnotin_replicon_43') }}",
            trigger_dag_id=config.department_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "department": "{{ item.level3 }}",
                "compaydepturi": "{{ result('log_parent_group_uri_53') }}",
                "departmentfullpath": "{{ item.fullpath }}",
                "parent": "Vinson & Elkins|" + "{{ item.level2 }}",
                "code": "{{ item.code }}",
                "groups_table": "{{ result('velawg3_groups_table_truncate_3') }}"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_department_add_v2_059 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_department_add_v2_059',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_department_add_v2_059") }}'
        )

        accumulate_list_items_61 = rail.GatherResultsFromDagRunsOperator(
            task_id='accumulate_list_items_61',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_department_add_v2_059') }}",
            dagrun_task_id='catch_group_error',
            flatten=True
        )

        get_location_details_66 = rail.RepliconServiceOperator(
            task_id='get_location_details_66',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path",
                    "urn:replicon:location-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        invoke_custom_ruby_code_69 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_69',
            python_callable=lambda: list(map(lambda row: {
                    "name": row['cells'][0]['textValue'],
                    "uri": row['cells'][0]['uri'],
                    "fullpath": "/".join(list(filter(lambda row: row, map(lambda y: y['textValue'], row['cells'][1]['cellCollection'])))),
                    "length": len(row['cells'][1]['cellCollection']),
                    "status": row['cells'][2]['textValue']
            }, rail.result('get_location_details_66')['rows']))
        )

        create_list_70 = rail.CreateCollectionOperator(
            task_id='create_list_70',
            source="{{ result('invoke_custom_ruby_code_69')| to_json }}",
            name="locationdata"
        )

        query_list_unique_locations_71 = rail.QueryCollectionOperator(
            task_id='query_list_unique_locations_71',
            query="""SELECT DISTINCT location FROM rawcollectiondata WHERE NULLIF(location,'') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')""",
            name="locationrawdata"
        )

        query_list_getalllocationsnotin_replicon_73 = rail.QueryCollectionOperator(
            task_id='query_list_getalllocationsnotin_replicon_73',
            query="""SELECT DISTINCT location FROM locationrawdata WHERE LOWER(location) NOT IN (SELECT DISTINCT LOWER(name) FROM locationdata)""",
        )

        query_list_getalllocationsinfeedfileanddisabledin_replicon_74 = rail.QueryCollectionOperator(
            task_id='query_list_getalllocationsinfeedfileanddisabledin_replicon_74',
            query="""SELECT * FROM locationdata WHERE (LOWER(name) IN (SELECT DISTINCT LOWER(location) FROM locationrawdata) AND status='False')""",
        )

        trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_location_77 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_location_77',
            retries=0,
            items="{{ result('query_list_getalllocationsinfeedfileanddisabledin_replicon_74') }}",
            trigger_dag_id=config.cost_center_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "costcenter": "{{ item.location }}",
                "type": "location",
                "uri": "{{ item.uri }}"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_location_77 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_location_77',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_location_77") }}'
        )

        if_query_list_getalllocationsnotin_replicon_73_rows_greater_than_0_80 = rail.IfOperator(
            task_id='if_query_list_getalllocationsnotin_replicon_73_rows_greater_than_0_80',
            test='''{{ result('query_list_getalllocationsnotin_replicon_73', 'length') > 0 }}''',
            yes_task="trigger_dag_run_velawg3_child_location_add_v2_084",
            no_task="get_employee_type_group_details_91"
        )

        trigger_dag_run_velawg3_child_location_add_v2_084 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velawg3_child_location_add_v2_084',
            retries=0,
            items="{{ result('query_list_getalllocationsnotin_replicon_73') }}",
            trigger_dag_id=config.location_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "location": "{{ item.location }}"
            }
        )

        wait_for_completion_trigger_dag_run_velawg3_child_location_add_v2_084 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velawg3_child_location_add_v2_084',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velawg3_child_location_add_v2_084") }}'
        )

        accumulate_list_items_86 = rail.GatherResultsFromDagRunsOperator(
            task_id='accumulate_list_items_86',
            dag_runs="{{ result('trigger_dag_run_velawg3_child_location_add_v2_084') }}",
            dagrun_task_id='catch_group_error',
            flatten=True
        )

        get_employee_type_group_details_91 = rail.RepliconServiceOperator(
            task_id='get_employee_type_group_details_91',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:employee-type-group",
                    "urn:replicon:employee-type-group-list-column:full-path",
                    "urn:replicon:employee-type-group-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        invoke_custom_ruby_code_94 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_94',
            python_callable=lambda: list(map(lambda row: {
                "name": row['cells'][0]['textValue'],
                "uri": row['cells'][0]['uri'],
                "fullpath": "/".join(list(filter(lambda row: row, map(lambda y: y['textValue'], row['cells'][1]['cellCollection'])))),
                "length": len(row['cells'][1]['cellCollection']),
                "status": row['cells'][2]['textValue']
            }, rail.result('get_employee_type_group_details_91')['rows']))
        )

        create_list_95 = rail.CreateCollectionOperator(
            task_id='create_list_95',
            source="{{ result('invoke_custom_ruby_code_94') | to_json }}",
            name="employeetypegroupdata",
        )

        query_list_get_distinctemployeegroup_groupfrom_input_96 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinctemployeegroup_groupfrom_input_96',
            query="""SELECT DISTINCT employeetype FROM rawcollectiondata WHERE NULLIF(employeetype,'') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')""",
            name="empoyeetypeinputdata"
        )

        query_list_getall_employee_typesnotin_replicon_98 = rail.QueryCollectionOperator(
            task_id='query_list_getall_employee_typesnotin_replicon_98',
            query="""SELECT DISTINCT employeetype FROM empoyeetypeinputdata WHERE LOWER(employeetype) NOT IN (SELECT DISTINCT LOWER(name) FROM employeetypegroupdata)""",
        )

        query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_99 = rail.QueryCollectionOperator(
            task_id='query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_99',
            query="""SELECT * FROM employeetypegroupdata WHERE (LOWER(name) IN (SELECT DISTINCT LOWER(employeetype) FROM empoyeetypeinputdata) AND status='False')""",
        )

        trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_102 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_102',
            retries=0,
            items="{{ result('query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_99') }}",
            trigger_dag_id=config.cost_center_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "costcenter": "{{ item.employeetype }}",
                "type": "employeeTypeGroup",
                "uri": "{{ item.uri }}"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_102 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_102',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_102") }}'
        )

        if_query_list_getall_employee_typesnotin_replicon_98_rows_greater_than_0_105 = rail.IfOperator(
            task_id='if_query_list_getall_employee_typesnotin_replicon_98_rows_greater_than_0_105',
            test='''{{ result('query_list_getall_employee_typesnotin_replicon_98', 'length') > 0 }}''',
            yes_task="trigger_dag_run_velaw_user_import_velawg3_child_employee_type_add_v2_0109",
            no_task="get_all_divisions_116"
        )

        trigger_dag_run_velaw_user_import_velawg3_child_employee_type_add_v2_0109 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_employee_type_add_v2_0109',
            retries=0,
            items="{{ result('query_list_getall_employee_typesnotin_replicon_98') }}",
            trigger_dag_id=config.employee_type_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "employeetype": "{{ item.employeetype }}"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_employee_type_add_v2_0109 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_employee_type_add_v2_0109',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_employee_type_add_v2_0109") }}'
        )

        accumulate_list_items_111 = rail.GatherResultsFromDagRunsOperator(
            task_id='accumulate_list_items_111',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_employee_type_add_v2_0109') }}",
            dagrun_task_id='catch_group_error',
            flatten=True
        )

        get_all_divisions_116 = rail.RepliconServiceOperator(
            task_id='get_all_divisions_116',
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:full-path",
                    "urn:replicon:division-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        invoke_custom_ruby_code_119 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_119',
            python_callable=lambda: list(map(lambda row: {
                    "name": row['cells'][0]['textValue'],
                    "uri": row['cells'][0]['uri'],
                    "fullpath": "/".join(list(filter(lambda row: row, map(lambda y: y['textValue'], row['cells'][1]['cellCollection'])))),
                    "length": len(row['cells'][1]['cellCollection']),
                    "status": row['cells'][2]['textValue']
            }, rail.result('get_all_divisions_116')['rows']))
        )

        create_list_120 = rail.CreateCollectionOperator(
            task_id='create_list_120',
            source="{{ result('invoke_custom_ruby_code_119') | to_json }}",
            name="divisiondata",
        )

        query_list_get_distinct_divsion_groupfrom_input_121 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_divsion_groupfrom_input_121',
            query="""SELECT DISTINCT paytype FROM rawcollectiondata WHERE NULLIF(paytype, '') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')"""
        )

        create_list_122 = rail.CreateCollectionOperator(
            task_id='create_list_122',
            source="{{ result('query_list_get_distinct_divsion_groupfrom_input_121') }}",
            name="divisondatainput",
            columns={
                'paytype': 'division'
            }
        )

        query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123',
            query="""SELECT DISTINCT division FROM divisondatainput WHERE LOWER(division) NOT IN (SELECT DISTINCT LOWER(name) FROM  divisiondata)""",
        )

        query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_124 = rail.QueryCollectionOperator(
            task_id='query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_124',
            query="""SELECT * FROM divisiondata WHERE (LOWER(name) IN (SELECT DISTINCT LOWER(division) FROM divisondatainput) AND status='False')""",
        )

        trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_127 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_127',
            retries=0,
            items="{{ result('query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_124') }}",
            trigger_dag_id=config.cost_center_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "costcenter": "{{ item.division }}",
                "type": "division",
                "uri": "{{ item.uri }}"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_127 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_127',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_127") }}'
        )

        accumulate_list_items_129 = rail.GatherResultsFromDagRunsOperator(
            task_id='accumulate_list_items_129',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_127') }}",
            dagrun_task_id='catch_group_error',
            flatten=True
        )

        if_query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123_rows_greater_than_0_130 = rail.IfOperator(
            task_id='if_query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123_rows_greater_than_0_130',
            test='''{{ result('query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123', 'length') > 0 }}''',
            yes_task="trigger_dag_run_velaw_user_import_velawg3_child_division_add_v2_0134",
            no_task="log_error_140",
        )

        trigger_dag_run_velaw_user_import_velawg3_child_division_add_v2_0134 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_division_add_v2_0134',
            retries=0,
            items="{{ result('query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123') }}",
            trigger_dag_id=config.division_add_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "division": "{{ item.division }}"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_division_add_v2_0134 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_division_add_v2_0134',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_division_add_v2_0134") }}'
        )

        accumulate_list_items_136 = rail.GatherResultsFromDagRunsOperator(
            task_id='accumulate_list_items_136',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_division_add_v2_0134') }}",
            dagrun_task_id='catch_group_error',
            flatten=True
        )

        def get_group_error_message():
            error_message = []
            if rail.result('accumulate_list_items_31'):
                error_message.append('Error creating cost center')
            if rail.result('accumulate_list_items_61'):
                error_message.append('Error creating department group')
            if rail.result('accumulate_list_items_86'):
                error_message.append('Error creating location')
            if rail.result('accumulate_list_items_111'):
                error_message.append('Error creating employee type group')
            if rail.result('accumulate_list_items_136'):
                error_message.append('Error creating Division group')
            if rail.result('accumulate_list_items_129'):
                error_message.append('Error enabling division')
            return ';'.join(error_message) if error_message else ''
        log_error_140 = rail.PythonOperator(
            task_id='log_error_140',
            python_callable=get_group_error_message
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> velawg3_groups_table_truncate_3
        velawg3_groups_table_truncate_3 >> parse_csv_5 >> create_csv_lines_6 >> load_csv_create_list_from_csv_7 \
            >> create_collection_create_list_from_csv_7 >> get_cost_center_details_11 \
            >> invoke_custom_ruby_code_14 >> create_list_15 >> query_list_get_distinct_cost_center_16 >> create_costcenterlist_17 >> query_list_getallcostcentersnotin_replicon_18 \
            >> query_list_get_allcostcenterspresentinfeedfileandisdisabledin_replicon_19 \
            >> trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_022 >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_022 \
            >> if_query_list_getallcostcentersnotin_replicon_18_rows_greater_than_0_25
        if_query_list_getallcostcentersnotin_replicon_18_rows_greater_than_0_25 >> rail.Label(
            'Yes') >> trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_029 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_029 \
            >> accumulate_list_items_31 >> get_department_group_details_36
        if_query_list_getallcostcentersnotin_replicon_18_rows_greater_than_0_25 >> rail.Label(
            'No') >> get_department_group_details_36 >> invoke_custom_ruby_code_39 \
            >> create_list_40 >> query_list_get_distinct_department_groupfrom_input_41 >> create_list_42 >> query_list_getall_departmentsnotin_replicon_43 \
            >> query_list_getall_departmentspresentinfeedfileanddisabledin_replicon_44 \
            >> trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_department_47 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_department_47 \
            >> if_query_list_getall_departmentsnotin_replicon_43_rows_greater_than_0_50
        if_query_list_getall_departmentsnotin_replicon_43_rows_greater_than_0_50 >> rail.Label(
            'Yes') >> velawg3_groups_table_add_entry_52 >> log_parent_group_uri_53 >> if_log_parent_group_uri_53_blank_54
        if_log_parent_group_uri_53_blank_54 >> rail.Label(
            'Yes') >> log_to_sumo
        if_log_parent_group_uri_53_blank_54 >> rail.Label(
            'No') >> if_foreach_query_list_getall_departmentsnotin_replicon_43_56_fullpath_present_57
        if_foreach_query_list_getall_departmentsnotin_replicon_43_56_fullpath_present_57 >> rail.Label(
            'Yes') >> trigger_dag_run_velaw_user_import_velawg3_child_department_add_v2_059 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_department_add_v2_059 \
            >> accumulate_list_items_61 >> get_location_details_66
        if_foreach_query_list_getall_departmentsnotin_replicon_43_56_fullpath_present_57 >> rail.Label(
            'No') >> get_location_details_66
        if_query_list_getall_departmentsnotin_replicon_43_rows_greater_than_0_50 >> rail.Label(
            'No') >> get_location_details_66 >> invoke_custom_ruby_code_69 >> create_list_70 \
            >> query_list_unique_locations_71 >> query_list_getalllocationsnotin_replicon_73 >> query_list_getalllocationsinfeedfileanddisabledin_replicon_74 \
            >> trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_location_77 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_location_77 \
            >> if_query_list_getalllocationsnotin_replicon_73_rows_greater_than_0_80
        if_query_list_getalllocationsnotin_replicon_73_rows_greater_than_0_80 >> rail.Label(
            'Yes') >> trigger_dag_run_velawg3_child_location_add_v2_084 >> wait_for_completion_trigger_dag_run_velawg3_child_location_add_v2_084 \
            >> accumulate_list_items_86 >> get_employee_type_group_details_91
        if_query_list_getalllocationsnotin_replicon_73_rows_greater_than_0_80 >> rail.Label(
            'No') >> get_employee_type_group_details_91 >> invoke_custom_ruby_code_94 >> create_list_95 \
            >> query_list_get_distinctemployeegroup_groupfrom_input_96 >> query_list_getall_employee_typesnotin_replicon_98 \
            >> query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_99 \
            >> trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_102 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_102 \
            >> if_query_list_getall_employee_typesnotin_replicon_98_rows_greater_than_0_105
        if_query_list_getall_employee_typesnotin_replicon_98_rows_greater_than_0_105 >> rail.Label(
            'Yes') >> trigger_dag_run_velaw_user_import_velawg3_child_employee_type_add_v2_0109 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_employee_type_add_v2_0109 \
            >> accumulate_list_items_111 >> get_all_divisions_116
        if_query_list_getall_employee_typesnotin_replicon_98_rows_greater_than_0_105 >> rail.Label(
            'No') >> get_all_divisions_116 >> invoke_custom_ruby_code_119 >> create_list_120 \
            >> query_list_get_distinct_divsion_groupfrom_input_121 >> create_list_122 >> query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123 \
            >> query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_124 \
            >> trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_127 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_cost_center_add_v2_0toenable_timeofftype_127 \
            >> accumulate_list_items_129 >> if_query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123_rows_greater_than_0_130
        if_query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123_rows_greater_than_0_130 >> rail.Label(
            'Yes') >> trigger_dag_run_velaw_user_import_velawg3_child_division_add_v2_0134 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_division_add_v2_0134 \
            >> accumulate_list_items_136 >> log_error_140
        if_query_list_get_distinct_divsion_groupfrom_inputwhereitisnottherein_replicon_123_rows_greater_than_0_130 >> rail.Label(
            'No') >> log_error_140 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
