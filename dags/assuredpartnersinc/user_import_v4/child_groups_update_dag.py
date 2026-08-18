from datetime import timedelta
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v4.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_groups_update_dag_id,
        description=f'Assured Partners User Import Groups Update Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='assuredpartners_groups_table'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='assuredpartners_groups_table',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        assuredpartners_groups_table = rail.CreateLogOperator(
            task_id='assuredpartners_groups_table'
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            name='valid_records',
            query="""SELECT * FROM rawinput_assuredpartners WHERE NULLIF(EmplID_Login,'') IS NOT NULL"""
        )

        query_distinct_cost_center_19 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_cost_center_19',
            name='distinct_cost_center_feed_file',
            query="""SELECT DISTINCT PayrollGrouping FROM valid_records""",
        )

        get_cost_center_details_12 = rail.RepliconServiceOperator(
            task_id='get_cost_center_details_12',
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
            },
            data_handler=lambda response: python_callable.data_handler_for_replicon_groups(
                response)
        )

        create_replicon_costcenters_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_costcenters_collection",
            columns=['name', 'uri', 'fullpath', 'length', 'status'],
            source="{{ result ('get_cost_center_details_12') | to_json }}",
            name="replicon_costcenters"
        )

        query_costcenters_to_create = rail.QueryCollectionOperator(
            task_id='query_costcenters_to_create',
            query="""SELECT DISTINCT * FROM distinct_cost_center_feed_file where LOWER(PayrollGrouping) NOT IN
                    (SELECT DISTINCT LOWER(fullpath) FROM replicon_costcenters)"""
        )

        query_list_costcenters_present_in_feed_file_and_disabled_in_replicon_21 = rail.QueryCollectionOperator(
            task_id='query_list_costcenters_present_in_feed_file_and_disabled_in_replicon_21',
            query="""SELECT * FROM  replicon_costcenters WHERE (LOWER(replicon_costcenters.fullpath) IN (SELECT LOWER(PayrollGrouping) FROM  distinct_cost_center_feed_file) AND  replicon_costcenters.status='False')""",
        )

        if_query_list_21_rows_greater_than_0_22 = rail.IfOperator(
            task_id='if_query_list_21_rows_greater_than_0_22',
            test='''{{ result('query_list_costcenters_present_in_feed_file_and_disabled_in_replicon_21','length') > 0 }}''',
            yes_task="trigger_dag_run_child_cost_center_add_024",
            no_task="if_query_list_getallcostcentersnotin_replicon_20_rows_greater_than_0_27",
        )

        trigger_dag_run_child_cost_center_add_024 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_cost_center_add_024',
            retries=0,
            items="{{ result('query_list_costcenters_present_in_feed_file_and_disabled_in_replicon_21') }}",
            trigger_dag_id=config.child_cost_center_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "costcenter": "{{item.name}}",
                "costcenterdescription": "{{item.fullpath}}",
                "type": "costCenter",
                "uri": "{{item.uri}}",
                'groups_table': "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_dag_runs_child_cost_center_add = rail.WaitForDagRunsSensor(
            task_id="wait_dag_run_childs_cost_center_add",
            dag_runs="{{result('trigger_dag_run_child_cost_center_add_024')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        if_query_list_getallcostcentersnotin_replicon_20_rows_greater_than_0_27 = rail.IfOperator(
            task_id='if_query_list_getallcostcentersnotin_replicon_20_rows_greater_than_0_27',
            test='''{{ result('query_costcenters_to_create','length') > 0 }}''',
            yes_task="assured_partners_groups_table_add_entry_29",
            no_task="get_department_group_details_40",
        )

        assured_partners_groups_table_add_entry_29 = rail.WriteLogOperator(
            task_id='assured_partners_groups_table_add_entry_29',
            log="{{ result('assuredpartners_groups_table') }}",
            items="{{ result('create_replicon_costcenters_collection')}}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "name": "{{ item.name }}",
                "uri": "{{ item.uri }}",
                "fullpath": "{{ item.fullpath }}",
                "type": "costCenter"
            }
        )

        trigger_dag_run_child_cost_center_add_033 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_cost_center_add_033',
            retries=0,
            items="{{ result('query_costcenters_to_create') }}",
            trigger_dag_id=config.child_cost_center_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "costcenter": "{{item.PayrollGrouping}}",
                "costcenterdescription": null,
                'type': "",
                'groups_table': "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_dag_run_child_cost_center_add_033 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_dag_run_child_cost_center_add_033',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_cost_center_add_033") }}'
        )

        get_department_group_details_40 = rail.RepliconServiceOperator(
            task_id='get_department_group_details_40',
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
            },
            data_handler=lambda response: python_callable.data_handler_for_replicon_groups(
                response)
        )

        create_replicon_department_group_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_department_group_collection",
            columns=['name', 'uri', 'fullpath', 'length', 'status'],
            source="{{ result ('get_department_group_details_40') | to_json }}",
            name="replicon_departmentgroupdata"
        )

        query_list_get_distinct_department_groupfrom_input_47 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_department_groupfrom_input_47',
            query="""SELECT DISTINCT Agency_Org2 AS department, AgencyDescription AS description FROM  valid_records WHERE ( NULLIF(Agency_Org2,'') IS NOT NULL AND LOWER(Agency_Org2)!='none')""",
            name='departmentrawdata'
        )

        query_list_get_all_departments_not_in_replicon_48 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_departments_not_in_replicon_48',
            query="""SELECT DISTINCT department,description FROM  departmentrawdata WHERE LOWER(department) NOT IN (SELECT DISTINCT LOWER(replicon_departmentgroupdata.name) FROM  replicon_departmentgroupdata)""",
            name='departments_not_in_replicon'
        )

        query_list_getall_departments_present_in_feed_file_and_disabled_in_replicon_49 = rail.QueryCollectionOperator(
            task_id='query_list_getall_departments_present_in_feed_file_and_disabled_in_replicon_49',
            query="""SELECT * FROM  replicon_departmentgroupdata WHERE (LOWER(replicon_departmentgroupdata.name) IN (SELECT DISTINCT LOWER(departmentrawdata.department) FROM  departmentrawdata) AND  replicon_departmentgroupdata.status='False')""",
        )

        if_query_list_getalldepartments_not_enabled_in_replicon = rail.IfOperator(
            task_id='if_query_list_getalldepartments_not_enabled_in_replicon',
            test='''{{ result('query_list_getall_departments_present_in_feed_file_and_disabled_in_replicon_49','length') > 0 }}''',
            yes_task="trigger_dag_run_child_cost_center_add_to_enable_department_52",
            no_task="if_query_list_getall_departmentsnotin_replicon_48_rows_greater_than_0_55",
        )

        trigger_dag_run_child_cost_center_add_to_enable_department_52 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_cost_center_add_to_enable_department_52',
            retries=0,
            items="{{ result('query_list_getall_departments_present_in_feed_file_and_disabled_in_replicon_49') }}",
            trigger_dag_id=config.child_cost_center_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "costcenter": "{{ item.name }}",
                "costcenterdescription": "{{ item.name }}",
                "type": "departmentGroup",
                "uri": "{{ item.uri }}",
                'groups_table': "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_department_52 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_department_52',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_cost_center_add_to_enable_department_52") }}'
        )

        if_query_list_getall_departmentsnotin_replicon_48_rows_greater_than_0_55 = rail.IfOperator(
            task_id='if_query_list_getall_departmentsnotin_replicon_48_rows_greater_than_0_55',
            test='''{{ result('query_list_get_all_departments_not_in_replicon_48', 'length') > 0}}''',
            yes_task="assured_partners_groups_table_add_entry_57",
            no_task="log_parent_group_assured_partners_uri_58",
        )

        assured_partners_groups_table_add_entry_57 = rail.WriteLogOperator(
            task_id='assured_partners_groups_table_add_entry_57',
            log="{{ result('assuredpartners_groups_table') }}",
            items="{{ result('create_replicon_department_group_collection') }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "name": "{{ item.name }}",
                "uri": "{{ item.uri }}",
                "fullpath": "{{ item.fullpath }}",
                "type": "department"
            }
        )

        log_parent_group_assured_partners_uri_58 = rail.PythonOperator(
            task_id='log_parent_group_assured_partners_uri_58',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_department_group_details_40'), 'fullpath', 'AssuredPartnersInc', 'uri') if rail.result('get_department_group_details_40') else null
        )

        if_log_parent_group_assured_partners_uri_58_blank_59 = rail.IfOperator(
            task_id='if_log_parent_group_assured_partners_uri_58_blank_59',
            test='''{{ result('log_parent_group_assured_partners_uri_58') | is_falsy }}''',
            yes_task="stop_60",
            no_task="query_valid_departments_not_in_replicon",
        )

        stop_60 = rail.FailOperator(
            task_id='stop_60',
            message='''AssuredPartnersInc parent department group is not available'''
        )

        query_valid_departments_not_in_replicon = rail.QueryCollectionOperator(
            task_id='query_valid_departments_not_in_replicon',
            query="""SELECT * FROM  departments_not_in_replicon WHERE NULLIF(department,'') IS NOT NULL""",
        )

        trigger_dag_run_child_department_add_064 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_department_add_064',
            retries=0,
            items="{{ result('query_valid_departments_not_in_replicon') }}",
            trigger_dag_id=config.child_department_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "department": "{{ item.department }}",
                "compaydepturi": "{{ result('log_parent_group_assured_partners_uri_58') }}",
                "description": "{{ item.description }}",
                "type": "department",
                'groups_table': "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_department_add_064 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_department_add_064',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_department_add_064") }}'
        )

        get_location_details_71 = rail.RepliconServiceOperator(
            task_id='get_location_details_71',
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
            },
            data_handler=lambda response: python_callable.data_handler_for_replicon_groups(
                response)
        )

        create_replicon_location_data_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_location_data_collection",
            columns=['name', 'uri', 'fullpath', 'length', 'status'],
            source="{{ result ('get_location_details_71') | to_json }}",
            name="replicon_locationdata"
        )

        valid_locationrawdata_entries = rail.QueryCollectionOperator(
            task_id='valid_locationrawdata_entries',
            query="""SELECT PayGroupCode AS location , PayGroup AS locationdescription FROM valid_records WHERE NULLIF(PayGroupCode,'') IS NOT NULL """,
            name='valid_entries_locationrawdata'
        )

        query_list_getalllocationsnotin_replicon_78 = rail.QueryCollectionOperator(
            task_id='query_list_getalllocationsnotin_replicon_78',
            query="""SELECT DISTINCT  valid_entries_locationrawdata.location,  valid_entries_locationrawdata.locationdescription FROM  valid_entries_locationrawdata WHERE LOWER( valid_entries_locationrawdata.location) NOT IN (SELECT DISTINCT LOWER( replicon_locationdata.fullpath) FROM  replicon_locationdata)""",
        )

        query_list_getalllocationsinfeedfileanddisabledin_replicon_79 = rail.QueryCollectionOperator(
            task_id='query_list_getalllocationsinfeedfileanddisabledin_replicon_79',
            query="""SELECT * FROM  replicon_locationdata WHERE (LOWER( replicon_locationdata.fullpath) IN (SELECT DISTINCT LOWER( valid_entries_locationrawdata.location) FROM  valid_entries_locationrawdata) AND  replicon_locationdata.status='False')""",
        )

        trigger_dag_run_to_enable_location_82 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_to_enable_location_82',
            retries=0,
            items="{{ result('query_list_getalllocationsinfeedfileanddisabledin_replicon_79') }}",
            trigger_dag_id=config.child_cost_center_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "costcenter": "{{ item.name }}",
                "costcenterdescription": "{{ item.name }}",
                "type": "location",
                "uri": "{{ item.uri }}",
                'groups_table': "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_dag_run_to_enable_location_82 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_damesh.wait_for_completion_dag_run_to_enable_location_82',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_to_enable_location_82") }}'
        )

        if_query_list_getalllocationsnotin_replicon_78_rows_greater_than_0_85 = rail.IfOperator(
            task_id='if_query_list_getalllocationsnotin_replicon_78_rows_greater_than_0_85',
            test='''{{ result('query_list_getalllocationsnotin_replicon_78', 'length') > 0 }}''',
            yes_task="assured_partners_groups_table_add_entry_87",
            no_task="get_employee_type_group_details_98",
        )

        assured_partners_groups_table_add_entry_87 = rail.WriteLogOperator(
            task_id='assured_partners_groups_table_add_entry_87',
            log="{{ result('assuredpartners_groups_table') }}",
            items="{{result('create_replicon_location_data_collection')}}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "name": "{{ item.name }}",
                "uri": "{{ item.uri }}",
                "fullpath": "{{ item.fullpath }}",
                "type": "location"
            }
        )

        trigger_dag_run_assured_partners_child_location_add_091 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_assured_partners_child_location_add_091',
            retries=0,
            items="{{result('query_list_getalllocationsnotin_replicon_78')}}",
            trigger_dag_id=config.child_location_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "location": "{{ item.location }}",
                "locationdescription": "{{ item.locationdescription }}",
                'groups_table': "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_assured_partners_child_location_add_091 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_assured_partners_child_location_add_091',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_assured_partners_child_location_add_091") }}'
        )

        get_employee_type_group_details_98 = rail.RepliconServiceOperator(
            task_id='get_employee_type_group_details_98',
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
            },
            data_handler=lambda response: python_callable.data_handler_for_replicon_groups(
                response)
        )

        create_replicon_employeetype_groupdata_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_employeetype_groupdata_collection",
            columns=['name', 'uri', 'fullpath', 'length', 'status'],
            source="{{ result ('get_employee_type_group_details_98') | to_json }}",
            name="replicon_employeetypegroupdata"
        )

        query_valid_records_empoyeetypeinputdata = rail.QueryCollectionOperator(
            task_id='query_valid_records_empoyeetypeinputdata',
            query="""SELECT Dept_Org4Desc AS employeetype , Dept_Org4 AS description FROM valid_records WHERE NULLIF(Dept_Org4Desc,'') IS NOT NULL""",
            name='validated_empoyeetypeinputdata'
        )

        query_list_getall_employee_typesnotin_replicon_105 = rail.QueryCollectionOperator(
            task_id='query_list_getall_employee_typesnotin_replicon_105',
            query="""SELECT DISTINCT  validated_empoyeetypeinputdata.employeetype, validated_empoyeetypeinputdata.description FROM  validated_empoyeetypeinputdata WHERE LOWER( validated_empoyeetypeinputdata.employeetype) NOT IN (SELECT DISTINCT LOWER( replicon_employeetypegroupdata.fullpath) FROM  replicon_employeetypegroupdata)""",
        )

        query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_106 = rail.QueryCollectionOperator(
            task_id='query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_106',
            query="""SELECT * FROM  replicon_employeetypegroupdata WHERE (LOWER( replicon_employeetypegroupdata.fullpath) IN (SELECT DISTINCT LOWER( validated_empoyeetypeinputdata.employeetype) FROM  validated_empoyeetypeinputdata) AND  replicon_employeetypegroupdata.status='False')""",
        )

        trigger_dag_run_child_cost_center_add_to_enable_employeetype_109 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_cost_center_add_to_enable_employeetype_109',
            retries=0,
            items="{{ result('query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_106') }}",
            trigger_dag_id=config.child_cost_center_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "costcenter": "{{ item.name }}",
                "costcenterdescription": "{{ item.name }}",
                "type": "employeeTypeGroup",
                "uri": "{{ item.uri }}",
                "groups_table": "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_employeetype_109 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_employeetype_109',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_cost_center_add_to_enable_employeetype_109") }}'
        )

        if_query_list_getall_employee_typesnotin_replicon_105_rows_greater_than_0_112 = rail.IfOperator(
            task_id='if_query_list_getall_employee_typesnotin_replicon_105_rows_greater_than_0_112',
            test='''{{ result('query_list_getall_employee_typesnotin_replicon_105', 'length') > 0 }}''',
            yes_task="trigger_dag_run_child_employee_type_add_116",
            no_task="get_all_divisions_123",
        )

        trigger_dag_run_child_employee_type_add_116 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_employee_type_add_116',
            retries=0,
            items="{{ result('query_list_getall_employee_typesnotin_replicon_105') }}",
            trigger_dag_id=config.child_employee_type_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "employeetype": "{{item.employeetype }}",
                "description": "{{ item.description }}",
                "groups_table": "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_employee_type_add_116 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_employee_type_add_116',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_employee_type_add_116") }}'
        )

        get_all_divisions_123 = rail.RepliconServiceOperator(
            task_id='get_all_divisions_123',
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
            },
            data_handler=lambda response: python_callable.data_handler_for_replicon_groups(
                response)
        )

        create_replicon_divisiondata_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_divisiondata_collection",
            columns=['name', 'uri', 'fullpath', 'length', 'status'],
            source="{{ result ('get_all_divisions_123') | to_json }}",
            name="replicon_divisiondata"
        )

        query_get_distinct_divisiondata_from_feed_file = rail.QueryCollectionOperator(
            task_id='query_get_distinct_divisiondata_from_feed_file',
            query="""SELECT DISTINCT LocationCode_Work FROM valid_records WHERE NULLIF(LocationCode_Work,'') IS NOT NULL""",
            name='divisionrawdata'
        )

        query_list_get_distinct_divsion_group_from_input_not_in_replicon_128 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_divsion_group_from_input_not_in_replicon_128',
            query="""SELECT DISTINCT  divisionrawdata.LocationCode_Work FROM  divisionrawdata WHERE LOWER( divisionrawdata.LocationCode_Work) NOT IN (SELECT DISTINCT LOWER( replicon_divisiondata.name) FROM  replicon_divisiondata)""",
        )

        query_list_get_distinct_divsion_group_from_input_present_in_replicon_129 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_divsion_group_from_input_present_in_replicon_129',
            query="""SELECT DISTINCT  divisionrawdata.LocationCode_Work FROM  divisionrawdata WHERE LOWER( divisionrawdata.LocationCode_Work) IN (SELECT DISTINCT LOWER( replicon_divisiondata.name) FROM  replicon_divisiondata)""",
        )

        query_list_get_all_divisions_in_feed_file_but_disabled_in_replicon_131 = rail.QueryCollectionOperator(
            task_id='query_list_get_all_divisions_in_feed_file_but_disabled_in_replicon_131',
            query="""SELECT * FROM  replicon_divisiondata WHERE (LOWER( replicon_divisiondata.name) IN (SELECT DISTINCT LOWER( divisionrawdata.LocationCode_Work) FROM  divisionrawdata) AND  replicon_divisiondata.status='False')""",
        )

        trigger_dag_run_child_cost_center_add_to_enable_employeetype_134 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_cost_center_add_to_enable_employeetype_134',
            retries=0,
            items="{{ result('query_list_get_all_divisions_in_feed_file_but_disabled_in_replicon_131') }}",
            trigger_dag_id=config.child_cost_center_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "costcenter": "{{ item.name }}",
                "costcenterdescription": "{{ item.name }}",
                "type": "division",
                "uri": "{{ item.uri }}",
                "groups_table": "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_division_134 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_division_134',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_cost_center_add_to_enable_employeetype_134") }}'
        )

        if_query_list_128_rows_greater_than_0_137 = rail.IfOperator(
            task_id='if_query_list_128_rows_greater_than_0_137',
            test='''{{ result('query_list_get_distinct_divsion_group_from_input_not_in_replicon_128','length' )> 0 }}''',
            yes_task="trigger_dag_run_child_division_add_141",
            no_task="get_all_service_centers_148",
        )

        trigger_dag_run_child_division_add_141 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_division_add_141',
            retries=0,
            items="{{ result('query_list_get_distinct_divsion_group_from_input_not_in_replicon_128') }}",
            trigger_dag_id=config.child_division_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "divisiondescription": "na",
                "division": "{{ item.LocationCode_Work }}",
                "groups_table": "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_division_add_116 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_division_add_116',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_division_add_141") }}'
        )

        get_all_service_centers_148 = rail.RepliconServiceOperator(
            task_id='get_all_service_centers_148',
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center",
                    "urn:replicon:service-center-list-column:full-path",
                    "urn:replicon:service-center-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: python_callable.data_handler_for_replicon_groups(
                response)
        )

        create_replicon_servicecenter_list_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_servicecenter_list_collection",
            columns={'name': 'servicecenter', 'uri': 'uri',
                     'fullpath': 'fullpath', 'length': 'length', 'status': 'status'},
            source="{{ result ('get_all_service_centers_148') | to_json }}",
            name="replicon_servicecenter_data"
        )

        query_get_distinct_servicecenter_data_from_feed_file_153 = rail.QueryCollectionOperator(
            task_id='query_get_distinct_servicecenter_data_from_feed_file_153',
            query="""SELECT DISTINCT ProfitCenter, ProfitCenterDescription FROM valid_records WHERE NULLIF(ProfitCenter,'') IS NOT NULL""",
            name='servicecenter_rawdata'
        )

        query_list_get_distinct_service_center_groupfrom_inputwhereitisnottherein_replicon_155 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_service_center_groupfrom_inputwhereitisnottherein_replicon_155',
            query="""SELECT * FROM  servicecenter_rawdata WHERE LOWER( servicecenter_rawdata.ProfitCenter) NOT IN (SELECT DISTINCT LOWER( replicon_servicecenter_data.servicecenter) FROM  replicon_servicecenter_data)""",
        )

        query_list_getall_servicecentersinfeedfilebutdisabledin_replicon_156 = rail.QueryCollectionOperator(
            task_id='query_list_getall_servicecentersinfeedfilebutdisabledin_replicon_156',
            query="""SELECT * FROM  replicon_servicecenter_data WHERE (LOWER( replicon_servicecenter_data.servicecenter) IN (SELECT DISTINCT LOWER( servicecenter_rawdata.ProfitCenter) FROM  servicecenter_rawdata) AND  replicon_servicecenter_data.status='False')""",
        )

        trigger_dag_run_child_cost_center_add_to_enable_servicecenter_159 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_cost_center_add_to_enable_servicecenter_159',
            retries=0,
            items="{{ result('query_list_getall_servicecentersinfeedfilebutdisabledin_replicon_156') }}",
            trigger_dag_id=config.child_cost_center_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "costcenter": "{{ item.servicecenter }}",
                "costcenterdescription": "{{ item.servicecenter }}",
                "type": "serviceCenter",
                "uri": "{{ item.uri }}",
                "groups_table": "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_servicecenter_159 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_servicecenter_159',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_cost_center_add_to_enable_servicecenter_159") }}'
        )

        if_query_list_get_distinctservicecenter_groupfrom_input_153_rows_greater_than_0_162 = rail.IfOperator(
            task_id='if_query_list_get_distinctservicecenter_groupfrom_input_153_rows_greater_than_0_162',
            test='''{{ result('query_get_distinct_servicecenter_data_from_feed_file_153', 'length') > 0 }}''',
            yes_task="trigger_dag_run_assured_partners_child_service_center_add_166",
            no_task="query_list_get_distinct_scheduledatafrom_input_169",
        )

        trigger_dag_run_assured_partners_child_service_center_add_166 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_assured_partners_child_service_center_add_166',
            retries=0,
            items="{{ result('query_list_get_distinct_service_center_groupfrom_inputwhereitisnottherein_replicon_155') }}",
            trigger_dag_id=config.child_service_center_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "servicecenterdescription": "{{ item.ProfitCenterDescription }}",
                "servicecenter": "{{ item.ProfitCenter }}",
                "groups_table": "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_assured_partners_child_service_center_add_166 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_assured_partners_child_service_center_add_166',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_assured_partners_child_service_center_add_166") }}'
        )

        query_list_get_distinct_scheduledatafrom_input_169 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_scheduledatafrom_input_169',
            query="""SELECT DISTINCT  Schedule, WeeklySTDHrs FROM  valid_records WHERE NULLIF(Schedule,'') IS NOT NULL""",
            name='office_schedule_list_from_input'
        )

        get_all_office_schedules_172 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_172',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        create_replicon_officeschedule_list_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_officeschedule_list_collection",
            columns=['displayText', 'slug', 'uri'],
            source="{{ result ('get_all_office_schedules_172') | to_json }}",
            name="replicon_office_schedule_list_data"
        )

        query_list_getofficeschedulesthatdoesntexistin_replicon_176 = rail.QueryCollectionOperator(
            task_id='query_list_getofficeschedulesthatdoesntexistin_replicon_176',
            query="""SELECT * FROM  office_schedule_list_from_input WHERE  office_schedule_list_from_input.Schedule NOT IN (SELECT DISTINCT  replicon_office_schedule_list_data.displayText FROM  replicon_office_schedule_list_data)""",
        )

        trigger_dag_run_assured_partners_child_office_schedule_180 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_assured_partners_child_office_schedule_180',
            retries=0,
            items="{{ result('query_list_getofficeschedulesthatdoesntexistin_replicon_176') }}",
            trigger_dag_id=config.child_office_schedule_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "jobid": "{{dag_run_ecid()}}",
                "schedulename": "{{ item.Schedule }}",
                "scheduledhoursweekly": "{{ item.WeeklySTDHrs }}",
                "groups_table": "{{result('assuredpartners_groups_table')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_assured_partners_child_office_schedule_180 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_assured_partners_child_office_schedule_180',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_assured_partners_child_office_schedule_180") }}'
        )

        check_error_in_groups_log = rail.FilterLogEntriesOperator(
            task_id='check_error_in_groups_log',
            log="{{result('assuredpartners_groups_table')}}",
            severity="Error"
        )

        if_error_in_group_logs = rail.IfOperator(
            task_id='if_error_in_group_logs',
            test=lambda: rail.result(
                "check_error_in_groups_log", "length") > 0,
            yes_task="accumulate_errors_from_group_import",
            no_task='finish'
        )

        def accumulate_errors():
            entries = rail.load_all_records(
                rail.result("check_error_in_groups_log"))
            final_error_message = []
            for item in entries:
                final_error_message.append(
                    item['properties']['details'].split(";")[0])
            return str(";".join(final_error_message))

        accumulate_errors_from_group_import = rail.PythonOperator(
            task_id='accumulate_errors_from_group_import',
            python_callable=accumulate_errors
        )

        fail_due_to_errors = rail.FailOperator(
            task_id='fail_due_to_errors',
            message="{{result('accumulate_errors_from_group_import')}}"
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> assuredpartners_groups_table

        assuredpartners_groups_table >> query_valid_records >> query_distinct_cost_center_19 >> get_cost_center_details_12 >> create_replicon_costcenters_collection \
            >> query_costcenters_to_create >> query_list_costcenters_present_in_feed_file_and_disabled_in_replicon_21 >> if_query_list_21_rows_greater_than_0_22

        if_query_list_21_rows_greater_than_0_22 >> rail.Label(
            'No') >> if_query_list_getallcostcentersnotin_replicon_20_rows_greater_than_0_27
        if_query_list_21_rows_greater_than_0_22 >> rail.Label('Yes') >> trigger_dag_run_child_cost_center_add_024 \
            >> wait_dag_runs_child_cost_center_add >> if_query_list_getallcostcentersnotin_replicon_20_rows_greater_than_0_27

        if_query_list_getallcostcentersnotin_replicon_20_rows_greater_than_0_27 >> rail.Label(
            'No') >> get_department_group_details_40
        if_query_list_getallcostcentersnotin_replicon_20_rows_greater_than_0_27 >> rail.Label(
            'Yes') >> assured_partners_groups_table_add_entry_29 \
            >> trigger_dag_run_child_cost_center_add_033 >> wait_for_completion_dag_run_child_cost_center_add_033

        wait_for_completion_dag_run_child_cost_center_add_033 >> get_department_group_details_40 \
            >> create_replicon_department_group_collection >> query_list_get_distinct_department_groupfrom_input_47 \
            >> query_list_get_all_departments_not_in_replicon_48 >> query_list_getall_departments_present_in_feed_file_and_disabled_in_replicon_49 \
            >> if_query_list_getalldepartments_not_enabled_in_replicon

        if_query_list_getalldepartments_not_enabled_in_replicon >> rail.Label(
            'No') >> if_query_list_getall_departmentsnotin_replicon_48_rows_greater_than_0_55
        if_query_list_getalldepartments_not_enabled_in_replicon >> rail.Label('Yes') >> trigger_dag_run_child_cost_center_add_to_enable_department_52 \
            >> wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_department_52 \
            >> if_query_list_getall_departmentsnotin_replicon_48_rows_greater_than_0_55

        if_query_list_getall_departmentsnotin_replicon_48_rows_greater_than_0_55 >> rail.Label(
            'No') >> log_parent_group_assured_partners_uri_58
        if_query_list_getall_departmentsnotin_replicon_48_rows_greater_than_0_55 >> rail.Label('Yes') >> assured_partners_groups_table_add_entry_57 \
            >> log_parent_group_assured_partners_uri_58 >> if_log_parent_group_assured_partners_uri_58_blank_59

        if_log_parent_group_assured_partners_uri_58_blank_59 >> rail.Label(
            'No') >> query_valid_departments_not_in_replicon
        if_log_parent_group_assured_partners_uri_58_blank_59 >> rail.Label(
            'Yes') >> stop_60 >> query_valid_departments_not_in_replicon

        query_valid_departments_not_in_replicon >> trigger_dag_run_child_department_add_064 \
            >> wait_for_completion_trigger_dag_run_child_department_add_064 >> get_location_details_71 \
            >> create_replicon_location_data_collection \
            >> valid_locationrawdata_entries >> query_list_getalllocationsnotin_replicon_78 >> query_list_getalllocationsinfeedfileanddisabledin_replicon_79 >> trigger_dag_run_to_enable_location_82 \
            >> wait_for_completion_dag_run_to_enable_location_82 >> if_query_list_getalllocationsnotin_replicon_78_rows_greater_than_0_85

        if_query_list_getalllocationsnotin_replicon_78_rows_greater_than_0_85 >> rail.Label(
            'No') >> get_employee_type_group_details_98
        if_query_list_getalllocationsnotin_replicon_78_rows_greater_than_0_85 >> rail.Label('Yes') >> assured_partners_groups_table_add_entry_87 >> trigger_dag_run_assured_partners_child_location_add_091 \
            >> wait_for_completion_trigger_dag_run_assured_partners_child_location_add_091 >> get_employee_type_group_details_98

        get_employee_type_group_details_98 >> create_replicon_employeetype_groupdata_collection \
            >> query_valid_records_empoyeetypeinputdata >> query_list_getall_employee_typesnotin_replicon_105 >> query_list_getall_employee_typesinfeedfilebutdisabledin_replicon_106 \
            >> trigger_dag_run_child_cost_center_add_to_enable_employeetype_109 >> wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_employeetype_109 \
            >> if_query_list_getall_employee_typesnotin_replicon_105_rows_greater_than_0_112

        if_query_list_getall_employee_typesnotin_replicon_105_rows_greater_than_0_112 >> rail.Label(
            'No') >> get_all_divisions_123
        if_query_list_getall_employee_typesnotin_replicon_105_rows_greater_than_0_112 >> rail.Label('Yes') >> trigger_dag_run_child_employee_type_add_116 \
            >> wait_for_completion_trigger_dag_run_child_employee_type_add_116 >> get_all_divisions_123

        get_all_divisions_123 >> create_replicon_divisiondata_collection >> query_get_distinct_divisiondata_from_feed_file \
            >> query_list_get_distinct_divsion_group_from_input_not_in_replicon_128 >> query_list_get_distinct_divsion_group_from_input_present_in_replicon_129 \
            >> query_list_get_all_divisions_in_feed_file_but_disabled_in_replicon_131 >> trigger_dag_run_child_cost_center_add_to_enable_employeetype_134 \
            >> wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_division_134 >> if_query_list_128_rows_greater_than_0_137

        if_query_list_128_rows_greater_than_0_137 >> rail.Label(
            'No') >> get_all_service_centers_148
        if_query_list_128_rows_greater_than_0_137 >> rail.Label(
            'Yes') >> trigger_dag_run_child_division_add_141 >> wait_for_completion_trigger_dag_run_child_division_add_116

        wait_for_completion_trigger_dag_run_child_division_add_116 >> get_all_service_centers_148 >> create_replicon_servicecenter_list_collection \
            >> query_get_distinct_servicecenter_data_from_feed_file_153 >> query_list_get_distinct_service_center_groupfrom_inputwhereitisnottherein_replicon_155 \
            >> query_list_getall_servicecentersinfeedfilebutdisabledin_replicon_156 >> trigger_dag_run_child_cost_center_add_to_enable_servicecenter_159 \
            >> wait_for_completion_trigger_dag_run_child_cost_center_add_to_enable_servicecenter_159 >> if_query_list_get_distinctservicecenter_groupfrom_input_153_rows_greater_than_0_162

        if_query_list_get_distinctservicecenter_groupfrom_input_153_rows_greater_than_0_162 >> rail.Label(
            'No') >> query_list_get_distinct_scheduledatafrom_input_169
        if_query_list_get_distinctservicecenter_groupfrom_input_153_rows_greater_than_0_162 >> rail.Label(
            'Yes') >> trigger_dag_run_assured_partners_child_service_center_add_166 >> wait_for_completion_trigger_dag_run_assured_partners_child_service_center_add_166

        wait_for_completion_trigger_dag_run_assured_partners_child_service_center_add_166 >> query_list_get_distinct_scheduledatafrom_input_169 \
            >> get_all_office_schedules_172

        get_all_office_schedules_172 >> create_replicon_officeschedule_list_collection >> query_list_getofficeschedulesthatdoesntexistin_replicon_176 \
            >> trigger_dag_run_assured_partners_child_office_schedule_180 >> wait_for_completion_trigger_dag_run_assured_partners_child_office_schedule_180 \
            >> check_error_in_groups_log

        check_error_in_groups_log >> if_error_in_group_logs

        if_error_in_group_logs >> rail.Label('No') >> finish
        if_error_in_group_logs >> rail.Label(
            'Yes') >> accumulate_errors_from_group_import >> fail_due_to_errors >> finish

        return dag


rail.for_each_instance(create_dag)
