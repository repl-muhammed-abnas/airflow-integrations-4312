
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_spain_groups_update_child_{config.instance}_{config.version}',
        description=f'MichaelKorsTnA Child_groups update {config.instance}_{config.version}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_report_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_report_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_report_3 = rail.PythonOperator(  # to be edited
            task_id='get_report_3',
            python_callable=lambda dag_run: dag_run.conf['report']
        )

        create_list_4 = rail.CreateCollectionOperator(
            task_id='create_list_4',
            source=lambda: rail.result('get_report_3'),
            name="grouprawdata",
        )

        query_list_get_country_related_data_5 = rail.QueryCollectionOperator(
            task_id='query_list_get_country_related_data_5',
            name="countrydata",
            query="""SELECT * FROM  grouprawdata WHERE  grouprawdata.Country = "{{ dag_run.conf.country }}" """,
        )

        create_groups_lookuptable = rail.CreateLogOperator(
            task_id='create_groups_lookuptable'
        )

        def get_existing_details_of_group(response):
            return [{
                'groupname': group['cells'][0].get('textValue'),
                'groupuri': group['cells'][0].get('uri'),
                'fullpath': rail.smartjoin_by_delim([item['textValue'] for item in group['cells'][1]['cellCollection']], '/'),
                'length': len([item['textValue'] for item in group['cells'][1]['cellCollection']]),
            } for group in response['rows']]

        get_cost_center_details_9 = rail.RepliconServiceOperator(
            task_id='get_cost_center_details_9',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_existing_details_of_group
        )

        create_list_15 = rail.CreateCollectionOperator(
            task_id='create_list_15',
            source=lambda: rail.result('get_cost_center_details_9'),
            name="costcenterdata",
        )

        create_csv_lines_cost_center_data_16 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_cost_center_data_16',
            source="{{ result('query_list_get_country_related_data_5') }}",
            header=['costcenter',
                    'costcenterdescription'],
            row=lambda item: [
                rail.smartjoin_by_delim(
                    (item['Cost_Center_Hierarchy'] + "|" + item['CostCenter_ID']).split("|"), "/"),
                item['CostCenter_Name']
            ],
        )

        create_collection_create_list_from_csv_17 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_17',
            source="{{ result('create_csv_lines_cost_center_data_16') }}",
            name="costcenterrawdata",
            columns={
                'costcenter': 'costcenter',
                'costcenterdescription': 'costcenterdescription'
            }
        )

        query_list_get_distinct_cost_center_18 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_cost_center_18',
            query="""SELECT DISTINCT  costcenterrawdata.costcenter,  costcenterrawdata.costcenterdescription FROM  costcenterrawdata""",
        )

        query_list_getallcostcentersnotin_replicon_19 = rail.QueryCollectionOperator(
            task_id='query_list_getallcostcentersnotin_replicon_19',
            query="""SELECT DISTINCT  costcenterrawdata.costcenter,  costcenterrawdata.costcenterdescription FROM
                costcenterrawdata WHERE LOWER( costcenterrawdata.costcenter) NOT IN (SELECT DISTINCT LOWER( costcenterdata.fullpath) FROM  costcenterdata)""",
        )

        if_query_list_getallcostcentersnotin_replicon_19_rows_greater_than_0_20 = rail.IfOperator(
            task_id='if_query_list_getallcostcentersnotin_replicon_19_rows_greater_than_0_20',
            test='''{{ result('query_list_getallcostcentersnotin_replicon_19','length') > 0 }}''',
            yes_task="michael_kors_gmbh_groups_table_add_entry_22",
            no_task="get_department_group_details_32",
        )

        michael_kors_gmbh_groups_table_add_entry_22 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_22',
            log="{{ result('create_groups_lookuptable') }}",
            items=lambda: rail.result('get_cost_center_details_9'),
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "name": "{{ item.groupname }}",
                "uri": "{{ item.groupuri }}",
                "fullpath": "{{ item.fullpath }}",
                "type": "costcenter"
            }
        )

        trigger_child_cost_center_add = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_cost_center_add',
            retries=0,
            items="{{ result('query_list_getallcostcentersnotin_replicon_19') }}",
            trigger_dag_id=f'michaelkorstna_spain_user_import_cost_center_add_child_{config.instance}_{config.version}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "costcenter": "{{ item.costcenter }}",
                "groupslookuptable": "{{result('create_groups_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}",
                "costcenterdescription": "{{ item.costcenterdescription }}"
            }
        )

        wait_for_child_cost_center_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_cost_center_add',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_cost_center_add") }}'
        )

        get_department_group_details_32 = rail.RepliconServiceOperator(
            task_id='get_department_group_details_32',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_existing_details_of_group
        )

        create_list_38 = rail.CreateCollectionOperator(
            task_id='create_list_38',
            source=lambda: rail.result('get_department_group_details_32'),
            name="departmentgroupdata",
        )

        create_csv_lines_department_data_39 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_department_data_39',
            source="{{ result('query_list_get_country_related_data_5') }}",
            header=['department'],
            row=lambda item: [
                rail.smartjoin_by_delim(
                    ("Michael Kors" + "|" + (item['Job_Family_Group'] + "|" + item['Job_Family'])).split("|"), '/')
            ],
        )

        create_collection_create_list_from_csv_40 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_40',
            source="{{ result('create_csv_lines_department_data_39') }}",
            name="departmentrawdata",
            columns={
                'department': 'department'
            }
        )

        query_list_get_distinct_department_groupfrom_input_41 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_department_groupfrom_input_41',
            query="""SELECT DISTINCT  departmentrawdata.department FROM  departmentrawdata""",
        )

        query_list_getall_departmentsnotin_replicon_42 = rail.QueryCollectionOperator(
            task_id='query_list_getall_departmentsnotin_replicon_42',
            query="""SELECT DISTINCT  departmentrawdata.department FROM
                departmentrawdata WHERE LOWER( departmentrawdata.department) NOT IN
                (SELECT DISTINCT LOWER( departmentgroupdata.fullpath) FROM  departmentgroupdata)""",
        )

        if_query_list_getall_departmentsnotin_replicon_42_rows_greater_than_0_43 = rail.IfOperator(
            task_id='if_query_list_getall_departmentsnotin_replicon_42_rows_greater_than_0_43',
            test='''{{ result('query_list_getall_departmentsnotin_replicon_42','length') > 0 }}''',
            yes_task="michael_kors_gmbh_groups_table_add_entry_45",
            no_task="get_location_details_58",
        )

        michael_kors_gmbh_groups_table_add_entry_45 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_45',
            items=lambda: rail.result('get_department_group_details_32'),
            log="{{ result('create_groups_lookuptable') }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "name": "{{ item.groupname }}",
                "uri": "{{ item.groupuri }}",
                "fullpath": "{{ item.fullpath }}",
                "type": "department"
            }
        )

        log_parent_group_michael_kors_uri_46 = rail.PythonOperator(
            task_id='log_parent_group_michael_kors_uri_46',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_department_group_details_32'), 'fullpath', 'Michael Kors', 'groupuri', '') if rail.result('get_department_group_details_32') else ''
        )

        if_log_parent_group_michael_kors_uri_46_blank_47 = rail.IfOperator(
            task_id='if_log_parent_group_michael_kors_uri_46_blank_47',
            test='''{{ result('log_parent_group_michael_kors_uri_46') | is_falsy }}''',
            yes_task="stop_48",
            no_task="trigger_child_department_add",
        )

        stop_48 = rail.FailOperator(
            task_id='stop_48',
            message='''Michael Kors parent department group is not available'''
        )

        trigger_child_department_add = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_department_add',
            retries=0,
            items="{{ result('query_list_getall_departmentsnotin_replicon_42') }}",
            trigger_dag_id=f'michaelkorstna_spain_user_import_department_add_child_{config.instance}_{config.version}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "department": "{{ item.department }}",
                "compaydepturi": "{{ result('log_parent_group_michael_kors_uri_46') }}",
                "groupslookuptable": "{{result('create_groups_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}"
            }
        )

        wait_for_child_department_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_department_add',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_department_add") }}'
        )

        get_location_details_58 = rail.RepliconServiceOperator(
            task_id='get_location_details_58',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_existing_details_of_group
        )

        create_list_64 = rail.CreateCollectionOperator(
            task_id='create_list_64',
            source=lambda: rail.result('get_location_details_58'),
            name="locationdata",
        )

        create_csv_lines_location_data_65 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_location_data_65',
            source="{{ result('query_list_get_country_related_data_5') }}",
            header=['location',
                    'locationdescription'],
            row=lambda item: [
                rail.smartjoin_by_delim(
                    (item['Business_Organization'] + "|" + (item['Location'])).split("|"), '/'),
                item['Location_Type']
            ],
        )

        create_collection_create_list_from_csv_66 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_66',
            source="{{ result('create_csv_lines_location_data_65') }}",
            name="locationrawdata",
            columns={
                'location': 'location',
                'locationdescription': 'locationdescription'
            }
        )

        query_list_get_distinct_locationfrom_input_67 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_locationfrom_input_67',
            query="""SELECT DISTINCT  locationrawdata.location FROM  locationrawdata""",
        )

        query_list_getalllocationsnotin_replicon_68 = rail.QueryCollectionOperator(
            task_id='query_list_getalllocationsnotin_replicon_68',
            query="""SELECT DISTINCT locationrawdata.location,  locationrawdata.locationdescription FROM
                locationrawdata WHERE LOWER( locationrawdata.location) NOT IN (SELECT DISTINCT LOWER( locationdata.fullpath) FROM  locationdata)""",
        )

        if_query_list_getalllocationsnotin_replicon_68_rows_greater_than_0_69 = rail.IfOperator(
            task_id='if_query_list_getalllocationsnotin_replicon_68_rows_greater_than_0_69',
            test='''{{ result('query_list_getalllocationsnotin_replicon_68','length') > 0 }}''',
            yes_task="michael_kors_gmbh_groups_table_add_entry_71",
            no_task="get_employee_type_group_details_81",
        )

        michael_kors_gmbh_groups_table_add_entry_71 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_71',
            log="{{ result('create_groups_lookuptable') }}",
            items=lambda: rail.result('get_location_details_58'),
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "name": "{{ item.groupname }}",
                "uri": "{{ item.groupuri }}",
                "fullpath": "{{ item.fullpath }}",
                "type": "location"
            }
        )

        trigger_child_location_add = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_location_add',
            retries=0,
            items="{{ result('query_list_getalllocationsnotin_replicon_68') }}",
            trigger_dag_id=f'michaelkorstna_spain_location_add_child_{config.instance}_{config.version}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "location": "{{ item.location }}",
                "groupslookuptable": "{{result('create_groups_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}",
                "locationdescription": "{{ item.locationdescription }}"
            }
        )

        wait_for_child_location_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_location_add',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_location_add") }}'
        )

        get_employee_type_group_details_81 = rail.RepliconServiceOperator(
            task_id='get_employee_type_group_details_81',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:employee-type-group",
                    "urn:replicon:employee-type-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_existing_details_of_group
        )

        create_list_86 = rail.CreateCollectionOperator(
            task_id='create_list_86',
            source=lambda: rail.result('get_employee_type_group_details_81'),
            name="employeetypegroupdata",
        )

        query_list_get_distinct_employee_type_groupfrom_input_87 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_employee_type_groupfrom_input_87',
            name="empoyeetypeinputdata",
            query="""SELECT DISTINCT  countrydata.Employee_Type as employeetype FROM  countrydata""",
        )

        query_list_getall_employee_typesnotin_replicon_89 = rail.QueryCollectionOperator(
            task_id='query_list_getall_employee_typesnotin_replicon_89',
            query="""SELECT DISTINCT empoyeetypeinputdata.employeetype FROM empoyeetypeinputdata WHERE LOWER( empoyeetypeinputdata.employeetype) NOT IN
                (SELECT DISTINCT LOWER( employeetypegroupdata.fullpath) FROM  employeetypegroupdata)""",
        )

        if_query_list_getall_employee_typesnotin_replicon_89_rows_greater_than_0_90 = rail.IfOperator(
            task_id='if_query_list_getall_employee_typesnotin_replicon_89_rows_greater_than_0_90',
            test='''{{ result('query_list_getall_employee_typesnotin_replicon_89','length') > 0 }}''',
            yes_task="trigger_child_employeetype_add",
            no_task="get_weekly_schedule_group_service_centers_details_100",
        )

        trigger_child_employeetype_add = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_employeetype_add',
            retries=0,
            items="{{ result('query_list_getall_employee_typesnotin_replicon_89') }}",
            trigger_dag_id=f'michaelkorstna_spain_user_import_employee_type_add_child_{config.instance}_{config.version}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "employeetype": "{{ item.employeetype }}",
                "groupslookuptable": "{{result('create_groups_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}",
            }
        )

        wait_for_child_employeetype_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_employeetype_add',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_employeetype_add") }}'
        )

        get_weekly_schedule_group_service_centers_details_100 = rail.RepliconServiceOperator(
            task_id='get_weekly_schedule_group_service_centers_details_100',
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
        )

        create_list_103 = rail.CreateCollectionOperator(
            task_id='create_list_103',
            source=lambda: rail.result('get_weekly_schedule_group_service_centers_details_100'),
            name="scheduletypegroupdata",
        )

        query_list_get_distinctservicecenter_groupfrom_input_104 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinctservicecenter_groupfrom_input_104',
            name="servicecenterinputdata",
            query="""SELECT DISTINCT  countrydata.Scheduled_Weekly_Hours as servicecenter FROM  countrydata""",
        )

        query_list_getall_weeklyschedulesnotin_replicon_106 = rail.QueryCollectionOperator(
            task_id='query_list_getall_weeklyschedulesnotin_replicon_106',
            query="""SELECT DISTINCT servicecenterinputdata.servicecenter FROM servicecenterinputdata WHERE LOWER( servicecenterinputdata.servicecenter) NOT IN
                (SELECT DISTINCT LOWER( scheduletypegroupdata.displayText) FROM  scheduletypegroupdata)""",
        )

        if_query_list_getall_weeklyschedulesnotin_replicon_106_rows_greater_than_0_107 = rail.IfOperator(
            task_id='if_query_list_getall_weeklyschedulesnotin_replicon_106_rows_greater_than_0_107',
            test='''{{ result('query_list_getall_weeklyschedulesnotin_replicon_106','length') > 0 }}''',
            yes_task="trigger_child_service_center_add",
            no_task="catch_error",
        )

        trigger_child_service_center_add = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_service_center_add',
            retries=0,
            items="{{ result('query_list_getall_weeklyschedulesnotin_replicon_106') }}",
            trigger_dag_id=f'michaelkorstna_spain_service_center_add_child_{config.instance}_{config.version}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "servicecenter": "{{ item.servicecenter }}",
                "groupslookuptable": "{{result('create_groups_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}",
            }
        )

        wait_for_child_service_center_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_service_center_add',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_service_center_add") }}'
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> get_report_3
        get_report_3 >> create_list_4 >> query_list_get_country_related_data_5 >> create_groups_lookuptable >> get_cost_center_details_9 >> create_list_15
        create_list_15 >> create_csv_lines_cost_center_data_16 >> create_collection_create_list_from_csv_17 >> query_list_get_distinct_cost_center_18
        query_list_get_distinct_cost_center_18 >> query_list_getallcostcentersnotin_replicon_19
        query_list_getallcostcentersnotin_replicon_19 >> if_query_list_getallcostcentersnotin_replicon_19_rows_greater_than_0_20
        if_query_list_getallcostcentersnotin_replicon_19_rows_greater_than_0_20 >> rail.Label(
            'Yes') >> michael_kors_gmbh_groups_table_add_entry_22 >> trigger_child_cost_center_add >> wait_for_child_cost_center_add
        wait_for_child_cost_center_add >> get_department_group_details_32
        if_query_list_getallcostcentersnotin_replicon_19_rows_greater_than_0_20 >> rail.Label(
            'No') >> get_department_group_details_32 >> create_list_38 >> create_csv_lines_department_data_39 >> create_collection_create_list_from_csv_40
        create_collection_create_list_from_csv_40 >> query_list_get_distinct_department_groupfrom_input_41 >> query_list_getall_departmentsnotin_replicon_42
        query_list_getall_departmentsnotin_replicon_42 >> if_query_list_getall_departmentsnotin_replicon_42_rows_greater_than_0_43
        if_query_list_getall_departmentsnotin_replicon_42_rows_greater_than_0_43 >> rail.Label(
            'Yes') >> michael_kors_gmbh_groups_table_add_entry_45 >> log_parent_group_michael_kors_uri_46 >> if_log_parent_group_michael_kors_uri_46_blank_47
        if_log_parent_group_michael_kors_uri_46_blank_47 >> rail.Label(
            'Yes') >> stop_48 >> catch_error
        if_log_parent_group_michael_kors_uri_46_blank_47 >> rail.Label(
            'No') >> trigger_child_department_add >> wait_for_child_department_add >> get_location_details_58
        if_query_list_getall_departmentsnotin_replicon_42_rows_greater_than_0_43 >> rail.Label(
            'No') >> get_location_details_58 >> create_list_64 >> create_csv_lines_location_data_65 >> create_collection_create_list_from_csv_66
        create_collection_create_list_from_csv_66 >> query_list_get_distinct_locationfrom_input_67 >> query_list_getalllocationsnotin_replicon_68
        query_list_getalllocationsnotin_replicon_68 >> if_query_list_getalllocationsnotin_replicon_68_rows_greater_than_0_69
        if_query_list_getalllocationsnotin_replicon_68_rows_greater_than_0_69 >> rail.Label(
            'Yes') >> michael_kors_gmbh_groups_table_add_entry_71 >> trigger_child_location_add >> wait_for_child_location_add
        wait_for_child_location_add >> get_employee_type_group_details_81
        if_query_list_getalllocationsnotin_replicon_68_rows_greater_than_0_69 >> rail.Label(
            'No') >> get_employee_type_group_details_81 >> create_list_86 >> query_list_get_distinct_employee_type_groupfrom_input_87
        query_list_get_distinct_employee_type_groupfrom_input_87 >> query_list_getall_employee_typesnotin_replicon_89
        query_list_getall_employee_typesnotin_replicon_89 >> if_query_list_getall_employee_typesnotin_replicon_89_rows_greater_than_0_90
        if_query_list_getall_employee_typesnotin_replicon_89_rows_greater_than_0_90 >> rail.Label(
            'Yes') >> trigger_child_employeetype_add >> wait_for_child_employeetype_add >> get_weekly_schedule_group_service_centers_details_100
        if_query_list_getall_employee_typesnotin_replicon_89_rows_greater_than_0_90 >> rail.Label(
            'No') >> get_weekly_schedule_group_service_centers_details_100 >> create_list_103 >> query_list_get_distinctservicecenter_groupfrom_input_104
        query_list_get_distinctservicecenter_groupfrom_input_104 >> query_list_getall_weeklyschedulesnotin_replicon_106
        query_list_getall_weeklyschedulesnotin_replicon_106 >> if_query_list_getall_weeklyschedulesnotin_replicon_106_rows_greater_than_0_107
        if_query_list_getall_weeklyschedulesnotin_replicon_106_rows_greater_than_0_107 >> rail.Label(
            'Yes') >> trigger_child_service_center_add >> wait_for_child_service_center_add >> catch_error
        if_query_list_getall_weeklyschedulesnotin_replicon_106_rows_greater_than_0_107 >> rail.Label(
            'No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
