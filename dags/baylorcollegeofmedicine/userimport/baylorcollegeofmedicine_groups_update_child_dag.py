
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'baylorcollegeofmedicine_groups_update_child_{config.instance}',
        description=f'BaylorCollegeOfMedicine_Child_groups update V1.0 {config.instance}',
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
            no_task='query_list_datafrom_raw_datafile_5'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_list_datafrom_raw_datafile_5',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_list_datafrom_raw_datafile_5 = rail.QueryCollectionOperator(
            task_id='query_list_datafrom_raw_datafile_5',
            name='rawgroupsdata',
            query="""SELECT * FROM  groupsdata""",
        )

        def get_employeetype_list(response):
            employeetypes = response['rows']
            return [{
                'employeetypename': employeetype['cells'][0].get('textValue'),
                'employeetypeuri': employeetype['cells'][0].get('uri'),
                'employeetypefullpath': rail.smartjoin_by_delim([cell['textValue'] for cell in employeetype['cells'][1]['cellCollection']], '|'),
                'employeetypelength': len([cell['textValue'] for cell in employeetype['cells'][1]['cellCollection']])
            } for employeetype in employeetypes]

        get_employeetype_groups_data_8 = rail.RepliconServiceOperator(
            task_id='get_employeetype_groups_data_8',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:employee-type-group",
                    "urn:replicon:employee-type-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_employeetype_list
        )

        create_list_replicondata_12 = rail.CreateCollectionOperator(
            task_id='create_list_replicondata_12',
            source=lambda: rail.result('get_employeetype_groups_data_8'),
            name="employeetypedata",
        )

        query_list_get_distinct_employeetypesintheinput_13 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_employeetypesintheinput_13',
            name='employeetypedataraw',
            query="""SELECT DISTINCT  rawgroupsdata.employeetype FROM  rawgroupsdata""",
        )

        query_list_getallemployeetypesnotin_replicon_15 = rail.QueryCollectionOperator(
            task_id='query_list_getallemployeetypesnotin_replicon_15',
            query="""SELECT DISTINCT  employeetypedataraw.employeetype FROM  employeetypedataraw WHERE LOWER( employeetypedataraw.employeetype) NOT IN
                (SELECT DISTINCT LOWER( employeetypedata.employeetypename) FROM  employeetypedata) AND  NULLIF(employeetype,'') IS NOT NULL AND
                employeetypedataraw.employeetype != "" """,
        )

        if_query_list_getallemployeetypesnotin_replicon_15_rows_greater_than_0_16 = rail.IfOperator(
            task_id='if_query_list_getallemployeetypesnotin_replicon_15_rows_greater_than_0_16',
            test='''{{ result('query_list_getallemployeetypesnotin_replicon_15','length') > 0 }}''',
            yes_task="trigger_child_create_employeetypegroups",
            no_task="get_location_details_26",
        )

        trigger_child_create_employeetypegroups = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_create_employeetypegroups',
            retries=0,
            items="{{ result('query_list_getallemployeetypesnotin_replicon_15') }}",
            trigger_dag_id=f'baylorcollegeofmedicine_employeetypegroups_add_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "employeetype": "{{ item.employeetype }}"
            }
        )

        wait_for_child_create_employeetypegroups = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_create_employeetypegroups',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_create_employeetypegroups") }}'
        )

        def get_location_list(response):
            locations = response['rows']
            return [{
                'locationname': location['cells'][0].get('textValue'),
                'locationuri': location['cells'][0].get('uri'),
                'fullpath': rail.smartjoin_by_delim([cell['textValue'] for cell in location['cells'][1]['cellCollection']], '|'),
                'length': len([cell['textValue'] for cell in location['cells'][1]['cellCollection']])
            } for location in locations]

        get_location_details_26 = rail.RepliconServiceOperator(
            task_id='get_location_details_26',
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
            data_handler=get_location_list
        )

        create_list_30 = rail.CreateCollectionOperator(
            task_id='create_list_30',
            source=lambda: rail.result('get_location_details_26'),
            name="locationdata",
        )

        query_list_get_distinct_locationfrom_input_31 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_locationfrom_input_31',
            name='locationrawdata',
            query="""SELECT DISTINCT  rawgroupsdata.timeapprover as location FROM  rawgroupsdata""",
        )

        query_list_getalllocationsnotin_replicon_33 = rail.QueryCollectionOperator(
            task_id='query_list_getalllocationsnotin_replicon_33',
            query="""SELECT DISTINCT  locationrawdata.location FROM  locationrawdata WHERE LOWER( locationrawdata.location) NOT IN
                (SELECT DISTINCT LOWER( locationdata.fullpath) FROM  locationdata) AND  NULLIF(location,'') IS NOT NULL AND  locationrawdata.location!= "" """,
        )

        if_query_list_getalllocationsnotin_replicon_33_rows_greater_than_0_34 = rail.IfOperator(
            task_id='if_query_list_getalllocationsnotin_replicon_33_rows_greater_than_0_34',
            test='''{{ result('query_list_getalllocationsnotin_replicon_33','length') > 0 }}''',
            yes_task="trigger_child_create_locations",
            no_task="get_department_group_details_44",
        )

        trigger_child_create_locations = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_create_locations',
            retries=0,
            items="{{ result('query_list_getalllocationsnotin_replicon_33') }}",
            trigger_dag_id=f'baylorcollegeofmedicine_location_add_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "location": "{{ item.location }}"
            }
        )

        wait_for_child_create_locations = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_create_locations',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_create_locations") }}'
        )

        def get_department_list(response):
            departments = response['rows']
            return [{
                'departmentname': department['cells'][0].get('textValue'),
                'departmenturi': department['cells'][0].get('uri'),
                'fullpath': rail.smartjoin_by_delim([cell['textValue'] for cell in department['cells'][1]['cellCollection']], '|'),
                'length': len([cell['textValue'] for cell in department['cells'][1]['cellCollection']])
            } for department in departments]

        get_department_group_details_44 = rail.RepliconServiceOperator(
            task_id='get_department_group_details_44',
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
            data_handler=get_department_list
        )

        create_list_48 = rail.CreateCollectionOperator(
            task_id='create_list_48',
            source=lambda: rail.result('get_department_group_details_44'),
            name="departmentgroupdata",
        )

        query_list_get_distinct_department_groupfrom_input_49 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_department_groupfrom_input_49',
            query="""SELECT DISTINCT  rawgroupsdata.departmentlevel2, rawgroupsdata.departmentlevel3 FROM  rawgroupsdata""",
        )

        create_csv_lines_50 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_50',
            source="{{ result('query_list_get_distinct_department_groupfrom_input_49') }}",
            header=['departmentfullpath',
                    'departmentlevel2',
                    'departmentlevel3'],
            row=[
                "Baylor|{{ item.departmentlevel2 }}|{{ item.departmentlevel3 }}",
                "{{ item.departmentlevel2 }}",
                "{{ item.departmentlevel3 }}"
            ],
        )

        create_collection_create_list_from_csv_51 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_51',
            source="{{ result('create_csv_lines_50') }}",
            name="departmentrawdata",
            columns={
                'departmentfullpath': 'departmentfullpath',
                'departmentlevel2': 'departmentlevel2',
                'departmentlevel3': 'departmentlevel3'
            }
        )

        query_list_getall_departmentsnotin_replicon_52 = rail.QueryCollectionOperator(
            task_id='query_list_getall_departmentsnotin_replicon_52',
            query="""SELECT DISTINCT  departmentrawdata.departmentfullpath, departmentrawdata.departmentlevel2, departmentrawdata.departmentlevel3 FROM
                departmentrawdata WHERE LOWER( departmentrawdata.departmentfullpath) NOT IN (SELECT DISTINCT LOWER( departmentgroupdata.fullpath) FROM
                departmentgroupdata)  AND  NULLIF(departmentfullpath,'') IS NOT NULL AND  departmentrawdata.departmentfullpath!= "" """,
        )

        if_query_list_getall_departmentsnotin_replicon_52_rows_greater_than_0_53 = rail.IfOperator(
            task_id='if_query_list_getall_departmentsnotin_replicon_52_rows_greater_than_0_53',
            test='''{{ result('query_list_getall_departmentsnotin_replicon_52','length') > 0 }}''',
            yes_task="baylorcollegeofmedicine_groups_table_add_batch_of_entries_54",
            no_task="catch_error",
        )

        baylorcollegeofmedicine_groups_table_add_batch_of_entries_54 = rail.WriteLogOperator(
            task_id='baylorcollegeofmedicine_groups_table_add_batch_of_entries_54',
            log="{{dag_run.conf.groupsupdatelookup}}",
            items=lambda: rail.result('get_department_group_details_44'),
            message='na',
            severity='na',
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "name": "{{item.departmentname}}",
                "uri": "{{item.departmenturi}}",
                "fullpath": "{{item.fullpath}}",
                "type": "department"
            }
        )

        trigger_child_create_departments = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_create_departments',
            retries=0,
            items="{{ result('query_list_getall_departmentsnotin_replicon_52') }}",
            trigger_dag_id=f'baylorcollegeofmedicine_department_add_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item,dag_run: {
                "departmentfullpath": item['departmentfullpath'],
                "companydepturi": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_department_group_details_44'), 'fullpath', 'Baylor', 'departmenturi', '') if rail.result('get_department_group_details_44') else '',
                "departmentlevel2": item['departmentlevel2'],
                "departmentlevel3": item['departmentlevel3'],
                "departmentlevel2uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_department_group_details_44'), 'fullpath', 'Baylor|'+item['departmentlevel2'], 'departmenturi', '') if rail.result(
                    'get_department_group_details_44') else '',
                "groupsupdatelookup": dag_run.conf['groupsupdatelookup'],
                "callerjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_child_create_departments = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_create_departments',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_create_departments") }}'
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: 'Error:' +
            rail.render_template("{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> query_list_datafrom_raw_datafile_5
        query_list_datafrom_raw_datafile_5 >> get_employeetype_groups_data_8 >> create_list_replicondata_12
        create_list_replicondata_12 >> query_list_get_distinct_employeetypesintheinput_13 >> query_list_getallemployeetypesnotin_replicon_15
        query_list_getallemployeetypesnotin_replicon_15 >> if_query_list_getallemployeetypesnotin_replicon_15_rows_greater_than_0_16
        if_query_list_getallemployeetypesnotin_replicon_15_rows_greater_than_0_16 >> rail.Label(
            'Yes') >> trigger_child_create_employeetypegroups
        trigger_child_create_employeetypegroups >> wait_for_child_create_employeetypegroups >> get_location_details_26
        if_query_list_getallemployeetypesnotin_replicon_15_rows_greater_than_0_16 >> rail.Label(
            'No') >> get_location_details_26 >> create_list_30 >> query_list_get_distinct_locationfrom_input_31 >> query_list_getalllocationsnotin_replicon_33
        query_list_getalllocationsnotin_replicon_33 >> if_query_list_getalllocationsnotin_replicon_33_rows_greater_than_0_34
        if_query_list_getalllocationsnotin_replicon_33_rows_greater_than_0_34 >> rail.Label(
            'Yes') >> trigger_child_create_locations >> wait_for_child_create_locations >> get_department_group_details_44
        if_query_list_getalllocationsnotin_replicon_33_rows_greater_than_0_34 >> rail.Label(
            'No') >> get_department_group_details_44 >> create_list_48 >> query_list_get_distinct_department_groupfrom_input_49 >> create_csv_lines_50
        create_csv_lines_50 >> create_collection_create_list_from_csv_51 >> query_list_getall_departmentsnotin_replicon_52
        query_list_getall_departmentsnotin_replicon_52 >> if_query_list_getall_departmentsnotin_replicon_52_rows_greater_than_0_53
        if_query_list_getall_departmentsnotin_replicon_52_rows_greater_than_0_53 >> rail.Label(
            'Yes') >> baylorcollegeofmedicine_groups_table_add_batch_of_entries_54 >> trigger_child_create_departments >> wait_for_child_create_departments
        wait_for_child_create_departments >> catch_error
        if_query_list_getall_departmentsnotin_replicon_52_rows_greater_than_0_53 >> rail.Label(
            'No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
