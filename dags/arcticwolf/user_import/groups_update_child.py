
from datetime import timedelta
from airflow.models import Variable
import rail
from arcticwolf.user_import.utils import response_filter

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.groups_update_child_dagid,
        description=f'Arcticwolf user import Child_groups update V1.0 {config.instance}',
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
            no_task='query_list_datafrom_raw_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_list_datafrom_raw_data',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_list_datafrom_raw_data = rail.QueryCollectionOperator(
            task_id='query_list_datafrom_raw_data',
            name='rawgroupsdata',
            query="""SELECT * FROM  groupsdata""",
        )

        get_employeetype_groups_data = rail.RepliconServiceOperator(
            task_id='get_employeetype_groups_data',
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
            data_handler=response_filter.get_employeetype_list
        )

        create_list_replicondata = rail.CreateCollectionOperator(
            task_id='create_list_replicondata',
            source=lambda: rail.result('get_employeetype_groups_data'),
            name="employeetypedata",
        )

        query_list_get_distinct_employeetypesintheinput = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_employeetypesintheinput',
            name='employeetypedataraw',
            query="""SELECT DISTINCT  rawgroupsdata.employeetype FROM  rawgroupsdata""",
        )

        query_list_getallemployeetypesnotin_replicon = rail.QueryCollectionOperator(
            task_id='query_list_getallemployeetypesnotin_replicon',
            query="""SELECT DISTINCT  employeetypedataraw.employeetype FROM  employeetypedataraw WHERE LOWER( employeetypedataraw.employeetype) NOT IN
                (SELECT DISTINCT LOWER( employeetypedata.employeetypename) FROM  employeetypedata) AND  NULLIF(employeetype,'') IS NOT NULL AND
                employeetypedataraw.employeetype != "" """,
        )

        if_query_list_getallemployeetypes_notin_replicon_has_records = rail.IfOperator(
            task_id='if_query_list_getallemployeetypes_notin_replicon_has_records',
            test='''{{ result('query_list_getallemployeetypesnotin_replicon','length') > 0 }}''',
            yes_task="trigger_child_create_employeetypegroups",
            no_task="get_all_division_groups_data",
        )

        trigger_child_create_employeetypegroups = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_create_employeetypegroups',
            retries=0,
            items="{{ result('query_list_getallemployeetypesnotin_replicon') }}",
            trigger_dag_id=config.employeetypegroups_add_child_dagid,
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

        get_all_division_groups_data = rail.RepliconServiceOperator(
            task_id='get_all_division_groups_data',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        create_collection_division_replicondata = rail.CreateCollectionOperator(
            task_id='create_collection_division_replicondata',
            source=lambda: rail.result('get_all_division_groups_data'),
            name="alldivisiondata",
        )

        query_list_get_distinct_divisioninput = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_divisioninput',
            name='divisiondataraw',
            query="""SELECT DISTINCT  rawgroupsdata.division FROM  rawgroupsdata""",
        )

        query_list_getall_divisions_notin_replicon = rail.QueryCollectionOperator(
            task_id='query_list_getall_divisions_notin_replicon',
            query="""SELECT DISTINCT  divisiondataraw.division FROM  divisiondataraw WHERE LOWER( divisiondataraw.division) NOT IN
                (SELECT DISTINCT LOWER( alldivisiondata.displayText) FROM  alldivisiondata) AND  NULLIF(division,'') IS NOT NULL AND
                divisiondataraw.division != "" """,
        )

        if_query_list_divisions_notin_replicon_has_records = rail.IfOperator(
            task_id='if_query_list_divisions_notin_replicon_has_records',
            test='''{{ result('query_list_getall_divisions_notin_replicon','length') > 0 }}''',
            yes_task="trigger_child_create_division_groups",
            no_task="get_all_cost_center_groups_data",
        )

        trigger_child_create_division_groups = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_create_division_groups',
            retries=0,
            items="{{ result('query_list_getall_divisions_notin_replicon') }}",
            trigger_dag_id=config.divisiongroups_add_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "division": "{{ item.division }}"
            }
        )

        wait_for_child_create_division_groups = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_create_division_groups',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_create_division_groups") }}'
        )

        get_all_cost_center_groups_data = rail.RepliconServiceOperator(
            task_id='get_all_cost_center_groups_data',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        create_collection_cost_center_replicondata = rail.CreateCollectionOperator(
            task_id='create_collection_cost_center_replicondata',
            source=lambda: rail.result('get_all_cost_center_groups_data'),
            name="allcostcenterdata",
        )

        query_list_get_distinct_costcenterinput = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_costcenterinput',
            name='costcenterdataraw',
            query="""SELECT DISTINCT  rawgroupsdata.cost_center, rawgroupsdata.cost_center_code FROM  rawgroupsdata""",
        )

        query_list_getall_cost_center_notin_replicon = rail.QueryCollectionOperator(
            task_id='query_list_getall_cost_center_notin_replicon',
            query="""SELECT DISTINCT  costcenterdataraw.cost_center, costcenterdataraw.cost_center_code FROM  costcenterdataraw WHERE LOWER( costcenterdataraw.cost_center) NOT IN
                (SELECT DISTINCT LOWER( allcostcenterdata.displayText) FROM  allcostcenterdata) AND  NULLIF(cost_center,'') IS NOT NULL AND
                costcenterdataraw.cost_center != "" """,
        )

        if_query_list_cost_center_notin_replicon_has_records = rail.IfOperator(
            task_id='if_query_list_cost_center_notin_replicon_has_records',
            test='''{{ result('query_list_getall_cost_center_notin_replicon','length') > 0 }}''',
            yes_task="trigger_child_create_cost_center_groups",
            no_task="get_all_position_title_groups_data",
        )

        trigger_child_create_cost_center_groups = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_create_cost_center_groups',
            retries=0,
            items="{{ result('query_list_getall_cost_center_notin_replicon') }}",
            trigger_dag_id=config.cost_center_groups_add_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "cost_center": "{{ item.cost_center }}",
                "cost_center_code": "{{ item.cost_center_code }}"
            }
        )

        wait_for_child_create_cost_center_groups = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_create_cost_center_groups',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_create_cost_center_groups") }}'
        )

        get_all_position_title_groups_data = rail.RepliconServiceOperator(
            task_id='get_all_position_title_groups_data',
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
        )

        create_collection_position_title_replicondata = rail.CreateCollectionOperator(
            task_id='create_collection_position_title_replicondata',
            source=lambda: rail.result('get_all_position_title_groups_data'),
            name="allpositiontitledata",
        )

        query_list_get_distinct_positiontitleinput = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_positiontitleinput',
            name='positiontitledataraw',
            query="""SELECT DISTINCT  rawgroupsdata.pos_title, rawgroupsdata.pos_title_code FROM  rawgroupsdata""",
        )

        query_list_getall_position_title_notin_replicon = rail.QueryCollectionOperator(
            task_id='query_list_getall_position_title_notin_replicon',
            query="""SELECT DISTINCT  positiontitledataraw.pos_title, positiontitledataraw.pos_title_code FROM  positiontitledataraw WHERE LOWER( positiontitledataraw.pos_title) NOT IN
                (SELECT DISTINCT LOWER( allpositiontitledata.displayText) FROM  allpositiontitledata) AND  NULLIF(pos_title,'') IS NOT NULL AND
                positiontitledataraw.pos_title != "" """,
        )

        if_query_list_post_title_notin_replicon_has_records = rail.IfOperator(
            task_id='if_query_list_post_title_notin_replicon_has_records',
            test='''{{ result('query_list_getall_position_title_notin_replicon','length') > 0 }}''',
            yes_task="trigger_child_create_position_title_groups",
            no_task="get_location_details",
        )

        trigger_child_create_position_title_groups = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_create_position_title_groups',
            retries=0,
            items="{{ result('query_list_getall_position_title_notin_replicon') }}",
            trigger_dag_id=config.position_title_groups_add_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "pos_title": "{{ item.pos_title }}",
                "pos_title_code": "{{ item.pos_title_code }}"
            }
        )

        wait_for_child_create_position_title_groups = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_create_position_title_groups',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_create_position_title_groups") }}'
        )

        def get_location_list(response):
            locations = response['rows']
            return [{
                'locationname': location['cells'][0].get('textValue'),
                'locationuri': location['cells'][0].get('uri'),
                'fullpath': rail.smartjoin_by_delim([cell['textValue'] for cell in location['cells'][1]['cellCollection']], '|'),
                'length': len([cell['textValue'] for cell in location['cells'][1]['cellCollection']])
            } for location in locations]

        get_location_details = rail.RepliconServiceOperator(
            task_id='get_location_details',
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

        create_list_locationdata = rail.CreateCollectionOperator(
            task_id='create_list_locationdata',
            source=lambda: rail.result('get_location_details'),
            name="locationdata",
        )

        query_list_get_distinct_locationfrom_input = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_locationfrom_input',
            name='locationrawdata',
            query="""SELECT DISTINCT  rawgroupsdata.location_level_1, rawgroupsdata.location_level_2 FROM  rawgroupsdata""",
        )

        query_list_getalllocationsnotin_replicon = rail.QueryCollectionOperator(
            task_id='query_list_getalllocationsnotin_replicon',
            query="""SELECT DISTINCT  locationrawdata.location_level_1, locationrawdata.location_level_2 FROM  locationrawdata WHERE LOWER( locationrawdata.location_level_2) NOT IN
                (SELECT DISTINCT LOWER( locationdata.locationname) FROM  locationdata WHERE locationdata.length=2) AND  NULLIF(location_level_2,'') IS NOT NULL AND  locationrawdata.location_level_2!= "" """,
        )

        if_query_list_getalllocationsnotin_replicon_has_data = rail.IfOperator(
            task_id='if_query_list_getalllocationsnotin_replicon_has_data',
            test='''{{ result('query_list_getalllocationsnotin_replicon','length') > 0 }}''',
            yes_task="trigger_child_create_locations",
            no_task="get_department_group_details",
        )

        trigger_child_create_locations = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_create_locations',
            retries=0,
            items="{{ result('query_list_getalllocationsnotin_replicon') }}",
            trigger_dag_id=config.location_add_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "location_level_1": "{{ item.location_level_1 }}",
                "location_level_2": "{{ item.location_level_2 }}",
                "all_existing_locations": "{{result('get_location_details')}}"
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

        get_department_group_details = rail.RepliconServiceOperator(
            task_id='get_department_group_details',
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

        create_list_department_group_details = rail.CreateCollectionOperator(
            task_id='create_list_department_group_details',
            source=lambda: rail.result('get_department_group_details'),
            name="departmentgroupdata",
        )

        query_list_get_distinct_department_groupfrom_input = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_department_groupfrom_input',
            query="""SELECT DISTINCT  rawgroupsdata.departmentlevel2, rawgroupsdata.departmentlevel3 FROM  rawgroupsdata""",
        )

        create_csv_distinct_department = rail.WriteCSVFileOperator(
            task_id='create_csv_distinct_department',
            source="{{ result('query_list_get_distinct_department_groupfrom_input') }}",
            header=['departmentfullpath',
                    'departmentlevel2',
                    'departmentlevel3'],
            row=[
                "Arctic Wolf|{{ item.departmentlevel2 }}|{{ item.departmentlevel3 }}",
                "{{ item.departmentlevel2 }}",
                "{{ item.departmentlevel3 }}"
            ],
        )

        create_collection_distict_department = rail.CreateCollectionOperator(
            task_id='create_collection_distict_department',
            source="{{ result('create_csv_distinct_department') }}",
            name="departmentrawdata",
            columns={
                'departmentfullpath': 'departmentfullpath',
                'departmentlevel2': 'departmentlevel2',
                'departmentlevel3': 'departmentlevel3'
            }
        )

        query_list_getall_departmentsnotin_replicon = rail.QueryCollectionOperator(
            task_id='query_list_getall_departmentsnotin_replicon',
            query="""SELECT DISTINCT  departmentrawdata.departmentfullpath, departmentrawdata.departmentlevel2, departmentrawdata.departmentlevel3 FROM
                departmentrawdata WHERE LOWER( departmentrawdata.departmentfullpath) NOT IN (SELECT DISTINCT LOWER( departmentgroupdata.fullpath) FROM
                departmentgroupdata)  AND  NULLIF(departmentfullpath,'') IS NOT NULL AND  departmentrawdata.departmentfullpath!= "" """,
        )

        if_query_list_getall_departmentsnotin_replicon_has_data = rail.IfOperator(
            task_id='if_query_list_getall_departmentsnotin_replicon_has_data',
            test='''{{ result('query_list_getall_departmentsnotin_replicon','length') > 0 }}''',
            yes_task="groups_table_add_batch_of_entries",
            no_task="catch_error",
        )

        groups_table_add_batch_of_entries = rail.WriteLogOperator(
            task_id='groups_table_add_batch_of_entries',
            log="{{dag_run.conf.groupsupdatelookup}}",
            items=lambda: rail.result('get_department_group_details'),
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
            items="{{ result('query_list_getall_departmentsnotin_replicon') }}",
            trigger_dag_id=config.department_add_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "departmentfullpath": item['departmentfullpath'],
                "companydepturi": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_department_group_details'), 'fullpath', 'Arctic Wolf', 'departmenturi', '') if rail.result('get_department_group_details') else '',
                "departmentlevel2": item['departmentlevel2'],
                "departmentlevel3": item['departmentlevel3'],
                "departmentlevel2uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_department_group_details'), 'fullpath', 'Arctic Wolf|'+item['departmentlevel2'], 'departmenturi', '') if rail.result(
                    'get_department_group_details') else '',
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
            'No') >> query_list_datafrom_raw_data
        query_list_datafrom_raw_data >> get_employeetype_groups_data >> create_list_replicondata
        create_list_replicondata >> query_list_get_distinct_employeetypesintheinput >> query_list_getallemployeetypesnotin_replicon
        query_list_getallemployeetypesnotin_replicon >> if_query_list_getallemployeetypes_notin_replicon_has_records
        if_query_list_getallemployeetypes_notin_replicon_has_records >> rail.Label(
            'Yes') >> trigger_child_create_employeetypegroups
        trigger_child_create_employeetypegroups >> wait_for_child_create_employeetypegroups >> get_all_division_groups_data >>\
            create_collection_division_replicondata >> query_list_get_distinct_divisioninput >> query_list_getall_divisions_notin_replicon >>\
            if_query_list_divisions_notin_replicon_has_records
        if_query_list_divisions_notin_replicon_has_records >> rail.Label('Yes') >> trigger_child_create_division_groups >> wait_for_child_create_division_groups >>\
            get_all_cost_center_groups_data
        if_query_list_divisions_notin_replicon_has_records >> rail.Label('No') >> get_all_cost_center_groups_data >> create_collection_cost_center_replicondata >> query_list_get_distinct_costcenterinput >>\
            query_list_getall_cost_center_notin_replicon >> if_query_list_cost_center_notin_replicon_has_records
        if_query_list_cost_center_notin_replicon_has_records >> rail.Label('Yes') >> trigger_child_create_cost_center_groups >> wait_for_child_create_cost_center_groups >> get_all_position_title_groups_data >>\
            create_collection_position_title_replicondata >> query_list_get_distinct_positiontitleinput >> query_list_getall_position_title_notin_replicon >>\
            if_query_list_post_title_notin_replicon_has_records
        if_query_list_post_title_notin_replicon_has_records >> rail.Label(
            'Yes') >> trigger_child_create_position_title_groups >> wait_for_child_create_position_title_groups >> get_location_details
        if_query_list_post_title_notin_replicon_has_records >> rail.Label(
            'No') >> get_location_details
        if_query_list_cost_center_notin_replicon_has_records >> rail.Label(
            'No') >> get_all_position_title_groups_data
        if_query_list_getallemployeetypes_notin_replicon_has_records >> rail.Label(
            'No') >> get_all_division_groups_data
        get_location_details >> create_list_locationdata >> query_list_get_distinct_locationfrom_input >> query_list_getalllocationsnotin_replicon
        query_list_getalllocationsnotin_replicon >> if_query_list_getalllocationsnotin_replicon_has_data
        if_query_list_getalllocationsnotin_replicon_has_data >> rail.Label(
            'Yes') >> trigger_child_create_locations >> wait_for_child_create_locations >> get_department_group_details
        if_query_list_getalllocationsnotin_replicon_has_data >> rail.Label(
            'No') >> get_department_group_details >> create_list_department_group_details >> query_list_get_distinct_department_groupfrom_input >> create_csv_distinct_department
        create_csv_distinct_department >> create_collection_distict_department >> query_list_getall_departmentsnotin_replicon
        query_list_getall_departmentsnotin_replicon >> if_query_list_getall_departmentsnotin_replicon_has_data
        if_query_list_getall_departmentsnotin_replicon_has_data >> rail.Label(
            'Yes') >> groups_table_add_batch_of_entries >> trigger_child_create_departments >> wait_for_child_create_departments
        wait_for_child_create_departments >> catch_error
        if_query_list_getall_departmentsnotin_replicon_has_data >> rail.Label(
            'No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
