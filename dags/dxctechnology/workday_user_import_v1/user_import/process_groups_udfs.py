from pendulum import datetime
import rail
from airflow.models import Variable


from dxctechnology.workday_user_import_v1.user_import.common_utils import request_payload
from datetime import timedelta

# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_groups_udfs_dag,
        description="dxctechnology workday user sync Master",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        def can_use_batch_task():
            return Variable.get(config.can_use_batch_task_variable, default_var= "true") == "true"

        can_use_batch_task_department = rail.IfOperator(
            task_id = "can_use_batch_task_department",
            test=can_use_batch_task,
            yes_task="batch_task_department",
            no_task="query_department_fields_from_input"
        )

        batch_task_department = rail.BatchTaskRunOperator(
            task_id = "batch_task_department",
            start_task="query_department_fields_from_input",
            end_task="create_missing_departments",
            execution_timeout=timedelta(days=14)
        )

        query_department_fields_from_input = rail.QueryCollectionOperator(
            task_id = "query_department_fields_from_input",
            query="""SELECT DISTINCT vr.orgcode, vr.orgname FROM valid_records vr WHERE NULLIF(vr.orgcode, '') IS NOT NULL""",
            name = "input_department_data"
        )

        get_departments_from_replicon = rail.RepliconServiceOperator(
            task_id = "get_departments_from_replicon",
            endpoint="services/DepartmentGroupService1.svc/GetAllDepartmentGroups"
        )

        departments_from_replicon_collection = rail.CreateCollectionOperator(
            task_id = "departments_from_replicon_collection",
            source="{{result('get_departments_from_replicon') | to_json}}",
            name="replicon_department_data"
        )

        query_departments_to_create = rail.QueryCollectionOperator(
            task_id = "query_departments_to_create",
            name="department_to_create",
            query="""SELECT * FROM input_department_data idd
            WHERE LOWER(idd.orgcode) NOT IN (SELECT DISTINCT LOWER(rdd.displayText) FROM replicon_department_data rdd)"""
        )

        create_missing_departments = rail.RepliconServiceCallForEachItemOperator(
            task_id= "create_missing_departments",
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            items="{{result('query_departments_to_create')}}",
            data= request_payload.get_department_creation_payload
        )


        can_use_batch_task_costcenter = rail.IfOperator(
            task_id = "can_use_batch_task_costcenter",
            test=can_use_batch_task,
            yes_task="batch_task_costcenter",
            no_task="query_costcenter_fields_from_input"
        )

        batch_task_costcenter = rail.BatchTaskRunOperator(
            task_id = "batch_task_costcenter",
            start_task="query_costcenter_fields_from_input",
            end_task="create_missing_costcenter",
            execution_timeout=timedelta(days=14)
        )

        query_costcenter_fields_from_input = rail.QueryCollectionOperator(
            task_id = "query_costcenter_fields_from_input",
            name = "input_costcenter_data",
            query = """SELECT DISTINCT vr.costcenter, vr.costcentername FROM valid_records vr WHERE NULLIF(vr.costcenter, '') IS NOT NULL"""
        )

        get_costcenters_from_replicon = rail.RepliconServiceOperator(
            task_id="get_costcenters_from_replicon",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters"
        )

        cost_center_from_replicon_collection = rail.CreateCollectionOperator(
            task_id = "cost_center_from_replicon_collection",
            name = "replicon_costcenter_data",
            source="{{result('get_costcenters_from_replicon') | to_json}}"
        )

        query_costcenter_to_create = rail.QueryCollectionOperator(
            task_id = "query_costcenter_to_create",
            query = """SELECT DISTINCT icd.costcenter, icd.costcentername FROM input_costcenter_data icd
                       WHERE LOWER(icd.costcenter) NOT IN (SELECT DISTINCT LOWER(rcd. displayText) FROM replicon_costcenter_data rcd)""",
            name="costcenter_to_create"
        )

        create_missing_costcenter = rail.RepliconServiceCallForEachItemOperator(
            task_id= "create_missing_costcenter",
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            items="{{result('query_costcenter_to_create')}}",
            data= request_payload.get_costcenter_creation_payload
        )

        def get_value(data, index, pluck_key):
            return data[index].get(pluck_key)

        def get_locations_from_replicon_data_handler(response):
            return list(map(lambda location: {
                    "name": get_value(location['cells'] , 0, 'textValue'),
                    "uri": get_value(location['cells'] , 0, 'uri'),
                    "fullpath": rail.smartjoin_by_delim([location['textValue'] for location in get_value(location['cells'] , 1, 'cellCollection')],
                                        request_payload.LOCATION_DELIMITER),
                    "length": len(get_value(location['cells'] , 1, 'cellCollection')),
                    "status": get_value(location['cells'] , 2, 'textValue'),
                }, response['rows']))

        get_locations_from_replicon = rail.RepliconServiceOperator(
            task_id="get_locations_from_replicon",
            endpoint="/services/LocationListService1.svc/GetData",
            data= {
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path",
                    "urn:replicon:location-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=get_locations_from_replicon_data_handler
        )

        location_from_replicon_collection = rail.CreateCollectionOperator(
            task_id = "location_from_replicon_collection",
            source="{{result('get_locations_from_replicon') | to_json}}",
            name="replicon_location_data"
        )

        can_use_batch_task_locations = rail.IfOperator(
            task_id = "can_use_batch_task_locations",
            test=can_use_batch_task,
            yes_task="batch_task_locations",
            no_task="query_location_fields_from_input"
        )

        batch_task_locations = rail.BatchTaskRunOperator(
            task_id = "batch_task_locations",
            start_task="query_location_fields_from_input",
            end_task="wait_process_create_locations",
            execution_timeout=timedelta(days=14)
        )

        query_location_fields_from_input = rail.QueryCollectionOperator(
            task_id = "query_location_fields_from_input",
            query=f"""SELECT DISTINCT vr.country, vr.state, vr.country || '{request_payload.LOCATION_DELIMITER}' || vr.state as full_path
                        FROM valid_records vr WHERE NULLIF(vr.country, '') IS NOT NULL""",
            name = "input_location_data_raw"
        )

        def _format_input_location_data(item):
            if not item:
                return []
            if not item['state']:
                return [
                    {
                        "country": item['country'],
                        "state": "",
                        "full_path": item['country'],
                        "length": 1
                    }
                ]
            return [
                {
                    "country": item['country'],
                    "state": "",
                    "full_path": item['country'],
                    "length": 1
                },
                {
                    "country": item['country'],
                    "state": item['state'],
                    "full_path": item['full_path'],
                    "length": 2
                }
            ]

        format_input_location_data = rail.DataAdaptorOperator(
            task_id = "format_input_location_data",
            source="{{result('query_location_fields_from_input')}}",
            columns=['country', 'state', 'full_path', 'length'],
            data=_format_input_location_data
        )

        create_formatted_input_location_data = rail.CreateCollectionOperator(
            task_id = "create_formatted_input_location_data",
            source="{{result('format_input_location_data')}}",
            name = "input_location_data"
        )

        query_location_to_create = rail.QueryCollectionOperator(
            task_id = "query_location_to_create",
            query="""SELECT DISTINCT * FROM input_location_data ild
                    WHERE LOWER(ild.full_path) NOT IN (SELECT DISTINCT LOWER(rld.fullpath) FROM replicon_location_data rld) ORDER BY ild.length""",
            name = "location_to_create"
        )

        query_disabled_location = rail.QueryCollectionOperator(
            task_id = "query_disabled_location",
            query="""SELECT rld.* FROM replicon_location_data rld
            WHERE (LOWER(rld.fullpath) IN (SELECT DISTINCT LOWER(ild.full_path) FROM input_location_data ild) AND rld.status='False')""",
            name = "disabled_location"
        )

        enable_locations = rail.RepliconServiceCallForEachItemOperator(
            task_id = "enable_locations",
            items=lambda: rail.result("query_disabled_location"),
            endpoint="/services/locationService1.svc/Enable",
            data={
                "locationUri": "{{item.uri}}"
            }
        )

        process_create_locations = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_create_locations",
            trigger_dag_id=config.workday_user_import_process_location_creation_dag,
            items="{{result('query_location_to_create')}}",
            conf=lambda dag_run, item: {
                **dag_run.conf,
                **{
                    "country": item["country"],
                    "state": item["state"],
                    "full_path": item['full_path'],
                    "parent_location_name": (item['full_path'].split(request_payload.LOCATION_DELIMITER)[0]),
                    "length": item["length"]
                }
            }
        )

        wait_process_create_locations = rail.WaitForDagRunsSensor(
            task_id = "wait_process_create_locations",
            dag_runs="{{result('process_create_locations')}}"
        )


        can_use_batch_task_service_centers = rail.IfOperator(
            task_id = "can_use_batch_task_service_centers",
            test=can_use_batch_task,
            yes_task="batch_task_service_centers",
            no_task="query_service_centers_field_from_input"
        )

        batch_task_service_centers = rail.BatchTaskRunOperator(
            task_id = "batch_task_service_centers",
            start_task="query_service_centers_field_from_input",
            end_task="create_service_centers",
            execution_timeout=timedelta(days=14)
        )

        query_service_centers_field_from_input = rail.QueryCollectionOperator(
            task_id = "query_service_centers_field_from_input",
            query="""SELECT DISTINCT paygroup FROM valid_records rd WHERE NULLIF(rd.paygroup, '') IS NOT NULL""",
            name="input_service_centers_data"
        )

        def get_service_centers_from_replicon_data_handler(response):
            return list(map(lambda service_center:{
                "name": get_value(service_center['cells'] , 0, 'textValue'),
                "uri": get_value(service_center['cells'] , 0, 'uri'),
                "status": get_value(service_center['cells'] , 1, 'textValue')
                }, response['rows']))

        get_service_centers_from_replicon = rail.RepliconServiceOperator(
            task_id = "get_service_centers_from_replicon",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center",
                    "urn:replicon:service-center-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=get_service_centers_from_replicon_data_handler
        )

        service_center_from_replicon_collection = rail.CreateCollectionOperator(
            task_id = "service_center_from_replicon_collection",
            source="{{result('get_service_centers_from_replicon') | to_json}}",
            name = "replicon_service_center_data"
        )

        service_centers_to_create = rail.QueryCollectionOperator(
            task_id = "service_centers_to_create",
            query="""SELECT * FROM input_service_centers_data iscd
                        WHERE LOWER(iscd.paygroup) NOT IN (SELECT DISTINCT LOWER(rscd.name) FROM replicon_service_center_data rscd)""",
            name = "service_center_to_create"
        )

        service_centers_to_enable = rail.QueryCollectionOperator(
            task_id = "service_centers_to_enable",
            query="""SELECT * FROM replicon_service_center_data rscd
                        WHERE LOWER(rscd.name) IN (SELECT DISTINCT LOWER(iscd.paygroup) FROM input_service_centers_data iscd)
                            AND rscd.status='False'""",
            name = "service_center_to_enable"
        )

        enable_service_centers = rail.RepliconServiceCallForEachItemOperator(
            task_id = "enable_service_centers",
            items=lambda: rail.result("service_centers_to_enable"),
            endpoint="/services/serviceCenterService1.svc/Enable",
            data={
                "serviceCenterUri": "{{item.uri}}"
            }
        )

        create_service_centers = rail.RepliconServiceCallForEachItemOperator(
            task_id = "create_service_centers",
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            items="{{result('service_centers_to_create')}}",
            data=request_payload.get_create_service_center_payload
        )


        can_use_batch_task_employee = rail.IfOperator(
            task_id = "can_use_batch_task_employee",
            test=can_use_batch_task,
            yes_task="batch_task_employee",
            no_task="query_employee_data"
        )

        batch_task_employee = rail.BatchTaskRunOperator(
            task_id = "batch_task_employee",
            start_task="query_employee_data",
            end_task="finish",
            execution_timeout=timedelta(days=14)
        )

        query_employee_data = rail.QueryCollectionOperator(
            task_id = "query_employee_data",
            query="SELECT DISTINCT rd.empgroupcode, rd.empgroupname, rd.empsubgroupcode, rd.empsubgroupname FROM valid_records rd",
            name = "input_employee_data"
        )

        has_any_employee_data = rail.IfOperator(
            task_id = "has_any_employee_data",
            test="{{result('query_employee_data', 'length') > 0}}",
            yes_task="get_all_dropdown_values_employee_grp"
        )

        get_all_dropdown_values_employee_grp = rail.RepliconServiceOperator(
            task_id = "get_all_dropdown_values_employee_grp",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{dag_run.conf.employee_group.uri}}"
            }
        )

        get_all_dropdown_values_employee_sub_grp = rail.RepliconServiceOperator(
            task_id = "get_all_dropdown_values_employee_sub_grp",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{dag_run.conf.employee_sub_group.uri}}"
            }
        )

        create_employee_grp_collection = rail.CreateCollectionOperator(
            task_id = "create_employee_grp_collection",
            source="{{result('get_all_dropdown_values_employee_grp') | to_json}}",
            name="replicon_employee_grp_data"
        )

        create_employee_sub_grp_collection = rail.CreateCollectionOperator(
            task_id = "create_employee_sub_grp_collection",
            source="{{result('get_all_dropdown_values_employee_sub_grp') | to_json}}",
            name="replicon_employee_sub_grp_data"
        )

        query_employee_grp_to_create = rail.QueryCollectionOperator(
            task_id = "query_employee_grp_to_create",
            query="""SELECT DISTINCT ied.empgroupcode || '|' || ied.empgroupname as emp_group FROM input_employee_data ied
                        WHERE LOWER(ied.empgroupcode || '|' || ied.empgroupname) NOT IN
                        (SELECT DISTINCT LOWER(regd.displayText) FROM replicon_employee_grp_data regd)""",
            name="employee_grp_to_create"
        )

        query_employee_sub_grp_to_create = rail.QueryCollectionOperator(
            task_id = "query_employee_sub_grp_to_create",
            query="""SELECT DISTINCT ied.empsubgroupcode || '|' || ied.empsubgroupname as emp_sub_group FROM input_employee_data ied
                        WHERE LOWER(ied.empsubgroupcode || '|' || ied.empsubgroupname) NOT IN
                        (SELECT DISTINCT LOWER(resgd.displayText) FROM replicon_employee_sub_grp_data resgd)""",
            name="employee_sub_grp_to_create"
        )

        has_any_employee_grp_to_create = rail.IfOperator(
            task_id = "has_any_employee_grp_to_create",
            test="{{result('query_employee_grp_to_create', 'length') > 0}}",
            yes_task="add_new_employee_groups",
            no_task="has_any_employee_sub_grp_to_create"
        )

        def get_add_new_employee_groups_payload(group_down_uri, replicon_data_task_id, data_to_create_task_id, key="emp_group"):
            replicon_data = rail.result(replicon_data_task_id)
            data_to_create = rail.load_all_records(rail.result(data_to_create_task_id))
            drop_down_data = list(map(lambda drop_down: {
                    "target": {
                        "uri": drop_down['uri'],
                        "name": ""
                    },
                    "name": drop_down['displayText'],
                    "isEnabled": drop_down['isEnabled']
                },replicon_data))
            for drop_down in data_to_create:
                drop_down_data.append(
                    {
                        "target": {
                            "uri": None,
                            "name": None
                        },
                        "name": drop_down[key],
                        "isEnabled": True
                    }
                )
            return {
                "customFieldUri": group_down_uri,
                "customFieldDropDownOptionUris": drop_down_data
            }


        add_new_employee_groups = rail.RepliconServiceOperator(
            task_id = "add_new_employee_groups",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: get_add_new_employee_groups_payload(
                dag_run.conf['employee_group']['uri'],
                get_all_dropdown_values_employee_grp.task_id,
                query_employee_grp_to_create.task_id
            )
        )

        has_any_employee_sub_grp_to_create = rail.IfOperator(
            task_id = "has_any_employee_sub_grp_to_create",
            test="{{result('query_employee_sub_grp_to_create', 'length') > 0}}",
            yes_task="add_new_employee_sub_groups",
            no_task="finish"
        )

        add_new_employee_sub_groups = rail.RepliconServiceOperator(
            task_id = "add_new_employee_sub_groups",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: get_add_new_employee_groups_payload(
                dag_run.conf['employee_sub_group']['uri'],
                get_all_dropdown_values_employee_sub_grp.task_id,
                query_employee_sub_grp_to_create.task_id,
                "emp_sub_group"
            )
        )

        finish = rail.EmptyOperator(
            task_id = "finish"
        )


        can_use_batch_task_office_schedule = rail.IfOperator(
            task_id = "can_use_batch_task_office_schedule",
            test=can_use_batch_task,
            yes_task="batch_task_office_schedule",
            no_task="get_all_office_schedules"
        )

        batch_task_office_schedule = rail.BatchTaskRunOperator(
            task_id = "batch_task_office_schedule",
            start_task="get_all_office_schedules",
            end_task="wait_process_schedule_creation",
            execution_timeout=timedelta(days=14)
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        create_office_schedule_collection = rail.CreateCollectionOperator(
            task_id = "create_office_schedule_collection",
            source="{{result('get_all_office_schedules') | to_json }}",
            name="replicon_schedule_data"
        )

        query_schedules_to_create = rail.QueryCollectionOperator(
            task_id = "query_schedules_to_create",
            name="schedules_to_create",
            query="""SELECT DISTINCT vr.workshift FROM valid_records vr
                     WHERE (LOWER(vr.workshift) NOT IN
                     (SELECT DISTINCT LOWER(rsd.displayText) FROM replicon_schedule_data rsd)) AND (vr.workshift LIKE "WS-%")"""
        )

        process_schedule_creation = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_schedule_creation",
            trigger_dag_id=config.workday_user_import_process_schedule_creation_dag,
            items="{{result('query_schedules_to_create')}}",
            conf=lambda dag_run, item :{
                    **dag_run.conf,
                    **{
                        "work_schedule": item["workshift"]
                    }
                }

        )

        wait_process_schedule_creation = rail.WaitForDagRunsSensor(
            task_id = "wait_process_schedule_creation",
            dag_runs="{{result('process_schedule_creation')}}"
        )


        can_use_batch_task_department >> rail.Label("No") >> query_department_fields_from_input
        can_use_batch_task_department >> rail.Label("Yes") >> batch_task_department >> create_missing_departments
        query_department_fields_from_input >> get_departments_from_replicon >> departments_from_replicon_collection \
            >> query_departments_to_create >> create_missing_departments


        can_use_batch_task_costcenter >> rail.Label("No") >> query_costcenter_fields_from_input
        can_use_batch_task_costcenter >> rail.Label("Yes") >> batch_task_costcenter >> create_missing_costcenter
        query_costcenter_fields_from_input >> get_costcenters_from_replicon >> cost_center_from_replicon_collection \
            >> query_costcenter_to_create >> create_missing_costcenter


        can_use_batch_task_locations >> rail.Label("No") >> query_location_fields_from_input
        can_use_batch_task_locations >> rail.Label("Yes") >> batch_task_locations >> wait_process_create_locations
        query_location_fields_from_input >> format_input_location_data >> create_formatted_input_location_data >> get_locations_from_replicon \
            >> location_from_replicon_collection >> query_location_to_create >> query_disabled_location >> enable_locations \
                >> process_create_locations >> wait_process_create_locations


        can_use_batch_task_service_centers >> rail.Label("No") >> query_service_centers_field_from_input
        can_use_batch_task_service_centers >> rail.Label("Yes") >> batch_task_service_centers >> create_service_centers
        query_service_centers_field_from_input >> get_service_centers_from_replicon >> service_center_from_replicon_collection \
            >> service_centers_to_create >> service_centers_to_enable >> enable_service_centers >> create_service_centers


        can_use_batch_task_employee >> rail.Label("No") >> query_employee_data
        can_use_batch_task_employee >> rail.Label("Yes") >> batch_task_employee >> finish
        query_employee_data >> has_any_employee_data >> rail.Label("Yes") >> get_all_dropdown_values_employee_grp >> get_all_dropdown_values_employee_sub_grp\
            >> create_employee_grp_collection >> create_employee_sub_grp_collection >> query_employee_grp_to_create >> query_employee_sub_grp_to_create\
                >> has_any_employee_grp_to_create >> rail.Label("Yes") >> add_new_employee_groups >> has_any_employee_sub_grp_to_create\
                    >> rail.Label("Yes") >> add_new_employee_sub_groups >> finish
        has_any_employee_grp_to_create >> rail.Label("No") >> has_any_employee_sub_grp_to_create >> rail.Label("No") >> finish


        can_use_batch_task_office_schedule >> rail.Label("No") >> get_all_office_schedules
        can_use_batch_task_office_schedule >> rail.Label("Yes") >> batch_task_office_schedule >> wait_process_schedule_creation
        get_all_office_schedules >> create_office_schedule_collection >> query_schedules_to_create \
            >> process_schedule_creation >> wait_process_schedule_creation


    return dag

rail.for_each_instance(create_dag)
