from datetime import timedelta
from airflow.models import Variable
import uuid
import rail
from darkmattertechnologiesllc.user_sync_v1.utils import request_payload, python_callable
null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_group_child_dagid,
        description=config.process_group_child_dagid,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_groups_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_updated_locations'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_updated_locations',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_updated_locations = rail.RepliconServiceOperator(
            task_id='get_updated_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data=request_payload.get_location_payload,
            data_handler=python_callable.get_all_group_data_from_replicon_filter
        )

        create_replicon_location_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_location_collection",
            source="{{ result('get_updated_locations') | to_json }}",
            name="replicon_location"
        )

        query_location_field_from_feed = rail.QueryCollectionOperator(
            task_id="query_location_field_from_feed",
            query="SELECT locationhierarchy, locationname, workstate, workcity FROM validrecords",
            name="location_feed_values"
        )

        convert_location_data = rail.DataAdaptorOperator(
            task_id="convert_location_data",
            source=lambda: rail.load_all_records(
                rail.result('query_location_field_from_feed')),
            columns=['location_fullpath', 'length',
                     'location_name', 'parent_full_path', 'parent_name'],
            data=python_callable.get_converted_locations_data
        )

        converted_location_data_collection = rail.CreateCollectionOperator(
            task_id="converted_location_data_collection",
            source="{{result('convert_location_data')}}",
            name="converted_feed_locations"
        )

        query_locations_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id="query_locations_not_present_in_replicon",
            query="""SELECT DISTINCT * FROM converted_feed_locations WHERE LOWER(location_fullpath) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_location ) ORDER BY length""",
            name="locations_to_add"
        )

        has_any_location_to_add = rail.IfOperator(
            task_id="has_any_location_to_add",
            test="{{result('query_locations_not_present_in_replicon','length')>0}}",
            yes_task="add_location_by_level",
            no_task="get_updated_departments"
        )

        add_location_by_level = rail.TriggerDagRunForEachItemOperator(
            task_id="add_location_by_level",
            items="{{result('query_locations_not_present_in_replicon')}}",
            trigger_dag_id=config.add_location_child_dagid,
            conf=lambda item: {
                "name": item['location_name'],
                "full_path": item['location_fullpath'],
                "parent_location_full_path": item['parent_full_path'],
                "parent_name": item['parent_name'],
                "length": item['length']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_add_locations_by_level = rail.WaitForDagRunsSensor(
            task_id="wait_for_add_locations_by_level",
            dag_runs="{{result('add_location_by_level')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_updated_departments = rail.RepliconServiceOperator(
            task_id="get_updated_departments",
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data = {
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:effectively-enabled"
                ]
            },
            data_handler=lambda response: python_callable.get_dept_data(response)
        )

        create_replicon_department_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_department_collection",
            source="{{ result('get_updated_departments') | to_json }}",
            name="replicon_department"
        )

        query_department_field_from_feed = rail.QueryCollectionOperator(
            task_id="query_department_field_from_feed",
            query="SELECT departmentname FROM validrecords",
            name="department_feed_values"
        )

        query_department_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id="query_department_not_present_in_replicon",
            query="""SELECT DISTINCT * FROM department_feed_values WHERE TRIM(LOWER(departmentname)) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_department ) """,
            name="department_to_add"
        )

        has_any_department_to_add = rail.IfOperator(
            task_id="has_any_department_to_add",
            test="{{result('query_department_not_present_in_replicon','length')>0}}",
            yes_task="add_department",
            no_task="get_department_uri_to_enable"
        )

        add_department = rail.RepliconServiceCallForEachItemOperator(
            task_id="add_department",
            items="{{result('query_department_not_present_in_replicon')}}",
            endpoint='/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=lambda item: {
                "departmentGroup": {
                    "parent": {
                        "name": 'DarkMatterTechnologiesLLC',
                    },
                },
                "modifications": {
                    "name": item['departmentname'],
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()) + item['departmentname'],
            }
        )

        get_department_uri_to_enable = rail.QueryCollectionOperator(
            task_id="get_department_uri_to_enable",
            query="""SELECT uri FROM replicon_department WHERE LOWER(enabled) == 'false' AND LOWER(name) IN
                    (SELECT DISTINCT TRIM(LOWER(departmentname)) FROM department_feed_values)""",
            name="department_to_enable"
        )

        has_department_to_enable = rail.IfOperator(
            task_id="has_department_to_enable",
            test="{{result('get_department_uri_to_enable','length')>0}}",
            yes_task="enable_department",
            no_task="finish"
        )

        enable_department = rail.RepliconServiceCallForEachItemOperator(
            task_id='enable_department',
            items="{{ result('get_department_uri_to_enable') }}",
            endpoint="/services/DepartmentGroupService1.svc/Enable",
            data = lambda item: {
                "departmentGroupUri": item['uri']
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_updated_locations

        get_updated_locations >> create_replicon_location_collection >> query_location_field_from_feed >> convert_location_data >> \
            converted_location_data_collection >> query_locations_not_present_in_replicon >> has_any_location_to_add

        has_any_location_to_add >> rail.Label('Yes') >> add_location_by_level >> wait_for_add_locations_by_level >> get_updated_departments
        has_any_location_to_add >> rail.Label('No') >> get_updated_departments

        get_updated_departments >> create_replicon_department_collection >> query_department_field_from_feed >> \
            query_department_not_present_in_replicon >> has_any_department_to_add

        has_any_department_to_add >> rail.Label('Yes') >> add_department >> get_department_uri_to_enable
        has_any_department_to_add >> rail.Label('No') >> get_department_uri_to_enable

        get_department_uri_to_enable >> has_department_to_enable

        has_department_to_enable >> rail.Label('Yes') >> enable_department >> finish
        has_department_to_enable >> rail.Label('No') >> finish

    return dag

rail.for_each_instance(create_dag)
