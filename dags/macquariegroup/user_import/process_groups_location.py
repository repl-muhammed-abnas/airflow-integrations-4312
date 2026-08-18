from datetime import timedelta
import rail
from macquariegroup.user_import.tasks.gather_details import get_gather_details_task
from macquariegroup.user_import.utils.data_handlers import get_all_drop_down_options_filter
from macquariegroup.user_import.utils.request_payload import get_add_cost_center_payload, get_create_add_location_payload

DEPARTMENT_DELIMITER = "^"


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'macquarie_user_import_process_groups_and_location_{config.instance}',
        description=f'Macquarie User Import process_groups and location(UDF) {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")
        start = rail.EmptyOperator(
            task_id="start"
        )

        gather_details = get_gather_details_task()

        get_cost_center_details_from_feed = rail.QueryCollectionOperator(
            task_id="get_cost_center_details_from_feed",
            query="""SELECT DISTINCT cost_center FROM final_data""",
            name="feed_cost_centers"
        )

        create_replicon_cost_center_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_cost_center_collection",
            source="{{result('get_all_cost_centers') | to_json }}",
            name="replicon_cost_centers"
        )

        query_cost_centers_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id="query_cost_centers_not_present_in_replicon",
            query="""SELECT * FROM feed_cost_centers WHERE LOWER(cost_center) NOT IN (SELECT LOWER(name) FROM replicon_cost_centers)""",
            name="cost_centers_to_add"
        )

        has_any_cost_centers_to_add = rail.IfOperator(
            task_id="has_any_cost_centers_to_add",
            test="{{result('query_cost_centers_not_present_in_replicon', 'length') > 0}}",
            yes_task="process_cost_centers",
            no_task="finish"
        )

        process_cost_centers = rail.RepliconServiceCallForEachItemOperator(
            task_id="process_cost_centers",
            items="{{result('query_cost_centers_not_present_in_replicon')}}",
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data=get_add_cost_center_payload
        )

        create_cost_center_added_successfully_log = rail.WriteCSVFileOperator(
            task_id='create_cost_center_added_successfully_log',
            source="{{result('query_cost_centers_not_present_in_replicon')}}",
            header=['Action', 'Status', 'Name'],
            row=lambda item: [
                "ADD",
                "Success",
                item['cost_center']
            ]
        )

        get_departments_from_feed = rail.QueryCollectionOperator(
            task_id="get_departments_from_feed",
            # Not added department level 1 as it is root
            query="""SELECT department_lvl_2, department_lvl_3, department_lvl_4 FROM final_data""",
            name="feed_departments"
        )

        def get_converted_departments_data(item):
            if not item:
                return []
            return [
                {
                    "department_fullpath": item['department_lvl_2'],
                    "length": len(item['department_lvl_2'].split(DEPARTMENT_DELIMITER))
                },
                {
                    "department_fullpath": item['department_lvl_3'],
                    "length": len(item['department_lvl_3'].split(DEPARTMENT_DELIMITER))
                },
                {
                    "department_fullpath": item['department_lvl_4'],
                    "length": len(item['department_lvl_4'].split(DEPARTMENT_DELIMITER))
                }
            ]

        convert_department_data = rail.DataAdaptorOperator(
            task_id="convert_department_data",
            source="{{result('get_departments_from_feed')}}",
            columns=['department_fullpath', 'length'],
            data=get_converted_departments_data
        )

        converted_department_data_collection = rail.CreateCollectionOperator(
            task_id="converted_department_data_collection",
            source="{{result('convert_department_data')}}",
            name="converted_feed_departments"
        )

        create_replicon_departments_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_departments_collection",
            source="{{result('get_all_departments') | to_json}}",
            name="replicon_departments"
        )

        query_departments_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id="query_departments_not_present_in_replicon",
            query="""SELECT DISTINCT * FROM converted_feed_departments WHERE LOWER(department_fullpath) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_departments ) ORDER BY length""",
            name="departments_to_add"
        )

        has_any_departments_to_add = rail.IfOperator(
            task_id="has_any_departments_to_add",
            test="{{result('query_departments_not_present_in_replicon','length')>0}}",
            yes_task="add_departments_by_level",
            no_task="finish"
        )

        add_departments_by_level = rail.TriggerDagRunForEachItemOperator(
            task_id="add_departments_by_level",
            items="{{result('query_departments_not_present_in_replicon')}}",
            trigger_dag_id=f'macquarie_user_import_add_new_departments_{config.instance}',
            conf=lambda item: {
                "name": item['department_fullpath'].split(DEPARTMENT_DELIMITER)[-1],
                "full_path": item['department_fullpath'],
                "parent_department_full_path": DEPARTMENT_DELIMITER.join(item['department_fullpath'].split(DEPARTMENT_DELIMITER)[0:-1])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_add_departments_by_level = rail.WaitForDagRunsSensor(
            task_id="wait_for_add_departments_by_level",
            dag_runs="{{result('add_departments_by_level')}}", execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        create_department_created_successfully_log = rail.WriteCSVFileOperator(
            task_id='create_department_created_successfully_log',
            source="{{result('query_departments_not_present_in_replicon')}}",
            header=['Action', 'Status', 'Name'],
            row=lambda item: [
                "ADD",
                "Success",
                item['department_fullpath'].split(DEPARTMENT_DELIMITER)[-1]
            ]
        )

        get_employee_locations_from_feed = rail.QueryCollectionOperator(
            task_id="get_employee_locations_from_feed",
            query="""SELECT DISTINCT office as location FROM final_data""",
            name="feed_employee_locations"
        )

        get_all_drop_down_options_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_drop_down_options_from_replicon",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'Employee Location', 'uri')
            },
            data_handler=get_all_drop_down_options_filter
        )

        create_replicon_location_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_location_collection",
            source="{{result('get_all_drop_down_options_from_replicon') | to_json}}",
            name="replicon_locations"
        )

        query_locations_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id="query_locations_not_present_in_replicon",
            query="""SELECT * FROM feed_employee_locations WHERE LOWER(location) NOT IN (SELECT LOWER(name) FROM replicon_locations)""",
            name="locations_to_add"
        )

        has_any_location_to_add = rail.IfOperator(
            task_id="has_any_location_to_add",
            test="{{result('query_locations_not_present_in_replicon', 'length') > 0}}",
            yes_task="create_add_location_payload",
            no_task="finish"
        )

        create_add_location_payload = rail.PythonOperator(
            task_id="create_add_location_payload",
            python_callable=get_create_add_location_payload
        )

        put_employee_locations = rail.RepliconServiceOperator(
            task_id="put_employee_locations",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'Employee Location', 'uri'),
                "customFieldDropDownOptionUris": rail.result('create_add_location_payload')
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        start >> gather_details >> [get_cost_center_details_from_feed,
                                    get_departments_from_feed, get_employee_locations_from_feed]

        get_cost_center_details_from_feed >> create_replicon_cost_center_collection >> query_cost_centers_not_present_in_replicon \
            >> has_any_cost_centers_to_add >> rail.Label("Yes") >> process_cost_centers >> create_cost_center_added_successfully_log >> finish
        has_any_cost_centers_to_add >> rail.Label("No") >> finish

        get_departments_from_feed >> create_replicon_departments_collection >> convert_department_data >> converted_department_data_collection\
            >> query_departments_not_present_in_replicon >> has_any_departments_to_add >> rail.Label("No") >> finish
        has_any_departments_to_add >> rail.Label(
            "Yes") >> add_departments_by_level >> wait_for_add_departments_by_level >> create_department_created_successfully_log >> finish

        get_employee_locations_from_feed >> get_all_drop_down_options_from_replicon >> \
            create_replicon_location_collection >> query_locations_not_present_in_replicon\
            >> has_any_location_to_add >> rail.Label("No") >> finish
        has_any_location_to_add >> rail.Label(
            "Yes") >> create_add_location_payload >> put_employee_locations >> finish

        finish
    return dag


rail.for_each_instance(create_child_dag)
