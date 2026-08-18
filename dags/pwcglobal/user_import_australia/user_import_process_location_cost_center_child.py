from datetime import timedelta
import rail
from pwcglobal.user_import_australia import custom_methods


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_import_process_location_cost_center_child_{config.instance}",
        description=f"PwCGlobal User Import Australia - User import process location cost center child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")

        get_input_cost_center_data = rail.QueryCollectionOperator(
            task_id="get_input_cost_center_data",
            query="SELECT cost_center_id, cost_center_level_1, cost_center_level_2, cost_center_level_3, cost_center_level_4 FROM input_data"
        )

        def get_costcenter_converted_data(item):
            if not item:
                return []

            res = [{
                "cost_center_id": item['cost_center_id'],
                "cost_center_fullpath": "/ ".join(list(filter(None, ["PwC", str(item['cost_center_level_4']), str(item['cost_center_level_3']),
                                                                     str(item['cost_center_level_2']), str(item['cost_center_level_1'])]))),
                "length": len(list(filter(None, ["PwC", str(item['cost_center_level_4']), str(item['cost_center_level_3']),
                                                 str(item['cost_center_level_2']), str(item['cost_center_level_1'])])))
            },
                {
                "cost_center_id": None,
                "cost_center_fullpath": "/ ".join(list(filter(None, ["PwC", str(item['cost_center_level_4']),
                                                                     str(item['cost_center_level_3']), str(item['cost_center_level_2'])]))),
                "length": len(list(filter(None, ["PwC", str(item['cost_center_level_4']),
                                                 str(item['cost_center_level_3']), str(item['cost_center_level_2'])])))
            },
                {
                "cost_center_id": None,
                "cost_center_fullpath": "/ ".join(list(filter(None, ["PwC", str(item['cost_center_level_4']), str(item['cost_center_level_3'])]))),
                "length": len(list(filter(None, ["PwC", str(item['cost_center_level_4']), str(item['cost_center_level_3'])])))
            },
                {
                "cost_center_id": None,
                "cost_center_fullpath": "/ ".join(["PwC", str(item['cost_center_level_4'])]),
                "length": len(list(filter(None, ["PwC", str(item['cost_center_level_4'])])))
            }]

            return res

        convert_cost_center_data = rail.DataAdaptorOperator(
            task_id="convert_cost_center_data",
            source="{{result('get_input_cost_center_data')}}",
            columns=['cost_center_id', 'cost_center_fullpath', "length"],
            data=get_costcenter_converted_data
        )

        create_cost_center_collection = rail.CreateCollectionOperator(
            task_id="create_cost_center_collection",
            source="{{result('convert_cost_center_data')}}"
        )

        get_unique_cost_centers = rail.QueryCollectionOperator(
            task_id="get_unique_cost_centers",
            query="""SELECT DISTINCT cost_center_id, cost_center_fullpath FROM  create_cost_center_collection"""
        )

        get_all_cost_centers_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_cost_centers_from_replicon",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:department-group-list-column:department-group",
                        "urn:replicon:department-group-list-column:full-path"
                    ],
                "sort": [],
                "filterExpression": None
            },
            response_filter=custom_methods.user_import_cost_center_response_filter
        )

        replicon_company_code_collection = rail.CreateCollectionOperator(
            task_id="replicon_company_code_collection",
            source="{{result('get_all_cost_centers_from_replicon') | to_json}}"
        )

        get_unique_replicon_company_code = rail.QueryCollectionOperator(
            task_id="get_unique_replicon_company_code",
            query="""SELECT DISTINCT full_path FROM replicon_company_code_collection"""
        )

        get_company_codes_not_in_replicon = rail.QueryCollectionOperator(
            task_id="get_company_codes_not_in_replicon",
            query="""SELECT DISTINCT cost_center_fullpath, cost_center_id, length FROM create_cost_center_collection WHERE LOWER(cost_center_fullpath) NOT IN \
                        (SELECT DISTINCT LOWER(full_path) FROM replicon_company_code_collection) ORDER BY length"""
        )

        has_any_company_code_data = rail.IfOperator(
            task_id="has_any_company_code_data",
            test="{{result('get_company_codes_not_in_replicon','length') > 0}}",
            yes_task="add_company_codes",
            no_task="finish"
        )

        add_company_codes = rail.TriggerDagRunForEachItemOperator(
            task_id="add_company_codes",
            trigger_dag_id=f"pwcglobal_user_import_australia_user_import_add_location_company_code_child_{config.instance}",
            items="{{result('get_company_codes_not_in_replicon')}}",
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf['file_name'],
                "costcenter_fullpath":  item['cost_center_fullpath'],
                "length":  int(item['length']),
                "cost_center_id":  item['cost_center_id'],
                "action": "costcenter",
                "parent_fullpath": "/ ".join(item['cost_center_fullpath'].split("/ ")[0:-1]),
                "parent_uri": rail.find_first_by_attr_and_get_attr(custom_methods.get_data_from_document(rail.result("replicon_company_code_collection")),
                                                                   "full_path", "/ ".join(item['cost_center_fullpath'].split("/ ")[0:-1]))
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_add_company_codes = rail.WaitForDagRunsSensor(
            task_id="wait_for_add_company_codes",
            dag_runs='{{ result("add_company_codes") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_input_location_data = rail.QueryCollectionOperator(
            task_id="get_input_location_data",
            query="SELECT location_level_1, location_level_2, location_level_3, location_level_4 FROM input_data"
        )

        def get_location_converted_data(item):
            if not item:
                return []
            res = [{
                "location_fullpath": " / ".join((filter(None, [str(item['location_level_4']), str(
                    item['location_level_3']), str(item['location_level_2']), str(item['location_level_1'])]))),
                "length": len(list(filter(None, [str(item['location_level_4']), str(
                    item['location_level_3']), str(item['location_level_2']), str(item['location_level_1'])])))
            },
                {"location_fullpath": " / ".join(filter(None, [str(item['location_level_4']), str(item['location_level_3']),
                                                               str(item['location_level_2'])])),
                 "length": len(list(filter(None, [str(item['location_level_4']), str(item['location_level_3']), str(item['location_level_2'])]))),
                 },
                {"location_fullpath": " / ".join(filter(None, [str(item['location_level_4']), str(item['location_level_3'])])),
                 "length": len(list(filter(None, [str(item['location_level_4']), str(item['location_level_3'])]))),
                 },
                {"location_fullpath": " / ".join(list(filter(None, [str(item['location_level_4'])]))),
                 "length": len(list(filter(None, [str(item['location_level_4'])])))
                 }]

            return res

        convert_location_data = rail.DataAdaptorOperator(
            task_id="convert_location_data",
            source="{{result('get_input_location_data')}}",
            columns=['location_fullpath', 'length'],
            data=get_location_converted_data
        )

        create_location_collection = rail.CreateCollectionOperator(
            task_id="create_location_collection",
            source="{{result('convert_location_data')}}"
        )

        get_unique_locations = rail.QueryCollectionOperator(
            task_id="get_unique_locations",
            query="""SELECT DISTINCT location_fullpath FROM  create_location_collection"""
        )

        get_all_location_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_location_from_replicon",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                    "columnUris": [
                        "urn:replicon:location-list-column:location",
                        "urn:replicon:location-list-column:full-path"
                    ],
                "sort": [],
                "filterExpression": None
            },
            response_filter=custom_methods.user_import_location_response_filter
        )

        replicon_location_collection = rail.CreateCollectionOperator(
            task_id="replicon_location_collection",
            source="{{result('get_all_location_from_replicon') | to_json}}"
        )

        get_unique_replicon_locations = rail.QueryCollectionOperator(
            task_id="get_unique_replicon_locations",
            query="""SELECT DISTINCT full_path,replicon_location_uri,length,replicon_location_name FROM replicon_location_collection"""
        )

        get_locations_not_in_replicon = rail.QueryCollectionOperator(
            task_id="get_locations_not_in_replicon",
            query="""SELECT DISTINCT location_fullpath,length FROM create_location_collection WHERE LOWER(location_fullpath) NOT IN \
                        (SELECT DISTINCT LOWER(full_path) FROM replicon_location_collection) ORDER BY length"""
        )

        has_any_locations_data = rail.IfOperator(
            task_id="has_any_locations_data",
            test="{{result('get_locations_not_in_replicon','length') > 0}}",
            yes_task="add_locations",
            no_task="finish"
        )
        add_locations = rail.TriggerDagRunForEachItemOperator(
            task_id="add_locations",
            trigger_dag_id=f"pwcglobal_user_import_australia_user_import_add_location_company_code_child_{config.instance}",
            items="{{result('get_locations_not_in_replicon')}}",
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf['file_name'],
                "costcenter_fullpath":  None,
                "length":  None,
                "cost_center_id": None,
                "action": "location",
                "parent_fullpath": None,
                "parent_uri": None,
                "location_fullpath": item['location_fullpath'],
                "location_length": int(item['length']),
                "parent_location_uri": rail.find_first_by_attr_and_get_attr(custom_methods.get_data_from_document(rail.result("get_unique_replicon_locations")),
                                                                            "full_path", "/ ".join(item['location_fullpath'].split("/ ")[0:-1]).strip()),
                "parent_location_fullpath": "/ ".join(item['location_fullpath'].split("/ ")[0:-1])
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_add_locations = rail.WaitForDagRunsSensor(
            task_id="wait_for_add_locations",
            dag_runs='{{ result("add_locations") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )
        start = rail.EmptyOperator(
            task_id="start"
        )
        finish = rail.EmptyOperator(
            task_id="finish"
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        start >> [get_input_cost_center_data, get_input_location_data]
        get_input_cost_center_data >> convert_cost_center_data >> create_cost_center_collection >> \
            [get_unique_cost_centers,
                get_all_cost_centers_from_replicon] >> replicon_company_code_collection
        replicon_company_code_collection >> get_unique_replicon_company_code >> get_company_codes_not_in_replicon >> has_any_company_code_data >> \
            rail.Label(
                "Yes") >> add_company_codes >> wait_for_add_company_codes >> finish

        get_input_location_data >> convert_location_data >> create_location_collection >> [get_unique_locations, get_all_location_from_replicon] \
            >> replicon_location_collection >> get_unique_replicon_locations >> get_locations_not_in_replicon >> has_any_locations_data >> rail.Label("Yes") >>\
            add_locations >> wait_for_add_locations >> finish

        has_any_company_code_data >> rail.Label("No") >> finish
        has_any_locations_data >> rail.Label("No") >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
