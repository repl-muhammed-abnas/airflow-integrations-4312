import uuid
import rail
from pwcglobal.user_import_australia import custom_methods


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_import_add_location_company_code_child_{config.instance}",
        description=f"PwCGlobal User Import Australia - add location or company code child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")

        is_level_1 = rail.IfOperator(
            task_id="is_level_1",
            test="{{dag_run.conf.length > 1 if dag_run.conf.length | is_truthy else dag_run.conf.location_length > 0}}",
            yes_task="is_for_location"
        )
        is_for_location = rail.IfOperator(
            task_id='is_for_location',
            test="{{dag_run.conf.action == 'location'}}",
            yes_task="get_parent_location_uri",
            no_task="get_parent_costcenter_uri"
        )
        get_parent_costcenter_uri = rail.RepliconServiceOperator(
            task_id="get_parent_costcenter_uri",
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
            response_filter=lambda response, dag_run: list(filter(lambda x: x['full_path'] == "/ ".join(
                dag_run.conf['costcenter_fullpath'].split("/ ")[0:-1]), custom_methods.user_import_cost_center_response_filter(response)))
        )
        get_parent_location_uri = rail.RepliconServiceOperator(
            task_id="get_parent_location_uri",
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
            response_filter=lambda response, dag_run: list(filter(lambda x: x['full_path'] == "/ ".join(dag_run.conf['location_fullpath'].split("/ ")[
                                                           0:-1]).strip(), custom_methods.user_import_location_response_filter(response)))
        )

        def get_parent_details(dag_run, task_id, conf_key, uri_key):
            if dag_run.conf[conf_key] or rail.result(task_id):
                return {
                    "uri": dag_run.conf[conf_key][uri_key] if dag_run.conf[conf_key] else rail.result(task_id)[0][uri_key],
                    "parent": None,
                    "name": None,
                    "parameterCorrelationId": None
                }
            return None

        def get_add_location_payload(dag_run):

            return{
                "location": {
                    "uri": None,
                    "parent": get_parent_details(dag_run, task_id="get_parent_location_uri", conf_key="parent_location_uri", uri_key="replicon_location_uri"),
                    "name": None,
                    "parameterCorrelationId": None
                },
                "modifications": {
                    "name": dag_run.conf['location_fullpath'].split("/ ")[-1],
                    "codeToApply": None,
                    "descriptionToApply": None,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        add_location = rail.RepliconServiceOperator(
            task_id="add_location",
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=get_add_location_payload
        )

        def get_add_costcenter_payload(dag_run):
            return {
                "departmentGroup": {
                    "uri": None,
                    "parent": get_parent_details(dag_run, task_id="get_parent_costcenter_uri", conf_key="parent_uri", uri_key="replicon_company_codes_uri"),
                    "name": None,
                    "parameterCorrelationId": None
                },
                "modifications": {
                    "name": dag_run.conf['costcenter_fullpath'].split("/ ")[-1],
                    "codeToApply": {
                        "value": dag_run.conf['cost_center_id']
                    } if dag_run.conf['cost_center_id'] else None,
                    "descriptionToApply": None,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        add_costcenter = rail.RepliconServiceOperator(
            task_id="add_costcenter",
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=get_add_costcenter_payload
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        is_level_1 >> rail.Label("Yes") >> is_for_location >> \
            rail.Label(
                "No") >> get_parent_costcenter_uri >> add_costcenter >> log_to_sumo
        is_for_location >> rail.Label(
            "Yes") >> get_parent_location_uri >> add_location >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
