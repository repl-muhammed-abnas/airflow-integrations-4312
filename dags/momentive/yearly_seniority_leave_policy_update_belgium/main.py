from pendulum import datetime
import rail
from momentive.yearly_seniority_leave_policy_update_belgium.request_payload import request_payload_user
from momentive.yearly_seniority_leave_policy_update_belgium.custom_methods import create_users_list

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"momentive_yearly_seniority_leave_policy_update_belgium_master_{config.instance}",
        description="momentive yearly leave update based on seniority master",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022,7,10, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        get_user_division_uri = rail.RepliconServiceOperator(
            task_id="get_user_division_uri",
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
            data_handler= lambda response:rail.find_first_by_attr_and_get_attr(
            response, "displayText", config.seniority_user_division, "uri")
        )

        get_user_timeoff_type_uri = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_type_uri",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data_handler=lambda response:rail.find_first_by_attr_and_get_attr(
            response, "displayText", config.seniority_user_timeofftype, "uri"
            )
        )

        is_division_and_timeoff_uri_present = rail.IfOperator(
            task_id="is_division_and_timeoff_uri_present",
            test=lambda : bool(rail.result("get_user_division_uri") and rail.result("get_user_timeoff_type_uri")),
            yes_task="get_users_to_process",
            no_task="fail_dagrun"
        )

        get_users_to_process = rail.RepliconServiceOperator(
            task_id="get_users_to_process",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload_user,
            data_handler=create_users_list
        )

        create_collection_of_users_to_process = rail.CreateCollectionOperator(
            task_id="create_collection_of_users_to_process",
            source='{{result("get_users_to_process")}}',
            name="users_to_process",
            columns=["name", "status", "uri", "startdate", "tenure", "fiveyearmultiplier"]
        )

        query_collection_for_users_to_update = rail.QueryCollectionOperator(
            task_id="query_collection_for_users_to_update",
            name="users_to_update",
            query="""SELECT * FROM users_to_process
            WHERE status="True" AND tenure > 0 AND fiveyearmultiplier=0 AND NULLIF(startdate,"") IS NOT NULL"""
        )

        update_leave_policy = rail.TriggerDagRunForEachItemOperator(
            task_id="update_leave_policy",
            items="{{result('query_collection_for_users_to_update')}}",
            trigger_dag_id=f"momentive_yearly_seniority_leave_policy_update_belgium_child_{config.instance}",
            retries=0,
            conf=lambda item:{
                'useruri':item["uri"],
                'timeoffuri':rail.result("get_user_timeoff_type_uri")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message="{{get_error_message()}}"
        )

        get_user_division_uri >> \
        get_user_timeoff_type_uri >> \
        is_division_and_timeoff_uri_present >> rail.Label("Yes") >>\
        get_users_to_process >>\
        create_collection_of_users_to_process >> query_collection_for_users_to_update >> update_leave_policy >> \
        log_to_sumo >> can_fail_dag >> fail_dagrun
        is_division_and_timeoff_uri_present >> rail.Label("No") >> fail_dagrun

        return dag

rail.for_each_instance(create_main_airflow_dag)
