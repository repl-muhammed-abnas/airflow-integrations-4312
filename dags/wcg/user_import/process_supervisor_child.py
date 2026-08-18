from airflow.models import Variable
from datetime import datetime
import rail

from uuid import uuid4
from wcg.user_import.utils.custom_methods import get_supervisor_status, get_supervisor_message

def create_supervisor_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_supervisor_child_dag_id,
        description="WCG User Import - Process Pending Supervisor Assignment",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="filter_user_logs"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="filter_user_logs",
            end_task="on_error"
        )

        filter_user_logs = rail.FilterLogEntriesOperator(
            task_id='filter_user_logs',
            log='{{ dag_run.conf.user_log }}',
            properties={
                'employeeid': '{{ dag_run.conf.get("employeeid", "") }}'
            },
            remove_filtered_entries=True
        )

        query_supervisor_by_netsuite_id = rail.QueryCollectionOperator(
            task_id="query_supervisor_by_netsuite_id",
            query="""SELECT * FROM refreshed_replicon_users WHERE Internal_ID = '{{dag_run.conf.get("supervisorempid", "")}}' LIMIT 1"""
        )

        if_supervisor_exists = rail.IfOperator(
            task_id="if_supervisor_exists",
            test='{{ result("query_supervisor_by_netsuite_id", "length") > 0 }}',
            yes_task="get_supervisor_details_from_replicon",
            no_task="log_supervisor_still_not_found"
        )

        get_supervisor_details_from_replicon = rail.RepliconServiceOperator(
            task_id="get_supervisor_details_from_replicon",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "uri": rail.load_all_records(rail.result("query_supervisor_by_netsuite_id"))[0]["uri"]
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else None
        )

        log_supervisor_still_not_found = rail.EmptyOperator(
            task_id="log_supervisor_still_not_found"
        )

        check_supervisor_permission = rail.IfOperator(
            task_id="check_supervisor_permission",
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_supervisor_details_from_replicon").get("permissionSets", []),
                "displayText",
                config.defaults_mapper_data["supervisor_permission"],
                "uri"
            ),
            yes_task="check_if_supervisor_disabled",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": rail.load_all_records(rail.result("query_supervisor_by_netsuite_id"))[0]["uri"],
                },
                "modifications": {
                    "permissionSets": [
                        {
                            "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                            "items": [
                            {
                                "permissionSetPolicy": {
                                    "name": config.defaults_mapper_data["supervisor_permission"]
                                },
                                "groupAccessFilter": None
                            }
                            ]
                        }
                    ]
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        assign_products_to_supervisor = rail.RepliconServiceOperator(
            task_id="assign_products_to_supervisor",
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result("get_supervisor_details_from_replicon")["userDetails"]["uri"],
                "productUris": [
                    "urn:replicon-saas:product:time-bill-plus",
                    "urn:replicon-saas:product:time-off-plus",
                    "urn:replicon-saas:product:wfm-enterprise"
                ]
            }
        )

        check_if_supervisor_disabled = rail.IfOperator(
            task_id="check_if_supervisor_disabled",
            test=lambda: not rail.result("get_supervisor_details_from_replicon").get("userDetails", {}).get("isEnabled", True),
            yes_task="enable_supervisor",
            no_task="determine_assignment_type"
        )

        enable_supervisor = rail.RepliconServiceOperator(
            task_id="enable_supervisor",
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data=lambda: {
                "userUri": rail.result("get_supervisor_details_from_replicon")["userDetails"]["uri"]
            }
        )

        determine_assignment_type = rail.IfOperator(
            task_id="determine_assignment_type",
            test='{{ dag_run.conf.get("action") == "Add" }}',
            yes_task="assign_supervisor_to_user",
            no_task="get_current_supervisor_assignment"
        )

        get_current_supervisor_assignment = rail.RepliconServiceOperator(
            task_id="get_current_supervisor_assignment",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("useruri"),
                "asOfDate": rail.get_replicon_date(datetime.now())
            }
        )

        check_if_supervisor_changed = rail.IfOperator(
            task_id="check_if_supervisor_changed",
            test=lambda: not rail.result("get_current_supervisor_assignment") or (rail.result("get_current_supervisor_assignment") and
                rail.result("get_supervisor_details_from_replicon", {}).get("userDetails", {}).get("uri") !=
                rail.result("get_current_supervisor_assignment").get("supervisor", {}).get("user", {}).get("uri")
            ),
            yes_task="assign_supervisor_to_user",
            no_task="log_supervisor_already_assigned"
        )

        log_supervisor_already_assigned = rail.EmptyOperator(
            task_id="log_supervisor_already_assigned"
        )

        assign_supervisor_to_user = rail.RepliconServiceOperator(
            task_id="assign_supervisor_to_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("useruri"),
                "supervisorUri": rail.result("get_supervisor_details_from_replicon")["userDetails"]["uri"],
                "dateRange": None if dag_run.conf.get('action') == 'Add' else {
                    "startDate": rail.get_replicon_date(datetime.now())
                        if rail.result("get_current_supervisor_assignment") else None
                }
            }
        )

        dummy_filter_user_logs = rail.EmptyOperator(
            task_id="dummy_filter_user_logs"
        )

        is_filtered_userlogs = rail.IfOperator(
            task_id='is_filtered_userlogs',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries',
            no_task='on_error'
        )

        update_userlog_entries = rail.WriteLogOperator(
            task_id='update_userlog_entries',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            items="{{ result('filter_user_logs') }}",
            properties=lambda item, dag_run: {
                "employeeid": item['properties']['employeeid'],
                "firstname": item['properties']['firstname'],
                "lastname": item['properties']['lastname'],
                'action': item['properties']['action'],
                'status': get_supervisor_status(item, dag_run),
                'details': get_supervisor_message(item, dag_run)
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        is_entries_present_error = rail.IfOperator(
            task_id='is_entries_present_error',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries_error'
        )

        update_userlog_entries_error = rail.WriteLogOperator(
            task_id='update_userlog_entries_error',
            message='update supervisor entries with error',
            log='{{ dag_run.conf.user_log }}',
            severity='Error',
            items="{{ result('filter_user_logs') }}",
            properties={
                'employeeid': '{{ item.properties.employeeid }}',
                'firstname': '{{ item.properties.firstname }}',
                'lastname': '{{ item.properties.lastname }}',
                'action': '{{ item.properties.action }}',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> on_error
        can_run_batch_task >> rail.Label("No") >> filter_user_logs >> query_supervisor_by_netsuite_id >> if_supervisor_exists

        if_supervisor_exists >> rail.Label("Yes") >> get_supervisor_details_from_replicon >> check_supervisor_permission
        if_supervisor_exists >> rail.Label("No") >> log_supervisor_still_not_found >> dummy_filter_user_logs

        check_supervisor_permission >> rail.Label("Yes") >> check_if_supervisor_disabled
        check_supervisor_permission >> rail.Label("No") >> assign_supervisor_permission >> assign_products_to_supervisor >> check_if_supervisor_disabled

        check_if_supervisor_disabled >> rail.Label("Yes") >> enable_supervisor >> determine_assignment_type
        check_if_supervisor_disabled >> rail.Label("No") >> determine_assignment_type

        determine_assignment_type >> rail.Label("Yes") >> assign_supervisor_to_user >> dummy_filter_user_logs
        determine_assignment_type >> rail.Label("No") >> get_current_supervisor_assignment >> check_if_supervisor_changed

        check_if_supervisor_changed >> rail.Label("Yes") >> assign_supervisor_to_user
        check_if_supervisor_changed >> rail.Label("No") >> log_supervisor_already_assigned >> dummy_filter_user_logs

        dummy_filter_user_logs >> is_filtered_userlogs
        is_filtered_userlogs >> rail.Label("Yes") >> update_userlog_entries
        is_filtered_userlogs >> rail.Label("No") >> on_error

        on_error >> is_entries_present_error >> rail.Label("Yes") >> update_userlog_entries_error

    return dag


rail.for_each_instance(create_supervisor_child_dag)
