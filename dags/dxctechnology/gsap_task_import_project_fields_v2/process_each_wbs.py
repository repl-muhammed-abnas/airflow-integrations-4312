from datetime import timedelta
import rail
from dxctechnology.gsap_task_import_project_fields_v2.utils import request_payload
from dxctechnology.gsap_task_import_project_fields_v2.utils import response_filter
from dxctechnology.gsap_task_import_project_fields_v2.utils.python_callable_method import get_task_to_add_callable
from airflow.models import Variable

# pylint: disable=too-many-statements
def create_child_sync_attribute_1_2_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_wbs,
        description=f'Sync GSAP Task{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_sync_gsap_task_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "create_wbs_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_wbs_log',
            end_task="catch_and_log_errors",
        )

        create_wbs_log = rail.CreateLogOperator(
            task_id = "create_wbs_log"
        )

        query_gsap_task_records_for_wbs = rail.QueryCollectionOperator(
            task_id="query_gsap_task_records_for_wbs",
            name="feed_task_for_project",
            query="SELECT * FROM valid_input_records WHERE WBS = :WBS",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}"
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=request_payload.get_project_details,
            response_filter=response_filter.map_get_project_details
        )

        is_wbs_present = rail.IfOperator(
            task_id="is_wbs_present",
            test="{{ result('get_project_details') | length > 0}}",
            yes_task="get_all_assigned_gsap_task_for_project",
            no_task="log_wbs_not_present",
        )

        get_all_assigned_gsap_task_for_project = rail.RepliconServiceOperator(
            task_id = "get_all_assigned_gsap_task_for_project",
            endpoint="/services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfEnabledProjectDependentTimeEntryObjectExtensionTags",
            data=request_payload.get_all_gsap_task_payload,
            data_handler=response_filter.get_all_assigned_gsap_task_for_project_filter
        )

        get_all_child_wbs_details = rail.RepliconServiceOperator(
            task_id="get_all_child_wbs_details",
            endpoint="services/ProjectListService1.svc/GetData",
            data=request_payload.get_child_wbs_payload,
            response_filter=response_filter.map_child_wbs
        )

        is_child_wbs_present = rail.IfOperator(
            task_id="is_child_wbs_present",
            test='{{ result("get_all_child_wbs_details") | length > 0 }}',
            yes_task='sync_child_wbs',
            no_task='is_wbs_start_date_empty',
        )

        sync_child_wbs = rail.TriggerDagRunForEachItemOperator(
            task_id='sync_child_wbs',
            retries=0,
            items=lambda: rail.result('get_all_child_wbs_details'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_each_child_wbs,
            conf=lambda item, dag_run: {
                'file_name': dag_run.conf['file_name'],
                "get_all_gsap_tasks_from_replicon": dag_run.conf['get_all_gsap_tasks_from_replicon'],
                'wbs': dag_run.conf['wbs'],
                'gsap_task_uri': dag_run.conf['gsap_task_uri'],
                'childWbs': item.split(" - ")[0].strip()
            }
        )

        log_wbs_not_present = rail.WriteLogOperator(
            task_id='log_wbs_not_present',
            message="All tasks failed to sync, since WBS not available in Replicon",
            log="{{result('create_wbs_log')}}",
            properties={
                'Level': "Project",
                'wbs': '{{dag_run.conf.wbs}}',
                'task_name': "",
                'task_code': "",
                'action': 'Skipped',
                'status': "Exception",
                'recordcount': "{{result('query_gsap_task_records_for_wbs','length')}}",
            }
        )

        get_reprocess_log = rail.CreateLogOperator(
            task_id = "get_reprocess_log",
            tenant_wide_name=config.reprocess_wbs_log_name,
            existing_log_mode="append"
        )

        log_wbs_record_for_reprocessing = rail.WriteLogOperator(
            task_id = "log_wbs_record_for_reprocessing",
            log="{{result('get_reprocess_log')}}",
            message="Logging WBS {{dag_run.conf.wbs}} for reprocessing",
            severity="Reprocess",
            properties=lambda dag_run: {
                **dag_run.conf
            }
        )

        is_wbs_start_date_empty = rail.IfOperator(
            task_id="is_wbs_start_date_empty",
            test=lambda: bool(rail.result('get_project_details')[
                              0]['start_date_year']),
            yes_task="should_process_tasks",
            no_task="log_wbs_start_date_empty",
        )

        log_wbs_start_date_empty = rail.WriteLogOperator(
            task_id='log_wbs_start_date_empty',
            log="{{result('create_wbs_log')}}",
            message="All tasks failed to sync, since WBS Start Date is Empty",
            properties={
                'Level': "Project",
                'wbs': '{{dag_run.conf.wbs}}',
                'task_name': "",
                'task_code': "",
                'action': 'Skipped',
                'status': "Exception",
                'recordcount': "{{result('query_gsap_task_records_for_wbs','length')}}",
            }
        )

        should_process_tasks = rail.IfOperator(
            task_id = "should_process_tasks",
            test=lambda : (len(rail.result('get_project_details')) > 0) and (bool(rail.result('get_project_details')[
                              0]['start_date_year'])),
            yes_task= "dummy_is_wbs_in_progress",
            no_task="catch_and_log_errors"
        )

        dummy_is_wbs_in_progress = rail.EmptyOperator(
            task_id= "dummy_is_wbs_in_progress"
        )

        is_wbs_in_progress = rail.IfOperator(
            task_id="is_wbs_in_progress",
            test=lambda: rail.result('get_project_details')[
                0]['status'] == "In Progress",
            yes_task="get_task_add_update",
            no_task="log_wbs_not_in_progress",
        )

        log_wbs_not_in_progress = rail.WriteLogOperator(
            task_id='log_wbs_not_in_progress',
            log="{{result('create_wbs_log')}}",
            message="All tasks were skipped, since this WBS is not in In Progress status.",
            properties={
                'Level': "Project",
                'wbs': '{{dag_run.conf.wbs}}',
                'task_name': "",
                'task_code': "",
                'action': 'pre-check',
                'status': "Exception",
                'recordcount': "{{result('query_gsap_task_records_for_wbs','length')}}",
            }
        )


        get_task_add_update = rail.PythonOperator(
            task_id = "get_task_add_update",
            python_callable=get_task_to_add_callable
        )

        has_task_to_skip = rail.IfOperator(
            task_id = "has_task_to_skip",
            test="{{ result('get_task_add_update', 'invalid_date_task_records') | load_json_artifact | length > 0}}",
            yes_task= "log_invalid_task",
            no_task="has_task_to_update"
        )

        log_invalid_task = rail.WriteLogOperator(
            task_id='log_invalid_task',
            items="{{ result('get_task_add_update', 'invalid_date_task_records') | load_json_artifact | to_json}}",
            log="{{result('create_wbs_log')}}",
            message="Task Failed to Sync As End Date is Prior to Start Date",
            properties={
                'Level': "Project",
                'wbs': "{{dag_run.conf.wbs }}",
                'task_name': "{{ item.task_name }}",
                'task_code': "{{ item.task_code }}",
                'action': 'Update',
                'status': "Ignored",
                'recordcount': '1',
            }
        )

        has_task_to_update = rail.IfOperator(
            task_id = "has_task_to_update",
            test="{{ result('get_task_add_update', 'task_records_to_update') | load_json_artifact | length > 0}}",
            yes_task="update_tasks",
            no_task= "has_task_to_add"
        )

        update_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id = "update_tasks",
            items=lambda: rail.load_json_artifact(rail.result('get_task_add_update', 'task_records_to_update')),
            batch_size=config.PROJECT_DEPENDANT_OEF_ADD_LIMIT,
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.batch_update_gsap_task_payload,
            all_result_data_handler=lambda response: response_filter.combine_task_add_update_output(response, 'task_records_to_update')
        )

        log_update_success_record = rail.WriteLogOperator(
            task_id='log_update_success_record',
            log="{{result('create_wbs_log')}}",
            items="{{ result('update_tasks').added | load_json_artifact | to_json }}",
            message="Task Updated Successfully",
            properties={
                'Level': "Project",
                'wbs': "{{ dag_run.conf.wbs }}",
                'task_name': "{{ item.task_details | attr_or_default('task_name', '') }}",
                'task_code': "{{ item.task_details | attr_or_default('task_code', '') }}",
                'action': 'Updated',
                'status': "Success",
                'recordcount': '1',
            }
        )

        log_update_errored_record = rail.WriteLogOperator(
            task_id='log_update_errored_record',
            log="{{result('create_wbs_log')}}",
            items="{{ result('update_tasks').errors | load_json_artifact | to_json }}",
            message="{{ item.notifications[0].displayText }}",
            properties={
                'Level': "Project",
                'wbs': "{{ dag_run.conf.wbs }}",
                'task_name': "{{ item.task_details | attr_or_default('task_name', '') }}",
                'task_code': "{{ item.task_details | attr_or_default('task_code', '') }}",
                'action': 'Updated',
                'status': "Success",
                'recordcount': '1',
            }
        )

        has_task_to_add = rail.IfOperator(
            task_id = "has_task_to_add",
            test="{{ result('get_task_add_update') | load_json_artifact | length > 0}}",
            yes_task="disable_task_from_projects",
            no_task="catch_and_log_errors"
        )

        disable_task_from_projects = rail.RepliconServiceCallForEachItemOperator(
            task_id = "disable_task_from_projects",
            items=lambda: rail.load_json_artifact(rail.result('get_task_add_update', 'task_to_disable')),
            batch_size=config.PROJECT_DEPENDANT_OEF_ADD_LIMIT,
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.batch_disable_gsap_task_payload
        )

        add_task_to_project = rail.RepliconServiceCallForEachItemOperator(
            task_id = "add_task_to_project",
            items=lambda: rail.load_json_artifact(rail.result('get_task_add_update')),
            batch_size=config.PROJECT_DEPENDANT_OEF_ADD_LIMIT,
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.batch_update_gsap_task_payload,
            all_result_data_handler=response_filter.combine_task_add_update_output
        )

        log_add_success_record = rail.WriteLogOperator(
            task_id='log_add_success_record',
            log="{{result('create_wbs_log')}}",
            items="{{ result('add_task_to_project').added | load_json_artifact | to_json }}",
            message="Task Updated Successfully",
            properties={
                'Level': "Project",
                'wbs': "{{dag_run.conf.wbs }}",
                'task_name': "{{ item.task_details | attr_or_default('task_name', '') }}",
                'task_code': "{{ item.task_details | attr_or_default('task_code', '') }}",
                'action': 'Add',
                'status': "Success",
                'recordcount': '1',
            }
        )

        log_add_errored_record = rail.WriteLogOperator(
            task_id='log_add_errored_record',
            log="{{result('create_wbs_log')}}",
            items="{{ result('add_task_to_project').errors | load_json_artifact | to_json }}",
            message="{{ item.notifications[0].displayText }}",
            properties={
                'Level': "Project",
                'wbs': "{{dag_run.conf.wbs }}",
                'task_name': "{{ item.task_details | attr_or_default('task_name', '') }}",
                'task_code': "{{ item.task_details | attr_or_default('task_code', '') }}",
                'action': 'Add',
                'status': "Error",
                'recordcount': '1',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{result('create_wbs_log')}}",
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'Level': "Project",
                'wbs': "{{dag_run.conf.wbs}}",
                'task_name': "NA",
                'task_code': "NA",
                'action': 'NA',
                'status': "Error",
                'recordcount': '1',
            })

        def get_log_to_sumo_extra_conf(dag_run):
            def get_message():
                get_project_details_task_xcom = rail.result('get_project_details') or []
                is_wbs_found = bool(get_project_details_task_xcom and get_project_details_task_xcom[0]['uri'])
                if not is_wbs_found:
                    return "WBS logged for reprocessing"
                return "WBS processed successfully"

            return {
                "Integration": "GSAP Task(OEF)",
                "wbs_reprocessed_count": int(dag_run.conf.get("reprocess_count", 0)),
                "wbs": dag_run.conf['wbs'],
                "message": get_message(),
                "task_count": rail.result("query_gsap_task_records_for_wbs", "length")
            }

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=get_log_to_sumo_extra_conf
        )


        is_run_for_reprocess = rail.IfOperator(
            task_id = "is_run_for_reprocess_and_failed",
            # when the master(gsap_task_master) trigger the dag, the conf will not have reprocess_count
            test = "{{ dag_run.conf | attr_or_default('reprocess_count', 'NA') != 'NA'}}",
            yes_task = "should_check_logs"
        )

        should_check_logs = rail.IfOperator(
            task_id = 'should_check_logs',
            #pylint: disable=line-too-long
            test="{{get_task_state('update_tasks') | lower == 'success' or get_task_state('disable_task_from_projects') | lower == 'success' or get_task_state('add_task_to_project') | lower == 'success'}}",
            yes_task="get_failure_logs",
            no_task="check_error_message"
        )

        get_failure_logs = rail.FilterLogEntriesOperator(
            task_id = "get_failure_logs",
            log="{{result('create_wbs_log')}}",
            properties={"action": "Error"}
        )

        has_any_failures = rail.IfOperator(
            task_id = 'has_any_failures',
            test="{{ result('get_failure_logs', 'length') > 0 }}",
            yes_task= 'fail_reprocess_dag_run'
        )

        check_error_message = rail.IfOperator(
            task_id = "check_error_message",
            test="{{ get_error_message() | is_truthy }}",
            yes_task="fail_reprocess_dag_run"
        )

        fail_reprocess_dag_run = rail.FailOperator(
            task_id = 'fail_reprocess_dag_run',
            message="Reprocessing failed with error: {{ get_error_message() }}"
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> \
            catch_and_log_errors
        should_process_tasks >> rail.Label("Yes") >> dummy_is_wbs_in_progress >> is_wbs_in_progress
        can_run_batch_task >> rail.Label("No") >> create_wbs_log >> query_gsap_task_records_for_wbs
        should_process_tasks >> rail.Label("On Error") >> catch_and_log_errors
        query_gsap_task_records_for_wbs >> get_project_details
        get_project_details >> is_wbs_present
        is_wbs_present >> rail.Label(
            "NO") >> log_wbs_not_present >> get_reprocess_log >> log_wbs_record_for_reprocessing >> should_process_tasks
        is_wbs_present >> rail.Label(
            "YES") >> get_all_assigned_gsap_task_for_project >> get_all_child_wbs_details >> is_child_wbs_present
        is_child_wbs_present >> rail.Label(
            "YES") >> sync_child_wbs >> is_wbs_start_date_empty
        is_child_wbs_present >> rail.Label("NO") >> is_wbs_start_date_empty
        is_wbs_start_date_empty >> rail.Label("YES") >> should_process_tasks
        is_wbs_start_date_empty >> rail.Label(
            "NO") >> log_wbs_start_date_empty >> should_process_tasks
        is_wbs_in_progress >> rail.Label(
            "NO") >> log_wbs_not_in_progress >> rail.Label("On Error") >> catch_and_log_errors >> log_to_sumo\
                >> rail.Label("Checking Failure for reprocessing") >> is_run_for_reprocess
        is_run_for_reprocess >> rail.Label("Yes") >> should_check_logs >> rail.Label("Yes") >> get_failure_logs \
            >> has_any_failures >> rail.Label("Yes") >> fail_reprocess_dag_run
        should_check_logs >> rail.Label("No") >> check_error_message >> rail.Label("Yes") >> fail_reprocess_dag_run
        is_wbs_in_progress >> rail.Label(
            "YES") >> get_task_add_update
        get_task_add_update >> has_task_to_skip >> rail.Label("Yes") >> log_invalid_task >> has_task_to_update
        has_task_to_update >> rail.Label("Yes") >> update_tasks >> log_update_success_record >> log_update_errored_record >> has_task_to_add
        has_task_to_skip >> rail.Label("No") >> has_task_to_update >> rail.Label("No") >> has_task_to_add >> rail.Label("On Error") >> catch_and_log_errors
        has_task_to_add >> rail.Label("Yes") >> disable_task_from_projects \
            >> add_task_to_project >> log_add_success_record >> log_add_errored_record >> rail.Label("On Error") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_sync_attribute_1_2_dag)
