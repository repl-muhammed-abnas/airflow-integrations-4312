from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_billing_key_master.utils import request_payload, response_filter
from dxctechnology.gsap_billing_key_master.utils import python_callable_method

null = None

# pylint: disable=too-many-statements


def create_attribute_1_process_wbs_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_billing_key_process_wbs_{config.dag_id_postfix}',
        description=f'DXC_Compass_GSAP Billing Key Child - Process each WBS V1.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_process_wbs_max_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_billing_key_log = rail.CreateLogOperator(
            task_id='create_billing_key_log'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_project_details_based_on_wbs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_project_details_based_on_wbs',
            end_task='catch_and_log_errors',
        )

        get_task_details_for_project = rail.QueryCollectionOperator(
            task_id = "get_task_details_for_project",
            query= """SELECT * FROM valid_wbs WHERE wbs = :WBS""",
            query_params={
                "WBS" : "{{ dag_run.conf.wbs}}"
            }
        )

        get_project_details_based_on_wbs = rail.RepliconServiceOperator(
            task_id='get_project_details_based_on_wbs',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": '{{ dag_run.conf.wbs }}',
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": null}])[0]['projectDetails']
        )

        check_wbs_exists = rail.IfOperator(
            task_id='check_wbs_exists',
            test=lambda: bool(rail.result('get_project_details_based_on_wbs') and
                              rail.result(
                'get_project_details_based_on_wbs')['uri']),
            yes_task='check_wbs_is_archived',
            no_task='dummy_process_billing_keys'
        )



        check_wbs_is_archived = rail.IfOperator(
            task_id='check_wbs_is_archived',
            test=lambda: rail.result('get_project_details_based_on_wbs')[
                'status']['name'] == 'Archived',
            yes_task='dummy_process_billing_keys',
            no_task='get_project_date_range',
        )



        get_project_date_range = rail.PythonOperator(
            task_id='get_project_date_range',
            python_callable=lambda: python_callable_method.project_date_range(
                'get_project_details_based_on_wbs')
        )

        get_all_project_team_member_details = rail.RepliconServiceOperator(
            task_id='get_all_project_team_member_details',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails',
            data={
                'projectUri': '{{ result("get_project_details_based_on_wbs").uri }}',
                'asOfDate': null},
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data))
        )

        get_children_task_details = rail.RepliconServiceOperator(
            task_id='get_children_task_details',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ result("get_project_details_based_on_wbs").uri }}'
            }
        )

        get_tasks_from_project = rail.PythonOperator(
            task_id='get_tasks_from_project',
            python_callable=python_callable_method.retrieve_task_list,
            op_args=['get_children_task_details']
        )

        tasks_from_project_collection = rail.CreateCollectionOperator(
            task_id='tasks_from_project_collection',
            source='{{ result("get_tasks_from_project") | to_json }}',
            columns=[
                'name',
                'code',
                'enddate',
                'oef',
                'uri'],
            name='tasks_from_project'
        )


        dummy_process_billing_keys = rail.EmptyOperator(
            task_id = "dummy_process_billing_keys"
        )

        should_process_tasks = rail.IfOperator(
            task_id = "should_process_tasks",
            test= lambda: bool((rail.result(
                'get_project_details_based_on_wbs') and rail.result(
                'get_project_details_based_on_wbs')['uri']) and ( rail.result(
                'get_project_details_based_on_wbs') and not rail.result('get_project_details_based_on_wbs')[
                'status']['name'] == 'Archived')),
            yes_task= "query_task_list",
            no_task= "log_exception_wbs_not_found"
        )

        log_exception_wbs_not_found  = rail.IfOperator(
            task_id = "log_exception_wbs_not_found",
            test= lambda: bool(rail.result(
                'get_project_details_based_on_wbs') and rail.result(
                'get_project_details_based_on_wbs')['uri']),
            yes_task= "log_wbs_is_archived",
            no_task= "log_wbs_not_available"
        )

        log_wbs_not_available = rail.WriteLogOperator(
            task_id='log_wbs_not_available',
            log='{{ result("create_billing_key_log") }}',
            message='Failed to sync, since WBS not available in Replicon',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'taskname': '',
                'taskcode': '',
                'action': 'skipped',
                'status': 'Exception',
                'details': 'Failed to sync, since WBS not available in Replicon',
            }
        )

        log_wbs_record_for_reprocessing = rail.PythonOperator(
            task_id = "log_wbs_record_for_reprocessing",
            python_callable=lambda dag_run:{
                "message": "Logging WBS {{dag_run.conf.wbs}} for reprocessing",
                "severity":"Reprocess",
                "properties": {
                    **dag_run.conf
                }
            }
        )

        log_wbs_is_archived = rail.WriteLogOperator(
            task_id='log_wbs_is_archived',
            # pylint: disable=line-too-long
            log='{{ result("create_billing_key_log") }}',
            message='Gsap Billing Key Sync skipped, since this WBS is in Archive status.',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'taskname': '',
                'taskcode': '',
                'action': 'pre-check',
                'status': 'skipped',
                'details': 'Gsap Billing Key skipped, since this WBS is in Archive status.'
            }
        )
        query_task_list = rail.QueryCollectionOperator(
            task_id='query_task_list',
            query="""SELECT * FROM tasks_from_project WHERE oef = 'GSAP Billing Key'"""
        )

        for_each_billing_key_start = rail.ForEachOperator(
            task_id = "for_each_billing_key_start",
            items= "{{ result('get_task_details_for_project') }}",
            start_task="check_if_task_present",
            end_task="for_each_billing_key_end"
        )

        check_if_task_present = rail.PythonOperator(
            task_id = "check_if_task_present",
            python_callable=  python_callable_method.is_task_already_present
        )

        is_task_found = rail.IfOperator(
            task_id = "is_task_found",
            test= "{{ result('check_if_task_present') | is_truthy}}",
            yes_task="update_billing_key",
            no_task= "create_billing_key"
        )

        update_billing_key = rail.RepliconServiceOperator(
            task_id='update_billing_key',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=request_payload.get_update_task_data,
        )

        create_billing_key = rail.RepliconServiceOperator(
            task_id='create_billing_key',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=request_payload.get_update_task_data,
        )

        def get_log_properties(dag_run, status):
            return{
                'wbs': dag_run.conf['wbs'],
                'taskname': rail.result('for_each_billing_key_start')['taskName'],
                'taskcode': rail.result('for_each_billing_key_start')['taskCode'],
                'action': 'Update' if python_callable_method.is_task_already_present() else "Added",
                'status': status,
                'details': f'GSAP Billing Key {"updated" if python_callable_method.is_task_already_present() else "added"} successfully',
            }

        log_success = rail.WriteLogOperator(
            task_id = "log_success",
            log='{{ result("create_billing_key_log") }}',
            severity="Success",
            message= 'GSAP Billing Key {{ "updated" if result("check_if_task_present") | is_truthy else "added"}} successfully',
            properties=lambda dag_run:get_log_properties(dag_run, "Success")
        )

        is_update_failed = rail.IfOperator(
            task_id = 'is_update_failed',
            trigger_rule = "all_done",
            test= "{{ get_task_state('update_billing_key') | lower == 'failed' or get_task_state('create_billing_key') | lower == 'failed' }}",
            yes_task= "log_task_processing_failed",
            no_task="can_log_success"
        )

        can_log_success = rail.IfOperator(
            task_id = "can_log_success",
            test= lambda: bool((rail.result(
                'get_project_details_based_on_wbs') and rail.result(
                'get_project_details_based_on_wbs')['uri']) and ( rail.result(
                'get_project_details_based_on_wbs') and not rail.result('get_project_details_based_on_wbs')[
                'status']['name'] == 'Archived')),
            yes_task="log_success"
        )

        log_task_processing_failed = rail.WriteLogOperator(
            task_id = "log_task_processing_failed",
            log='{{ result("create_billing_key_log") }}',
            message= '{{ get_error_message() }}',
            severity='Error',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'taskname': "{{ result('for_each_billing_key_start')['taskName'] }}",
                'taskcode': "{{ result('for_each_billing_key_start')['taskName'] }}",
                'action': '{{ "Update" if result("check_if_task_present") | is_truthy else "Add" }}',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            }
        )

        for_each_billing_key_end = rail.EmptyOperator(
            task_id = "for_each_billing_key_end"
        )

        get_all_filter_defination = rail.RepliconServiceOperator(
            task_id="get_all_filter_defination",
            endpoint="services/ProjectListService1.svc/GetAllFilterDefinitions",
            data={},
            response_filter=response_filter.map_parent_wbs_oef_uri
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="services/ProjectListService1.svc/GetAllColumns",
            data={},
            response_filter=response_filter.map_parent_column_uri
        )

        get_all_child_wbs_details = rail.RepliconServiceOperator(
            task_id="get_all_child_wbs_details",
            endpoint="services/ProjectListService1.svc/GetData",
            data=request_payload.get_child_wbs_payload_new,
            data_handler=response_filter.map_child_wbs_new
        )

        has_any_child_wbs_to_process = rail.IfOperator(
            task_id = "has_any_child_wbs_to_process",
            test = "{{ result('get_all_child_wbs_details') | is_truthy }}",
            yes_task= "process_child_wbs",
            no_task= "catch_and_log_errors"
        )

        process_child_wbs = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_child_wbs",
            trigger_dag_id= f'dxctechnology_gsab_billing_key_process_child_wbs_{config.dag_id_postfix}',
            items= "{{ result('get_all_child_wbs_details') | to_json }}",
            conf=lambda item, dag_run:{
                "wbs" : item['wbs'],
                "parent_wbs": dag_run.conf["wbs"],
                "wbs_uri": item['wbs_uri'],
                "tasktypeoptionuri" : dag_run.conf['tasktypeoptionuri'],
                "tasktypeuri" : dag_run.conf['tasktypeuri']
                },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_billing_key_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
            },
        )

        def get_log_to_sumo_extra_conf(dag_run):
            def get_message():
                get_project_details_based_on_wbs_task_xcom = rail.result('get_project_details_based_on_wbs') or {}
                is_wbs_found = bool(get_project_details_based_on_wbs_task_xcom and get_project_details_based_on_wbs_task_xcom['uri'])
                wbs_is_archived = get_project_details_based_on_wbs_task_xcom.get('status', {}).get('name',"") == 'Archived'
                if not is_wbs_found:
                    return "WBS logged for reprocessing"
                if is_wbs_found and wbs_is_archived:
                    return "WBS is archived"
                return "WBS processed successfully"

            return {
                "Integration": "GSAP Billing Key",
                "wbs_reprocessed_count": int(dag_run.conf.get("reprocess_count", 0)),
                "wbs": dag_run.conf['wbs'],
                "message": get_message(),
                "billing_key_count": rail.result("get_task_details_for_project", "length")
            }

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=get_log_to_sumo_extra_conf
        )

        is_run_for_reprocess_and_failed = rail.IfOperator(
            task_id = "is_run_for_reprocess_and_failed",
            # when the master(billing_key) trigger the dag, the conf will not have reprocess_count
            test = "{{ get_error_message() | is_truthy and dag_run.conf | attr_or_default('reprocess_count', 'NA') != 'NA'}}",
            yes_task = "fail_reprocess_dag_run"
        )

        fail_reprocess_dag_run = rail.FailOperator(
            task_id = 'fail_reprocess_dag_run',
            message="Reprocessing failed with error: {{ get_error_message() }}"
        )

        create_billing_key_log >> can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_project_details_based_on_wbs
        get_project_details_based_on_wbs >> check_wbs_exists

        check_wbs_exists >> rail.Label(
            'No') >> dummy_process_billing_keys
        check_wbs_exists >> rail.Label('Yes') >> check_wbs_is_archived

        dummy_process_billing_keys >> should_process_tasks >> rail.Label("No") >> log_exception_wbs_not_found
        log_exception_wbs_not_found >> rail.Label("WBS not found") >> log_wbs_not_available\
              >> log_wbs_record_for_reprocessing >> rail.Label("On Error") >> catch_and_log_errors
        log_exception_wbs_not_found >> rail.Label("WBS is Archived") >> log_wbs_is_archived >> rail.Label("On Error") >> catch_and_log_errors
        should_process_tasks >> rail.Label("Yes") >> query_task_list
        check_wbs_is_archived >> rail.Label(
            'Yes') >> dummy_process_billing_keys
        check_wbs_is_archived >> rail.Label(
            'No') >> get_project_date_range >> get_task_details_for_project >> get_all_project_team_member_details

        get_all_project_team_member_details \
            >> get_children_task_details >> get_tasks_from_project >> tasks_from_project_collection >> dummy_process_billing_keys
        query_task_list >> for_each_billing_key_start
        for_each_billing_key_start >> for_each_billing_key_end
        for_each_billing_key_start >> check_if_task_present >> is_task_found >> rail.Label("Yes")\
            >> update_billing_key >> is_update_failed >> rail.Label("Yes") >> log_task_processing_failed >> for_each_billing_key_end
        is_update_failed >> rail.Label("No") >> can_log_success >> rail.Label("Yes") >> log_success >> for_each_billing_key_end
        is_task_found >> rail.Label("No") >> create_billing_key >> is_update_failed

        for_each_billing_key_end >> get_all_filter_defination\
            >> get_all_columns >> get_all_child_wbs_details >> \
                has_any_child_wbs_to_process >> rail.Label("Yes") >> process_child_wbs >> rail.Label(
            'On Error') >> catch_and_log_errors >> log_to_sumo\
                >> rail.Label("Checking Failure for reprocessing") >> is_run_for_reprocess_and_failed >> fail_reprocess_dag_run
        has_any_child_wbs_to_process >> rail.Label("No") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_attribute_1_process_wbs_child_dag)
