import itertools
from datetime import timedelta, datetime as dt
from pendulum import datetime
import rail
from dxctechnology.gsap_task_import_project_fields.utils import request_payload
from dxctechnology.gsap_task_import_project_fields.utils import response_filter
from dxctechnology.gsap_task_import_project_fields.utils.python_callable_method import do_format_task_logs
from airflow.models import Variable

#pylint: disable=too-many-statements
def create_child_sync_attribute_1_2_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_project_field_task_import_process_each_wbs_{config.instance}',
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
            end_task="should_process_tasks",
        )

        create_wbs_log = rail.CreateLogOperator(
            task_id = "create_wbs_log"
        )

        start_of_year = datetime(year=dt.now().year, month=1, day=1).strftime("%d.%m.%Y")

        #pylint: disable=line-too-long
        query_gsap_task_records_for_wbs = rail.QueryCollectionOperator(
            task_id="query_gsap_task_records_for_wbs",
            name="wbsattributeentries",
            query=f"""SELECT * FROM valid_input_records WHERE WBS = :WBS
            AND date(substr(task_end_date, 7, 4) || '-' || substr(task_end_date, 4, 2) || '-' || substr(task_end_date, 1, 2), 'start of day') > date(substr('{start_of_year}', 7, 4) || '-' || substr('{start_of_year}', 4, 2) || '-' || substr('{start_of_year}', 1, 2), 'start of day')""",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}"
            }
        )

        query_invalid_end_date_data_for_wbs = rail.QueryCollectionOperator(
            task_id = "query_invalid_end_date_data_for_wbs",
            query=f"""SELECT * FROM valid_input_records WHERE WBS = :WBS
            AND date(substr(task_end_date, 7, 4) || '-' || substr(task_end_date, 4, 2) || '-' || substr(task_end_date, 1, 2), 'start of day') <= date(substr('{start_of_year}', 7, 4) || '-' || substr('{start_of_year}', 4, 2) || '-' || substr('{start_of_year}', 1, 2), 'start of day')""",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}"
            }
        )

        log_task_end_date_is_prior = rail.WriteLogOperator(
            task_id='log_task_end_date_is_prior',
            message="Task End date is prior to year start date",
            items="{{result('query_invalid_end_date_data_for_wbs')}}",
            log="{{result('create_wbs_log')}}",
            properties={
                'Level': "Project",
                'wbs': '{{dag_run.conf.wbs}}',
                'task_name': "{{ item.task_name }}",
                'task_code': "{{ item.task_code }}",
                'action': 'Skipped',
                'status': "Exception",
                'recordcount': "{{result('query_invalid_end_date_data_for_wbs','length')}}",
            }
        )

        has_any_task_to_process = rail.IfOperator(
            task_id = "has_any_task_to_process",
            test="{{ result('query_gsap_task_records_for_wbs', 'length') > 0}}",
            yes_task= "get_project_details",
            no_task="should_process_tasks"
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
            yes_task="get_all_columns",
            no_task="log_wbs_not_present",
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="services/ProjectListService1.svc/GetAllColumns",
            data={},
            response_filter=response_filter.map_parent_column_uri
        )

        get_all_filter_defination = rail.RepliconServiceOperator(
            task_id="get_all_filter_defination",
            endpoint="services/ProjectListService1.svc/GetAllFilterDefinitions",
            data={},
            response_filter=response_filter.map_parent_wbs_oef_uri
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
            trigger_dag_id=f'dxctechnology_gsap_project_field_task_import_sync_child_wbs_{config.instance}',
            conf=lambda item, dag_run: {
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
            test=lambda : (
                        (rail.result("query_gsap_task_records_for_wbs", 'length') > 0 )
                        and len(rail.result('get_project_details')) > 0
                        and (bool(rail.result('get_project_details')[0]['start_date_year']))),
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
            yes_task="sync_each_gsap_task_start",
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

        sync_each_gsap_task_start  = rail.EmptyOperator(
            task_id = "sync_each_gsap_task_start"
        )

        sync_each_gsap_task = rail.trigger_parallel_dagrun(
            task_id='sync_each_gsap_task',
            parallel_count=config.parallel_dag_run_count,
            items="{{ result('query_gsap_task_records_for_wbs') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_project_field_task_import_sync_each_gsap_task_child_{config.instance}',
            conf=lambda item, dag_run: {
                'WBS': item['wbs'],
                "log":rail.result('create_wbs_log'),
                "wbs_type": "parent",
                'task_name': item['task_name'],
                'task_code': item['task_code'] if item['task_code'] else "",
                "task_start_date": item['task_start_date'],
                'task_end_date': item['task_end_date'],
                'gsap_task_uri': dag_run.conf['gsap_task_uri'],
                'start_date_year': rail.result('get_project_details')[0]['start_date_year'],
                'start_date_month': rail.result('get_project_details')[0]['start_date_month'],
                'start_date_day': rail.result('get_project_details')[0]['start_date_day'],
                'end_date_year': rail.result('get_project_details')[0]['end_date_year'],
                'end_date_month': rail.result('get_project_details')[0]['end_date_month'],
                'end_date_day': rail.result('get_project_details')[0]['end_date_day'],
            }
        )

        starts_gather_logs = rail.EmptyOperator(
            task_id = "starts_gather_logs"
        )

        get_sync_gsap_task_child_dag_ids =rail.PythonOperator(
            task_id= 'get_sync_gsap_task_child_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'sync_each_gsap_task_{x+1}'), range(config.parallel_dag_run_count))))),
            show_return_value_in_logs= False
        )

        gather_each_task_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_each_task_logs',
            dag_runs='{{ result("get_sync_gsap_task_child_dag_ids") }}',
            dagrun_task_id='create_task_log',
            execution_timeout=timedelta(
                hours=config.execution_timeout_hours),
            flatten=True
        )

        format_task_logs = rail.PythonOperator(
            task_id = "format_task_logs",
            python_callable=do_format_task_logs,
            op_args=[gather_each_task_logs.task_id]
        )

        log_to_wbs_log = rail.WriteLogOperator(
            task_id = "log_to_wbs_log",
            log="{{result('create_wbs_log')}}",
            items=lambda: rail.result("format_task_logs"),
            message=lambda item: item['message'],
            severity=lambda item: item['severity'],
            properties= lambda item: {
                'Level': item['properties']['Level'],
                'wbs': item['properties']['wbs'],
                'task_name': item['properties']['task_name'],
                'task_code': item['properties']['task_code'],
                'action': item['properties']['action'],
                'status':item['properties']['status'],
                'recordcount': item['properties']['recordcount'],
            },
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
                get_project_details_task_xcom = rail.result('get_project_details') or {}
                is_wbs_found = bool(get_project_details_task_xcom and get_project_details_task_xcom[0]['uri'])
                if not is_wbs_found:
                    return "WBS logged for reprocessing"
                return "WBS processed successfully"

            return {
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

        is_run_for_reprocess = rail.IfOperator(
            task_id = "is_run_for_reprocess_and_failed",
            # when the master(gsap_task_master) trigger the dag, the conf will not have reprocess_count
            test = "{{ dag_run.conf | attr_or_default('reprocess_count', 'NA') != 'NA'}}",
            yes_task = "should_check_logs"
        )

        should_check_logs = rail.IfOperator(
            task_id = 'should_check_logs',
            test="{{get_task_state('sync_each_gsap_task_start') | lower == 'success'}}",
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
            should_process_tasks >> rail.Label("Yes") >> dummy_is_wbs_in_progress >> is_wbs_in_progress
        can_run_batch_task >> rail.Label("No") >> create_wbs_log >> query_gsap_task_records_for_wbs
        should_process_tasks >> rail.Label("No") >> catch_and_log_errors
        query_gsap_task_records_for_wbs >> query_invalid_end_date_data_for_wbs >> log_task_end_date_is_prior >> has_any_task_to_process >> rail.Label(
            "Yes") >> get_project_details
        has_any_task_to_process >> rail.Label("No") >> should_process_tasks
        get_project_details >> is_wbs_present
        is_wbs_present >> rail.Label(
            "NO") >> log_wbs_not_present >> get_reprocess_log >> log_wbs_record_for_reprocessing >> should_process_tasks
        is_wbs_present >> rail.Label(
            "YES") >> get_all_columns >> get_all_filter_defination >> get_all_child_wbs_details >> is_child_wbs_present
        is_child_wbs_present >> rail.Label(
            "YES") >> sync_child_wbs >> is_wbs_start_date_empty
        is_child_wbs_present >> rail.Label("NO") >> is_wbs_start_date_empty
        is_wbs_start_date_empty >> rail.Label("YES") >> should_process_tasks
        is_wbs_start_date_empty >> rail.Label(
            "NO") >> log_wbs_start_date_empty >> should_process_tasks
        is_wbs_in_progress >> rail.Label(
            "NO") >> log_wbs_not_in_progress >> catch_and_log_errors >> log_to_sumo\
                >> rail.Label("Checking Failure for reprocessing") >> is_run_for_reprocess
        is_run_for_reprocess >> rail.Label("Yes") >> should_check_logs >> rail.Label("Yes") >> get_failure_logs \
            >> has_any_failures >> rail.Label("Yes") >> fail_reprocess_dag_run
        should_check_logs >> rail.Label("No") >> check_error_message >> rail.Label("Yes") >> fail_reprocess_dag_run
        is_wbs_in_progress >> rail.Label(
            "YES") >> sync_each_gsap_task_start
        sync_each_gsap_task_start >> sync_each_gsap_task  >> starts_gather_logs >> \
            get_sync_gsap_task_child_dag_ids >> gather_each_task_logs >> format_task_logs >> log_to_wbs_log >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_sync_attribute_1_2_dag)
