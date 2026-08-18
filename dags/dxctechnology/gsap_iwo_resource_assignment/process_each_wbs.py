from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_iwo_resource_assignment.utils import request_payload
from dxctechnology.gsap_iwo_resource_assignment.utils import python_callable_method

null = None

# pylint: disable=too-many-statements


def create_attribute_1_process_wbs_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_iwo_resource_process_wbs_{config.dag_id_postfix}',
        description=f'DXC_Compass_GSAP IWO Resource Child - Process each WBS V1.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='catch_and_log_errors',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
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
            no_task='log_wbs_not_available'
        )

        log_wbs_not_available = rail.WriteLogOperator(
            task_id='log_wbs_not_available',
            log='{{ result("create_log") }}',
            message='Failed to sync, since WBS not available in Replicon',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'taskcode': '',
                'action': 'skipped',
                'status': 'Exception',
                'details': 'Failed to sync, since WBS not available in Replicon',
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

        check_wbs_is_archived = rail.IfOperator(
            task_id='check_wbs_is_archived',
            test=lambda: rail.result('get_project_details_based_on_wbs')[
                'status']['name'] == 'Archived',
            yes_task='log_wbs_is_archived',
            no_task='get_project_date_range',
        )

        log_wbs_is_archived = rail.WriteLogOperator(
            task_id='log_wbs_is_archived',
            log='{{ result("create_log") }}',
            message='Gsap IWO Resource Assignment Sync skipped, since this WBS is in Archive status.',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'taskcode': '',
                'action': 'pre-check',
                'status': 'skipped',
                'details': 'Gsap IWO Resource Assignment skipped, since this WBS is in Archive status.'
            }
        )

        get_project_date_range = rail.PythonOperator(
            task_id='get_project_date_range',
            python_callable=lambda: python_callable_method.project_date_range(
                'get_project_details_based_on_wbs')
        )

        get_parent_project_name = rail.PythonOperator(
            task_id='get_parent_project_name',
            python_callable=lambda: python_callable_method.parent_project_name(
                'get_project_details_based_on_wbs')
        )

        get_parent_project_details = rail.RepliconServiceOperator(
            task_id='get_parent_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name":  "{{ result('get_parent_project_name') }}",
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": null}])[0]['projectDetails']
        )

        if_parent_project_present = rail.IfOperator(
            task_id="if_parent_project_present",
            test="{{result('get_parent_project_details') | is_truthy }}",
            yes_task="get_parent_project_division",
            no_task="log_parent_project_not_present"
        )

        log_parent_project_not_present = rail.WriteLogOperator(
            task_id='log_parent_project_not_present',
            log='{{ result("create_log") }}',
            message='Gsap/C1 Compass Resource Assignment Sync skipped, since the parent project is not available.',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'taskcode': '',
                'action': 'skipped',
                'status': 'skipped',
                'details': 'Gsap/C1 Compass Resource Assignment Sync skipped, since the parent project is not available.',
            }
        )

        get_parent_project_division = rail.PythonOperator(
            task_id='get_parent_project_division',
            python_callable=lambda: python_callable_method.parent_project_division(
                'get_parent_project_details')
        )

        is_parent_project_division_present = rail.IfOperator(
            task_id="is_parent_project_division_present",
            test="{{result('get_parent_project_division') | is_truthy }}",
            no_task="log_parent_project_division_not_available",
            yes_task="check_parent_project_division"
        )

        log_parent_project_division_not_available = rail.WriteLogOperator(
            task_id='log_parent_project_division_not_available',
            log='{{ result("create_log") }}',
            message='Failed to sync, since parent project division not available in Replicon',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'taskcode': '',
                'action': 'skipped',
                'status': 'Exception',
                'details': 'Gsap/C1 Compass Resource Assignment Sync skipped, since parent project division not available in Replicon',
            }
        )

        check_parent_project_division = rail.IfOperator(
            task_id="check_parent_project_division",
            test="{{result('get_parent_project_division') | matches(['C1','Compass']) }}",
            no_task="process_gsap_assignment",
            yes_task="process_c1_compass_assignment"
        )

        process_c1_compass_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='process_c1_compass_assignment',
            retries=0,
            items=["one_run"],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsab_iwo_resource_c1_compass_assignment_{config.dag_id_postfix}',
            conf=request_payload.get_c1_compass_conf)

        wait_for_process_c1_compass_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_c1_compass_assignment',
            dag_runs='{{ result("process_c1_compass_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        process_gsap_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='process_gsap_assignment',
            retries=0,
            items=["one_run"],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsab_iwo_resource_gsap_assignment_{config.dag_id_postfix}',
            conf=request_payload.get_c1_compass_conf
        )

        wait_for_process_gsap_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_gsap_assignment',
            dag_runs='{{ result("process_gsap_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ result("create_log") }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )

        def get_log_to_sumo_extra_conf(dag_run):
            def get_message():
                get_project_details_task_xcom = rail.result('get_project_details_based_on_wbs') or []
                is_wbs_found = bool(get_project_details_task_xcom and get_project_details_task_xcom['uri'])
                if not is_wbs_found:
                    return "WBS logged for reprocessing"
                return "WBS processed successfully"

            return {
                "Integration": "GSAP IWO Resources",
                "wbs_reprocessed_count": int(dag_run.conf.get("reprocess_count", 0)),
                "wbs": dag_run.conf['wbs'],
                "message": get_message(),
                "resource_count": 1
            }

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=get_log_to_sumo_extra_conf
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> create_log
        create_log >> get_project_details_based_on_wbs >> check_wbs_exists

        check_wbs_exists >> rail.Label(
            'Yes') >> log_wbs_not_available >> \
                get_reprocess_log >> log_wbs_record_for_reprocessing >> rail.Label("On Error") >> catch_and_log_errors
        check_wbs_exists >> rail.Label('No') >> check_wbs_is_archived

        check_wbs_is_archived >> rail.Label(
            'Yes') >> log_wbs_is_archived >> catch_and_log_errors
        check_wbs_is_archived >> rail.Label(
            'No') >> get_project_date_range >> get_parent_project_name >> get_parent_project_details >> if_parent_project_present

        if_parent_project_present >> rail.Label(
            "Yes") >> get_parent_project_division
        if_parent_project_present >> rail.Label(
            "No") >> log_parent_project_not_present >> catch_and_log_errors

        get_parent_project_division >> is_parent_project_division_present >> rail.Label(
            "Yes") >> check_parent_project_division
        is_parent_project_division_present >> rail.Label(
            "No") >> log_parent_project_division_not_available >> catch_and_log_errors

        check_parent_project_division >> rail.Label(
            'Yes') >> process_c1_compass_assignment >> wait_for_process_c1_compass_assignment >> catch_and_log_errors
        check_parent_project_division >> rail.Label(
            'No') >> process_gsap_assignment >> wait_for_process_gsap_assignment >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_wbs_child_dag)
