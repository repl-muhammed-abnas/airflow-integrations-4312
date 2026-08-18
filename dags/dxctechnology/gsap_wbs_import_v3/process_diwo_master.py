from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.gsap_wbs_import_v3.utils import python_callable_methods, request_payload


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_diwo_master_dagid,
        description='DXC_GSAP_WBS_Automation Process DIWO Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 4, 1, tz=config.est_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_gsap_diwo_master,
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.gsap_diwo_report_name,
        )

        run_report_generation = rail.run_report2(
            group_id="report_generation",
            report_params=lambda :{
                "reportParameters": [
                    {
                        "reportUri": rail.result("get_report_details")['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("report_generation.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="has_report_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('report_generation.get_report_result').reportGenerationResults[0].error}}"
        )

        has_report_data = rail.IfOperator(
            task_id="has_report_data",
            test='{{"No Data" in result("report_generation.get_report_result").reportGenerationResults[0].payload}}',
            yes_task="finish",
            no_task='report_has_expected_columns',
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string line-too-long
            test="{{ result('report_generation.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_gsap_diwo_report_columns,
            yes_task="report_payload_to_csv",
            no_task="fail_invalid_report_columns"
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv",
            document='{{result("report_generation.get_report_result").reportGenerationResults[0].payload}}'
        )

        base_report_collection = rail.CreateCollectionOperator(
            task_id="base_report_collection",
            name="report_data",
            source="{{result('report_payload_to_csv')}}",
            columns={
                'Project Name': 'project_name',
                'Project Uri': 'project_uri',
                'Project Type': 'gsap_project_type',
                'Sold to Party': 'sold_to_party',
                'Controlling Area': 'controlling_area',
                'Parent Controlling Area': 'parent_controlling_area',
                'WBS Type': 'wbs_type',
            }
        )

        get_required_wbs_to_process = rail.PythonOperator(
            task_id = "get_required_wbs_to_process",
            python_callable= python_callable_methods.get_required_wbs_to_process
        )

        has_any_wbs_to_process = rail.IfOperator(
            task_id = 'has_any_wbs_to_process',
            test=lambda: bool(rail.result('get_required_wbs_to_process')),
            yes_task='get_all_object_extension_fields',
            no_task='finish'
        )

        get_all_object_extension_fields = rail.RepliconServiceOperator(
            task_id="get_all_object_extension_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"},
            data_handler=lambda oefs: {
                'wbstypeuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'WBS Type', 'uri'),
            },
        )

        get_oef_drop_down_values_wbs_type = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_wbs_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').wbstypeuri }}"},
        )

        update_wbs_type_oef = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_wbs_type_oef',
            items=lambda: rail.result('get_required_wbs_to_process'),
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.update_wbs_type_oef,
            execution_timeout=timedelta(
                hours=config.execution_timeout_hrs)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info =lambda:{
                'wbs_processesed': rail.result('get_required_wbs_to_process')
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id = "can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task= "fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id = "fail_dagrun",
            message='{{ get_error_message() }}'
        )

        get_report_details >> run_report_generation >> is_report_failed >> rail.Label('Yes') >> fail_report_generation
        is_report_failed >> rail.Label('No') >> has_report_data >> rail.Label('No') >> finish
        has_report_data >> rail.Label('Yes') >> report_has_expected_columns >> rail.Label('No') >> fail_invalid_report_columns
        report_has_expected_columns >> rail.Label('Yes') >> report_payload_to_csv >> base_report_collection
        base_report_collection >> get_required_wbs_to_process >> has_any_wbs_to_process >> rail.Label('No') >> finish
        has_any_wbs_to_process >> rail.Label('Yes') >> get_all_object_extension_fields >> get_oef_drop_down_values_wbs_type
        get_oef_drop_down_values_wbs_type >> update_wbs_type_oef >> finish >> log_to_sumo >> can_fail_dag
        can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_master_dag)
