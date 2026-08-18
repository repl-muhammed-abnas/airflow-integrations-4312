
from datetime import timedelta
from pendulum import datetime as dt
import rail
from nber.project_import.utils import custom_methods
from nber.project_import.tasks.process_log_generation import process_log_task_group

null = None

def create_dag(config):
    """
    Master DAG for NBER Grant Import v1 (webhook-driven, no encoding).
    Validates webhook payload, validates each grant row, logs invalids,
    and triggers parallel child DAG runs for valid grants.
    """

    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f"Grant Import v1 Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")


        has_valid_payload = rail.IfOperator(
            task_id="has_valid_payload",
            test=lambda dag_run: bool(
                dag_run.conf.get("project_data")
            ),
            yes_task="create_input_collection",
            no_task="fail_missing_payload",
        )


        fail_missing_payload = rail.FailOperator(
            task_id="fail_missing_payload",
            message=(
                "Invalid webhook payload"
            ),
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection", 
            source=lambda dag_run: 
                dag_run.conf["project_data"]
            ,
            name="create_input_collection",
            columns={
                "Grant_Name":"grant_name",
                "Grant_Code":"grant_code",
                "Grant_Start_Date":"grant_start_date",
                "Grant_End_Date":"grant_end_date",
                "Cost_category":"cost_category",
                "Funding_Source":"funding_source",
                "Program":"program_name",
                "Grant_Status":"grant_status",
                "Award_Number":"award_number",
                "CFDA_Number":"cfda_number",
                "Misc_Field_1":"misc_field_1",
                "Misc_Field_2":"misc_field_2",
                "Grant_Manager":"grant_manager"
            }
        )

        validate_new_grants = rail.DataAdaptorOperator(
            task_id="validate_new_grants",
            source="{{result('create_input_collection')}}",
            columns=[
                "grant_name",
                "grant_code",
                "grant_start_date",
                "grant_end_date",
                "cost_category",
                "funding_source",
                "program_name",
                "grant_status",
                "award_number",
                "cfda_number",
                "misc_field_1",
                "misc_field_2",
                "grant_manager",
                "is_valid",
                "validation_errors"
            ],
            data=custom_methods.validate_grant_row,
        )

        create_validated_collection = rail.CreateCollectionOperator(
            task_id="create_validated_collection",
            source="{{ result('validate_new_grants') }}",
            name="validated_grants",
        )

        valid_grants = rail.QueryCollectionOperator(
            task_id="valid_grants",
            query="SELECT * FROM validated_grants WHERE is_valid = TRUE",
        )

        invalid_grants = rail.QueryCollectionOperator(
            task_id="invalid_grants",
            query="SELECT * FROM validated_grants WHERE is_valid = FALSE",
        )

        create_project_log = rail.CreateLogOperator(task_id="create_project_log")

        write_invalid_csv = rail.WriteLogOperator(
            task_id="write_invalid_csv",
            log="{{ result('create_project_log') }}",
            items="{{ result('invalid_grants') }}",
            severity="Exception",
            message="Invalid grant",
            properties={
                "grant_name": "{{ item | attr_or_default('grant_name','') }}",
                "grant_code": "{{ item | attr_or_default('grant_code','') }}",
                "status": "Exception",
                "action": "Validation",
                "details": "{{ item.validation_errors }}",
            },
        )

        get_all_programs = rail.RepliconServiceOperator(
            task_id="get_all_programs",
            endpoint="/services/ProgramService1.svc/GetAllPrograms",
        )

        trigger_parallel_grant_processing = rail.trigger_parallel_dagrun(
            task_id="trigger_parallel_grant_processing",
            trigger_dag_id=config.process_project_dagid,
            items="{{ result('valid_grants') }}",
            conf=lambda item: {
                **item,
                "source": "webhook",
                "instance": config.instance,
                "program": rail.find_first_by_attr_and_get_attr(rail.result("get_all_programs"),
                 "name", item["program_name"], "name"),
            },
            execution_timeout=timedelta(days=14),
            parallel_count=config.parallel_trigger_count,
        )

        generate_processing_log = process_log_task_group(config)


        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test="{{ get_error_message() | is_truthy }}",
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message="{{ get_error_message() }}",
        )

        create_project_log >> has_valid_payload 

        has_valid_payload >> rail.Label("No") >> fail_missing_payload
        has_valid_payload >> rail.Label("Yes") >> create_input_collection

        create_input_collection >> validate_new_grants >> create_validated_collection
        create_validated_collection >> [valid_grants, invalid_grants]

        invalid_grants >> write_invalid_csv
        valid_grants >> get_all_programs >> trigger_parallel_grant_processing

        trigger_parallel_grant_processing \
            >> generate_processing_log \
            >> can_fail_dag

        can_fail_dag >> rail.Label("Yes") >> fail_dagrun


        return dag

rail.for_each_instance(create_dag)
