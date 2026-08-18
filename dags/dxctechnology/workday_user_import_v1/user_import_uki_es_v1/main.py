from datetime import timedelta
from functools import lru_cache
from pendulum import datetime
import pendulum
import rail
from os import path

from dxctechnology.workday_user_import_v1.user_import_uki_es_v1.tasks.get_all_required_fields import get_all_required_fields
from dxctechnology.workday_user_import_v1.user_import_uki_es_v1.utils.custom_methods import get_process_uki_es_user_data_config, cached_write_json_artifact, get_all_run_ids_callable, get_trigger_dag_id, get_item_index

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_uki_es_data_child_dag,
        description="DXC Technology Workday User Sync UK&I CSC Data Child",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        company_key=config.company_key,
        start_date=datetime(2025, 4, 1),
        max_active_runs=1,
        default_args={
            "sftp_conn_id": config.sftp_connection_id
        }
    ) as dag:

        # Monitor for new file arrival
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.input_file_path,
            soft_fail_timeout=timedelta(minutes=10)
        )

        # Check if file is CSV format
        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        # Send email for bad file format
        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon user import for workday UK&I CSC - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        # Download the file from SFTP
        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        # Check if new file was found
        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        # Archive the processed file
        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_file_path +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        # Delete DAG run if no new file
        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        # Load CSV data
        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}",
            encoding="utf-8-sig"
        )

        # Create supervisor log
        create_supervisor_log = rail.CreateLogOperator(
            task_id="create_supervisor_log"
        )

        # Create collection for UK&I CSC user data
        create_uki_es_user_data_collection = rail.CreateCollectionOperator(
            task_id="create_uki_es_user_data_collection",
            source="{{result('load_data')}}",
            name="raw_user_data",
            columns={
                "empid": "empid",
                "pernerid": "pernerid",
                "email": "email",
                "firstname": "firstname",
                "lastname": "lastname",
                "country": "country",
                "state": "state",
                "exempt": "exempt",
                "exempteffectivedate": "exempteffectivedate",
                "employeetype": "employeetype",
                "hiredate": "hiredate",
                "gender": "gender",
                "servicedate": "servicedate",
                "termdate": "termdate",
                "status": "status",
                "onleave": "onleave",
                "companycode": "companycode",
                "companyname": "companyname",
                "areacode": "areacode",
                "areaname": "areaname",
                "subareacode": "subareacode",
                "empgroupcode": "empgroupcode",
                "empgroupname": "empgroupname",
                "empsubgroupcode": "empsubgroupcode",
                "empsubgroupname": "empsubgroupname",
                "supervisorid": "supervisorid",
                "supervisordate": "supervisordate",
                "supervisorfname": "supervisorfname",
                "supervisorlname": "supervisorlname",
                "supervisoremail": "supervisoremail",
                "paygroup": "paygroup",
                "locationeffectivedate": "locationeffectivedate",
                "homecountry": "homecountry",
                "costcenter": "costcenter",
                "costcentername": "costcentername",
                "costcentereffectivedate": "costcentereffectivedate",
                "orgcode": "orgcode",
                "orgname": "orgname",
                "workshift": "workshift",
                "workshifteffectivedate": "workshifteffectivedate",
                "joblevel": "joblevel",
                "jobchangeeffectivedate": "jobchangeeffectivedate",
                "fte": "fte",
                "ftepct": "ftepct",
                "isia": "isia",
                "iastartdate": "iastartdate",
                "iaenddate": "iaenddate",
                "rut": "rut",
                "middlename": "middlename",
                "timetype": "timetype",
                "dob": "dob",
                "managementlvl": "managementlvl",
                "ausjc": "ausjc",
                "termsconditions": "termsconditions",
                "industrialinstrumentclassification": "industrialinstrumentclassification",
                "additionaldataeffectivedate": "additionaldataeffectivedate",
                "terminationreason": "terminationreason",
                "scheduledweeklyhours": "scheduledweeklyhours",
                "assignmenttype": "assignment_type",
                "homestate": "home_state",
                "countrytouse": "countrytouse",
                "statetouse": "statetouse",
                "Work_City": "workcity",
                "Marital_Status_Ind": "marital_status_ind",
                "Marital_Status_efft_dt": "marital_status_efft_dt",
                
                # Four new UK&I specific fields
                "Additional_Job_Classifications": "additionaljobclassifications",
                "Holiday_Schedule_Calendar": "holidayschedulecalendar",
                "Employee_Representative_Status": "employeerepresentativestatus",
                "Employee_Representative_Effective_Date": "employeerepresentativeeffectivedate",
                
                # NEW: Default Weekly Hours OEF for overtime users
                "Default_Weekly_Hours": "defaultweeklyhours"
            }
        )

        # Get data from Replicon needed for processing
        get_data_start, get_data_end = get_all_required_fields("get_replicon_details", config)

        # Filter for records with valid UK&I CSC company codes
        get_valid_uki_es_data = rail.QueryCollectionOperator(
            task_id="get_valid_uki_es_data",
            query="""SELECT *, ROW_NUMBER() OVER(ORDER BY ROWID) as user_record_index FROM raw_user_data rd WHERE
                    UPPER(rd.companycode) IN ("IEEU", "IEES", "GBA5", "GBC5")""",
            name="valid_uki_es_data"
        )

        has_any_valid_data = rail.IfOperator(
            task_id = "has_any_valid_data",
            test=lambda: rail.result(get_valid_uki_es_data.task_id, 'length') > 0,
            yes_task= "process_users_start",
            no_task="gather_all_logs"
        )

        process_users_start = rail.EmptyOperator(
            task_id = "process_users_start"
        )

        # Process user records in parallel batches
        process_users = rail.trigger_parallel_dagrun(
            task_id="process_users",
            items="{{result('get_valid_uki_es_data')}}",
            trigger_dag_id=lambda item: get_trigger_dag_id(config.workday_user_import_process_uki_es_user_records_child_dag,
                                        config.DAG_BATCH_COUNT,
                                        item_index=get_item_index(None, config.DAG_BATCH_COUNT, item=item, use_item=True)),
            parallel_count=config.DAG_BATCH_COUNT,
            execution_timeout=timedelta(days=1),
            conf=lambda dag_run, item: get_process_uki_es_user_data_config(dag_run, item, config)
        )

        get_all_run_ids = rail.PythonOperator(
            task_id = "get_all_run_ids",
            python_callable = lambda: get_all_run_ids_callable('process_users', config.DAG_BATCH_COUNT),
        )

        supervisor_log_has_any_data = rail.IfOperator(
            task_id = "supervisor_log_has_any_data",
            test=lambda: len(rail.load_all_records(rail.result('create_supervisor_log'))) > 0,
            yes_task="process_supervisor_assignment_start",
            no_task="gather_all_logs"
        )

        process_supervisor_assignment_start = rail.EmptyOperator(
            task_id = "process_supervisor_assignment_start"
        )

        @lru_cache(maxsize=8)
        def get_supervisor_assignment_data():
            return {
                    "employee_type_data": cached_write_json_artifact('get_all_employeegroup_data'),
                    "division_data": cached_write_json_artifact('get_all_companycode_data'),
                }

        process_supervisor_assignment = rail.trigger_parallel_dagrun(
            task_id = "process_supervisor_assignment",
            items="{{result('create_supervisor_log')}}",
            trigger_dag_id=config.workday_user_import_process_supervisor_assignment,
            parallel_count=10,
            execution_timeout = timedelta(days=14),
            conf=lambda item: {
                **get_supervisor_assignment_data(),
                **{
                    "file_name": path.split(rail.result("new_file_sensor"))[1],
                    "user_uri": item['properties']["user_uri|country"].split('|')[0],
                },
                **item['properties']
            }

        )

        process_supervisor_assignment_end = rail.EmptyOperator(
            task_id = "process_supervisor_assignment_end"
        )

        # Gather logs from all child DAGs
        gather_all_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_all_logs",
            dagrun_task_id="create_user_log",
            dag_runs="{{result('get_all_run_ids')}}",
            flatten=True
        )

        def get_log_details():
            current_time = pendulum.now()
            log_timestamp = current_time.strftime("%y%m%dT%H%M%S")
            email_body_subject_timestamp = current_time.strftime("%y-%m-%dT%H:%M:%S")
            return {
                "current_time_used": current_time.isoformat(),
                "log_timestamp": log_timestamp,
                "email_body_subject_timestamp": email_body_subject_timestamp,
                "log_filename": f"log_{rail.render_template('''{{result('new_file_sensor') | file_base}}''')}_{log_timestamp}.csv"
            }

        # Generate logs
        generate_logs = rail.TriggerDagRunOperator(
            task_id="generate_logs",
            trigger_dag_id=config.workday_user_import_uki_es_log_generation_dag,
            wait_for_completion=True,
            poke_interval=5,
            conf=lambda: {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                'logs': rail.result("gather_all_logs"),
                "total_record_count": rail.result("create_uki_es_user_data_collection", "length") if rail.result("create_uki_es_user_data_collection", "length") else 0,
                "location": "UK&I ES",
                "location_code": "uki_es",
                **get_log_details()
            }
        )

        # End task
        process_end = rail.EmptyOperator(
            task_id="process_end",
            trigger_rule="all_done"
        )

        # Set up task dependencies
        new_file_sensor >> is_csv
        is_csv >> download_file >> load_data
        is_csv >> send_bad_file_format_email
        
        load_data >> create_supervisor_log >> create_uki_es_user_data_collection >> get_data_start
        get_data_end >> get_valid_uki_es_data >> has_any_valid_data
        
        has_any_valid_data >> process_users_start >> process_users >> get_all_run_ids >> supervisor_log_has_any_data >> rail.Label("No") >> gather_all_logs >> generate_logs
        supervisor_log_has_any_data >> rail.Label("Yes") >> process_supervisor_assignment_start >> process_supervisor_assignment >> process_supervisor_assignment_end >> gather_all_logs >> generate_logs
        has_any_valid_data >> gather_all_logs
        
        download_file >> archive_file
        generate_logs >> process_end
        
        new_file_sensor >> was_new_file_found >> delete_this_dagrun

    return dag

# Create DAGs for all instances
rail.for_each_instance(create_dag)