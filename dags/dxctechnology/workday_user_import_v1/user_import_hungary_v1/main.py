from datetime import timedelta
from functools import lru_cache
from pendulum import datetime
import pendulum
import rail
from os import path
from dxctechnology.workday_user_import_v1.user_import_hungary_v1.utils.custom_methods import get_all_run_ids_callable

from dxctechnology.workday_user_import_v1.user_import_hungary_v1.tasks.get_all_required_fields import get_all_required_fields
from dxctechnology.workday_user_import_v1.user_import_hungary_v1.utils.custom_methods import get_process_hungary_user_data_config, cached_write_json_artifact, get_trigger_dag_id, get_item_index

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_hungary_main_dag,
        description=config.workday_user_import_hungary_main_dag_description,
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_run_master,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        # SFTP File Sensor - Monitor for new files
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
            path=config.input_file_path
        )

        # Validate file format
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
            cc=config.internal_logs_email,
            subject='{{config.company_key}} | Replicon User Import for Workday Hungary  - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        # Download the file
        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        # Archive the original file
        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=f'{config.archive_file_path}/{{{{ dag_run_ecid() }}}}_{{{{ result("new_file_sensor") | file_name }}}}'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')


        # Load CSV data
        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document='{{ result("download_file") }}',
            encoding="utf-8-sig"
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id = "create_supervisor_log"
        )

        # Create collection for Hungary user data
        create_hun_user_data_collection = rail.CreateCollectionOperator(
            task_id="create_hun_user_data_collection",
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
                "Work_City":"workcity",
                "Marital_Status_Ind":"marital_status_ind",
                "Marital_Status_efft_dt": "marital_status_efft_dt"
                }
        )

        def get_updated_country_state_based_on_ia(item):
            if not item:
                return []
            country_to_use = item['country']
            state_to_use = item['state']
            if item['isia']:
                if item['isia'] in [1,'1']:
                    if  "home pay" in item['assignment_type'].lower():
                        country_to_use = item['homecountry']
                        state_to_use = item['home_state']

            re_val = {
                'empid': item['empid'],
                'pernerid': item['pernerid'],
                'email': item['email'],
                'firstname': item['firstname'],
                'lastname': item['lastname'],
                'country': item['country'],
                'state': item['state'],
                'exempt': item['exempt'],
                'exempteffectivedate': item['exempteffectivedate'],
                'employeetype': item['employeetype'],
                'hiredate': item['hiredate'],
                'gender': item['gender'],
                'servicedate': item['servicedate'],
                'termdate': item['termdate'],
                'status': item['status'],
                'onleave': item['onleave'],
                'companycode': item['companycode'],
                'companyname': item['companyname'],
                'areacode': item['areacode'],
                'areaname': item['areaname'],
                'subareacode': item['subareacode'],
                'empgroupcode': item['empgroupcode'],
                'empgroupname': item['empgroupname'],
                'empsubgroupcode': item['empsubgroupcode'],
                'empsubgroupname': item['empsubgroupname'],
                'supervisorid': item['supervisorid'],
                'supervisordate': item['supervisordate'],
                'supervisorfname': item['supervisorfname'],
                'supervisorlname': item['supervisorlname'],
                'supervisoremail': item['supervisoremail'],
                'paygroup': item['paygroup'],
                'locationeffectivedate': item['locationeffectivedate'],
                'homecountry': item['homecountry'],
                'costcenter': item['costcenter'],
                'costcentername': item['costcentername'],
                'costcentereffectivedate': item['costcentereffectivedate'],
                'orgcode': item['orgcode'],
                'orgname': item['orgname'],
                'workshift': item['workshift'],
                'workshifteffectivedate': item['workshifteffectivedate'],
                'joblevel': item['joblevel'],
                'jobchangeeffectivedate': item['jobchangeeffectivedate'],
                'fte': item['fte'],
                'ftepct': item['ftepct'],
                'isia': item['isia'],
                'iastartdate': item['iastartdate'],
                'iaenddate': item['iaenddate'],
                'rut': item['rut'],
                'middlename': item['middlename'],
                'timetype': item['timetype'],
                'dob': item['dob'],
                'managementlvl': item['managementlvl'],
                'ausjc': item['ausjc'],
                'termsconditions': item['termsconditions'],
                'industrialinstrumentclassification': item['industrialinstrumentclassification'],
                'additionaldataeffectivedate': item['additionaldataeffectivedate'],
                'terminationreason': item['terminationreason'],
                'scheduledweeklyhours': item['scheduledweeklyhours'],
                'assignment_type': item['assignment_type'],
                'home_state': item['home_state'],
                '_actual_country': item['country'],
                '_actual_state': item['state'],
                '_actual_home_country': item['homecountry'],
                '_actual_home_state': item['home_state'],
                '_country_to_use_for_query': country_to_use,
                '_state_to_use_for_query': state_to_use,
                'workcity': item['workcity'] if item['workcity'] is not None else '',
                "marital_status_ind": item["marital_status_ind"] if item['marital_status_ind'] is not None else '',
                "marital_status_efft_dt": item["marital_status_efft_dt"] if item['marital_status_efft_dt'] is not None else ''
            }
            return re_val

        updated_country_state_based_on_ia = rail.DataAdaptorOperator(
            task_id = "updated_country_state_based_on_ia",
            source="{{result('create_hun_user_data_collection')}}",
            columns=['empid', 'pernerid', 'email',
                     'firstname', 'lastname', 'country', 'state', 'exempt', 'exempteffectivedate',
                     'employeetype', 'hiredate', 'gender', 'servicedate', 'termdate', 'status', 'onleave',
                     'companycode', 'companyname', 'areacode', 'areaname', 'subareacode', 'empgroupcode',
                     'empgroupname', 'empsubgroupcode', 'empsubgroupname', 'supervisorid', 'supervisordate', 'supervisorfname',
                     'supervisorlname', 'supervisoremail', 'paygroup', 'locationeffectivedate', 'homecountry', 'costcenter', 'costcentername',
                     'costcentereffectivedate', 'orgcode', 'orgname', 'workshift', 'workshifteffectivedate', 'joblevel', 'jobchangeeffectivedate',
                     'fte', 'ftepct', 'isia', 'iastartdate', 'iaenddate', 'rut', 'middlename', 'timetype', 'dob', 'managementlvl', 'ausjc',
                     'termsconditions', 'industrialinstrumentclassification', 'additionaldataeffectivedate', 'terminationreason', 'scheduledweeklyhours',
                     'assignment_type', 'home_state', '_actual_country' ,'_actual_state', '_country_to_use_for_query', '_state_to_use_for_query', 'workcity', 'marital_status_ind', 'marital_status_efft_dt'],
            data=get_updated_country_state_based_on_ia
        )

        create_input_collection2 = rail.CreateCollectionOperator(
            task_id = "create_input_collection2",
            source="{{result('updated_country_state_based_on_ia')}}",
            name = "hungary_data",
        )

        get_data_start, get_data_end = get_all_required_fields("get_replicon_details", config)
        
         # Filter for records with company_code values HU00
        get_valid_hungary_data = rail.QueryCollectionOperator(
            task_id="get_valid_hungary_data",
            query="""SELECT *, ROW_NUMBER() OVER(ORDER BY ROWID) as user_record_index FROM hungary_data pdr WHERE
                    LOWER(pdr.companycode) = 'hu00' and LOWER(pdr._country_to_use_for_query) == 'hungary'""",
            name="valid_hungary_data"
        )

        has_any_valid_data = rail.IfOperator(
            task_id = "has_any_valid_data",
            test=lambda: rail.result(get_valid_hungary_data.task_id, 'length') > 0,
            yes_task= "process_users_start",
            # no_task is not added as the workato master / airflow master (../user_import/main.py)
            # will take care of everything
        )

        process_users_start = rail.EmptyOperator(
            task_id = "process_users_start"
        )

        def get_trigger_dag_id(trigger_dag_id, max_dag_batch_count, item_index):
            batch_number = (item_index % max_dag_batch_count) + 1
            prefix = f"_{batch_number}"
            if batch_number == 1:
                prefix = ""
            if batch_number not in range(1, max_dag_batch_count+1):
                raise Exception("Batch number is outside of max batch count")
            return f"{trigger_dag_id}{prefix}"

        
       # Process each valid user
        process_users = rail.trigger_parallel_dagrun(
            task_id="process_users",
            items="{{result('get_valid_hungary_data')}}",
            trigger_dag_id=lambda item: get_trigger_dag_id(
                config.workday_user_import_hungary_process_users_child_dag,
                config.DAG_BATCH_COUNT,
                item_index=int(item['user_record_index'])
            ),
            parallel_count=config.process_users_parallel_count,
            execution_timeout=timedelta(days=1),
            conf=lambda dag_run, item: get_process_hungary_user_data_config(dag_run, item, config)
        )

        get_all_run_ids = rail.PythonOperator(
            task_id = "get_all_run_ids",
            python_callable = lambda: get_all_run_ids_callable('process_users', config.process_users_parallel_count),
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

        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id = "trigger_log_generation",
            trigger_dag_id=config.process_log_generation_dagid_phl,
            conf=lambda: {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                'logs': rail.result("gather_all_logs"),
                "total_record_count": rail.result("create_hun_user_data_collection", "length") if rail.result("create_hun_user_data_collection", "length") else 0,
                "location": "Hungary",
                "location_code": "HUN",
                **get_log_details(),

            }
        )

        new_file_sensor >> is_csv >> rail.Label("Yes") >> download_file >> archive_file >> load_data
        is_csv >> rail.Label("No") >> send_bad_file_format_email
        download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        load_data >> create_supervisor_log >> create_hun_user_data_collection >> updated_country_state_based_on_ia >> create_input_collection2 >> get_data_start
        get_data_end >> get_valid_hungary_data

        get_valid_hungary_data >> has_any_valid_data >> rail.Label("Yes") >> process_users_start >> process_users
        has_any_valid_data >> rail.Label("No") >> trigger_log_generation

        process_users >> get_all_run_ids >> supervisor_log_has_any_data >> rail.Label("No") >> gather_all_logs >> trigger_log_generation
        supervisor_log_has_any_data >> rail.Label("Yes") >> process_supervisor_assignment_start >> process_supervisor_assignment >> process_supervisor_assignment_end >> gather_all_logs
    
    return dag

rail.for_each_instance(create_dag)
