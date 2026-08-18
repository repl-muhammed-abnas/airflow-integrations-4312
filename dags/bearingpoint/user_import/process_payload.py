from datetime import timedelta
import itertools
from pendulum import datetime
from bearingpoint.user_import.utils import custom_methods
from bearingpoint.user_import.tasks.process_user_prereq_data import create_prerequisite_data
from bearingpoint.user_import.tasks.send_logs import get_send_logs
from airflow.models import Variable
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_payload_child_dag_id,
        description=f"BearingPoint User Import Process Payload Child {config.instance}",
        start_date=datetime(2024, 12, 18, tz=config.time_zone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_payload_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='logging_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='logging_details',
            end_task='finish_import',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.logging_details,
            op_args=[config.time_zone]
        )

        create_groups_log = rail.CreateLogOperator(
            task_id='create_groups_log'
        )

        create_users_payload_collection = rail.CreateCollectionOperator(
            task_id='create_users_payload_collection',
            source='{{ dag_run.conf.webhook.data | to_json }}',
            columns={
                "EmployeeID": "employee_id",
                "WorkforceID": "workforce_id",
                "FirstName": "first_name",
                "LastName": "last_name",
                "EmployeeEmail": "employee_email",
                "StartDate": "start_date",
                "EmployeeStatus": "employee_status",
                "TerminationDate": "termination_date",
                "EmployeeTypeCode": "employee_type_code",
                "EmployeeTypeName": "employee_type_name",
                "LocationCode": "location_code",
                "LocationName": "location_name",
                "DepartmentCode": "department_code",
                "DepartmentName": "department_name",
                "CompanyCode": "company_code",
                "CompanyCodeName": "company_code_name",
                "CostCenterCode": "costcenter_code",
                "CostCenterName": "costcenter_name",
                "Supervisor": "supervisor",
                "WorkSchedule": "work_schedule",
                "HolidayCalendar": "holiday_calendar"
            },
            name='users_data'
        )

        query_invalid_user_records = rail.QueryCollectionOperator(
            task_id="query_invalid_user_records",
            query="""SELECT * FROM users_data WHERE NULLIF("employee_id","") IS NULL OR
                NULLIF("workforce_id","") IS NULL OR NULLIF("first_name","") IS NULL OR
                NULLIF("last_name","") IS NULL OR NULLIF("employee_email","") IS NULL OR
                NULLIF("start_date","") IS NULL OR NULLIF("employee_status","") IS NULL OR
                NULLIF("employee_type_code","") IS NULL OR NULLIF("employee_type_name","") IS NULL OR
                NULLIF("location_code","") IS NULL OR NULLIF("location_name","") IS NULL OR
                NULLIF("department_code","") IS NULL OR NULLIF("department_name","") IS NULL OR
                NULLIF("company_code","") IS NULL OR NULLIF("company_code_name","") IS NULL OR
                NULLIF("costcenter_code","") IS NULL OR NULLIF("costcenter_name","") IS NULL OR
                NULLIF("supervisor","") IS NULL OR NULLIF("work_schedule","") IS NULL OR
                NULLIF("holiday_calendar","") IS NULL"""
        )

        if_invalid_user_records = rail.IfOperator(
            task_id="if_invalid_user_records",
            test='{{result("query_invalid_user_records", "length") > 0}}',
            yes_task="write_invalid_users_log",
            no_task="query_valid_user_records"
        )

        write_invalid_users_log = rail.WriteLogOperator(
            task_id="write_invalid_users_log",
            log='{{ result("create_groups_log") }}',
            items='{{result("query_invalid_user_records")}}',
            message="Invalid user data",
            severity="Exception",
            properties=lambda item: {
                "employeeid": item["employee_id"],
                "action": "",
                "status": "Exception",
                "details": custom_methods.get_invalid_user_log_details(item)
            }
        )

        query_valid_user_records = rail.QueryCollectionOperator(
            task_id="query_valid_user_records",
            query="""SELECT * FROM users_data WHERE NULLIF("employee_id","") IS NOT NULL AND
                NULLIF("workforce_id","") IS NOT NULL AND NULLIF("first_name","") IS NOT NULL AND
                NULLIF("last_name","") IS NOT NULL AND NULLIF("employee_email","") IS NOT NULL AND
                NULLIF("start_date","") IS NOT NULL AND NULLIF("employee_status","") IS NOT NULL AND
                NULLIF("employee_type_code","") IS NOT NULL AND NULLIF("employee_type_name","") IS NOT NULL AND
                NULLIF("location_code","") IS NOT NULL AND NULLIF("location_name","") IS NOT NULL AND
                NULLIF("department_code","") IS NOT NULL AND NULLIF("department_name","") IS NOT NULL AND
                NULLIF("company_code","") IS NOT NULL AND NULLIF("company_code_name","") IS NOT NULL AND
                NULLIF("costcenter_code","") IS NOT NULL AND NULLIF("costcenter_name","") IS NOT NULL AND
                NULLIF("supervisor","") IS NOT NULL AND NULLIF("work_schedule","") IS NOT NULL AND
                NULLIF("holiday_calendar","") IS NOT NULL""",
            name="valid_users_data"
        )

        if_valid_user_records = rail.IfOperator(
            task_id="if_valid_user_records",
            test='{{result("query_valid_user_records", "length") > 0}}',
            yes_task="groups_data",
            no_task="format_log_records"
        )

        groups_data = rail.EmptyOperator(task_id="groups_data")
        get_all_prerequisite_data = create_prerequisite_data(
            config)

        process_groups_creation = rail.EmptyOperator(
            task_id='process_groups_creation'
        )

        create_locations = rail.TriggerDagRunOperator(
            task_id="create_locations",
            trigger_dag_id=config.create_locations_child_dag_id,
            conf=lambda: {
                "groups_log_artifact": rail.result("create_groups_log")
            }
        )

        create_departments = rail.TriggerDagRunOperator(
            task_id="create_departments",
            trigger_dag_id=config.create_departments_child_dag_id,
            conf=lambda: {
                "groups_log_artifact": rail.result("create_groups_log")
            }
        )

        create_costcenters = rail.TriggerDagRunOperator(
            task_id="create_costcenters",
            trigger_dag_id=config.create_costcenters_child_dag_id,
            conf=lambda: {
                "groups_log_artifact": rail.result("create_groups_log")
            }
        )

        create_employeetypes = rail.TriggerDagRunOperator(
            task_id="create_employeetypes",
            trigger_dag_id=config.create_employeetypes_child_dag_id,
            conf=lambda: {
                "groups_log_artifact": rail.result("create_groups_log")
            }
        )

        create_servicecenters = rail.TriggerDagRunOperator(
            task_id="create_servicecenters",
            trigger_dag_id=config.create_servicecenters_child_dag_id,
            conf=lambda: {
                "groups_log_artifact": rail.result("create_groups_log")
            }
        )

        def gather_all_the_run_ids_callable():
            run_ids = []
            if rail.result(create_locations.task_id):
                run_ids.append(rail.result(create_locations.task_id))
            if rail.result(create_departments.task_id):
                run_ids.append(rail.result(create_departments.task_id))
            if rail.result(create_costcenters.task_id):
                run_ids.append(rail.result(create_costcenters.task_id))
            if rail.result(create_employeetypes.task_id):
                run_ids.append(rail.result(create_employeetypes.task_id))
            if rail.result(create_servicecenters.task_id):
                run_ids.append(rail.result(create_servicecenters.task_id))
            return run_ids
 
        gather_all_the_run_ids = rail.PythonOperator(
            task_id="gather_all_the_run_ids",
            python_callable=gather_all_the_run_ids_callable
        )

        wait_for_groups_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_groups_creation',
            dag_runs='{{ result("gather_all_the_run_ids") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        process_user_record = rail.TriggerDagRunForEachItemOperator(
            task_id="process_user_record",
            items='{{ result("query_valid_user_records") }}',
            trigger_dag_id=config.process_user_record_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=custom_methods.get_prereq_data_conf
        )

        wait_for_process_user_record = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_user_record',
            dag_runs='{{ result("process_user_record") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("process_user_record") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        format_log_records = rail.CreateCollectionOperator(
            task_id='format_log_records',
            source=custom_methods.do_format_logs,
            columns=["employeeid", "action", "status", "details", "runid"],
            name='user_logs'
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        finish_import = rail.EmptyOperator(
            task_id='finish_import'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish_import
        can_run_batch_task >> rail.Label(
            'No') >> logging_details >> create_groups_log >> create_users_payload_collection >> query_invalid_user_records >> if_invalid_user_records
        if_invalid_user_records >> rail.Label(
            "Yes") >> write_invalid_users_log >> query_valid_user_records
        if_invalid_user_records >> rail.Label(
            "No") >> query_valid_user_records >> if_valid_user_records

        if_valid_user_records >> rail.Label("Yes") >> groups_data >> get_all_prerequisite_data >> process_groups_creation \
            >> create_locations >> create_departments >> create_costcenters >> create_employeetypes \
                    >> create_servicecenters >> gather_all_the_run_ids >> wait_for_groups_creation \
                        >> process_user_record >> wait_for_process_user_record \
                            >> gather_user_logs >> format_log_records >> send_logs_enter
        send_logs_end >> finish_import
        if_valid_user_records >> rail.Label("No") >> format_log_records

    return dag


rail.for_each_instance(create_main_dag)
