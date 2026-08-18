from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from dxctechnology.workday_user_import_v1.user_import_usa_csc.utils import request_payload, custom_methods
from dxctechnology.workday_user_import_v1.user_import_usa_csc.tasks.supervisor_assignment import assign_supervisor

def create_add_user_dag(config):
    
    with rail.create_airflow_dag(
        dag_id = config.usa_csc_add_user_dag_id,
        description = "add user",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.max_run_add_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_us_csc, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="create_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="create_user",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        create_user = rail.RepliconServiceOperator(
            task_id = "create_user",
            endpoint = "/services/ImportService1.svc/PutUser3",
            data=lambda dag_run: request_payload.crete_user_payload(dag_run, config)
        )

        remove_timeoffs = rail.RepliconServiceOperator(
            task_id = "remove_timeoffs",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data = {
                "userUri": "{{result('create_user').uri}}",
                "timeOffTypeUris": []
            }
        )

        can_update_notification_preference = rail.IfOperator(
            task_id = "can_update_notification_preference",
            test="{{ dag_run.conf.file_data.management_lvl in ['L1', 'L2']}}",
            yes_task="update_notification_preference",
            no_task="update_product_assignment"
        )

        update_notification_preference = rail.RepliconServiceOperator(
            task_id = "update_notification_preference",
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data=request_payload.get_notification_preference_to_assign
        )

        update_product_assignment = rail.RepliconServiceOperator(
            task_id = "update_product_assignment",
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=request_payload.get_product_to_assign_to_user_payload
        )

        can_update_timeentry_path = rail.IfOperator(
            task_id = "can_update_timeentry_path",
            test="{{dag_run.conf.mapper_data.timeentry_approval_path_name | is_truthy}}",
            yes_task="update_time_entry_path",
            no_task="empty_supervisor_start"
        )

        update_time_entry_path = rail.RepliconServiceOperator(
            task_id = "update_time_entry_path",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.get_update_time_entry_path_payload
        )

        empty_supervisor_start = rail.EmptyOperator(
            task_id= "empty_supervisor_start"
        )

        supervisor_start, supervisor_end = assign_supervisor("add_user_supervisor_assignment", "add")

        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id = "get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        is_parent_company_c1_and_empgrpcode_not_equal_3 = rail.IfOperator(
            task_id = "is_parent_company_c1_and_empgrpcode_not_equal_3",
            test=lambda dag_run: dag_run.conf['file_data']['parent_company'] =="C1" and dag_run.conf['file_data']['emp_subgroup_code'] not in ["3",3],
            yes_task="query_timeoff_data_us_csc",
            no_task= "query_timeoff_data_us_les"
        )

        def get_timeoff_list_from_mapper(dag_run):
            list1 =  list(filter(lambda row: row['Type']=='Timeoff' and\
                row['Country']==dag_run.conf['file_data']['country'] and\
                row['Function']=='Workday User Sync' and\
                row['Source'] == dag_run.conf['file_data']['parent_company'] and\
                row['URI'] == (dag_run.conf['file_data']['sub_area_code'] if dag_run.conf['file_data']['sub_area_code'] in ["U04A","U02A","U05A","U06A"] else "All - Others") and\
                row['personnelsubarea'] == dag_run.conf['mapper_data']['psg'] and\
                row['status'] == ((dag_run.conf['file_data']['state'] if dag_run.conf['file_data']['state'] in ["U04A","U02A","U05A","U06A"] else "All - Others")
                     if dag_run.conf['file_data']['country'] == "United States of America" else dag_run.conf['file_data']['country']) and\
                row['employeegroup'] == dag_run.conf['file_data']['emp_group_code'] and\
                row['employeesubgroup'] == dag_run.conf['file_data']['emp_subgroup_code'], config.MAPPER))

            list2=[]
            if dag_run.conf['file_data']['state'] in ["California","Colorado","Nevada","Puerto Rico"] or dag_run.conf['file_data']['country'] == "Puerto Rico":
                list2 = list(filter(lambda row: row['Type']=='Timeoff' and\
                    row['Country']==dag_run.conf['file_data']['country'] and\
                    row['Function']=='Workday User Sync' and\
                    row['Source'] == dag_run.conf['file_data']['parent_company'] and\
                    row['URI'] == "Null" and\
                    row['personnelsubarea'] == "Null" and\
                    row['status'] == (dag_run.conf['file_data']['state'] if dag_run.conf['file_data']['state'] in ["U04A","U02A","U05A","U06A"] else "All - Others")
                        if dag_run.conf['file_data']['country'] == "United States of America" else dag_run.conf['file_data']['country'] and\
                    row['employeegroup'] == "Null" and\
                    row['employeesubgroup'] == dag_run.conf['file_data']['emp_subgroup_code'], config.MAPPER))
        
            return list1+list2

        query_timeoff_data_us_csc = rail.PythonOperator(
            task_id = "query_timeoff_data_us_csc",
            python_callable=lambda dag_run: get_timeoff_list_from_mapper(dag_run)
        )

        map_mapper_replicon_timeoff_us_csc = rail.PythonOperator(
            task_id = "map_mapper_replicon_timeoff_us_csc",
            python_callable=lambda: custom_methods.map_mapper_replicon_timeoffs("query_timeoff_data_us_csc")
        )

        has_any_timeoff_to_assign_us_csc = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign_us_csc",
            test=lambda: len(rail.result("map_mapper_replicon_timeoff_us_csc")) > 0,
            yes_task="process_new_user_timeoff_assignment_us_csc",
            no_task="log_user_completion"
        )

        process_new_user_timeoff_assignment_us_csc = rail.TriggerDagRunOperator(
            task_id = "process_new_user_timeoff_assignment_us_csc",
            trigger_dag_id=config.usa_csc_add_user_timeoff_assignment_dag_id,
            conf=lambda dag_run:{
                "file_name": dag_run.conf['master_file_name'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "user_uri": rail.result('create_user')['uri'],
                "loginName": rail.result('create_user')['loginName'],
                "company_code": dag_run.conf['file_data']['company_code'],
                "parent_company_code": dag_run.conf['file_data']['parent_company'],
                "country": dag_run.conf['file_data']['country'],
                "state": dag_run.conf['file_data']['state'],
                "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                "contineous_service_date": dag_run.conf['json_formatted_dates']['service_date'],
                "work_schedule": dag_run.conf['mapper_data']['schedule_hours'],
                "fte": dag_run.conf['file_data']['fte'],
                "employeetype":"Exempt – Salaried" if dag_run.conf['groups']['employee_type']['is_exempt'] else "Non Exempt - Hourly",
                "paygroup": dag_run.conf['file_data']['pay_group'],
                "timeoffs": rail.result('map_mapper_replicon_timeoff_us_csc'),
                "workshift": dag_run.conf['file_data']['work_shift'],
                "map_mapper_replicon_timeoff": rail.result("map_mapper_replicon_timeoff_us_csc", key="mapped_timeoff_data"),
                "mapped_timeoff_data": {timeoff['uri']: timeoff['name'] for timeoff in rail.result("map_mapper_replicon_timeoff_us_csc", key="mapped_timeoff_data")},
                "personnal_sub_area":dag_run.conf['file_data']['sub_area_code'], #neeed to check mapping
                "employee_group":dag_run.conf['file_data']['emp_group_code'],
                "employee_sub_group":dag_run.conf['file_data']['emp_subgroup_code'],
                "psg":dag_run.conf['mapper_data']['psg'],
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
            },
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        wait_for_timeoff_assignment_us_csc = rail.WaitForDagRunsSensor(
            task_id = "wait_for_timeoff_assignment_us_csc",
            dag_runs="""{{ result('process_new_user_timeoff_assignment_us_csc') }}""",
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        def get_employee_subgroup(dag_run):
            if dag_run.conf['file_data']['employee_type'] == "Non Exempt - Hourly":
                if dag_run.conf['file_data']['pay_group']:
                    if (dag_run.conf['file_data']['pay_group']).lower()=="usa-bi-weekly":
                        return "USA-Bi-Weekly"
                    # return "All Others"
                # return "All Others"
            return "All Others"
        

        query_timeoff_data_us_les = rail.PythonOperator(
            task_id = "query_timeoff_data_us_les",
            python_callable=lambda dag_run: list(filter(lambda row: row['Type']=='Timeoff' and\
                row['Country']==dag_run.conf['file_data']['country'] and\
                row['Function']=='Workday User Sync' and\
                row['Source'] == dag_run.conf['file_data']['parent_company'] and\
                row['personnelsubarea'] == dag_run.conf['file_data']['employee_type'] and\
                row['employeesubgroup'] == get_employee_subgroup(dag_run), config.MAPPER))
        )

        map_mapper_replicon_timeoff_us_les = rail.PythonOperator(
            task_id = "map_mapper_replicon_timeoff_us_les",
            python_callable=lambda: custom_methods.map_mapper_replicon_timeoffs("query_timeoff_data_us_les")
        )

        has_any_timeoff_to_assign_us_les = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign_us_les",
            test=lambda: len(rail.result("map_mapper_replicon_timeoff_us_les")) > 0,
            yes_task="process_new_user_timeoff_assignment_us_les",
            no_task= "log_user_completion"
        )

        process_new_user_timeoff_assignment_us_les = rail.TriggerDagRunOperator(
            task_id = "process_new_user_timeoff_assignment_us_les",
            trigger_dag_id=config.usa_lse_add_user_timeoff_assignment_dag_id,
            conf=lambda dag_run:{
                "file_name": dag_run.conf['master_file_name'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "user_uri": rail.result('create_user')['uri'],
                "loginName": rail.result('create_user')['loginName'],
                "company_code": dag_run.conf['file_data']['company_code'],
                "parent_company_code": dag_run.conf['file_data']['parent_company'],
                "country": dag_run.conf['file_data']['country'],
                "state": dag_run.conf['file_data']['state'],
                "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                "contineous_service_date": dag_run.conf['json_formatted_dates']['service_date'],
                "work_schedule": dag_run.conf['mapper_data']['schedule_hours'],
                "fte": dag_run.conf['file_data']['fte'],
                "employeetype":"Exempt – Salaried" if dag_run.conf['groups']['employee_type']['is_exempt'] else "Non Exempt - Hourly",
                "paygroup": dag_run.conf['file_data']['pay_group'],
                "timeoffs": rail.result('map_mapper_replicon_timeoff_us_les'),
                "workshift": dag_run.conf['file_data']['work_shift'],
                "map_mapper_replicon_timeoff": rail.result("map_mapper_replicon_timeoff_us_les", key="mapped_timeoff_data"),
                "mapped_timeoff_data": {timeoff['uri']: timeoff['name'] for timeoff in rail.result("map_mapper_replicon_timeoff_us_les", key="mapped_timeoff_data")},
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
            },
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        wait_for_timeoff_assignment_us_les = rail.WaitForDagRunsSensor(
            task_id = "wait_for_timeoff_assignment_us_les",
            dag_runs="""{{ result('process_new_user_timeoff_assignment_us_les') }}""",
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        def get_log_message():
            exception_msg = rail.result('create_user', 'exception_log')
            if exception_msg:
                return f"User created partially - {rail.smartjoin_by_delim(exception_msg, ',')}"
            return "User created successfully"

        log_user_completion = rail.WriteLogOperator(
            task_id = "log_user_completion",
            message = "User Add",
            log="{{dag_run.conf.user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Add",
                "Status": "Exception" if bool(rail.result('create_user', 'exception_log')) else "Success",
                "Details": get_log_message()
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{dag_run.conf.user_log}}",
            message = "User Add Error",
            severity = "Error",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Add",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_user

        create_user >> remove_timeoffs >> can_update_notification_preference >> rail.Label("Yes") >> update_notification_preference
        can_update_notification_preference >> rail.Label("No") >> update_product_assignment
        update_notification_preference >> update_product_assignment

        update_product_assignment >> can_update_timeentry_path >> rail.Label("No") >> empty_supervisor_start
        can_update_timeentry_path >> rail.Label("Yes") >> update_time_entry_path >> empty_supervisor_start
        empty_supervisor_start >> supervisor_start >> supervisor_end
        supervisor_end >> get_all_timeoffs

        get_all_timeoffs >> is_parent_company_c1_and_empgrpcode_not_equal_3 >> rail.Label('Yes') >> query_timeoff_data_us_csc >> map_mapper_replicon_timeoff_us_csc
        is_parent_company_c1_and_empgrpcode_not_equal_3 >> rail.Label('No') >> query_timeoff_data_us_les >> map_mapper_replicon_timeoff_us_les

        map_mapper_replicon_timeoff_us_csc >> has_any_timeoff_to_assign_us_csc >> rail.Label('Yes') >> process_new_user_timeoff_assignment_us_csc
        has_any_timeoff_to_assign_us_csc >> rail.Label('No') >> log_user_completion
        process_new_user_timeoff_assignment_us_csc >> wait_for_timeoff_assignment_us_csc >> log_user_completion

        map_mapper_replicon_timeoff_us_les >> has_any_timeoff_to_assign_us_les >> rail.Label('No') >> log_user_completion
        has_any_timeoff_to_assign_us_les >> rail.Label('Yes') >>  process_new_user_timeoff_assignment_us_les

        process_new_user_timeoff_assignment_us_les >> wait_for_timeoff_assignment_us_les >> log_user_completion >> catch_and_log_error

        return dag

rail.for_each_instance(create_add_user_dag)
