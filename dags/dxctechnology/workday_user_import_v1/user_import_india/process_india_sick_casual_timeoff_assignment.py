
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from json import dumps, loads
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable

from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_date_for_timezone_in_json
from dxctechnology.workday_user_import_v1.user_import_global.utils import custom_methods as gbl_custom_methods

DATE_FORMAT = "%Y-%d-%m"
null = None

# pylint: disable=too-many-statements
def create_user_sick_casual_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id = config.india_timeoff_assignment_ind_sick_casual_dag_id,
        description = "DXC Workday User Import INDIA - Process IND - SICK/Casual TimeOff Assignment",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.max_active_run_ind_sick_casual_timeoff_assignment_india
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_india, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_default_policy_for_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_default_policy_for_user",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        get_default_policy_for_user = rail.RepliconServiceOperator(
            task_id = "get_default_policy_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.time_off_uri }}"
            }
        )
        
        def get_policy_to_assign_callable(dag_run):
            default_policy_for_user = rail.result('get_default_policy_for_user')
            policy_to_assign = []

            updated_years_based_on_dob_for_40 = datetime.strptime(dag_run.conf['years_based_on_dob_for_40'], DATE_FORMAT)+ relativedelta(years=1)
            date_begining_of_year = updated_years_based_on_dob_for_40.replace(month=1,day=1)
            start_date_obj = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['start_date_json_format'])

            if dag_run.conf['add'] =="Yes":
                for item in default_policy_for_user:
                    if item['startOffset']['offsetValue'] == 0:
                        if date_begining_of_year.date() > gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json()):
                            policy_to_assign.append(
                            {
                                'description' : f"Effective on {start_date_obj.day}/{start_date_obj.month}/{start_date_obj.year}",
                                "effectiveDate" : dag_run.conf['start_date_json_format'],
                                "policySet": item['policySet']
                            }
                        )
                    
                    if item['startOffset']['offsetValue'] == 1:
                        date_begining_of_year_minus_one_day = date_begining_of_year - timedelta(days=1)
                        if date_begining_of_year.date() > gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json()):
                            policy_to_assign.append(
                                {
                                    'description' : f"Effective on {date_begining_of_year_minus_one_day.day}/{date_begining_of_year_minus_one_day.month}/{date_begining_of_year_minus_one_day.year}",
                                    "effectiveDate" : {
                                    "year": date_begining_of_year_minus_one_day.year,
                                    "month": date_begining_of_year_minus_one_day.month,
                                    "day": date_begining_of_year_minus_one_day.day
                                },
                                    "policySet": item['policySet']
                                }
                            )
                        else:
                            policy_to_assign.append(
                                {
                                    'description' : f"Effective on {start_date_obj.day}/{start_date_obj.month}/{start_date_obj.year}",
                                    "effectiveDate" : dag_run.conf['start_date_json_format'],
                                    "policySet": item['policySet']
                                }
                            )
            else:
                start_date_obj = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['start_date_json_format'])
                schedule_change_date_obj = datetime.strptime(dag_run.conf['schedule_change_date'], DATE_FORMAT).date()
                _todays_date = gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json())
                if dag_run.conf['add'] =="No":
                    _policy_set = dag_run.conf['policy_set'] if dag_run.conf['policy_set'] else []
                    for policy in _policy_set:
                        effective_date = policy['effectiveDate']
                        effective_date_obj = gbl_custom_methods.convert_json_date_to_date(effective_date)
                        if effective_date_obj < schedule_change_date_obj:
                            policy_to_assign.append(
                                {
                                    'description' : policy['description'],
                                    "effectiveDate" : effective_date,
                                    "policySet": policy['policySet']
                                }
                        )

                    for item in default_policy_for_user:
                        if item['startOffset']['offsetValue'] == 0:
                            if date_begining_of_year.date() > _todays_date:
                                policy_to_assign.append(
                                {
                                    'description' : f"Effective on {schedule_change_date_obj.day}/{schedule_change_date_obj.month}/{schedule_change_date_obj.year}",
                                    "effectiveDate" : {
                                            "year": schedule_change_date_obj.year,
                                            "month": schedule_change_date_obj.month,
                                            "day": schedule_change_date_obj.day
                                        },
                                    "policySet": item['policySet']
                                }
                            )
                        
                        if item['startOffset']['offsetValue'] == 1:
                            date_begining_of_year_minus_one_day = date_begining_of_year - timedelta(days=1)
                            if date_begining_of_year.date() > _todays_date:
                                policy_to_assign.append(
                                    {
                                        'description' : f"Effective on {date_begining_of_year_minus_one_day.day}/{date_begining_of_year_minus_one_day.month}/{date_begining_of_year_minus_one_day.year}",
                                        "effectiveDate" : {
                                            "year": date_begining_of_year_minus_one_day.year,
                                            "month": date_begining_of_year_minus_one_day.month,
                                            "day": date_begining_of_year_minus_one_day.day
                                        },
                                        "policySet": item['policySet']
                                    }
                                )
                            else:
                                policy_to_assign.append(
                                    {
                                        'description' : f"Effective on {schedule_change_date_obj.day}/{schedule_change_date_obj.month}/{schedule_change_date_obj.year}",
                                        "effectiveDate" : {
                                            "year": schedule_change_date_obj.year,
                                            "month": schedule_change_date_obj.month,
                                            "day": schedule_change_date_obj.day
                                        },
                                        "policySet": item['policySet']
                                    }
                                )
            rail.set_result(key="add", val=dag_run.conf['add'])
            
            return policy_to_assign

        get_policy_to_assign= rail.PythonOperator(
            task_id = "get_policy_to_assign",
            python_callable=lambda dag_run:get_policy_to_assign_callable(dag_run)
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test=lambda: bool(rail.result('get_policy_to_assign')),
            yes_task="assign_policy",
            no_task="catch_and_log_error"
        )

        assign_policy = rail.RepliconServiceOperator(
            task_id = "assign_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['time_off_uri']
                },
                "policySetScheduleEntries": loads(dumps(rail.result('get_policy_to_assign')).replace("\"script\"", "\"scriptTarget\""))
            }        
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Add/Update",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid":  "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Update' if dag_run.conf['add']!= 'Yes' else 'Add',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_default_policy_for_user

        get_default_policy_for_user >> get_policy_to_assign >> has_any_policy_to_assign
        has_any_policy_to_assign >> rail.Label('Yes') >> assign_policy >> catch_and_log_error
        has_any_policy_to_assign >> rail.Label('No') >> catch_and_log_error

        return dag

rail.for_each_instance(create_user_sick_casual_timeoff_assignment_dag)
