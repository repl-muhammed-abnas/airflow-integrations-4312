from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from json import dumps, loads
from pendulum import datetime as dt
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable

from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import get_tenure_value
from dxctechnology.workday_user_import.user_import_global.utils import custom_methods as gbl_custom_methods
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_todays_date_for_timezone_in_json

null = None

DATE_FORMAT = '%Y-%d-%m'

def create_add_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.india_add_user_timeoff_assignment_dag_id,
        description="DXC Workday User Import INDIA - Process Add User TimeOff Assignment",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=dt(2023, 9, 26),
        max_active_runs=config.max_active_run_add_user_timeoff_assignemnt_india
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_india, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="assign_timeoff_to_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="assign_timeoff_to_user",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUris": dag_run.conf['timeoffs_to_assign_uri_list']
                }
        )

        for_each_default_timeoff_start = rail.ForEachOperator(
            task_id = "for_each_default_timeoff_start",
            items=lambda dag_run: dag_run.conf["formatted_timeoff_to_assign_uri_list"],
            start_task="is_ind_sick_casual_leave",
            end_task="for_each_default_timeoff_end"
        )

        is_ind_sick_casual_leave = rail.IfOperator(
            task_id = "is_ind_sick_casual_leave",
            test=lambda dag_run:  rail.find_first_by_attr_and_get_attr(dag_run.conf['timeoffs_to_assign_list'],"uri",
                rail.result("for_each_default_timeoff_start")['timeoff_uri'],"name") == "[IND] Sick/ Casual leave",
            yes_task="process_timeoff_assignment_ind_sick_casual",
            no_task="get_default_timeoff_policy"
        )

        def get_years_based_on_dob_for_40(dag_run):
            dob_date_obj = datetime.strptime(dag_run.conf['dob'], DATE_FORMAT)
            new_date = dob_date_obj + relativedelta(years = 40)
            return new_date.strftime(DATE_FORMAT)

        process_timeoff_assignment_ind_sick_casual = rail.TriggerDagRunOperator(
            task_id = "process_timeoff_assignment_ind_sick_casual",
            trigger_dag_id=config.india_timeoff_assignment_ind_sick_casual_dag_id,
            conf=lambda dag_run:{
                "file_name": dag_run.conf['file_name'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['source'],
                "country": dag_run.conf['country'],
                "dob":dag_run.conf['dob'],
                "dob_json_format": dag_run.conf['dob_json_format'],
                "start_date": dag_run.conf['start_date'],
                "start_date_json_format": dag_run.conf['start_date_json_format'],
                "time_off_uri": rail.result("for_each_default_timeoff_start")['timeoff_uri'],
                "time_off_name": rail.find_first_by_attr_and_get_attr(dag_run.conf['timeoffs_to_assign_list'],"uri",
                    rail.result("for_each_default_timeoff_start")['timeoff_uri'],"name"),
                "add": 'Yes',
                "policy_set": null,
                "schedule_change_date": null,
                "tenure_based_on_dob":get_tenure_value(
                    gbl_custom_methods.convert_json_date_to_date(dag_run.conf['dob_json_format']),
                    gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json())),
                "years_based_on_dob_for_40": get_years_based_on_dob_for_40(dag_run),
                "rehire": "No"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        get_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run:{
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_default_timeoff_start")['timeoff_uri']
                }
            }
        )

        def get_policy_set_to_assign(dag_run):
            if rail.result("get_default_timeoff_policy"):
                if rail.find_first_by_attr_and_get_attr(dag_run.conf['timeoffs_to_assign_list'],
                    'uri', rail.result("for_each_default_timeoff_start")['timeoff_uri'], 'name') == "[IND] Restricted Leave":
                    if datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT).strftime("%d/%m") =="01/01":
                        return loads(dumps(rail.result("get_default_timeoff_policy")).replace("null", "\"effective\"").replace("\"script\"", "\"scriptTarget\""))
                    else:
                        if datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT).strftime("%d/%m") =="01/07":
                            return loads(dumps(rail.result("get_default_timeoff_policy")).replace("null", "\"effective\"").replace("\"script\"", "\"scriptTarget\""))
                        else:
                            return loads(dumps(rail.result("get_default_timeoff_policy")).replace("null", "\"effective\"").replace("\"script\"", "\"scriptTarget\"").replace(
                                '''[{"keyUri":"urn:replicon:script-key:parameter:amount","value":{"number":0.0}},{"keyUri":"urn:replicon:script-key:parameter:precedence","value":{"number":10.0}}],"scriptTarget":{"description":"Set initial balance for the first day of a policy","name":"Starting Balance Set To","uri":''',
                                '''[{"keyUri":"urn:replicon:script-key:parameter:amount","value":{"number":1.0}},{"keyUri":"urn:replicon:script-key:parameter:precedence","value":{"number":10.0}}],"scriptTarget":{"description":"Set initial balance for the first day of a policy","name":"Starting Balance Set To","uri":'''))
                else:
                    return loads(dumps(rail.result("get_default_timeoff_policy")).replace("null", "\"effective\"").replace("\"script\"", "\"scriptTarget\""))
            return []

        policy_set_to_assign = rail.PythonOperator(
            task_id='policy_set_to_assign',
            python_callable=lambda dag_run: get_policy_set_to_assign(dag_run)
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test=lambda: bool(rail.result("policy_set_to_assign")),
            yes_task="put_user_timeoff_policy_set",
            no_task= "for_each_default_timeoff_end"
        )

        put_user_timeoff_policy_set = rail.RepliconServiceOperator(
            task_id = "put_user_timeoff_policy_set",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_default_timeoff_start")['timeoff_uri']
                },
                "policySetScheduleEntries": rail.result('policy_set_to_assign')
            }
        )

        for_each_default_timeoff_end = rail.EmptyOperator(
            task_id = "for_each_default_timeoff_end"
        )

        is_ind_sick_casual_wait_required = rail.IfOperator(
            task_id='is_ind_sick_casual_wait_required',
            test=lambda: bool(rail.result("process_timeoff_assignment_ind_sick_casual")),
            yes_task='wait_for_timeoff_assignment_ind_sick_casual',
            no_task='catch_and_log_error'
        )

        wait_for_timeoff_assignment_ind_sick_casual= rail.WaitForDagRunsSensor(
            task_id = "wait_for_timeoff_assignment_ind_sick_casual",
            dag_runs="{{ result('process_timeoff_assignment_ind_sick_casual') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )
        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = lambda dag_run: dag_run.conf['user_log'],
            trigger_rule = "one_failed",
            message="User Add",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Add',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> assign_timeoff_to_user

        assign_timeoff_to_user >> for_each_default_timeoff_start

        for_each_default_timeoff_start >> is_ind_sick_casual_leave

        is_ind_sick_casual_leave >> rail.Label('No') >> get_default_timeoff_policy
        is_ind_sick_casual_leave >> rail.Label('Yes') >> process_timeoff_assignment_ind_sick_casual >> for_each_default_timeoff_end

        get_default_timeoff_policy >> policy_set_to_assign >> has_any_policy_to_assign
        has_any_policy_to_assign >> rail.Label('Yes') >> put_user_timeoff_policy_set
        has_any_policy_to_assign >> rail.Label('No') >> for_each_default_timeoff_end
        put_user_timeoff_policy_set >> for_each_default_timeoff_end
        for_each_default_timeoff_start >> for_each_default_timeoff_end >> is_ind_sick_casual_wait_required
        
        is_ind_sick_casual_wait_required >> rail.Label('Yes') >> wait_for_timeoff_assignment_ind_sick_casual >> catch_and_log_error
        is_ind_sick_casual_wait_required >> rail.Label('No') >> catch_and_log_error

        return dag

rail.for_each_instance(create_add_user_timeoff_assignment_dag)
