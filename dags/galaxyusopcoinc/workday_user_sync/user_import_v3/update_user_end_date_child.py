from datetime import datetime, timedelta
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils.request_payload import get_replicon_date, effective_dateformat_payload
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import custom_methods
from airflow.models import Variable

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_enddate_dag_id,
        description=f'VialtoPartners_User_Import Process User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_user_via_employee_id'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="search_user_via_employee_id",
            end_task="catch_and_log_error"
        )


        search_user_via_employee_id = rail.RepliconServiceOperator(
            task_id = "search_user_via_employee_id",
            endpoint= "/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": "{{dag_run.conf.EmployeeID}}",
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        is_user_present = rail.IfOperator(
            task_id = "is_user_present",
            test= "{{result('search_user_via_employee_id') | is_truthy}}",
            yes_task="is_user_disabled",
            no_task = "log_user_not_found"
        )

        log_user_not_found = rail.WriteLogOperator(
            task_id = "log_user_not_found",
            message="User not found in Replicon, end date update skipped.",
            severity='Exception',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['EmployeeID'],
                'username': dag_run.conf['username'],
                'loginname': dag_run.conf['loginname'],
                'status': 'Exception',
                'action': 'Update',
                'message': "User not found in Replicon, end date update skipped.",
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": ""
            }
        )

        is_user_disabled = rail.IfOperator(
            task_id = "is_user_disabled",
            test = lambda: not rail.result("search_user_via_employee_id")[0]['userDetails']['isEnabled'],
            yes_task= "log_user_in_disabled_state",
            no_task="update_user_end_date"
        )

        log_user_in_disabled_state = rail.WriteLogOperator(
            task_id = "log_user_in_disabled_state",
            message="User in disabled status, end date update skipped.",
            severity='Exception',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['EmployeeID'],
                'username': dag_run.conf['username'],
                'loginname': dag_run.conf['loginname'],
                'status': 'Exception',
                'action': 'Update',
                'message': "User in disabled status, end date update skipped.",
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": ""
            }
        )

        update_user_end_date = rail.RepliconServiceOperator(
            task_id = "update_user_end_date",
            endpoint= "/services/ImportService1.svc/ApplyUserModifications3",
            data = lambda dag_run :{
                "user": {
                    "uri": rail.result("search_user_via_employee_id")[0]['userDetails']['uri']
                },
                "modifications": {
                    "userDetailsToApply": {
                        "employmentEndDate": {
                            "date": get_replicon_date(dag_run.conf['TerminationDate'])
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        is_user_end_date_less_than_today = rail.IfOperator(
            task_id = "is_user_end_date_less_than_today",
            test = lambda dag_run: datetime.strptime(dag_run.conf['TerminationDate'], '%Y-%m-%d').date()< datetime.now().date(),
            yes_task= "trigger_disable_user_child",
            no_task="log_update_user_end_date_success"
        )

        trigger_disable_user_child = rail.TriggerDagRunOperator(
            task_id='trigger_disable_user_child',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.disable_user_child_dag_id,
            conf=lambda dag_run: {
                'employeeid': dag_run.conf['EmployeeID'],
                'username': dag_run.conf['username'],
                'loginname': dag_run.conf['loginname'],
                "user_uri": rail.result("search_user_via_employee_id")[0]['userDetails']['uri'],
                "user_end_date":dag_run.conf['TerminationDate'],
                "caller":"update_user_end_date_dag"
            }
        )

        log_update_user_end_date_success = rail.WriteLogOperator(
            task_id = "log_update_user_end_date_success",
            message="User end date updated successfully",
            severity='Success',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['EmployeeID'],
                'username': dag_run.conf['username'],
                'loginname': dag_run.conf['loginname'],
                'status': 'Success',
                'action': 'Update',
                'message': "User end date updated successfully",
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": ""
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule="one_failed",
            message="{{get_error_message()}}",
            severity='Error',
            properties={
                'employeeid': "{{dag_run.conf.EmployeeID}}",
                'username': "{{dag_run.conf.username}}",
                'loginname': "{{dag_run.conf.loginname}}",
                'status': 'Error',
                'action': 'Update',
                'message': "{{get_error_message()}}",
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": ""
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> search_user_via_employee_id

        search_user_via_employee_id >> is_user_present >> rail.Label("No"
                ) >> log_user_not_found >> rail.Label("On error") >> catch_and_log_error
        is_user_present >> rail.Label("Yes") >> is_user_disabled >> rail.Label("No") >> update_user_end_date >> is_user_end_date_less_than_today

        is_user_end_date_less_than_today >> rail.Label('Yes') >> trigger_disable_user_child >> log_update_user_end_date_success
        is_user_end_date_less_than_today >> rail.Label('No') >> log_update_user_end_date_success

        log_update_user_end_date_success >> rail.Label("On error") >> catch_and_log_error
        is_user_disabled >> rail.Label("Yes") >> log_user_in_disabled_state >> rail.Label("On error") >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
