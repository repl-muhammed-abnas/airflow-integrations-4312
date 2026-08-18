from datetime import datetime, timedelta
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils.request_payload import get_replicon_date, effective_dateformat_payload
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils import custom_methods
from airflow.models import Variable

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_user_child_dag_id,
        description=f'VialtoPartners_User_Import disable user child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_disable_user_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='disable_login'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="disable_login",
            end_task="catch_and_log_error"
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        def get_format(dag_run):
           return '%B %d, %Y' if dag_run.conf['caller'] == "disable_user" else '%Y-%m-%d'

        get_alltimeoff_after_enddate = rail.RepliconServicePageOperator(
            task_id='get_alltimeoff_after_enddate',
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:time-off-list-column:time-off"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "dateRange": {
                                    "startDate": effective_dateformat_payload(datetime.strptime(dag_run.conf['user_end_date'], get_format(dag_run)) + timedelta(days=1))
                                }
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uri": dag_run.conf['user_uri']
                            }
                        }
                    }
                }
            },
            page_handler=custom_methods.page_handler,
            all_result_data_handler=custom_methods.get_timeoff_uris
        )

        is_timeoffuris_to_delete = rail.IfOperator(
            task_id='is_timeoffuris_to_delete',
            test="{{ result('get_alltimeoff_after_enddate') | length > 0 }}",
            yes_task="create_timeoff_delete_batch",
            no_task="catch_and_log_error",
        )

        create_timeoff_delete_batch = rail.RepliconServiceOperator(
            task_id='create_timeoff_delete_batch',
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=lambda: {
                "timeOffUris": rail.result('get_alltimeoff_after_enddate')
            }
        )

        batch_entry, batch_exit = rail.batch_execution(
            group_id='execute_batch_management',
            creation_task_id='create_timeoff_delete_batch'
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule="one_failed",
            message="{{get_error_message()}}",
            severity='Error',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['EmployeeID']  if dag_run.conf.caller != 'disable_user' else "",
                'username': dag_run.conf['username']  if dag_run.conf.caller != 'disable_user' else "",
                'loginname': dag_run.conf['loginname']  if dag_run.conf.caller != 'disable_user' else "",
                'status': 'Error',
                'action': 'Update',
                'message': rail.render_template("{{get_error_message()}}"),
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": ""
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> disable_login

        disable_login >> get_alltimeoff_after_enddate >> is_timeoffuris_to_delete >> rail.Label('No') >> catch_and_log_error

        is_timeoffuris_to_delete >> rail.Label('Yes') >> create_timeoff_delete_batch >> batch_entry
        batch_exit >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
