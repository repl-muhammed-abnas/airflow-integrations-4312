from datetime import datetime, timedelta
from airflow.models import Variable
import rail
from strayeruniversity.user_sync_v3.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'strayeruniversity_usersync_remove_future_time_off_bookings_v3_{config.instance}',
        description=f'strayeruniversity_usersync_remove_future_time_off_bookings_v3_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.disable_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_alltimeoff_after_enddate'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_alltimeoff_after_enddate',
            end_task='catch_and_log_error',
        )

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
                                    "startDate": request_payload.effective_dateformat_payload(datetime.strptime(dag_run.conf['terminationdate'], '%d-%b-%Y') + timedelta(days=1))
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
                                "uri": dag_run.conf['useruri']
                            }
                        }
                    }
                }
            },
            page_handler=python_callable.page_handler,
            all_result_data_handler=python_callable.get_timeoff_uris
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

        catch_and_log_error = rail.PythonOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Remove future timeoff bookings for disabled users : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_and_log_error') or ("Future time off bookings removed successfully" if rail.result(
                "create_timeoff_delete_batch") else "No future time off bookings found for the user")
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error

        can_run_batch_task >> rail.Label(
            'No') >> get_alltimeoff_after_enddate

        get_alltimeoff_after_enddate >> is_timeoffuris_to_delete
        is_timeoffuris_to_delete >> rail.Label(
            'Yes') >> create_timeoff_delete_batch >> batch_entry
        batch_exit >> catch_and_log_error
        is_timeoffuris_to_delete >> rail.Label(
            'No') >> catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
