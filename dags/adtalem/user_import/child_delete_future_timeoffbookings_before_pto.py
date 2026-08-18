from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.request_payload import get_datetime_obj
from adtalem.user_import.utils.response_filter import get_timeoff_uris, page_handler


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_delete_future_timeoffbookings_beforepto_{config.instance}',
        description=f'Adtalem Delete future time off bookings before PTO transfer Child_V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

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
            end_task='dagrun_log_to_sumo',
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
                            "leftExpression": {
                                "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "value": {
                                    "dateRange": {
                                        "startDate": get_datetime_obj(dag_run.conf['rundate'])
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
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-type"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uri": dag_run.conf['ptotypeuri']
                            }
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=get_timeoff_uris
        )

        is_timeoffuris_to_delete = rail.IfOperator(
            task_id='is_timeoffuris_to_delete',
            test="{{ result('get_alltimeoff_after_enddate') | length > 0 }}",
            yes_task="create_timeoff_delete_batch",
            no_task="dagrun_log_to_sumo",
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

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_alltimeoff_after_enddate

        get_alltimeoff_after_enddate >> is_timeoffuris_to_delete
        is_timeoffuris_to_delete >> rail.Label(
            'Yes') >> create_timeoff_delete_batch >> batch_entry
        batch_exit >> dagrun_log_to_sumo
        is_timeoffuris_to_delete >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_dag)
