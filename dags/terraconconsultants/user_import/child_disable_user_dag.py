from datetime import timedelta
from airflow.models import Variable
import rail
from terraconconsultants.user_import.utils.request_payload import get_today_date, get_update_terminate_costcenter


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_disableuser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_disable_user_{config.instance}',
        description=f'TerraconConsultants Child Disable User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='update_enddate'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='update_enddate',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        update_enddate = rail.RepliconServiceOperator(
            task_id='update_enddate',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['uri']
                },
                "modifications": {
                    "userDetailsToApply": {
                        "employmentEndDate": {
                            "date": get_today_date()
                        }
                    }
                }
            }
        )

        get_terminatecostcenter = rail.RepliconServiceOperator(
            task_id='get_terminatecostcenter',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Terminate Assignment', 'uri', '')
        )

        is_terminatecostcenter_present = rail.IfOperator(
            task_id='is_terminatecostcenter_present',
            test="{{ result('get_terminatecostcenter') | is_truthy }}",
            yes_task="update_terminate_costcenter",
            no_task="get_assigned_timeoff_types",
        )

        update_terminate_costcenter = rail.RepliconServiceOperator(
            task_id='update_terminate_costcenter',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=get_update_terminate_costcenter
        )

        get_assigned_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_assigned_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            }
        )

        trigger_usersync_timeoff_put_0_balance = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_usersync_timeoff_put_0_balance',
            retries=0,
            items=lambda: rail.result('get_assigned_timeoff_types'),
            trigger_dag_id=f'terraconconsultants_userimport_child_timeoff_put_0_balance_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "timeoffuri": "{{ item.uri }}",
                "useruri": "{{ dag_run.conf.uri }}"
            }
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            }
        )

        trigger_delete_to_bookings = rail.TriggerDagRunOperator(
            task_id='trigger_delete_to_bookings',
            retries=0,
            trigger_dag_id=f'terraconconsultants_userimport_child_delete_to_bookings_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "useruri": "{{ dag_run.conf.uri }}",
                "enddate": "{{ current_time('%d/%m/%Y') }}"
            }
        )

        write_disableuser_log = rail.WriteLogOperator(
            task_id='write_disableuser_log',
            log="{{ dag_run.conf.log }}",
            message="NA",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "uri": "{{ dag_run.conf.uri }}",
                "action": "Disable",
                "status": "Success",
                "reason": "NA"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "uri": "{{ dag_run.conf.uri }}",
                "action": "Disable",
                "status": "Error",
                "reason": "{{ get_error_message() }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> update_enddate
        update_enddate >> get_terminatecostcenter >> is_terminatecostcenter_present
        is_terminatecostcenter_present >> rail.Label(
            'Yes') >> update_terminate_costcenter >> get_assigned_timeoff_types
        is_terminatecostcenter_present >> rail.Label(
            'No') >> get_assigned_timeoff_types

        get_assigned_timeoff_types >> trigger_usersync_timeoff_put_0_balance >> \
            disable_login >> trigger_delete_to_bookings >> write_disableuser_log >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_disableuser_dag)
