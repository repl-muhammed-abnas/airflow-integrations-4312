
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'horizonmedia_supervisororg_group_assignment_child_{config.instance}',
        description=f'HorizonMedia_supervisororg_group_assignment_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='put_policy_data_access_scopes_for_user_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='put_policy_data_access_scopes_for_user_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        put_policy_data_access_scopes_for_user_3 = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_user_3',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=lambda: {
                "userUri": rail.get_dag_run_conf()['supervisoruri'],
                "policyDataAccessScopes": [
                    {
                        "policyUri": "urn:replicon:policy:team-management",
                        "locations": [],
                        "divisions": [],
                        "costCenters": [],
                        "serviceCenters": [],
                        "departmentGroups":rail.get_dag_run_conf()['policyaccessdata'],
                        "employeeTypeGroups": []
                    }
                ]
            }
        )

        horizonmedia_supervisororg_logs_add_entry_4 = rail.WriteLogOperator(
            task_id='horizonmedia_supervisororg_logs_add_entry_4',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Success",
            properties={
                "supervisorname": "{{ dag_run.conf.supervisorname }}",
                "status": "Success",
                "details": ""
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Error",
            properties={
                "supervisorname": "{{ dag_run.conf.supervisorname }}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> put_policy_data_access_scopes_for_user_3
        put_policy_data_access_scopes_for_user_3 >> horizonmedia_supervisororg_logs_add_entry_4 >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
