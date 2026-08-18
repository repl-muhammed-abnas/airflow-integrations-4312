from datetime import timedelta
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'allenphilip_project_rate_update_unassigned_users_child_{config.instance}',
        description=f'Allenphilip__project_rate_update_unassigned_users_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child_unassigned,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='assign_user_to_project'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='assign_user_to_project',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id='assign_user_to_project',
            endpoint="/services/ProjectService1.svc/AssignResourceToProject",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['project_uri'],
                "resourceUri": dag_run.conf['unassigned_users']['useruri'],

            }
        )

        put_project_team_member_billing_rates_allowed_for_billing_time = rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates_allowed_for_billing_time',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['project_uri'],
                "resourceUri": dag_run.conf['unassigned_users']['useruri'],
                "billingRateUris": ["urn:replicon:user-specific-billing-rate"]
            }
        )

        insert_item_to_list = rail.WriteLogOperator(
            task_id='insert_item_to_list',
            log="{{ dag_run.conf.user_lookuptable}}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['unassigned_users']['loginname'],
                "useruri": dag_run.conf['unassigned_users']['useruri'],
            }
        )

        add_success_entry_for_user_not_assigned_to_project = rail.WriteLogOperator(
            task_id='add_success_entry_for_user_not_assigned_to_project',
            log="{{ dag_run.conf.log_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['job_id'],
                "loginname":  dag_run.conf['unassigned_users']['loginname'],
                "projectname": dag_run.conf['project_name'],
                "defaultbillingrate": dag_run.conf['unassigned_users']['defaultbillingrate'],
                "status": "Success",
                "details": "Assigned to Project",
                "childjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        task_end = rail.EmptyOperator(
            task_id='task_end'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> assign_user_to_project
        assign_user_to_project >> put_project_team_member_billing_rates_allowed_for_billing_time
        put_project_team_member_billing_rates_allowed_for_billing_time >> insert_item_to_list
        insert_item_to_list >> add_success_entry_for_user_not_assigned_to_project >> task_end >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
