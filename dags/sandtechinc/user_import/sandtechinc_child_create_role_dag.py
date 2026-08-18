"""
Sand Tech Inc - Child DAG for Creating Missing Roles (Job Titles)
Creates project roles that don't exist in Replicon
"""

from datetime import timedelta
import uuid
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.create_role_child_dagid,
        description=f'Sand Tech Inc - Child Create Role {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_role'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_role',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ========== CREATE ROLE ==========
        # Using ProjectRoleService1.svc/CreateProjectRoleOrApplyModifications
        # Per spec: Billable=Yes, Status=Enabled, Cost Rate=$0.00, Billing Rate=$0.00
        create_role = rail.RepliconServiceOperator(
            task_id='create_role',
            endpoint="/services/ProjectRoleService1.svc/CreateProjectRoleOrApplyModifications",
            data=lambda dag_run: {
                "target": null,
                "modifications": {
                    "name": dag_run.conf['role_name'],
                    "descriptionToApply": null,
                    "isArchivedToApply": False,  # Status = Enabled
                    "isBillableToApply": True,   # Billable = Yes
                    "billingRateScheduleToApply": null,  # Billing Rate = $0.00 (default)
                    "costRateScheduleToApply": null      # Cost Rate = $0.00 (default)
                },
                "projectRoleModificationOptionUri": "urn:replicon:project-role-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        # ========== LOG SUCCESS ==========
        log_success = rail.WriteLogOperator(
            task_id='log_success',
            message="Success",
            severity="Success",
            properties={
                "Empid": "N/A",
                "Username": "System",
                "Action": "Create Role",
                "Status": "Success",
                "Details": "Role '{{ dag_run.conf.role_name }}' created successfully",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        log_error = rail.WriteLogOperator(
            task_id='log_error',
            message="{{ get_error_message() }}",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "Empid": "N/A",
                "Username": "System",
                "Action": "Create Role",
                "Status": "Error",
                "Details": "Failed to create role '{{ dag_run.conf.role_name }}': {{ get_error_message() }}",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        # ========== TASK DEPENDENCIES ==========
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> create_role >> log_success >> log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)