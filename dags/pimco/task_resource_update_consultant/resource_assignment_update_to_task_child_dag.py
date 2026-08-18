from datetime import timedelta
import rail
from airflow.models import Variable
from pimco.task_resource_update_consultant.utils import python_callable_method

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_resource_assignment_update_to_consultant_task_child_{config.instance}',
        description=f'PIMCO Resource assignment update to consultant task child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)


        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='bulk_get_task_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_task_details',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        bulk_get_task_details=rail.RepliconServiceOperator(
            task_id='bulk_get_task_details',
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails2",
            data=lambda dag_run: {
                "pageIndex": "1",
                "pageSize": "10000",
                "projectUris": [dag_run.conf['projecturi']],
                "taskDataInclusionOptionUris": []
            }
        )

        bulk_update_project_team_members_assignment=rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data=lambda dag_run: {
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign",
                "projectUri": dag_run.conf['projecturi'],
                "resourceUri": [ resource['resourceuri'] for resource in dag_run.conf['resourceteamassignment']]
            }
        )

        foreach_item_in_resource=rail.ForEachOperator(
            task_id='foreach_item_in_resource',
            items=lambda dag_run: dag_run.conf['resource'],
            start_task = 'get_uri_for_task_name_and_code',
            end_task = 'foreach_item_in_resource_end'
        )

        get_uri_for_task_name_and_code= rail.PythonOperator(
            task_id='get_uri_for_task_name_and_code',
            python_callable=python_callable_method.get_uri_for_task_name_and_code,
            op_args=['{{result("foreach_item_in_resource").taskname}}','{{result("foreach_item_in_resource").taskcode}}']
        )

        if_uri_for_task_name_and_code_present=rail.IfOperator(
            task_id='if_uri_for_task_name_and_code_present',
            test="{{ result('get_uri_for_task_name_and_code') | is_truthy}}",
            yes_task="bulk_update_resource_assignments",
            no_task="foreach_item_in_resource_end",
        )

        bulk_update_resource_assignments=rail.RepliconServiceOperator(
            task_id='bulk_update_resource_assignments',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: {
                "taskUri": rail.result('get_uri_for_task_name_and_code'),
                "resourceUris": rail.result('foreach_item_in_resource')['resourceuri'],
                "isAssigned": "true"
            }
        )

        foreach_item_in_resource_end = rail.EmptyOperator(
            task_id = 'foreach_item_in_resource_end',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> bulk_get_task_details
        bulk_get_task_details >> bulk_update_project_team_members_assignment >> foreach_item_in_resource >> get_uri_for_task_name_and_code
        get_uri_for_task_name_and_code >> if_uri_for_task_name_and_code_present
        if_uri_for_task_name_and_code_present >> rail.Label('Yes') >> bulk_update_resource_assignments >> foreach_item_in_resource_end
        if_uri_for_task_name_and_code_present >> rail.Label('No') >> foreach_item_in_resource_end
        foreach_item_in_resource >> foreach_item_in_resource_end >> finish >> log_to_sumo
    return dag

rail.for_each_instance(create_dag)
