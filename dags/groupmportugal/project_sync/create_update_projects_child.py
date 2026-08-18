from airflow.models import Variable
import rail
from datetime import timedelta
from groupmportugal.project_sync.utils import python_callable, request_payload

null = None

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.create_update_projects,
        description=f"Create Update Projects child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_enabled_service_centers'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='get_enabled_service_centers',
            end_task='finish',
        )

        get_enabled_service_centers = rail.RepliconServiceOperator(
            task_id='get_enabled_service_centers',
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": '{{ dag_run.conf.mergedprojectname }}',
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda resp: resp[0]['projectDetails'] if resp[0]['projectDetails'] else null,
        )

        groupmportugal_project_sync_logs = rail.CreateLogOperator(
            task_id='groupmportugal_project_sync_logs'
        )

        log_project_and_exception_log = rail.PythonOperator(
            task_id="log_project_and_exception_log",
            python_callable=lambda dag_run: {
                "project_log": rail.result("groupmportugal_project_sync_logs")
            }
        )

        if_project_details_not_present = rail.IfOperator(
            task_id='if_project_details_not_present',
            test=python_callable.if_project_details_not_present,
            yes_task='create_project_data',
            no_task='update_project_data'
        )

        create_project_data = rail.RepliconServiceOperator(
            task_id='create_project_data',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.create_project_data_payload
        )

        foreach_client_group = rail.ForEachOperator(
            task_id='foreach_client_group',
            items='{{ dag_run.conf.client_group | to_json }}',
            start_task='update_project_members_billing_rate',
            end_task='foreach_client_group_end'
        )

        update_project_members_billing_rate = rail.RepliconServiceOperator(
            task_id="update_project_members_billing_rate",
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateProjectTeamMemberBillingRateAllowedForBillingTime",
            data=lambda: {
                "projectUri": rail.result('create_project_data')['uri'],
                "resourceUri": request_payload.get_update_billing_rate_resource_uri(rail.result('foreach_client_group')),
                "billingRateUri": "urn:replicon:project-specific-billing-rate",
                "assigned": "true"
            }
        )

        foreach_client_group_end = rail.EmptyOperator(
            task_id='foreach_client_group_end',
        )

        update_project_client=rail.RepliconServiceOperator(
            task_id='update_project_client',
            endpoint="/services/ProjectService1.svc/UpdateClients",
            data=lambda dag_run :{
                "projectUri": rail.result('create_project_data')['uri'],
                "clients": [
                    {
                    "client": {
                        "uri": null,
                        "name": dag_run.conf['advertiser'],
                        "code": null,
                        "parameterCorrelationId": null
                    },
                    "costAllocationPercentage": "100"
                    }
                ]
            }
        )

        update_billing_rate = rail.RepliconServiceOperator(
            task_id='update_billing_rate',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data=lambda dag_run: {
                "projectUri": rail.result('create_project_data')['uri'],
                "billingRateUri": "urn:replicon:project-specific-billing-rate",
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        if_campaign_present = rail.IfOperator(
            task_id='if_campaign_present',
            test='{{ dag_run.conf.campaign | is_truthy }}',
            yes_task='create_update_task',
            no_task='add_groupmportugal_project_sync_logs'
        )

        create_update_task = rail.RepliconServiceOperator(
            task_id="create_update_task",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.create_update_task
        )

        add_groupmportugal_project_sync_logs = rail.WriteLogOperator(
            task_id='add_groupmportugal_project_sync_logs',
            log="{{result('groupmportugal_project_sync_logs')}}",
            message="na",
            severity="Success",
            properties={
                'clientname': "{{dag_run.conf.advertiser}}",
                'projectname': "{{dag_run.conf.mergedprojectname}}",
                'taskname': "{{dag_run.conf.campaign}}",
                'status': 'success',
                'details': "Project/Task created"
            }
        )

        update_project_data = rail.RepliconServiceOperator(
            task_id='update_project_data',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.update_project_data_payload
        )

        update_billing_rate_1 = rail.RepliconServiceOperator(
            task_id='update_billing_rate_1',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data=lambda dag_run: {
                "projectUri": rail.result('update_project_data')['uri'],
                "billingRateUri": "urn:replicon:project-specific-billing-rate",
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        if_campaign_present_1 = rail.IfOperator(
            task_id='if_campaign_present_1',
            test='{{ dag_run.conf.campaign | is_truthy }}',
            yes_task='get_all_project_task',
            no_task="add_groupmportugal_project_sync_logs_1"
        )

        get_all_project_task = rail.RepliconServiceOperator(
            task_id='get_all_project_task',
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data={
                    "parentUri": "{{ result('get_project_details')['uri'] }}"
            },
        )

        if_task_not_present = rail.IfOperator(
            task_id='if_task_not_present',
            test=python_callable.if_task_not_present,
            yes_task='create_task',
            no_task='update_task'
        )

        create_task = rail.RepliconServiceOperator(
            task_id="create_task",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_create_task_payload
        )

        update_task = rail.RepliconServiceOperator(
            task_id="update_task",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_update_task_payload
        )

        add_groupmportugal_project_sync_logs_1 = rail.WriteLogOperator(
            task_id='add_groupmportugal_project_sync_logs_1',
            log="{{result('groupmportugal_project_sync_logs')}}",
            message="na",
            severity="Success",
            properties={
                'clientname': "{{dag_run.conf.advertiser}}",
                'projectname': "{{dag_run.conf.mergedprojectname}}",
                'taskname': "{{dag_run.conf.campaign}}",
                'status': 'success',
                'details': "Project/Task updated"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_enabled_service_centers
        get_enabled_service_centers >> get_project_details >> groupmportugal_project_sync_logs >> log_project_and_exception_log >> \
        if_project_details_not_present >> rail.Label("Yes") >> create_project_data
        if_project_details_not_present >> rail.Label("No") >> update_project_data
        create_project_data >> foreach_client_group >> foreach_client_group_end
        foreach_client_group >> update_project_members_billing_rate >> foreach_client_group_end >> update_project_client >> update_billing_rate >> \
        if_campaign_present >> rail.Label(
            "Yes") >> create_update_task >> add_groupmportugal_project_sync_logs
        if_campaign_present >> add_groupmportugal_project_sync_logs >> finish
        update_project_data >> update_billing_rate_1 >> if_campaign_present_1 >> rail.Label(
            "Yes") >> get_all_project_task >> if_task_not_present
        if_campaign_present_1 >> rail.Label("No") >> add_groupmportugal_project_sync_logs_1
        if_task_not_present >> rail.Label("Yes") >> create_task >> add_groupmportugal_project_sync_logs_1
        if_task_not_present >> rail.Label("Yes") >> update_task >> add_groupmportugal_project_sync_logs_1
        add_groupmportugal_project_sync_logs_1 >> finish

    return dag


rail.for_each_instance(create_child_dag)
