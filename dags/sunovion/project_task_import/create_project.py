from datetime import timedelta
from airflow.models import Variable
from sunovion.project_task_import.utils import request_payload
from sunovion.project_task_import.utils import response_filter
from sunovion.project_task_import.utils import custom_methods
import rail

null = None

# pylint: disable=too-many-statements


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'sunovion_project_sync_create_project_child_{config.instance}',
        description='Sunovion Project and Task Sync - Create Project',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_code,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='false').lower() == 'true',
            yes_task="batch_task",
            no_task="get_project_records"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_project_records',
            end_task="catch_and_log_errors",
        )

        get_project_records = rail.PythonOperator(
            task_id="get_project_records",
            python_callable=custom_methods.get_project_records
        )

        is_start_date_present = rail.IfOperator(
            task_id='is_start_date_present',
            test=request_payload.is_start_date_present,
            yes_task='is_start_date_correct',
            no_task='is_end_date_present'
        )

        is_start_date_correct = rail.IfOperator(
            task_id='is_start_date_correct',
            test=request_payload.is_start_date_correct,
            yes_task='is_end_date_present',
            no_task='log_start_date_incorrect'
        )

        log_start_date_incorrect = rail.WriteLogOperator(
            task_id="log_start_date_incorrect",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Invalid Start Date',
            severity='Failed',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Failed',
                'details': '{{ dag_run_ecid() }} - Invalid Start Date'
            }
        )

        is_end_date_present = rail.IfOperator(
            task_id='is_end_date_present',
            test=request_payload.is_end_date_present,
            yes_task='is_end_date_correct',
            no_task='create_new_draft'
        )

        is_end_date_correct = rail.IfOperator(
            task_id='is_end_date_correct',
            test=request_payload.is_end_date_correct,
            yes_task='create_new_draft',
            no_task='log_end_date_incorrect'
        )

        log_end_date_incorrect = rail.WriteLogOperator(
            task_id="log_end_date_incorrect",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Invalid End Date',
            severity='Failed',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Failed',
                'details': '{{ dag_run_ecid() }} - Invalid End Date'
            }
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id="create_new_draft",
            endpoint="services/ProjectService1.svc/CreateNewDraft",
            data={},
        )

        update_name = rail.RepliconServiceOperator(
            task_id="update_name",
            endpoint="services/ProjectService1.svc/UpdateName",
            data=lambda: {
                "projectUri": rail.result("create_new_draft"),
                "name": rail.result('get_project_records')[-1]['projectname']+" - "+rail.result('get_project_records')[-1]['projectcode']
            },
        )

        update_code = rail.RepliconServiceOperator(
            task_id="update_code",
            endpoint="services/ProjectService1.svc/UpdateCode",
            data=lambda dag_run: {
                "projectUri": rail.result("create_new_draft"),
                "code": rail.result('get_project_records')[-1]['projectcode']
            },
        )

        get_custom_field_groups = rail.RepliconServiceOperator(
            task_id="get_custom_field_groups",
            endpoint="services/CustomFieldService1.svc/GetCustomFieldGroups",
            data={},
            response_filter=response_filter.map_custom_field_groups
        )

        get_registration_udf_uri = rail.RepliconServiceOperator(
            task_id="get_registration_udf_uri",
            endpoint="services/CustomFieldService1.svc/GetAllCustomFields",
            data=lambda: {
                "objectUri": rail.result('get_custom_field_groups')[0]['uri']
            },
            response_filter=response_filter.map_registration_udf_uri
        )

        get_enabled_custom_field_dropdown_option = rail.RepliconServiceOperator(
            task_id="get_enabled_custom_field_dropdown_option",
            endpoint="services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_registration_udf_uri')[0]['uri']
            }
        )

        is_custom_field_registered = rail.IfOperator(
            task_id='is_custom_field_registered',
            test=request_payload.is_cutsom_field_registered,
            yes_task='update_dropdown_registered',
            no_task='is_custom_field_non_registered'
        )

        update_dropdown_registered = rail.RepliconServiceOperator(
            task_id="update_dropdown_registered",
            endpoint="services/CustomFieldService1.svc/UpdateDropdownValue",
            data=request_payload.create_dropdown_registered
        )

        is_custom_field_non_registered = rail.IfOperator(
            task_id='is_custom_field_non_registered',
            test=request_payload.is_cutsom_field_non_registered,
            yes_task='update_dropdown_non_registered',
            no_task='is_custom_field_non_present'
        )

        update_dropdown_non_registered = rail.RepliconServiceOperator(
            task_id="update_dropdown_non_registered",
            endpoint="services/CustomFieldService1.svc/UpdateDropdownValue",
            data=request_payload.create_dropdown_non_registered
        )

        is_custom_field_non_present = rail.IfOperator(
            task_id='is_custom_field_non_present',
            test=request_payload.is_custom_field_non_present,
            yes_task='update_dropdown_non_present',
            no_task='publish_draft'
        )

        update_dropdown_non_present = rail.RepliconServiceOperator(
            task_id="update_dropdown_non_present",
            endpoint="services/CustomFieldService1.svc/UpdateDropdownValue",
            data=request_payload.create_dropdown_non_present
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id="publish_draft",
            endpoint="services/ProjectService1.svc/PublishDraft",
            data=lambda: {
                "draftUri": rail.result("create_new_draft")
            }
        )

        is_start_end_in_feed_file = rail.IfOperator(
            task_id='is_start_end_in_feed_file',
            test=request_payload.is_start_date_present,
            yes_task='update_date_range_project',
            no_task='is_project_description_present'
        )

        update_date_range_project = rail.RepliconServiceOperator(
            task_id="update_date_range_project",
            endpoint="services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(rail.result('get_project_records')[-1]['startdate']),
                    "endDate": request_payload.get_replicon_date(rail.result('get_project_records')[-1]['enddate']),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        is_project_description_present = rail.IfOperator(
            task_id='is_project_description_present',
            test=request_payload.is_project_description_present,
            yes_task='update_description',
            no_task='update_project_leader'
        )

        update_description = rail.RepliconServiceOperator(
            task_id="update_description",
            endpoint="services/ProjectService1.svc/UpdateDescription",
            data=lambda dag_run: {
                "projectUri": rail.result("publish_draft")["uri"],
                "description": rail.result('get_project_records')[-1]['projectdescription']
            },
        )

        update_project_leader = rail.RepliconServiceOperator(
            task_id="update_project_leader",
            endpoint="services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
                "userUri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":user:2"
            },
        )

        get_resource_department_uri = rail.RepliconServiceOperator(
            task_id="get_resource_department_uri",
            endpoint="services/DepartmentService1.svc/GetEnabledDepartments",
            data={},
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'name',
                        config.default_department_name, 'uri')
        )

        update_project_team_member_assignment = rail.RepliconServiceOperator(
            task_id="update_project_team_member_assignment",
            endpoint="services/ProjectService1.svc/PutProjectTeamMemberAssignments",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
                "resourceUris": [rail.result("get_resource_department_uri")]
            },
        )

        update_allow_timeentry_against_tasks_only = rail.RepliconServiceOperator(
            task_id="update_allow_timeentry_against_tasks_only",
            endpoint="services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
                "allowTimeEntryAgainstTasksOnly": True
            },
        )

        update_project_leader_approval_is_required = rail.RepliconServiceOperator(
            task_id="update_project_leader_approval_is_required",
            endpoint="services/ProjectService1.svc/UpdateProjectLeaderApprovalIsRequired",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
                "isRequired": False
            },
        )

        update_cost_type = rail.RepliconServiceOperator(
            task_id="update_cost_type",
            endpoint="services/ProjectService1.svc/UpdateCostType",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
                "costTypeUri": null
            },
        )

        update_default_billing_currency = rail.RepliconServiceOperator(
            task_id="update_default_billing_currency",
            endpoint="services/ProjectService1.svc/UpdateDefaultBillingCurrency",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
                "currency": {"uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":currency:1"}
            },
        )

        log_project_create_success = rail.WriteLogOperator(
            task_id="log_project_create_success",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Created Project',
            severity='Success',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Success',
                'details': '{{ dag_run_ecid() }} - Created Project'
            }
        )

        get_project_data = rail.RepliconServiceOperator(
            task_id="get_project_data",
            endpoint="services/ProjectService1.svc/GetProjectDetails",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
            },
        )

        is_end_date_present_feed_file = rail.IfOperator(
            task_id='is_end_date_present_feed_file',
            test=request_payload.is_end_date_present,
            yes_task='update_time_entry_date_project',
            no_task='get_children_task_details'
        )

        update_time_entry_date_project = rail.RepliconServiceOperator(
            task_id="update_time_entry_date_project",
            endpoint="services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(rail.result('get_project_records')[-1]['startdate']),
                    "endDate": request_payload.get_replicon_date(rail.result('get_project_records')[-1]['enddate']),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
        )

        update_project_status = rail.RepliconServiceOperator(
            task_id="update_project_status",
            endpoint="services/ProjectService1.svc/UpdateStatus",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"],
                "projectStatusUri": "urn:replicon:project-status-type:completed"
            },
        )

        log_project_status_update_success = rail.WriteLogOperator(
            task_id="log_project_status_update_success",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Project marked Completed',
            severity='Success',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Success',
                'details': '{{ dag_run_ecid() }} - Project marked Completed'
            }
        )

        get_children_task_details = rail.RepliconServiceOperator(
            task_id="get_children_task_details",
            endpoint="services/TaskService1.svc/GetChildrenTaskDetails",
            data=lambda: {
                "parentUri": rail.result("publish_draft")["uri"]
            }
        )

        get_create_project_details = rail.RepliconServiceOperator(
            task_id="get_create_project_details",
            endpoint="services/ProjectService1.svc/GetProjectDetails",
            data=lambda: {
                "projectUri": rail.result("publish_draft")["uri"]
            },
        )

        process_each_task = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_task',
            items='{{ result("get_project_records") | to_json }}',
            trigger_dag_id=f'sunovion_project_sync_process_each_task_child_{config.instance}',
            conf=request_payload.process_create_each_task_conf,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_each_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_task',
            dag_runs='{{ result("process_each_task") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} Project Create - {{ get_error_message() }}',
            severity='Failed',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Failed',
                'details': '{{ dag_run_ecid() }} Project Create - {{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_project_records
        get_project_records >> is_start_date_present
        is_start_date_present >> rail.Label("Yes") >> is_start_date_correct
        is_start_date_present >> rail.Label("No") >> is_end_date_present
        is_start_date_correct >> rail.Label("Yes") >> is_end_date_present
        is_start_date_correct >> rail.Label(
            "No") >> log_start_date_incorrect >> catch_and_log_errors >> log_to_sumo
        is_end_date_present >> rail.Label("No") >> create_new_draft
        is_end_date_present >> rail.Label("Yes") >> is_end_date_correct
        is_end_date_correct >> rail.Label(
            "Yes") >> create_new_draft >> update_name >> update_code
        is_end_date_correct >> rail.Label(
            "No") >> log_end_date_incorrect >> catch_and_log_errors
        update_code >> get_custom_field_groups >> get_registration_udf_uri >> get_enabled_custom_field_dropdown_option >> is_custom_field_registered
        is_custom_field_registered >> rail.Label(
            "Yes") >> update_dropdown_registered >> is_custom_field_non_registered
        is_custom_field_registered >> rail.Label(
            "No") >> is_custom_field_non_registered
        is_custom_field_non_registered >> rail.Label(
            "Yes") >> update_dropdown_non_registered >> is_custom_field_non_present
        is_custom_field_non_registered >> rail.Label(
            "No") >> is_custom_field_non_present
        is_custom_field_non_present >> rail.Label(
            "Yes") >> update_dropdown_non_present >> publish_draft
        is_custom_field_non_present >> rail.Label(
            "No") >> publish_draft >> is_start_end_in_feed_file
        is_start_end_in_feed_file >> rail.Label(
            "Yes") >> update_date_range_project >> is_project_description_present
        is_start_end_in_feed_file >> rail.Label(
            "No") >> is_project_description_present
        is_project_description_present >> rail.Label(
            "Yes") >> update_description >> update_project_leader
        is_project_description_present >> rail.Label(
            "No") >> update_project_leader >> get_resource_department_uri >> update_project_team_member_assignment >> update_allow_timeentry_against_tasks_only
        update_allow_timeentry_against_tasks_only >> update_project_leader_approval_is_required >> update_cost_type
        update_cost_type >> update_default_billing_currency >> log_project_create_success >> get_project_data >> is_end_date_present_feed_file
        is_end_date_present_feed_file >> rail.Label(
            "Yes") >> update_time_entry_date_project >> update_project_status >> log_project_status_update_success >> get_children_task_details
        is_end_date_present_feed_file >> rail.Label(
            "No") >> get_children_task_details >> get_create_project_details >> process_each_task >> wait_for_process_each_task >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
