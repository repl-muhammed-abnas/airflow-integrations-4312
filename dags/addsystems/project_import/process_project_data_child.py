import rail
from datetime import timedelta
from addsystems.project_import.utils import request_payload, response_filter
from airflow.models import Variable
null = None


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"addsystems_project_data_process_each_child_{config.instance}",
        description=f"addsystems Projectsync Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='has_mandatory_fields'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='has_mandatory_fields',
            end_task='catch_and_log_errors',
        )

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.mandatory_fields_check,
            yes_task="get_project_details",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_madatory_fields_not_present',
            message=lambda dag_run: request_payload.get_exception_message(
                dag_run, request_payload.MANDATORY_FIELDS['project_fields']),
            severity='Exception',
            properties={
                'Projectname': "{{dag_run.conf.item.ClienteleCallName}}",
                'Projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'Projectdescription': "{{dag_run.conf.item.ClienteleCallSummary}}",
                'Projectstartdate': "{{dag_run.conf.item.CallOpenDate}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}",
                'status': 'Exception',
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": '{{ dag_run.conf.item.ClienteleCallNum }}',
                        "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda resp: resp[0]['projectDetails'] if resp[0]['projectDetails'] else null,
        )

        is_project_present = rail.IfOperator(
            task_id="is_project_present",
            test="{{ result('get_project_details') | is_truthy }}",
            yes_task="update_project_data",
            no_task="create_project_data"
        )

        update_project_data = rail.RepliconServiceOperator(
            task_id='update_project_data',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.update_project_create_or_modifiy
        )

        bulk_get_task_details = rail.RepliconServiceOperator(
            task_id='bulk_get_task_details',
            endpoint='/services/ProjectService1.svc/BulkGetTaskDetails2',
            data=request_payload.get_task_details,
            data_handler=response_filter.get_task_value

        )

        create_project_data = rail.RepliconServiceOperator(
            task_id='create_project_data',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.create_project_create_or_modifiy
        )

        assign_all_users = rail.RepliconServiceOperator(
            task_id='assign_all_users',
            endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
            data=request_payload.assign_all_users
        )

        update_billing_rates = rail.RepliconServiceOperator(
            task_id='update_billing_rates',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers',
            data=request_payload.update_billing_rates
        )

        put_billing_rates = rail.RepliconServiceOperator(
            task_id='put_billing_rates',
            endpoint='/services//TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3',
            data=request_payload.put_billingrates
        )

        is_task_present = rail.IfOperator(
            task_id="is_task_present",
            test="{{ result('bulk_get_task_details') | is_truthy }}",
            yes_task="create_task_existing_project",
            no_task="is_expense_present"
        )

        is_tasks_present = rail.IfOperator(
            task_id="is_tasks_present",
            test="{{ dag_run.conf.item.Projects | is_truthy }}",
            yes_task="create_task_new_project",
            no_task="is_expense_present"
        )

        is_expense_present = rail.IfOperator(
            task_id="is_expense_present",
            test="{{ dag_run.conf.item.ExpenseCodes | is_truthy }}",
            yes_task="get_expense_code",
            no_task="project_sync_success"
        )

        create_task_existing_project = rail.RepliconServiceOperator(
            task_id='create_task_existing_project',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            data=lambda dag_run: request_payload.create_tasks_exist_project(
                rail.result('bulk_get_task_details'), dag_run)
        )

        create_task_new_project = rail.RepliconServiceOperator(
            task_id='create_task_new_project',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            data=request_payload.create_tasks_new_project
        )

        get_expense_code = rail.RepliconServiceOperator(
            task_id='get_expense_code',
            endpoint='/services/ProjectService1.svc/GetExpenseCodes',
            data=request_payload.get_expense_code,
            data_handler=response_filter.get_expense_code_value
        )

        put_expense_code = rail.RepliconServiceOperator(
            task_id='put_expense_code',
            endpoint='/services/ProjectService1.svc/PutExpenseCodesAllowingExpenseEntry',
            data=request_payload.put_expense_code

        )

        project_sync_success = rail.WriteLogOperator(
            task_id='project_sync_success',
            message="Project Sync is successfully processed to replicon",
            severity='Success',
            properties={
                'Projectname': "{{dag_run.conf.item.ClienteleCallName}}",
                'Projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'Projectdescription': "{{dag_run.conf.item.ClienteleCallSummary}}",
                'Projectstartdate': "{{dag_run.conf.item.CallOpenDate}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}",
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'Projectname': "{{dag_run.conf.item.ClienteleCallName}}",
                'Projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'Projectdescription': "{{dag_run.conf.item.ClienteleCallSummary}}",
                'Projectstartdate': "{{dag_run.conf.item.CallOpenDate}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}",
                'status': 'Error',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> has_mandatory_fields

        has_mandatory_fields >> rail.Label("Yes") >> get_project_details >> is_project_present >> rail.Label("Yes") >> update_project_data >> bulk_get_task_details >> is_task_present >> rail.Label(
            "Yes") >> create_task_existing_project >> is_expense_present >> rail.Label("Yes") >> get_expense_code >> put_expense_code

        is_task_present >> rail.Label("No") >> is_expense_present
        is_project_present >> rail.Label("No") >> create_project_data >> assign_all_users >> update_billing_rates\
            >> put_billing_rates >> is_tasks_present >> rail.Label("Yes") >> create_task_new_project\
            >> is_expense_present >> rail.Label("Yes") >> get_expense_code >> put_expense_code\
            >> project_sync_success >> catch_and_log_errors >> log_to_sumo

        is_tasks_present >> rail.Label("No") >> is_expense_present

        is_expense_present >> rail.Label("No") >> project_sync_success

        has_mandatory_fields >> rail.Label(
            "No") >> log_madatory_fields_not_present >> catch_and_log_errors >> log_to_sumo
    return dag


rail.for_each_instance(create_child_dag)
