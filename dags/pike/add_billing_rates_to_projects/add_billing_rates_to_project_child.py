import rail
from pike.add_billing_rates_to_projects.utils import request_payload

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pike_adding_billing_rates_to_project_child_{config.instance}',
        description=f'Pike Adding Billing Rates to Project Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_action_available = rail.IfOperator(
            task_id='is_action_available',
            test='{{ dag_run.conf.item.action | is_truthy }}',
            yes_task='process_billing_rate',
            no_task='log_action_not_available'
        )

        log_action_not_available = rail.WriteLogOperator(
            task_id='log_action_not_available',
            message="Action not Available",
            severity="Skipped",
            properties={
                "project_name": "{{ dag_run.conf.item.projectname }}",
                "billing_rate_name": '{{ dag_run.conf.item.Billingratename }}',
                "status": "Action not Available"
            }
        )

        process_billing_rate = rail.EmptyOperator(
            task_id='process_billing_rate'
        )

        is_action_add = rail.IfOperator(
            task_id='is_action_add',
            test=lambda dag_run: bool(dag_run.conf["item"]["action"] == 'ADD'
                    or dag_run.conf["item"]["action"] == 'Add'
                        or dag_run.conf["item"]["action"] == 'add' ),
            yes_task='is_billing_rate_available'
        )

        is_billing_rate_available = rail.IfOperator(
            task_id='is_billing_rate_available',
            test='{{ dag_run.conf.billing_rate_uri | is_truthy }}',
            yes_task='get_project_details',
            no_task='log_billing_rate_not_present'
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_details_payload
        )

        is_material_inprogress_and_not_non_billable = rail.IfOperator(
            task_id='is_material_inprogress_and_not_non_billable',
            test=lambda: bool('Materials' in rail.result("get_project_details")[0]["projectDetails"]["billingType"]["displayText"]
                              and 'In Progress' in rail.result("get_project_details")[0]["projectDetails"]["status"]["displayText"]
                              and rail.result("get_project_details")[0]["projectDetails"]["timeAndExpenseEntryType"]["displayText"] != 'Non-Billable'),
            yes_task='update_project_billing_rate',
            no_task='is_material_inprogress_and_non_billable'
        )

        update_project_billing_rate = rail.RepliconServiceOperator(
            task_id='update_project_billing_rate',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers',
            data=request_payload.get_update_billing_rate_payload
        )

        log_billing_rate_update = rail.WriteLogOperator(
            task_id='log_billing_rate_update',
            message="Added",
            severity="Success",
            properties={
                "project_name": "{{ dag_run.conf.item.projectname }}",
                "billing_rate_name": '{{ dag_run.conf.item.Billingratename }}',
                "status": "Added"
            }
        )

        is_material_inprogress_and_non_billable = rail.IfOperator(
            task_id='is_material_inprogress_and_non_billable',
            test=lambda: bool('Materials' in rail.result("get_project_details")[0]["projectDetails"]["billingType"]["displayText"]
                              and 'In Progress' in rail.result("get_project_details")[0]["projectDetails"]["status"]["displayText"]
                              and rail.result("get_project_details")[0]["projectDetails"]["timeAndExpenseEntryType"]["displayText"] == 'Non-Billable'),
            yes_task='log_non_billable',
            no_task='is_material_and_not_inprogress'
        )

        log_non_billable = rail.WriteLogOperator(
            task_id='log_non_billable',
            message='Project Time & Expense entry type is Non-Billable',
            severity="Exception",
            properties={
                "project_name": "{{ dag_run.conf.item.projectname }}",
                "billing_rate_name": '{{ dag_run.conf.item.Billingratename }}',
                "status": 'Project Time & Expense entry type is Non-Billable'
            }
        )

        is_material_and_not_inprogress = rail.IfOperator(
            task_id='is_material_and_not_inprogress',
            test=lambda: bool('Materials' in rail.result("get_project_details")[0]["projectDetails"]["billingType"]["displayText"]
                              and 'In Progress' not in rail.result("get_project_details")[0]["projectDetails"]["status"]["displayText"]),
            yes_task='log_project_not_inprogress',
            no_task='is_billtype_not_material'
        )

        log_project_not_inprogress = rail.WriteLogOperator(
            task_id='log_project_not_inprogress',
            message='Project isn\'t in "In Progress" status',
            severity="Exception",
            properties={
                "project_name": "{{ dag_run.conf.item.projectname }}",
                "billing_rate_name": '{{ dag_run.conf.item.Billingratename }}',
                "status": 'Project isn\'t in "In Progress" status'
            }
        )

        is_billtype_not_material = rail.IfOperator(
            task_id='is_billtype_not_material',
            test=lambda: bool('Materials' not in rail.result("get_project_details")[0]["projectDetails"]["billingType"]["displayText"]),
            yes_task='log_billing_not_time_and_material'
        )

        log_billing_not_time_and_material = rail.WriteLogOperator(
            task_id='log_billing_not_time_and_material',
            message='Project billing type isn\'t  "Time and Materials"',
            severity="Exception",
            properties={
                "project_name": "{{ dag_run.conf.item.projectname }}",
                "billing_rate_name": '{{ dag_run.conf.item.Billingratename }}',
                "status": 'Project billing type isn\'t  "Time and Materials"'
            }
        )

        log_billing_rate_not_present = rail.WriteLogOperator(
            task_id='log_billing_rate_not_present',
            message='Billing rate not available',
            severity="Skipped",
            properties={
                "project_name": "{{ dag_run.conf.item.projectname }}",
                "billing_rate_name": '{{ dag_run.conf.item.Billingratename }}',
                "status": 'Billing rate not available'
            }
        )

        is_action_disable = rail.IfOperator(
            task_id='is_action_disable',
            test=lambda dag_run: bool(dag_run.conf["item"]["action"] == 'DISABLE'
                    or dag_run.conf["item"]["action"] == 'Disable'
                        or dag_run.conf["item"]["action"] == 'disable' ),
            yes_task='is_billing_rate_available_2'
        )

        is_billing_rate_available_2 = rail.IfOperator(
            task_id='is_billing_rate_available_2',
            test='{{ dag_run.conf.billing_rate_uri | is_truthy }}',
            yes_task='get_project_details_2',
            no_task='log_billing_rate_not_present_2'
        )

        get_project_details_2 = rail.RepliconServiceOperator(
            task_id='get_project_details_2',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_details_payload
        )

        is_billtype_material_and_status_inprogress_2 = rail.IfOperator(
            task_id='is_billtype_material_and_status_inprogress_2',
            test=lambda: bool('Materials' in rail.result("get_project_details_2")[0]["projectDetails"]["billingType"]["displayText"]
                              and 'In Progress' in rail.result("get_project_details_2")[0]["projectDetails"]["status"]["displayText"]
                              and 'Non-Billable' not in rail.result("get_project_details_2")[0]["projectDetails"]["timeAndExpenseEntryType"]["displayText"]),
            yes_task='disable_project_billing_rate'
        )

        disable_project_billing_rate = rail.RepliconServiceOperator(
            task_id='disable_project_billing_rate',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers',
            data=lambda dag_run: {
                "projectUri": rail.result('get_project_details_2')[0]["projectDetails"]["uri"],
                "billingRateUri": dag_run.conf["billing_rate_uri"],
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:force-unavailable"
            }
        )

        log_disable_billing_rate = rail.WriteLogOperator(
            task_id='log_disable_billing_rate',
            message="Disabled",
            severity="Success",
            properties={
                "project_name": "{{ dag_run.conf.item.projectname }}",
                "billing_rate_name": '{{ dag_run.conf.item.Billingratename }}',
                "status": "Disabled"
            }
        )

        log_billing_rate_not_present_2 = rail.WriteLogOperator(
            task_id='log_billing_rate_not_present_2',
            message='Billing rate not available',
            severity="Success",
            properties={
                "project_name": "{{ dag_run.conf.item.projectname }}",
                "billing_rate_name": '{{ dag_run.conf.item.Billingratename }}',
                "status": 'Billing rate not available'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity="Error",
            properties={
                "project_name": "{{ dag_run.conf.item.projectname }}",
                "billing_rate_name": '{{ dag_run.conf.item.Billingratename }}',
                "status": '{{ get_error_message() }}'
            }
        )

        is_action_available >> rail.Label("Yes") >> process_billing_rate >> is_action_add >> rail.Label("Yes") >> is_billing_rate_available
        is_billing_rate_available >> rail.Label("Yes") >> get_project_details >> is_material_inprogress_and_not_non_billable

        is_material_inprogress_and_not_non_billable >> rail.Label("No") >> is_material_inprogress_and_non_billable
        is_material_inprogress_and_not_non_billable >> rail.Label("Yes") >> update_project_billing_rate >> log_billing_rate_update >> catch_and_log_errors

        is_material_inprogress_and_non_billable >> rail.Label("Yes") >> log_non_billable >> catch_and_log_errors
        is_material_inprogress_and_non_billable >> rail.Label("No") >> is_material_and_not_inprogress

        is_material_and_not_inprogress >> rail.Label("Yes") >> log_project_not_inprogress >> catch_and_log_errors
        is_material_and_not_inprogress >> rail.Label("No") >> is_billtype_not_material

        is_billtype_not_material >> rail.Label("Yes") >> log_billing_not_time_and_material >> catch_and_log_errors

        is_billing_rate_available >> rail.Label("No") >> log_billing_rate_not_present >> catch_and_log_errors

        process_billing_rate >> is_action_disable >> rail.Label("Yes") >> is_billing_rate_available_2

        is_billing_rate_available_2 >> rail.Label("Yes") >> get_project_details_2 >> is_billtype_material_and_status_inprogress_2
        is_billtype_material_and_status_inprogress_2 >> rail.Label("Yes")>> disable_project_billing_rate >> log_disable_billing_rate >> catch_and_log_errors

        is_billing_rate_available_2 >> rail.Label("No") >> log_billing_rate_not_present_2 >> catch_and_log_errors
        is_action_available >> rail.Label("No") >> log_action_not_available >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
