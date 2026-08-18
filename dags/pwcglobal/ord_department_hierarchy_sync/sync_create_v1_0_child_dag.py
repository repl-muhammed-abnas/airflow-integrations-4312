from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'pwc_ord_department_group_hierarchy_sync_create_v10_{config.instance}',
        description=f'PwC | ORD Department Group Hierarchy Sync Create V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_sync_create_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_ord_department_sync_child_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_ord_department_sync_child_logs',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_ord_department_sync_child_logs = rail.CreateLogOperator(
            task_id='create_ord_department_sync_child_logs'
        )

        create_department_group_or_apply_modification_6 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_6',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": dag_run.conf['parenturi'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": dag_run.conf['name'],
                    "codeToApply": {"value": dag_run.conf['code']} if dag_run.conf['code'] else null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        if_log_checkifthecodeisavailable_85_present_87 = rail.IfOperator(
            task_id='if_log_checkifthecodeisavailable_85_present_87',
            test='''{{ dag_run.conf.existing_dep_uri | is_truthy }}''',
            yes_task="trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088",
            no_task="finish",
        )

        trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088',
            retries=0,
            items=[0],
            trigger_dag_id=f'pwc_ord_department_group_hierarchy_sync_existingdepartment_disable_and_project_userupdate_v10_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "name": "{{ dag_run.conf.name }}",
                "existinguri": "{{ dag_run.conf.existing_dep_uri }}",
                "newuri": "{{ result('create_department_group_or_apply_modification_6').uri }}"
            }
        )

        # pylint: disable=line-too-long
        wait_for_completion_trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088") }}'
        )

        gather_userlist_from_update = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_userlist_from_update',
            dag_runs='{{ result("trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088") }}',
            dagrun_task_id='getallusersassociatedwitholdgroup_4',
            flatten=True
        )

        gather_projectlist_from_update = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_projectlist_from_update',
            dag_runs='{{ result("trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088") }}',
            dagrun_task_id='getallprojectsassociatedwitholdgroup_8',
            flatten=True
        )

        def get_message():
            message = "New hierarchy created, old hierarchy disabled and "
            message += (str(len(rail.result('gather_userlist_from_update'))) +
                        " number of users updated to new group") if rail.result('gather_userlist_from_update') else "No users updated"
            message += ","
            message += (str(len(rail.result('gather_projectlist_from_update'))) +
                        " number of projects updated to new group") if rail.result('gather_projectlist_from_update') else "No projects updated"
            return message

        build_message = rail.PythonOperator(
            task_id='build_message',
            python_callable=get_message
        )

        log_department_new_hierarchy_success = rail.WriteLogOperator(
            task_id='log_department_new_hierarchy_success',
            log="{{ result('create_ord_department_sync_child_logs') }}",
            # pylint: disable=line-too-long
            message="New hierarchy created, old hierarchy disabled and ",
            properties={
                "name": "{{ dag_run.conf.name }}",
                "level": "{{ dag_run.conf.level }}",
                "fullpath": "{{ dag_run.conf.fullpath }}",
                "status": "Success",
                "details": "{{ result('build_message') }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> create_ord_department_sync_child_logs >> create_department_group_or_apply_modification_6 >> if_log_checkifthecodeisavailable_85_present_87
        if_log_checkifthecodeisavailable_85_present_87 >> rail.Label('Yes') >> \
            trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088 >> \
            wait_for_completion_trigger_dag_run_pwc_ord_departmentgroup_hierarchy_existing_department_disable_and_project_user_update_v1_088 >> \
            build_message >> gather_userlist_from_update >> gather_projectlist_from_update \
            >> log_department_new_hierarchy_success >> finish
        if_log_checkifthecodeisavailable_85_present_87 >> rail.Label(
            'No') >> finish
        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
