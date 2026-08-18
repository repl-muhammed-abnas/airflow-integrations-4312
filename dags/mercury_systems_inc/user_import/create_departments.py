import uuid
import rail

from mercury_systems_inc.user_import.utils import custom_methods, response_filter

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_department_dagid,
        description='MercurySystermsInc User Import Process New Department Add',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        if_parent_uri_not_exists = rail.IfOperator(
            task_id='if_parent_uri_not_exists',
            test="{{dag_run.conf.parent_uri | is_falsy or dag_run.conf.parent_uri == 'None'}}",
            yes_task='check_newly_created_departments_for_parent',
            no_task='create_department_in_replicon'
        )

        check_newly_created_departments_for_parent = rail.FilterLogEntriesOperator(
            task_id='check_newly_created_departments_for_parent',
            log='{{dag_run.conf.groups_log_table}}',
            properties={
                'group': 'department',
                "action": "Add",
                "status": "Success",
                "parent_fullpath": "{{ dag_run.conf.parent_fullpath.rsplit('|', 1)[0] if '|' in dag_run.conf.parent_fullpath else dag_run.conf.parent_fullpath }}",
                "name": "{{ dag_run.conf.parent_fullpath.rsplit('|', 1)[1] }}"
            }
        )

        get_parent_department_uri = rail.PythonOperator(
            task_id='get_parent_department_uri',
            python_callable=custom_methods.get_parent_uri_from_created_departments
        )

        create_department_in_replicon = rail.RepliconServiceOperator(
            task_id='create_department_in_replicon',
            endpoint='/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=lambda dag_run: {
                "departmentGroup": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "uri": rail.result('get_parent_department_uri') if rail.result('get_parent_department_uri') else dag_run.conf['parent_uri'],
                    },
                },
                "modifications": {
                    "name": dag_run.conf['department_name'],
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        )

        log_new_department_added = rail.WriteLogOperator(
            task_id="log_new_department_added",
            log='{{dag_run.conf.groups_log_table}}',
            message="na",
            severity='Success',
            properties=lambda dag_run: {
                'group': 'department',
                "name": dag_run.conf['department_name'],
                "parent_fullpath": dag_run.conf['parent_fullpath'],
                "parent_uri": dag_run.conf['parent_uri'],
                "action": "Add",
                "status": "Success",
                "detail": f"Department {dag_run.conf['department_name']} created successfully.",
                "uri_if_created": rail.result('create_department_in_replicon')['uri'],
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            log='{{dag_run.conf.groups_log_table}}',
            message="na",
            severity='Error',
            properties=lambda dag_run: {
                'group': 'department',
                "name": dag_run.conf['department_name'],
                "parent_fullpath": dag_run.conf['parent_fullpath'],
                "parent_uri": dag_run.conf['parent_uri'],
                "action": "Add",
                "status": "Error",
                "detail": rail.render_template("{{get_error_message()}}"),
                "uri_if_created": rail.result('create_department_in_replicon')['uri'] if rail.result('create_department_in_replicon') else null,
            }
        )

        if_parent_uri_not_exists >> rail.Label(
            "No") >> create_department_in_replicon

        if_parent_uri_not_exists >> rail.Label(
            "Yes") >> check_newly_created_departments_for_parent >> get_parent_department_uri >> create_department_in_replicon

        create_department_in_replicon >> log_new_department_added >> catch_and_log_error

    return dag


rail.for_each_instance(create_child_dag)
