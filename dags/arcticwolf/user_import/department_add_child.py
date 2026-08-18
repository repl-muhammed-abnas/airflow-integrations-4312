
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.department_add_child_dagid,
        description=f'Arcticwolf Child department add {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_group,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_departmentlevel2uri_present_1_level2isavailable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_departmentlevel2uri_present_1_level2isavailable',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_departmentlevel2uri_present_1_level2isavailable = rail.IfOperator(
            task_id='if_request_departmentlevel2uri_present_1_level2isavailable',
            test='''{{ dag_run.conf.departmentlevel2uri | is_truthy }}''',
            yes_task="if_request_departmentlevel3_present",
            no_task="search_departments_in_group_lookup",
        )

        if_request_departmentlevel3_present = rail.IfOperator(
            task_id='if_request_departmentlevel3_present',
            test='''{{ dag_run.conf.departmentlevel3 | is_truthy }}''',
            yes_task="create_department_group_or_apply_modification_level3",
            no_task="catch_error",
        )

        create_department_group_or_apply_modification_level3 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": "{{ dag_run.conf.departmentlevel2uri }}"
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.departmentlevel3 }}",
                    "codeToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        search_departments_in_group_lookup = rail.FilterLogEntriesOperator(
            task_id='search_departments_in_group_lookup',
            log="{{dag_run.conf.groupsupdatelookup}}",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}"
            }
        )

        load_found_departments = rail.PythonOperator(
            task_id='load_found_departments',
            python_callable=lambda: rail.load_all_records(
                rail.result('search_departments_in_group_lookup'))
        )

        if_request_departmentlevel2uri_present = rail.IfOperator(
            task_id='if_request_departmentlevel2uri_present',
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'load_found_departments'), 'properties.fullpath', 'Arctic Wolf|' + dag_run.conf['departmentlevel2'], 'properties.uri', '')),
            yes_task="create_department",
            no_task="create_department_group_or_apply_modification_level2",
        )

        create_department = rail.RepliconServiceOperator(
            task_id='create_department',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.find_first_by_attr_and_get_attr(rail.result('load_found_departments'), 'fullpath', 'Arctic Wolf|' +
                                                                    dag_run.conf['departmentlevel2'], 'uri', '')
                    },
                },
                "modifications": {
                    "name": "{{ dag_run.conf.d }}",
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        create_department_group_or_apply_modification_level2 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level2',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": "{{ dag_run.conf.companydepturi }}"
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.departmentlevel2 }}",
                    "codeToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}Level2"
            }
        )

        groups_table_add_entry = rail.WriteLogOperator(
            task_id='groups_table_add_entry',
            log="{{ dag_run.conf.groupsupdatelookup }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "name": "{{ result('create_department_group_or_apply_modification_level2').displayText }}",
                "uri": "{{ result('create_department_group_or_apply_modification_level2').uri }}",
                "fullpath": "Arctic Wolf|{{ dag_run.conf.departmentlevel2 }}",
                "type": "department"
            }
        )

        if_request_department_level3_present = rail.IfOperator(
            task_id='if_request_department_level3_present',
            test='''{{ dag_run.conf.departmentlevel3 | is_truthy }}''',
            yes_task="create_department_group_or_apply_modification_departmentlevel3",
            no_task="catch_error",
        )

        create_department_group_or_apply_modification_departmentlevel3 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_departmentlevel3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": "{{ result('create_department_group_or_apply_modification_level2').uri }}"
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.departmentlevel3 }}",
                    "codeToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}Level3"
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> if_request_departmentlevel2uri_present_1_level2isavailable
        if_request_departmentlevel2uri_present_1_level2isavailable >> rail.Label(
            'Yes') >> if_request_departmentlevel3_present
        if_request_departmentlevel3_present >> rail.Label(
            'Yes') >> create_department_group_or_apply_modification_level3 >> catch_error
        if_request_departmentlevel3_present >> rail.Label(
            'No') >> catch_error
        if_request_departmentlevel2uri_present_1_level2isavailable >> rail.Label(
            'No') >> search_departments_in_group_lookup >> load_found_departments >> if_request_departmentlevel2uri_present
        if_request_departmentlevel2uri_present >> rail.Label(
            'Yes') >> create_department >> catch_error
        if_request_departmentlevel2uri_present >> rail.Label(
            'No') >> create_department_group_or_apply_modification_level2 >> groups_table_add_entry
        groups_table_add_entry >> if_request_department_level3_present
        if_request_department_level3_present >> rail.Label(
            'Yes') >> create_department_group_or_apply_modification_departmentlevel3 >> catch_error
        if_request_department_level3_present >> rail.Label(
            'No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
