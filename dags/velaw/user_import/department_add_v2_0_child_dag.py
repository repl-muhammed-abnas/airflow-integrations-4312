
from datetime import timedelta
import uuid
from airflow.models import Variable
import rail
from rail import load_all_records

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_user_import_velawg3_child_department_add_v2_0_{config.instance}',
        description=f'VelawG3 Child_department add V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_fullpath_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_fullpath_present',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        is_fullpath_present = rail.IfOperator(
            task_id='is_fullpath_present',
            test='''{{ dag_run.conf.departmentfullpath | is_truthy }}''',
            yes_task="if_split_length_equals_to_3_3",
            no_task="log_to_sumo",
        )

        if_split_length_equals_to_3_3 = rail.IfOperator(
            task_id='if_split_length_equals_to_3_3',
            test=lambda dag_run: len(
                dag_run.conf['departmentfullpath'].split("|")) == 3,
            yes_task="velaw_department_add_import_logs",
            no_task="if_split_length_equals_to_2_14",
        )

        velaw_department_add_import_logs = rail.CreateLogOperator(
            task_id='velaw_department_add_import_logs',
        )

        def do_format_logs(dag_run):
            def load_records(log_artifact):
                try:
                    logs = load_all_records(log_artifact)
                    return logs
                except:  # pylint: disable=bare-except
                    return []

            log_artifacts = []

            if dag_run.conf['groups_table']:
                log_artifacts.append(dag_run.conf['groups_table'])

            log_records = []
            if log_artifacts:
                for log in log_artifacts:
                    each_log_records = load_records(log)

                    if each_log_records:
                        log_records.extend(each_log_records)

            return list(map(lambda x: {
                **{k: v for k, v in x['properties'].items() if k != 'email'},
                **{
                    'jobid': x['ecid']
                }}, log_records))

        get_groups_table_data = rail.PythonOperator(
            task_id='get_groups_table_data',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs
        )

        velawg3_groups_table_search_entries_4 = rail.PythonOperator(
            task_id='velawg3_groups_table_search_entries_4',
            python_callable=lambda dag_run:  next(iter(filter(
                lambda x: x["type"] == "department" and x["fullpath"] == dag_run.conf.parent, rail.result('get_groups_table_data'))))
        )

        if_entry_col3_present_5 = rail.IfOperator(
            task_id='if_entry_col3_present_5',
            test='''{{ result('velawg3_groups_table_search_entries_4').col3 | is_truthy }}''',
            yes_task="create_department_group_or_apply_modification_level3_6",
            no_task="log_parent_department_9",
        )

        create_department_group_or_apply_modification_level3_6 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level3_6',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": "{{ result('velawg3_groups_table_search_entries_4').uri }}"
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.department }}",
                    "codeToApply": {
                        "value": "{{ dag_run.conf.code }}"
                    },
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        velawg3_groups_table_add_entry_7 = rail.WriteLogOperator(
            task_id='velawg3_groups_table_add_entry_7',
            log="{{ result('velaw_department_add_import_logs') }}",
            message="na",
            severity="Info",
            properties={
                "name": "{{ dag_run.conf.department }}",
                "uri": "{{ result('create_department_group_or_apply_modification_level3_6').uri }}",
                "fullpath": "{{ dag_run.conf.departmentfullpath }}",
                "type": "department"
            }
        )

        log_parent_department_9 = rail.PythonOperator(
            task_id='log_parent_department_9',
            python_callable=lambda dag_run: dag_run.conf.parent.rsplit(
                " | ", 1)[-1].strip()
        )

        create_department_group_or_apply_modification_level2_10 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level2_10',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": "{{ dag_run.conf.compaydepturi }}"
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ result('log_parent_department_9') }}",
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())+"PR"
            }
        )

        velawg3_groups_table_add_entry_11 = rail.WriteLogOperator(
            task_id='velawg3_groups_table_add_entry_11',
            log="{{ result('velaw_department_add_import_logs') }}",
            message="na",
            severity="fixme",
            properties={
                "name": "{{ result('log_parent_department_9') }}",
                "uri": "{{ result('create_department_group_or_apply_modification_level2_10').uri }}",
                "fullpath": "{{ dag_run.conf.parent }}",
                "type": "department"
            }
        )

        create_department_group_or_apply_modification_level3_12 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level3_12',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": "{{ result('create_department_group_or_apply_modification_level2_10').uri }}"
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.department }}",
                    "codeToApply": {
                        "value": "{{ dag_run.conf.code }}"
                    },
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()) + "CH"
            }
        )

        velawg3_groups_table_add_entry_13 = rail.WriteLogOperator(
            task_id='velawg3_groups_table_add_entry_13',
            log="{{ result('velaw_department_add_import_logs') }}",
            message="na",
            severity="fixme",
            properties={
                "name": "{{ dag_run.conf.department }}",
                "uri": "{{ result('create_department_group_or_apply_modification_level3_12').uri }}",
                "fullpath": "{{ dag_run.conf.departmentfullpath }}",
                "type": "department"
            }
        )

        if_split_length_equals_to_2_14 = rail.IfOperator(
            task_id='if_split_length_equals_to_2_14',
            test=lambda dag_run: len(
                dag_run.conf['departmentfullpath'].split("|")) == 2,
            yes_task="create_department_group_or_apply_modification_level2_15",
            no_task="if_split_length_greater_than_3_17",
        )

        create_department_group_or_apply_modification_level2_15 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level2_15',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": "{{ dag_run.conf.compaydepturi }}"
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.department }}",
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        velawg3_groups_table_add_entry_16 = rail.WriteLogOperator(
            task_id='velawg3_groups_table_add_entry_16',
            log="{{ result('velaw_department_add_import_logs') }}",
            message="na",
            severity="fixme",
            properties={
                "name": "{{ dag_run.conf.department }}",
                "uri": "{{ result('create_department_group_or_apply_modification_level3_6').uri }}",
                "fullpath": "{{ dag_run.conf.departmentfullpath }}",
                "type": "department"
            }
        )

        if_split_length_greater_than_3_17 = rail.IfOperator(
            task_id='if_split_length_greater_than_3_17',
            test=lambda dag_run: len(
                dag_run.conf['departmentfullpath'].split("|")) > 3,
            yes_task="log_to_sumo",
            no_task="catch_19"
        )

        catch_19 = rail.EmptyOperator(
            task_id='catch_19',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> is_fullpath_present
        is_fullpath_present >> rail.Label(
            'Yes') >> if_split_length_equals_to_3_3
        is_fullpath_present >> rail.Label('No') >> log_to_sumo
        if_split_length_equals_to_3_3 >> rail.Label(
            'Yes') >> velaw_department_add_import_logs >> get_groups_table_data >> velawg3_groups_table_search_entries_4 \
            >> if_entry_col3_present_5
        if_entry_col3_present_5 >> rail.Label(
            'Yes') >> create_department_group_or_apply_modification_level3_6 >> velawg3_groups_table_add_entry_7 \
            >> if_split_length_equals_to_2_14
        if_entry_col3_present_5 >> rail.Label(
            'No') >> log_parent_department_9 >> create_department_group_or_apply_modification_level2_10 \
            >> velawg3_groups_table_add_entry_11 >> create_department_group_or_apply_modification_level3_12 \
            >> velawg3_groups_table_add_entry_13 >> if_split_length_equals_to_2_14
        if_split_length_equals_to_3_3 >> rail.Label(
            'No') >> if_split_length_equals_to_2_14
        if_split_length_equals_to_2_14 >> rail.Label(
            'Yes') >> create_department_group_or_apply_modification_level2_15 >> velawg3_groups_table_add_entry_16 \
            >> if_split_length_greater_than_3_17
        if_split_length_equals_to_2_14 >> rail.Label(
            'No') >> if_split_length_greater_than_3_17
        if_split_length_greater_than_3_17 >> rail.Label(
            'Yes') >> log_to_sumo
        if_split_length_greater_than_3_17 >> rail.Label(
            'No') >> catch_19 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
