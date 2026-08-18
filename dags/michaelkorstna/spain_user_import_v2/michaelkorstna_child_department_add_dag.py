
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_spain_user_import_department_add_child_{config.instance}_{config.version}',
        description=f'MichaelKorsTnA Child_department add {config.instance}_{config.version}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_groups,
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
            no_task='log_required_level_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_required_level_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_required_level_3 = rail.PythonOperator(
            task_id='log_required_level_3',
            python_callable=lambda dag_run: {
                'level': len((dag_run.conf['department'].replace("Michael Kors/", "")).split("/")),
                'name': dag_run.conf['department'].replace("Michael Kors/", "")
            }
        )

        if_log_required_level_3_greater_than_6_5 = rail.IfOperator(
            task_id='if_log_required_level_3_greater_than_6_5',
            test=lambda: int(rail.result('log_required_level_3')['level']) > 6,
            yes_task="accumulate_list_items_6",
            no_task="if_log_required_level_3_equals_to_1_level1_8",
        )

        accumulate_list_items_6 = rail.PythonOperator(
            task_id='accumulate_list_items_6',
            python_callable=lambda: "Maximum allowed hirearchy level is 7" +
            " so skipped for " +
            str(int(rail.result('log_required_level_3')['level']) + 1)
        )

        if_log_required_level_3_equals_to_1_level1_8 = rail.IfOperator(
            task_id='if_log_required_level_3_equals_to_1_level1_8',
            test=lambda: int(rail.result('log_required_level_3')['level']) == 1,
            yes_task="create_department_group_or_apply_modification_level2_9",
            no_task="log_required_department_group_name_12",
        )

        create_department_group_or_apply_modification_level2_9 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level2_9',
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
                    "name": "{{ result('log_required_level_3').name }}",
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        michael_kors_gmbh_groups_table_add_entry_10 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_10',
            log="{{ dag_run.conf.groupslookuptable }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "name": "{{ result('create_department_group_or_apply_modification_level2_9').displayText }}",
                "uri": "{{ result('create_department_group_or_apply_modification_level2_9').uri }}",
                "fullpath": "{{ result('create_department_group_or_apply_modification_level2_9').displayText }}",
                "type": "department"
            }
        )

        log_required_department_group_name_12 = rail.PythonOperator(
            task_id='log_required_department_group_name_12',
            # pylint: disable = unnecessary-lambda
            python_callable=lambda: ((rail.result('log_required_level_3')['name']).split("/"))[-1].strip()
        )

        log_required_parent_department_group_full_path_13 = rail.PythonOperator(
            task_id='log_required_parent_department_group_full_path_13',
            python_callable=lambda:  "Michael Kors/" + (rail.result('log_required_level_3')['name'].split("/"))[0]
        )

        michael_kors_gmbh_groups_table_search_entries_14 = rail.FilterLogEntriesOperator(
            task_id='michael_kors_gmbh_groups_table_search_entries_14',
            log="{{dag_run.conf.groupslookuptable}}",
            properties={
                'fullpath': "{{result('log_required_parent_department_group_full_path_13')}}",
                'type': 'department'
            }
        )

        log_checkifthe_department_groupisalreadyaddedtothelist_15 = rail.PythonOperator(
            task_id='log_checkifthe_department_groupisalreadyaddedtothelist_15',
            python_callable=lambda: (rail.load_all_records(rail.result('michael_kors_gmbh_groups_table_search_entries_14')))[
                0]['properties']['uri'] if rail.result('michael_kors_gmbh_groups_table_search_entries_14', 'length') > 0 else ''
        )

        if_log_checkifthe_department_groupisalreadyaddedtothelist_15_blank_16 = rail.IfOperator(
            task_id='if_log_checkifthe_department_groupisalreadyaddedtothelist_15_blank_16',
            test='''{{ result('log_checkifthe_department_groupisalreadyaddedtothelist_15') | is_falsy }}''',
            yes_task="log_required_department_group_level2_name_17",
            no_task="log_requiredparent_uri_20",
        )

        log_required_department_group_level2_name_17 = rail.PythonOperator(
            task_id='log_required_department_group_level2_name_17',
            python_callable=lambda:  (rail.result(
                'log_required_parent_department_group_full_path_13').split("/"))[-1]
        )

        create_department_group_or_apply_modification_level2_18 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level2_18',
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
                    "name": "{{ result('log_required_department_group_level2_name_17') }}",
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}1"
            }
        )

        michael_kors_gmbh_groups_table_add_entry_19 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_19',
            log="{{ dag_run.conf.groupslookuptable }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "fullpath": "{{ result('log_required_parent_department_group_full_path_13') }}",
                "type": "department",
                "name": "{{ result('create_department_group_or_apply_modification_level2_18').displayText }}",
                "uri": "{{ result('create_department_group_or_apply_modification_level2_18').uri }}"
            }
        )

        log_requiredparent_uri_20 = rail.PythonOperator(
            task_id='log_requiredparent_uri_20',
            python_callable=lambda: rail.result('create_department_group_or_apply_modification_level2_18')['uri'] if rail.result(
                'create_department_group_or_apply_modification_level2_18') else rail.result('log_checkifthe_department_groupisalreadyaddedtothelist_15')
        )

        create_department_group_or_apply_modification_level3_21 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level3_21',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": "{{ result('log_requiredparent_uri_20') }}"
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ result('log_required_department_group_name_12') }}",
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}3"
            }
        )

        michael_kors_gmbh_groups_table_add_entry_22 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_22',
            log="{{ dag_run.conf.groupslookuptable }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "fullpath": "{{ dag_run.conf.department }}",
                "type": "department",
                "name": "{{ result('create_department_group_or_apply_modification_level3_21').displayText }}",
                "uri": "{{ result('create_department_group_or_apply_modification_level3_21').uri }}"
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> log_required_level_3
        log_required_level_3 >> if_log_required_level_3_greater_than_6_5
        if_log_required_level_3_greater_than_6_5 >> rail.Label(
            'Yes') >> accumulate_list_items_6 >> catch_error
        if_log_required_level_3_greater_than_6_5 >> rail.Label(
            'No') >> if_log_required_level_3_equals_to_1_level1_8
        if_log_required_level_3_equals_to_1_level1_8 >> rail.Label(
            'Yes') >> create_department_group_or_apply_modification_level2_9 >> michael_kors_gmbh_groups_table_add_entry_10 >> catch_error
        if_log_required_level_3_equals_to_1_level1_8 >> rail.Label(
            'No') >> log_required_department_group_name_12 >> log_required_parent_department_group_full_path_13
        log_required_parent_department_group_full_path_13 >> michael_kors_gmbh_groups_table_search_entries_14
        michael_kors_gmbh_groups_table_search_entries_14 >> log_checkifthe_department_groupisalreadyaddedtothelist_15
        log_checkifthe_department_groupisalreadyaddedtothelist_15 >> if_log_checkifthe_department_groupisalreadyaddedtothelist_15_blank_16
        if_log_checkifthe_department_groupisalreadyaddedtothelist_15_blank_16 >> rail.Label(
            'Yes') >> log_required_department_group_level2_name_17 >> create_department_group_or_apply_modification_level2_18
        create_department_group_or_apply_modification_level2_18 >> michael_kors_gmbh_groups_table_add_entry_19 >> log_requiredparent_uri_20
        if_log_checkifthe_department_groupisalreadyaddedtothelist_15_blank_16 >> rail.Label(
            'No') >> log_requiredparent_uri_20 >> create_department_group_or_apply_modification_level3_21 >> michael_kors_gmbh_groups_table_add_entry_22
        michael_kors_gmbh_groups_table_add_entry_22 >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
