
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.location_add_child_dag_id,
        description=f'MichaelKorsTnA Child_location add V1.0 {config.instance}',
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
            python_callable=lambda dag_run:  len(
                dag_run.conf['location'].split("/"))
        )

        if_log_required_level_3_greater_than_7_4 = rail.IfOperator(
            task_id='if_log_required_level_3_greater_than_7_4',
            test=lambda: int(rail.result('log_required_level_3')) > 7,
            yes_task="accumulate_list_items_5",
            no_task="if_log_required_level_3_equals_to_1_level1_7",
        )

        accumulate_list_items_5 = rail.PythonOperator(
            task_id='accumulate_list_items_5',
            python_callable=lambda: "Maximum allowed hirearchy level is 7" +
            " so skipped for " + str(int(rail.result('log_required_level_3')))
        )

        if_log_required_level_3_equals_to_1_level1_7 = rail.IfOperator(
            task_id='if_log_required_level_3_equals_to_1_level1_7',
            test=lambda: int(rail.result('log_required_level_3')) == 1,
            yes_task="create_location_or_apply_modification_level1_8",
            no_task="log_required_location_name_11",
        )

        create_location_or_apply_modification_level1_8 = rail.RepliconServiceOperator(
            task_id='create_location_or_apply_modification_level1_8',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.location }}",
                    "codeToApply": null,
                    "descriptionToApply": {
                        "value": "{{ dag_run.conf.locationdescription }}"
                    },
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        michael_kors_gmbh_groups_table_add_entry_9 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_9',
            log="{{ dag_run.conf.groupslookuptable }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "name": "{{ result('create_location_or_apply_modification_level1_8').displayText }}",
                "uri": "{{ result('create_location_or_apply_modification_level1_8').uri }}",
                "fullpath": "{{ result('create_location_or_apply_modification_level1_8').displayText }}",
                "type": "location"
            }
        )

        log_required_location_name_11 = rail.PythonOperator(
            task_id='log_required_location_name_11',
            python_callable=lambda dag_run: {
                'requiredlocation': (dag_run.conf['location'].split("/"))[-1].strip(),
                'parentlocation': (dag_run.conf['location'].split("/"))[0]
            }
        )

        michael_kors_gmbh_groups_table_search_entries_13 = rail.FilterLogEntriesOperator(
            task_id='michael_kors_gmbh_groups_table_search_entries_13',
            log="{{dag_run.conf.groupslookuptable}}",
            properties={
                'fullpath': "{{result('log_required_location_name_11').parentlocation}}",
                'type': 'location'
            }
        )

        log_checkifthelocationisalreadyaddedtothelist_14 = rail.PythonOperator(
            task_id='log_checkifthelocationisalreadyaddedtothelist_14',
            python_callable=lambda: (rail.load_all_records(rail.result('michael_kors_gmbh_groups_table_search_entries_13')))[
                0]['properties']['uri'] if rail.result('michael_kors_gmbh_groups_table_search_entries_13', 'length') > 0 else ''
        )

        if_log_checkifthelocationisalreadyaddedtothelist_14_blank_15 = rail.IfOperator(
            task_id='if_log_checkifthelocationisalreadyaddedtothelist_14_blank_15',
            test='''{{ result('log_checkifthelocationisalreadyaddedtothelist_14') | is_falsy }}''',
            yes_task="create_location_or_apply_modification_level1_16",
            no_task="log_requiredparent_uri_18",
        )

        create_location_or_apply_modification_level1_16 = rail.RepliconServiceOperator(
            task_id='create_location_or_apply_modification_level1_16',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ result('log_required_location_name_11').parentlocation }}",
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}1"
            }
        )

        michael_kors_gmbh_groups_table_add_entry_17 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_17',
            log="{{ dag_run.conf.groupslookuptable }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "fullpath": "{{ result('log_required_location_name_11').parentlocation }}",
                "type": "location",
                "name": "{{ result('create_location_or_apply_modification_level1_16').displayText }}",
                "uri": "{{ result('create_location_or_apply_modification_level1_16').uri }}"
            }
        )

        log_requiredparent_uri_18 = rail.PythonOperator(
            task_id='log_requiredparent_uri_18',
            python_callable=lambda: rail.result('create_location_or_apply_modification_level1_16')['uri'] if rail.result(
                'create_location_or_apply_modification_level1_16') else rail.result('log_checkifthelocationisalreadyaddedtothelist_14')
        )

        create_location_or_apply_modification_level2_19 = rail.RepliconServiceOperator(
            task_id='create_location_or_apply_modification_level2_19',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": null,
                        "uri": "{{ result('log_requiredparent_uri_18') }}",
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ result('log_required_location_name_11').requiredlocation }}",
                    "codeToApply": null,
                    "descriptionToApply": {
                        "value": "{{ dag_run.conf.locationdescription }}"
                    },
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}2"
            }
        )

        michael_kors_gmbh_groups_table_add_entry_20 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_20',
            log="{{ dag_run.conf.groupslookuptable }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "fullpath": "{{ dag_run.conf.location }}",
                "type": "location",
                "name": "{{ result('create_location_or_apply_modification_level2_19').displayText }}",
                "uri": "{{ result('create_location_or_apply_modification_level2_19').uri }}"
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
        log_required_level_3 >> if_log_required_level_3_greater_than_7_4
        if_log_required_level_3_greater_than_7_4 >> rail.Label(
            'Yes') >> accumulate_list_items_5 >> catch_error
        if_log_required_level_3_greater_than_7_4 >> rail.Label(
            'No') >> if_log_required_level_3_equals_to_1_level1_7
        if_log_required_level_3_equals_to_1_level1_7 >> rail.Label(
            'Yes') >> create_location_or_apply_modification_level1_8 >> michael_kors_gmbh_groups_table_add_entry_9 >> catch_error
        if_log_required_level_3_equals_to_1_level1_7 >> rail.Label(
            'No') >> log_required_location_name_11 >> michael_kors_gmbh_groups_table_search_entries_13 >> log_checkifthelocationisalreadyaddedtothelist_14
        log_checkifthelocationisalreadyaddedtothelist_14 >> if_log_checkifthelocationisalreadyaddedtothelist_14_blank_15
        if_log_checkifthelocationisalreadyaddedtothelist_14_blank_15 >> rail.Label(
            'Yes') >> create_location_or_apply_modification_level1_16 >> michael_kors_gmbh_groups_table_add_entry_17 >> log_requiredparent_uri_18
        if_log_checkifthelocationisalreadyaddedtothelist_14_blank_15 >> rail.Label(
            'No') >> log_requiredparent_uri_18 >> create_location_or_apply_modification_level2_19 >> michael_kors_gmbh_groups_table_add_entry_20
        michael_kors_gmbh_groups_table_add_entry_20 >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
