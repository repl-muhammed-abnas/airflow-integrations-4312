
from datetime import timedelta, datetime
import json
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Audaxgroup Master check for Project Task Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)


        list_processing_dir_0 = rail.SFTPListFilesOperator(
            task_id='list_processing_dir_0',
            paths=[config.processing_filepath],
        )

        if_processing_dir_not_empty_1 = rail.IfOperator(
            task_id='if_processing_dir_not_empty_1',
            test=lambda: bool(rail.result('list_processing_dir_0')[config.processing_filepath][0]['name'] if rail.result(
                'list_processing_dir_0') and rail.result(
                'list_processing_dir_0')[config.processing_filepath] else null),
            yes_task="log_jobhistory_skip_processing_1b",
            no_task="list_dir_2",
        )

        log_jobhistory_skip_processing_1b = rail.PythonOperator(
            task_id='log_jobhistory_skip_processing_1b',
            python_callable=lambda: "Skipped - files already present in Processing folder from a previous run"
        )

        list_dir_2 = rail.SFTPListFilesOperator(
            task_id='list_dir_2',
            paths=[config.input_filepath],
        )

        log_timenow_3 = rail.PythonOperator(
            task_id='log_timenow_3',
            python_callable=lambda: datetime.now().strftime('%m-%d-%Y %H:%M')
        )

        if_first_name_blank_4 = rail.IfOperator(
            task_id='if_first_name_blank_4',
            test=lambda: not bool(rail.result('list_dir_2')[config.input_filepath][0]['name'] if rail.result(
                'list_dir_2') and rail.result(
                'list_dir_2')[config.input_filepath][0] else null),
            yes_task="log_jobhistory_5",
            no_task="audax_project_task_file_processing_lookup_table",
        )

        log_jobhistory_5=rail.PythonOperator(
            task_id='log_jobhistory_5',
            python_callable= lambda:  "No files to process"
        )

        audax_project_task_file_processing_lookup_table = rail.CreateLogOperator(
            task_id='audax_project_task_file_processing_lookup_table'
        )

        foreach_dir_2_12=rail.ForEachOperator(
            task_id='foreach_dir_2_12',
            items=lambda: rail.result("list_dir_2")[config.input_filepath],
            start_task = 'rename_13',
            end_task = 'foreach_dir_2_12_end'
        )


        rename_13=rail.SFTPMoveFileOperator(
            task_id='rename_13',
            new_filename=config.processing_filepath+'''/{{ dag_run_ecid() }}_{{ result('foreach_dir_2_12').name }}''',
            existing_filename=config.input_filepath+'''/{{ result('foreach_dir_2_12').name }}''',
        )


        if_name_downcase_contains_putproject_14=rail.IfOperator(
            task_id='if_name_downcase_contains_putproject_14',
            test='''{{ result('foreach_dir_2_12').name.lower() | matches('putproject') }}''',
            yes_task="accumulate_list_items_15",
            no_task="if_name_downcase_contains_updateproject_16",
        )

        accumulate_list_items_15=rail.SetVariableOperator(
            task_id='accumulate_list_items_15',
            name='putproject',
            append=True,
            value= {
                    "originalfilename": "{{ result('foreach_dir_2_12').name }}",
                    "newfilepath": config.processing_filepath+"/{{ dag_run_ecid() }}_{{ result('foreach_dir_2_12').name }}"
                }
        )

        if_name_downcase_contains_updateproject_16=rail.IfOperator(
            task_id='if_name_downcase_contains_updateproject_16',
            test='''{{result('foreach_dir_2_12').name.lower() | matches('updateproject') }}''',
            yes_task="accumulate_list_items_17",
            no_task="if_name_downcase_contains_puttask_18",
        )


        accumulate_list_items_17=rail.SetVariableOperator(
            task_id='accumulate_list_items_17',
            name='updateproject',
            append=True,
            value= {
                "originalfilename": "{{ result('foreach_dir_2_12').name }}",
                "newfilepath": config.processing_filepath + "/{{ dag_run_ecid() }}_{{ result('foreach_dir_2_12').name }}"
            }
        )


        if_name_downcase_contains_puttask_18=rail.IfOperator(
            task_id='if_name_downcase_contains_puttask_18',
            test='''{{ result('foreach_dir_2_12').name.lower() | matches('puttask') }}''',
            yes_task="accumulate_list_items_19",
            no_task="if_name_downcase_contains_updatetask_20",
        )


        accumulate_list_items_19=rail.SetVariableOperator(
            task_id='accumulate_list_items_19',
            name='puttask',
            append=True,
            value= {
                "originalfilename": "{{ result('foreach_dir_2_12').name }}",
                "newfilepath": config.processing_filepath + "/{{ dag_run_ecid() }}_{{ result('foreach_dir_2_12').name }}"
            }
        )


        if_name_downcase_contains_updatetask_20=rail.IfOperator(
            task_id='if_name_downcase_contains_updatetask_20',
            test='''{{ result('foreach_dir_2_12').name.lower() | matches('updatetask') }}''',
            yes_task="accumulate_list_items_21",
            no_task="if_name_downcase_not_contains_putproject_22",
        )


        accumulate_list_items_21=rail.SetVariableOperator(
            task_id='accumulate_list_items_21',
            name='updatetask',
            append=True,
            value= {
            "originalfilename": "{{ result('foreach_dir_2_12').name }}",
            "newfilepath": config.processing_filepath + "/{{ dag_run_ecid() }}_{{ result('foreach_dir_2_12').name }}"
        }
        )


        if_name_downcase_not_contains_putproject_22=rail.IfOperator(
            task_id='if_name_downcase_not_contains_putproject_22',
            test='''{{ result('foreach_dir_2_12').name.lower() | not matches('putproject') and result('foreach_dir_2_12').name.lower() | not matches('updateproject')  and result('foreach_dir_2_12').name.lower() | not matches('puttask') and result('foreach_dir_2_12').name.lower() | not matches('updatetask') }}''',
            yes_task="accumulate_list_items_23",
            no_task="foreach_dir_2_12_end",
        )


        accumulate_list_items_23=rail.SetVariableOperator(
            task_id='accumulate_list_items_23',
            name='invalidfilenames',
            append=True,
            value= {
            "filenames": "{{ result('foreach_dir_2_12').name }}",
            "filepath": config.processing_filepath + "/{{ dag_run_ecid() }}_{{ result('foreach_dir_2_12').name }}"
        }
        )


        foreach_dir_2_12_end=rail.EmptyOperator(
            task_id='foreach_dir_2_12_end',
        )

        if_accumulate_list_items_23_list_items_greater_than_0_24=rail.IfOperator(
            task_id='if_accumulate_list_items_23_list_items_greater_than_0_24',
            test=lambda: len(rail.result('accumulate_list_items_23')) > 0 if rail.result('accumulate_list_items_23') else False ,
            yes_task="log_invalid_filenames_25",
            no_task="trigger_dag_run_live_audaxgroup_process_put_projects_file33",
        )

        log_invalid_filenames_25=rail.PythonOperator(
            task_id='log_invalid_filenames_25',
            python_callable= lambda:  ",".join([item['filenames'] for item in rail.result('accumulate_list_items_23')])
        )


        send_mail_26=rail.EmailOperator(
            task_id='send_mail_26',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject= ''' {{ get_company_key() }} | Project Task Import - Invalid file names found {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />Hello, <br />
            <br />
            The Project Task Import job has not processed the below file(s) because of invalid file name: <br />
            - {{ result('log_invalid_filenames_25') }} < br />
            <br />
            They have been archived.<br />
            <br />
            <p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
            ''',
            params=None,
        )

        foreach_accumulate_list_items_23_27=rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_23_27',
            items="{{ result('accumulate_list_items_23').value | to_json }}",
            start_task = 'rename_29',
            end_task = 'foreach_accumulate_list_items_23_27_end'
        )


        rename_29=rail.SFTPMoveFileOperator(
            task_id='rename_29',
            new_filename=config.archive_filepath +'''/{{ result('foreach_accumulate_list_items_23_27').filenames }}''',
            existing_filename= '''{{ result('foreach_accumulate_list_items_23_27').filepath }}''',
        )


        foreach_accumulate_list_items_23_27_end=rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_23_27_end',
        )

        trigger_dag_run_live_audaxgroup_process_put_projects_file33=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_audaxgroup_process_put_projects_file33',
            retries=0,
            items=lambda : json.dumps(rail.result('accumulate_list_items_15')['value']) if rail.result('accumulate_list_items_15') else [],
            trigger_dag_id=config.process_put_projects_file_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
            "filename": item["originalfilename"],
            "fullpath": item["newfilepath"],
            "companykey": config.company_key,
            "emailaddress": config.tenant_email,
            "parent_ecid": rail.render_template("{{dag_run_ecid()}}"),
            "audax_project_task_file_processing_lookup_table": rail.result('audax_project_task_file_processing_lookup_table')
            }
        )

        wait_for_completion_trigger_dag_run_live_audaxgroup_process_put_projects_file33 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_audaxgroup_process_put_projects_file33',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_audaxgroup_process_put_projects_file33") }}'
        )

        trigger_dag_run_live_audaxgroup_process_update_projects_file37=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_audaxgroup_process_update_projects_file37',
            retries=0,
            items=lambda : json.dumps(rail.result('accumulate_list_items_17')['value']) if rail.result('accumulate_list_items_17') else [],
            trigger_dag_id=config.process_update_projects_file_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "filename": item["originalfilename"],
                "fullpath": item["newfilepath"],
                "companykey": config.company_key,
                "emailaddress": config.tenant_email,
                "parent_ecid": rail.render_template("{{dag_run_ecid()}}"),
                "audax_project_task_file_processing_lookup_table": rail.result('audax_project_task_file_processing_lookup_table')
            }
        )

        wait_for_completion_trigger_dag_run_live_audaxgroup_process_update_projects_file37 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_audaxgroup_process_update_projects_file37',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_audaxgroup_process_update_projects_file37") }}'
        )


        trigger_dag_run_live_audaxgroup_process_tasks_file41=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_audaxgroup_process_tasks_file41',
            retries=0,
            items=lambda : json.dumps(rail.result('accumulate_list_items_19')['value']) if rail.result('accumulate_list_items_19') else [],
            trigger_dag_id=config.process_tasks_file_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "filename": item["originalfilename"],
                "fullpath": item["newfilepath"],
                "companykey": config.company_key,
                "emailaddress": config.tenant_email,
                "parent_ecid": rail.render_template("{{dag_run_ecid()}}"),
                "audax_project_task_file_processing_lookup_table": rail.result('audax_project_task_file_processing_lookup_table')
            }
        )

        wait_for_completion_trigger_dag_run_live_audaxgroup_process_tasks_file41 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_audaxgroup_process_tasks_file41',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_audaxgroup_process_tasks_file41") }}'
        )

        trigger_dag_run_live_audaxgroup_process_tasks_file45=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_audaxgroup_process_tasks_file45',
            retries=0,
            items=lambda : json.dumps(rail.result('accumulate_list_items_21')['value']) if rail.result('accumulate_list_items_21') else [],
            trigger_dag_id=config.process_tasks_file_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "filename": item["originalfilename"],
                "fullpath": item["newfilepath"],
                "companykey": config.company_key,
                "emailaddress": config.tenant_email,
                "parent_ecid": rail.render_template("{{dag_run_ecid()}}"),
                "audax_project_task_file_processing_lookup_table": rail.result('audax_project_task_file_processing_lookup_table')
            }
        )

        wait_for_completion_trigger_dag_run_live_audaxgroup_process_tasks_file45 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_audaxgroup_process_tasks_file45',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_audaxgroup_process_tasks_file45") }}'
        )

        log_jobhistory_47=rail.PythonOperator(
            task_id='log_jobhistory_47',
            python_callable= lambda: "Processed"
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )
        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        list_processing_dir_0 >> if_processing_dir_not_empty_1
        if_processing_dir_not_empty_1 >> rail.Label(
            'Yes') >> log_jobhistory_skip_processing_1b >> finish
        if_processing_dir_not_empty_1 >> rail.Label(
            'No') >> list_dir_2

        list_dir_2 >> log_timenow_3 >> if_first_name_blank_4
        if_first_name_blank_4 >> rail.Label(
            'Yes') >> log_jobhistory_5 >> finish
        if_first_name_blank_4 >> rail.Label(
            'No') >> audax_project_task_file_processing_lookup_table >> foreach_dir_2_12
        foreach_dir_2_12 >> rename_13 >> if_name_downcase_contains_putproject_14
        if_name_downcase_contains_putproject_14 >> rail.Label(
            'Yes') >> accumulate_list_items_15 >> foreach_dir_2_12_end
        if_name_downcase_contains_putproject_14 >> rail.Label(
            'No') >> if_name_downcase_contains_updateproject_16

        if_name_downcase_contains_updateproject_16  >> rail.Label(
            'Yes') >> accumulate_list_items_17 >> foreach_dir_2_12_end
        if_name_downcase_contains_updateproject_16  >> rail.Label(
            'No') >> if_name_downcase_contains_puttask_18
        if_name_downcase_contains_puttask_18 >>  rail.Label(
            'Yes') >> accumulate_list_items_19 >> foreach_dir_2_12_end
        if_name_downcase_contains_puttask_18 >>  rail.Label(
            'No') >> if_name_downcase_contains_updatetask_20
        if_name_downcase_contains_updatetask_20 >>  rail.Label(
            'Yes') >> accumulate_list_items_21 >> foreach_dir_2_12_end
        if_name_downcase_contains_updatetask_20 >>  rail.Label(
            'No') >> if_name_downcase_not_contains_putproject_22
        if_name_downcase_not_contains_putproject_22 >> rail.Label(
            'Yes') >> accumulate_list_items_23 >> foreach_dir_2_12_end
        if_name_downcase_not_contains_putproject_22 >> rail.Label(
            'No') >> foreach_dir_2_12_end
        foreach_dir_2_12 >> foreach_dir_2_12_end >> if_accumulate_list_items_23_list_items_greater_than_0_24
        if_accumulate_list_items_23_list_items_greater_than_0_24 >>  rail.Label(
            'Yes') >> log_invalid_filenames_25 >> send_mail_26 >> foreach_accumulate_list_items_23_27
        foreach_accumulate_list_items_23_27 >> rename_29 >> foreach_accumulate_list_items_23_27_end
        foreach_accumulate_list_items_23_27 >> foreach_accumulate_list_items_23_27_end >> trigger_dag_run_live_audaxgroup_process_put_projects_file33 \
        >> wait_for_completion_trigger_dag_run_live_audaxgroup_process_put_projects_file33 >> trigger_dag_run_live_audaxgroup_process_update_projects_file37 \
        >> wait_for_completion_trigger_dag_run_live_audaxgroup_process_update_projects_file37 >> trigger_dag_run_live_audaxgroup_process_tasks_file41 \
        >> wait_for_completion_trigger_dag_run_live_audaxgroup_process_tasks_file41 >> trigger_dag_run_live_audaxgroup_process_tasks_file45 \
            >> wait_for_completion_trigger_dag_run_live_audaxgroup_process_tasks_file45 >> log_jobhistory_47 \
        >> finish >> log_to_sumo


        if_accumulate_list_items_23_list_items_greater_than_0_24>>  rail.Label(
            'No') >> trigger_dag_run_live_audaxgroup_process_put_projects_file33

        log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
