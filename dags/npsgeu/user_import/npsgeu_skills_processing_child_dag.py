
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'npsgeu_user_import_skills_processing_child_{config.instance}',
        description=f'NPSGEU_Skills_processing_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
            no_task='get_all_skill_categories_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_skill_categories_4',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_skill_categories_4 = rail.RepliconServiceOperator(
            task_id='get_all_skill_categories_4',
            endpoint="/services/SkillService1.svc/GetAllSkillCategories",
        )

        get_all_skills_5 = rail.RepliconServiceOperator(
            task_id='get_all_skills_5',
            endpoint="/services/SkillService1.svc/GetAllSkills",
        )

        get_all_skill_level_details_6 = rail.RepliconServiceOperator(
            task_id='get_all_skill_level_details_6',
            endpoint="/services/SkillService1.svc/GetAllSkillLevelDetails",
        )

        create_npsgeu_skillassignment_logtable = rail.CreateLogOperator(
            task_id='create_npsgeu_skillassignment_logtable',
        )

        def check_skill_present_under_category(replicon_skill_list,skill, category):
            for item in replicon_skill_list:
                if item['category']['displayText'] == category and item['displayText'] == skill:
                    return True
            return False

        def get_skilldetails(dag_run):
            input_records = rail.load_all_records(dag_run.conf['inputdata'])
            skillcategories = [{
                'name': skill['skillcategory'] if skill['skillcategory'] else '',
                'present': 'Yes' if rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_skill_categories_4'), 'displayText', skill['skillcategory']) else 'No'
            } for skill in input_records]
            skills = [{
                'name': (skill['skills'] if skill['skillcategory'] else '') if skill else '',
                'present': 'Yes' if check_skill_present_under_category(rail.result('get_all_skills_5'),skill['skills'], skill['skillcategory']) else 'No',
                'categoryname': skill['skillcategory'] if skill['skillcategory'] else ''
            } for skill in input_records]
            filter1_skillcategories = list(
                filter(lambda x: x['name'] and x['present'] == 'No', skillcategories))
            unique_skillcategories = list(
                set(list(map(lambda item: item['name'], filter1_skillcategories))))
            final_skillcategories = list(
                map(lambda item: {"name": item}, unique_skillcategories))

            filter2_skills = list(
                filter(lambda x: x['name'] and x['present'] == 'No', skills))
            final_skills = list(map(lambda item: {
                                "name": item['name'], "catogeryname": item['categoryname']}, filter2_skills))
            unique = []
            [unique.append(x['name'])
             for x in final_skills if x['name'] not in unique]
            distinct_skills = list(map(lambda item: {"name": item}, unique))
            return {"skillcategories_output": final_skillcategories, "skills_output": final_skills, "distinct_skills": distinct_skills}

        invoke_custom_py_code_9 = rail.PythonOperator(
            task_id='invoke_custom_py_code_9',
            python_callable=get_skilldetails
        )

        create_list_10 = rail.CreateCollectionOperator(
            task_id='create_list_10',
            source="{{ dag_run.conf.inputdata }}",
            name="skilldata",
        )

        query_list_distinctusersforskillassignment_11 = rail.QueryCollectionOperator(
            task_id='query_list_distinctusersforskillassignment_11',
            query="""SELECT DISTINCT  skilldata.loginname FROM  skilldata WHERE  NULLIF(skills,'') IS NOT NULL AND  NULLIF(skillcategory,'') IS NOT NULL AND
                NULLIF(useruri,'') IS NOT NULL""",
        )

        query_list_usersforcertifacateassignment_12 = rail.QueryCollectionOperator(
            task_id='query_list_usersforcertifacateassignment_12',
            query="""SELECT * FROM  skilldata WHERE  NULLIF(certificate,'') IS NOT NULL""",
        )

        query_list_for_logging_13 = rail.QueryCollectionOperator(
            task_id='query_list_for_logging_13',
            query="""SELECT * FROM  skilldata WHERE ( NULLIF(skills,'') IS NULL AND  NULLIF(skillcategory,'') IS NULL AND
                NULLIF(certificate,'') IS NOT NULL) OR ( NULLIF(skills,'') IS NOT NULL AND  NULLIF(skillcategory,'') IS NOT NULL AND
                NULLIF(certificate,'') IS NULL) OR ( NULLIF(skills,'') IS NOT NULL AND  NULLIF(skillcategory,'') IS NOT NULL AND
                NULLIF(certificate,'') IS NOT NULL) OR ( NULLIF(skills,'') IS NULL AND  NULLIF(certificate,'') IS NULL AND NULLIF(certificate,'') IS NULL)""",
        )

        if_query_list_for_logging_13_rows_greater_than_0_14 = rail.IfOperator(
            task_id='if_query_list_for_logging_13_rows_greater_than_0_14',
            test='''{{ result('query_list_for_logging_13','length') > 0 }}''',
            yes_task="add_skipped_logs",
            no_task="if_output_skillcategories_output_greater_than_0_20",
        )

        add_skipped_logs = rail.WriteLogOperator(
            task_id='add_skipped_logs',
            log="{{result('create_npsgeu_skillassignment_logtable')}}",
            items="{{ result('query_list_for_logging_13') }}",
            message='na',
            severity='skipped',
            properties={
                "loginname": "{{item.loginname}}",
                "skills|certificates": "",
                "status": "Skipped",
                "details": "Skills|Skill Category is not present OR No Certifcates present to update",
                "parentjobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_output_skillcategories_output_greater_than_0_20 = rail.IfOperator(
            task_id='if_output_skillcategories_output_greater_than_0_20',
            test=lambda: len(rail.result('invoke_custom_py_code_9')[
                             'skillcategories_output']) > 0,
            yes_task="foreach_output_21",
            no_task="if_output_distinct_skills_greater_than_0_29",
        )

        foreach_output_21 = rail.ForEachOperator(
            task_id='foreach_output_21',
            items=lambda: rail.result('invoke_custom_py_code_9')[
                'skillcategories_output'],
            start_task='if_foreach_output_21_name_present_22',
            end_task='foreach_output_21_end'
        )

        if_foreach_output_21_name_present_22 = rail.IfOperator(
            task_id='if_foreach_output_21_name_present_22',
            test='''{{ result('foreach_output_21').name | is_truthy }}''',
            yes_task="create_new_skill_category_draft_24",
            no_task="foreach_output_21_end",
        )

        create_new_skill_category_draft_24 = rail.RepliconServiceOperator(
            task_id='create_new_skill_category_draft_24',
            endpoint="/services/SkillService1.svc/CreateNewSkillCategoryDraft",
        )

        update_skill_category_name_25 = rail.RepliconServiceOperator(
            task_id='update_skill_category_name_25',
            endpoint="/services/SkillService1.svc/UpdateSkillCategoryName",
            data={
                "skillCategoryUri": "{{ result('create_new_skill_category_draft_24') }}",
                "name": "{{ result('foreach_output_21').name }}"
            }
        )

        publish_skill_category_draft_26 = rail.RepliconServiceOperator(
            task_id='publish_skill_category_draft_26',
            endpoint="/services/SkillService1.svc/PublishSkillCategoryDraft",
            data={
                "draftUri": "{{ result('create_new_skill_category_draft_24') }}"
            }
        )

        foreach_output_21_end = rail.EmptyOperator(
            task_id='foreach_output_21_end',
        )

        if_output_distinct_skills_greater_than_0_29 = rail.IfOperator(
            task_id='if_output_distinct_skills_greater_than_0_29',
            test=lambda: bool(rail.result('invoke_custom_py_code_9')[
                              'distinct_skills']),
            yes_task="foreach_output_30",
            no_task="trigger_put_certificates_for_users_child_dag",
        )

        foreach_output_30 = rail.ForEachOperator(
            task_id='foreach_output_30',
            items=lambda: rail.result('invoke_custom_py_code_9')[
                'distinct_skills'],
            start_task='put_skill_32',
            end_task='foreach_output_30_end'
        )

        def get_category_name():
            skills_output = rail.result('invoke_custom_py_code_9')[
                'skills_output']
            skill_name = rail.result('foreach_output_30')['name']
            return rail.smartjoin_by_delim(list(map(lambda x: x['catogeryname'],list(filter(lambda item: item['name'] == skill_name,skills_output)))), " ")

        def get_payload_for_put_skill():
            categoryname = get_category_name()
            return {
                    "skill": {
                        "target": {
                            "uri": null,
                            "category": {
                                "uri": null,
                                "name": categoryname
                            },
                            "name": rail.result('foreach_output_30')['name']
                        },
                        "category": {
                            "uri": null,
                            "name": categoryname
                        },
                        "name": rail.result('foreach_output_30')['name'],
                        "description": null,
                        "enabled": "true"
                    },
                    "skillModificationOptionUri": "urn:replicon:skill-modification-option:save"
            }

        put_skill_32 = rail.RepliconServiceOperator(
            task_id='put_skill_32',
            endpoint="/services/SkillService1.svc/PutSkill",
            data=get_payload_for_put_skill
        )

        foreach_output_30_end = rail.EmptyOperator(
            task_id='foreach_output_30_end',
        )

        trigger_put_certificates_for_users_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_put_certificates_for_users_child_dag',
            items="{{result('query_list_usersforcertifacateassignment_12')}}",
            trigger_dag_id=f'npsgeu_user_import_certificates_assignment_child_{config.instance}',
            conf=lambda item: {
                'useruri': item['useruri'],
                'certificate': item['certificate'],
                'issuingorganization': item['issuingorganization'],
                'issuedate': item['issuedate'],
                'expirydate': item['expirydate'],
                'loginname': item['loginname'],
                'skillslogtable': rail.result('create_npsgeu_skillassignment_logtable'),
                'parentjobid': rail.render_template("{{dag_run.conf.callerjobid}}"),
                'childjobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_put_certificates_for_users_child_dag = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_put_certificates_for_users_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('trigger_put_certificates_for_users_child_dag')}}"
        )

        trigger_skill_assignment_for_users_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_skill_assignment_for_users_child_dag',
            items="{{result('query_list_distinctusersforskillassignment_11')}}",
            trigger_dag_id=f'npsgeu_user_import_skills_assignment_child_{config.instance}',
            conf=lambda item: {
                'all_skills': rail.result('get_all_skills_5'),
                'loginname': item['loginname'],
                'skill_level_details': rail.result('get_all_skill_level_details_6'),
                'skillslogtable': rail.result('create_npsgeu_skillassignment_logtable'),
                'parentjobid': rail.render_template("{{dag_run.conf.callerjobid}}"),
                'childjobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_skill_assignment_for_users_child_dag = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_skill_assignment_for_users_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('trigger_skill_assignment_for_users_child_dag')}}"
        )

        npsgeu_skillassignment_logs_search_entries_53 = rail.FilterLogEntriesOperator(
            task_id='npsgeu_skillassignment_logs_search_entries_53',
            log="{{result('create_npsgeu_skillassignment_logtable')}}",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}"
            }
        )

        if_npsgeu_skillassignment_logs_search_entries_53_entries_greater_than_0_54 = rail.IfOperator(
            task_id='if_npsgeu_skillassignment_logs_search_entries_53_entries_greater_than_0_54',
            test='''{{ result('npsgeu_skillassignment_logs_search_entries_53','length') > 0 }}''',
            yes_task="create_csv_lines_56",
            no_task="log_to_sumo",
        )
        create_csv_lines_56 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_56',
            source="{{ result('npsgeu_skillassignment_logs_search_entries_53') }}",
            header=['loginname',
                    'skill|certificates',
                    'status',
                    'details',
                    'jobid'],
            row=lambda item: [
                item['properties']['loginname'],
                item['properties']['skills|certificates'],
                item['properties']['status'],
                item['properties']['details'],
                item['properties']['parentjobid'] +
                " | " + item['properties']['childjobid']
            ],
        )

        upload_58 = rail.SFTPUploadFileOperator(
            task_id='upload_58',
            content='''{{ result('create_csv_lines_56') }}''',
            remote_filepath=config.log_filepath +
            'log_{{dag_run.conf.filename | file_name}}_{{dag_run.conf.callerjobid}}.csv',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_all_skill_categories_4 >> get_all_skills_5 >> get_all_skill_level_details_6
        get_all_skill_level_details_6 >> create_npsgeu_skillassignment_logtable >> invoke_custom_py_code_9 >> create_list_10
        create_list_10 >> query_list_distinctusersforskillassignment_11 >> query_list_usersforcertifacateassignment_12 >> query_list_for_logging_13
        query_list_for_logging_13 >> if_query_list_for_logging_13_rows_greater_than_0_14
        if_query_list_for_logging_13_rows_greater_than_0_14 >> rail.Label(
            'Yes') >> add_skipped_logs >> if_output_skillcategories_output_greater_than_0_20
        if_query_list_for_logging_13_rows_greater_than_0_14 >> rail.Label(
            'No') >> if_output_skillcategories_output_greater_than_0_20
        if_output_skillcategories_output_greater_than_0_20 >> rail.Label(
            'Yes') >> foreach_output_21 >> if_foreach_output_21_name_present_22
        if_foreach_output_21_name_present_22 >> rail.Label(
            'Yes') >> create_new_skill_category_draft_24 >> update_skill_category_name_25
        update_skill_category_name_25 >> publish_skill_category_draft_26 >> foreach_output_21_end
        if_foreach_output_21_name_present_22 >> rail.Label(
            'No') >> foreach_output_21_end
        foreach_output_21 >> foreach_output_21_end >> if_output_distinct_skills_greater_than_0_29
        if_output_skillcategories_output_greater_than_0_20 >> rail.Label(
            'No') >> if_output_distinct_skills_greater_than_0_29
        if_output_distinct_skills_greater_than_0_29 >> rail.Label(
            'Yes') >> foreach_output_30 >> put_skill_32 >> foreach_output_30_end
        foreach_output_30 >> foreach_output_30_end >> trigger_put_certificates_for_users_child_dag
        if_output_distinct_skills_greater_than_0_29 >> rail.Label(
            'No') >> trigger_put_certificates_for_users_child_dag
        trigger_put_certificates_for_users_child_dag >> wait_for_put_certificates_for_users_child_dag
        wait_for_put_certificates_for_users_child_dag >> trigger_skill_assignment_for_users_child_dag
        trigger_skill_assignment_for_users_child_dag >> wait_for_skill_assignment_for_users_child_dag
        wait_for_skill_assignment_for_users_child_dag >> npsgeu_skillassignment_logs_search_entries_53
        npsgeu_skillassignment_logs_search_entries_53 >> if_npsgeu_skillassignment_logs_search_entries_53_entries_greater_than_0_54
        if_npsgeu_skillassignment_logs_search_entries_53_entries_greater_than_0_54 >> rail.Label(
            'Yes') >> create_csv_lines_56 >> upload_58 >> log_to_sumo
        if_npsgeu_skillassignment_logs_search_entries_53_entries_greater_than_0_54 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
