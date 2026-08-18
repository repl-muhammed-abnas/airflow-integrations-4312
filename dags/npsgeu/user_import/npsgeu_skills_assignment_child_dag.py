
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'npsgeu_user_import_skills_assignment_child_{config.instance}',
        description=f'NPSGEU_Skills_assignment_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_skills,
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
            no_task='query_list_45'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_list_45',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_list_45 = rail.QueryCollectionOperator(
            task_id='query_list_45',
            query="""SELECT * FROM skilldata WHERE skilldata.loginname='{{ dag_run.conf.loginname }}' AND
                ( NULLIF(skills,'') IS NOT NULL AND  NULLIF(skillcategory,'') IS NOT NULL )""",
        )

        def get_skills_belonging_to_category(category, all_skills):
            return list(filter(lambda x: x['category']['displayText'] == category, all_skills))

        def get_addskillassignment(dag_run):
            skillssource = rail.load_all_records(rail.result('query_list_45'))
            return [{
                'name': skill['skills'],
                'skilluri': rail.find_first_by_attr_and_get_attr(
                    get_skills_belonging_to_category(skill['skillcategory'], dag_run.conf['all_skills']),'displayText', skill['skills'], 'uri', ''),
                'categoryname': skill['skillcategory'],
                'skillleveluri': rail.find_first_by_attr_and_get_attr(dag_run.conf['skill_level_details'], 'name', skill['skilllevel'], 'uri', ''),
                'useruri': skill['useruri']
            } for skill in skillssource]

        invoke_custom_ruby_code_46 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_46',
            python_callable=get_addskillassignment
        )

        adhoc_http_action_48 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_48',
            endpoint="/services/SkillService1.svc/BulkAssignSkillToUser",
            data=lambda: {
                "userUri": rail.result('invoke_custom_ruby_code_46')[0]['useruri'],
                "assignSkillParameters": [{
                    "skillUri": skill['skilluri'],
                    "skillLevelUri": skill['skillleveluri']
                } for skill in rail.result('invoke_custom_ruby_code_46')]
            }
        )

        npsgeu_skillassignment_logs_add_entry_49 = rail.WriteLogOperator(
            task_id='npsgeu_skillassignment_logs_add_entry_49',
            log="{{ dag_run.conf.skillslogtable }}",
            message="na",
            severity="success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "skills|certificates": rail.smartjoin_by_delim([skill['name'] for skill in rail.result('invoke_custom_ruby_code_46')], "|"),
                "status": "success",
                "details": "Skills updated successfully",
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": dag_run.conf['childjobid']
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.skillslogtable }}",
            message="na",
            trigger_rule='one_failed',
            severity="error",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "skills|certificates": rail.smartjoin_by_delim([skill['name'] for skill in rail.result('invoke_custom_ruby_code_46')], "|"),
                "status": "error",
                "details": rail.render_template("{{get_error_message()}}"),
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": dag_run.conf['childjobid']
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> query_list_45 >> invoke_custom_ruby_code_46 >> adhoc_http_action_48
        adhoc_http_action_48 >> npsgeu_skillassignment_logs_add_entry_49 >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
