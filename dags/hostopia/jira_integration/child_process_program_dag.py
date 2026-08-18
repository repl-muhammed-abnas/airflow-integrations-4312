from datetime import timedelta
import rail
from hostopia.jira_integration.utils import request_payload
from hostopia.jira_integration.utils import response_filter
from airflow.models import Variable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"hostopia_jira_import_child_process_program_{config.instance}",
        description=f"hostopia jira import child program {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='board_check'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='board_check',
            end_task='finish',
        )

        board_check = rail.SimpleHttpOperator(
            task_id='board_check',
            method='GET',
            endpoint='/rest/agile/1.0/board?projectKeyOrId={{ dag_run.conf.projectkey }}',
            http_conn_id='hostopia_jira_connection',
            response_filter=lambda response: response.json()['values']
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("board_check") | is_truthy }}',
            yes_task='project_lead_from_jira',
            no_task='finish'
        )

        project_lead_from_jira = rail.SimpleHttpOperator(
            task_id='project_lead_from_jira',
            method='GET',
            endpoint='rest/api/2/project/{{ dag_run.conf.projectkey }}',
            http_conn_id='hostopia_jira_connection',
            response_filter=lambda response: response.json()['lead']
        )

        search_user_in_replicon = rail.RepliconServiceOperator(
            task_id='search_user_in_replicon',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: request_payload.get_user_data_payload(dag_run,
                                                                       rail.result("project_lead_from_jira")['accountId']),
            response_filter=response_filter.get_users_data
        )

        get_all_programs_from_replicon = rail.RepliconServiceOperator(
            task_id='get_all_programs_from_replicon',
            endpoint='/services/ProgramService1.svc/GetAllPrograms',
            response_filter=response_filter.filter_programs_data
        )

        has_any_data_for_programs = rail.IfOperator(
            task_id='has_any_data_for_programs',
            test='{{ result("get_all_programs_from_replicon") | is_truthy }}',
            yes_task='program_manager_in_replicon',
            no_task='create_program_in_replicon',
        )

        program_manager_in_replicon = rail.RepliconServiceOperator(
            task_id='program_manager_in_replicon',
            endpoint='/services/ProgramService1.svc/GetProgramDetails',
            data={
                "programUri": '{{ result("get_all_programs_from_replicon")[0]["uri"] }}'
            },
        )

        compare_program_manager = rail.IfOperator(
            task_id='compare_program_manager',
            test=lambda: rail.result("project_lead_from_jira")['displayName'] == rail.result(
                "program_manager_in_replicon")['programManager'],
            yes_task='finish',
            no_task='is_user_available_in_replicon'
        )

        is_user_available_in_replicon = rail.IfOperator(
            task_id='is_user_available_in_replicon',
            test='{{ result("search_user_in_replicon") | is_truthy }}',
            yes_task='update_program_manager',
            no_task='finish'
        )

        update_program_manager = rail.RepliconServiceOperator(
            task_id='update_program_manager',
            endpoint='/services/ProgramService1.svc/UpdateProgramManager',
            data={
                "programUri": '{{ result("get_all_programs_from_replicon")[0]["uri"] }}',
                "programManagerUri": '{{ result("search_user_in_replicon")[0].uri }}'
            }
        )

        create_program_in_replicon = rail.RepliconServiceOperator(
            task_id='create_program_in_replicon',
            endpoint='/services/ProgramService1.svc/PutProgram',
            data=request_payload.get_data_for_program
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'Programname': '{{ dag_run.conf.programname }}',
                'Status': 'Processed',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> board_check

        board_check >> has_data >> rail.Label("Yes") >> project_lead_from_jira >> \
            search_user_in_replicon >> get_all_programs_from_replicon >> has_any_data_for_programs

        has_any_data_for_programs >> rail.Label(
            "Yes") >> program_manager_in_replicon >> compare_program_manager

        has_data >> rail.Label(
            "No") >> finish

        has_any_data_for_programs >> rail.Label(
            "No") >> create_program_in_replicon >> finish

        compare_program_manager >> rail.Label(
            "Yes") >> finish

        compare_program_manager >> rail.Label(
            "No") >> is_user_available_in_replicon

        is_user_available_in_replicon >> rail.Label(
            "Yes") >> update_program_manager >> finish

        is_user_available_in_replicon >> rail.Label(
            "No") >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
