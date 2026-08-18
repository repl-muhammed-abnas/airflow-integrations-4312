from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_billing_key_master.utils import request_payload
from dxctechnology.gsap_billing_key_master.utils import python_callable_method

null = None


def create_attribute_1_process_child_wbs_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsab_billing_key_process_child_wbs_{config.dag_id_postfix}',
        description=f'DXC_GSAB Billing Key Child - Process Child WBS {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_project_details_based_on_wbs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_project_details_based_on_wbs',
            end_task='dummy_batch_task_end',
        )

        get_task_details_for_project_child = rail.QueryCollectionOperator(
            task_id = "get_task_details_for_project_child",
            query= """SELECT * FROM valid_wbs WHERE wbs = :WBS""",
            query_params={
                "WBS" : "{{ dag_run.conf.parent_wbs}}"
            }
        )

        get_project_details_based_on_wbs = rail.RepliconServiceOperator(
            task_id='get_project_details_based_on_wbs',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": '{{ dag_run.conf.wbs_uri }}',
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": null}])[0]['projectDetails']
        )

        check_wbs_exists = rail.IfOperator(
            task_id='check_wbs_exists',
            test=lambda: bool(rail.result('get_project_details_based_on_wbs') and
                              rail.result(
                'get_project_details_based_on_wbs')['uri']),
            yes_task='check_wbs_is_archived',
            no_task='dummy_batch_task_end'
        )

        check_wbs_is_archived = rail.IfOperator(
            task_id='check_wbs_is_archived',
            test=lambda: rail.result('get_project_details_based_on_wbs')[
                'status']['name'] == 'Archived',
            yes_task='dummy_batch_task_end',
            no_task='get_project_date_range',
        )

        get_project_date_range = rail.PythonOperator(
            task_id='get_project_date_range',
            python_callable=lambda: python_callable_method.project_date_range(
                'get_project_details_based_on_wbs')
        )

        get_all_project_team_member_details = rail.RepliconServiceOperator(
            task_id='get_all_project_team_member_details',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails',
            data={
                'projectUri': '{{ result("get_project_details_based_on_wbs").uri }}',
                'asOfDate': null},
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data))
        )

        get_children_task_details = rail.RepliconServiceOperator(
            task_id='get_children_task_details',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ result("get_project_details_based_on_wbs").uri }}'
            }
        )

        get_tasks_from_project = rail.PythonOperator(
            task_id='get_tasks_from_project',
            python_callable=python_callable_method.retrieve_task_list,
            op_args=['get_children_task_details']
        )

        tasks_from_project_collection = rail.CreateCollectionOperator(
            task_id='tasks_from_project_collection',
            source='{{ result("get_tasks_from_project") | to_json }}',
            columns=[
                'name',
                'code',
                'enddate',
                'oef',
                'uri'],
            name='tasks_from_project_child'
        )

        dummy_batch_task_end = rail.EmptyOperator(
            task_id = "dummy_batch_task_end"
        )

        should_process_tasks = rail.IfOperator(
            task_id = "should_process_tasks",
            test= lambda: bool((rail.result(
                'get_project_details_based_on_wbs') and rail.result(
                'get_project_details_based_on_wbs')['uri']) and ( rail.result(
                'get_project_details_based_on_wbs') and not rail.result('get_project_details_based_on_wbs')[
                'status']['name'] == 'Archived')),
            yes_task= "query_task_list_child"
        )

        query_task_list_child = rail.QueryCollectionOperator(
            task_id='query_task_list_child',
            query="""SELECT * FROM tasks_from_project_child WHERE oef = 'GSAP Billing Key'"""
        )

        for_each_billing_key_start = rail.ForEachOperator(
            task_id = "for_each_billing_key_start",
            items= "{{ result('get_task_details_for_project_child') }}",
            start_task="check_if_task_present",
            end_task="for_each_billing_key_end"
        )

        check_if_task_present = rail.PythonOperator(
            task_id = "check_if_task_present",
            python_callable=  python_callable_method.is_task_already_present_child
        )

        is_task_found = rail.IfOperator(
            task_id = "is_task_found",
            test= "{{ result('check_if_task_present') | is_truthy}}",
            yes_task="update_billing_key",
            no_task= "create_billing_key"
        )

        update_billing_key = rail.RepliconServiceOperator(
            task_id='update_billing_key',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=request_payload.get_update_task_data,
        )

        create_billing_key = rail.RepliconServiceOperator(
            task_id='create_billing_key',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=request_payload.get_update_task_data,
        )

        is_update_failed = rail.IfOperator(
            task_id = 'is_update_failed',
            trigger_rule = "all_done",
            test= "{{ get_task_state('update_billing_key') | lower == 'failed' or get_task_state('create_billing_key') | lower == 'failed' }}",
            yes_task= "catch_error",
            no_task="for_each_billing_key_end"
        )

        catch_error = rail.EmptyOperator(
            task_id = "catch_error",
        )

        for_each_billing_key_end = rail.EmptyOperator(
            task_id = "for_each_billing_key_end"
        )

        has_any_failures = rail.IfOperator(
            task_id = "has_any_failures",
            test= "{{ get_task_state('catch_error') | lower == 'success' or result('catch_error', 'error') | is_truthy }}",
            yes_task= "fail_dag"
        )

        fail_dag = rail.FailOperator(
            task_id = "fail_dag",
            message= "{{ result('catch_error', 'error')}}"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> dummy_batch_task_end
        can_run_batch_task >> rail.Label("No") >> get_project_details_based_on_wbs
        get_project_details_based_on_wbs >> check_wbs_exists

        check_wbs_exists >> rail.Label(
            'No') >> dummy_batch_task_end
        check_wbs_exists >> rail.Label('Yes') >> check_wbs_is_archived

        dummy_batch_task_end >> should_process_tasks >> rail.Label("Yes") >> query_task_list_child
        check_wbs_is_archived >> rail.Label(
            'Yes') >> dummy_batch_task_end
        check_wbs_is_archived >> rail.Label(
            'No') >> get_project_date_range >> get_task_details_for_project_child >> get_all_project_team_member_details

        get_all_project_team_member_details \
            >> get_children_task_details >> get_tasks_from_project >> tasks_from_project_collection >> dummy_batch_task_end
        query_task_list_child >> for_each_billing_key_start
        for_each_billing_key_start >> for_each_billing_key_end
        for_each_billing_key_start >> check_if_task_present >> is_task_found >> rail.Label("Yes")\
            >> update_billing_key >> is_update_failed >> rail.Label("Yes") >> catch_error >> for_each_billing_key_end
        is_update_failed >> rail.Label("No") >> for_each_billing_key_end
        is_task_found >> rail.Label("No") >> create_billing_key >> is_update_failed

        for_each_billing_key_end >> has_any_failures >> rail.Label("Yes") >> fail_dag >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_child_wbs_dag)
