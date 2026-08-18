from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_activity_assignment_dag_id,
        description=f'Assured Partners User Import Activity Assignment Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_activities_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_activities_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_activities_3 = rail.RepliconServiceOperator(
            task_id='get_all_activities_3',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
        )

        get_activity_assignments_for_user_4 = rail.RepliconServiceOperator(
            task_id='get_activity_assignments_for_user_4',
            endpoint="/services/ActivityService1.svc/GetActivityAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        declare_list_8 = rail.SetVariableOperator(
            task_id='declare_list_8',
            append=False,
            name='response_from_dag',
            value=None
        )

        if_request_activity_present_10 = rail.IfOperator(
            task_id='if_request_activity_present_10',
            test='''{{ dag_run.conf.activity | is_truthy }}''',
            yes_task="log_newactivitiestoassign_15",
            no_task="if_request_activity_blank_19",
        )

        def create_activity_list_uris(dag_run):
            activities = dag_run.conf['activity'].split("|")
            replicon_activities = rail.result('get_all_activities_3')
            if len(activities) > 0:
                activity_uri_list = []
                for item in activities:
                    uri = rail.find_first_by_attr_and_get_attr(
                        replicon_activities, 'displayText', item, 'uri')
                    if uri:
                        activity_uri_list.append(uri)
                return activity_uri_list
            return null

        log_newactivitiestoassign_15 = rail.PythonOperator(
            task_id='log_newactivitiestoassign_15',
            python_callable=create_activity_list_uris
        )

        if_log_newactivitiestoassign_15_present_16 = rail.IfOperator(
            task_id='if_log_newactivitiestoassign_15_present_16',
            test='''{{ result('log_newactivitiestoassign_15') | is_truthy }}''',
            yes_task="put_activity_assignments_for_user_17",
            no_task="if_request_activity_blank_19",
        )

        put_activity_assignments_for_user_17 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_17',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "activityUris": rail.result('log_newactivitiestoassign_15')
            }
        )

        message_to_response_18 = rail.SetVariableOperator(
            task_id='message_to_response_18',
            name="{{result('declare_list_8').name }}",
            value="Activities updated"
        )

        if_request_activity_blank_19 = rail.IfOperator(
            task_id='if_request_activity_blank_19',
            test='''{{ dag_run.conf.activity | is_falsy  and result('get_activity_assignments_for_user_4')| is_truthy }}''',
            yes_task="put_activity_assignments_for_user_removeactivitiesassigned_20",
            no_task="catch_and_log_error",
        )

        put_activity_assignments_for_user_removeactivitiesassigned_20 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_removeactivitiesassigned_20',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "activityUris": []
            }
        )

        message_to_response_21 = rail.SetVariableOperator(
            task_id='message_to_response_21',
            name="{{result('declare_list_8').name }}",
            append=False,
            value="Activities removed"
        )

        catch_and_log_error = rail.SetVariableOperator(
            task_id='catch_and_log_error',
            name="{{result('declare_list_8').name }}",
            trigger_rule="one_failed",
            value="Error while assigning activities: {{get_error_message()}}"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule="all_done",
            python_callable=lambda: rail.get_dag_run_var('response_from_dag')
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_all_activities_3
        get_all_activities_3 >> get_activity_assignments_for_user_4 >> declare_list_8 >> if_request_activity_present_10

        if_request_activity_present_10 >> rail.Label(
            'No') >> if_request_activity_blank_19
        if_request_activity_present_10 >> rail.Label(
            'Yes') >> log_newactivitiestoassign_15 >> if_log_newactivitiestoassign_15_present_16

        if_log_newactivitiestoassign_15_present_16 >> rail.Label(
            'Yes') >> put_activity_assignments_for_user_17 >> message_to_response_18 >> if_request_activity_blank_19
        if_log_newactivitiestoassign_15_present_16 >> rail.Label(
            'No') >> if_request_activity_blank_19

        if_request_activity_blank_19 >> rail.Label(
            'Yes') >> put_activity_assignments_for_user_removeactivitiesassigned_20 >> message_to_response_21 >> catch_and_log_error
        if_request_activity_blank_19 >> rail.Label('No') >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
