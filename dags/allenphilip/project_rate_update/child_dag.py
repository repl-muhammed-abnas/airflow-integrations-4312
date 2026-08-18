from datetime import timedelta
import rail
from allenphilip.project_rate_update.utils import python_callable, response_filter
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'allenphilip_project_rate_update_child_{config.instance}',
        description=f'Allenphilip__project_rate_update_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='update_billing_rate'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='update_billing_rate',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        update_billing_rate = rail.RepliconServiceOperator(
            task_id='update_billing_rate',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['projecturi'],
                "billingRateUri": "urn:replicon:user-specific-billing-rate",
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        create_userdata_list_from_csv = rail.CreateCollectionOperator(
            task_id='create_userdata_list_from_csv',
            source="{{ dag_run.conf.userdata | to_json}}",
            name="userdata",
            columns={
                'loginname': 'loginname',
                'username': 'username',
                'defaultbillingrate': 'defaultbillingrate',
                'useruri': 'useruri'
            }
        )

        parse_userdata_csv = rail.LoadCSVFileOperator(
            task_id='parse_userdata_csv',
            headers=["loginname", "username", "defaultbillingrate", "useruri"],
            delimiter=',',
            document="{{ dag_run.conf.userdata }}",
        )

        get_project_team_members = rail.RepliconServiceOperator(
            task_id='get_project_team_members',
            endpoint="/services/ProjectService1.svc/BulkGetAllProjectTeamMembers2",
            data=lambda dag_run: {
                "projectUris": [dag_run.conf['projecturi']]
            },
            data_handler=response_filter.get_project_teammembers_data
        )

        def get_useruri():
            records = rail.result('get_project_team_members')
            if records and records[0]['useruri']:
                return True
            return False

        if_first_useruri_present = rail.IfOperator(
            task_id='if_first_useruri_present',
            test=get_useruri,
            yes_task="create_team_members_list",
            no_task="process_usedata",
        )

        create_team_members_list = rail.CreateCollectionOperator(
            task_id='create_team_members_list',
            source=lambda: rail.result(
                'get_project_team_members'),
            name="teammembers",
        )

        query_list_supplied_users = rail.QueryCollectionOperator(
            task_id='query_list_supplied_users',
            query="""SELECT * FROM  userdata WHERE  userdata.loginname IN (SELECT  teammembers.loginname FROM  teammembers)""",
        )

        if_create_team_members_list_greater_than = rail.IfOperator(
            task_id='if_create_team_members_list_greater_than',
            test="{{result('create_team_members_list')| length > 0}}",
            yes_task="get_all_user_specific_billing_rates",
            no_task="query_list_usersnotassignedto_project",
        )

        get_all_user_specific_billing_rates = rail.RepliconServiceOperator(
            task_id='get_all_user_specific_billing_rates',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/GetAllUserSpecificBillingRates",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['projecturi'],
                "asOfDate": {
                    "year": python_callable.get_date()['year'],
                    "month": python_callable.get_date()['month'],
                    "day": python_callable.get_date()['day']
                }
            },
            data_handler=response_filter.get_billing_rates
        )

        process_supplied_users = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supplied_users',
            retries=0,
            items="{{result('query_list_supplied_users')}}",
            trigger_dag_id=f'allenphilip_project_rate_update_supplied_users_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "suppliedusers": item,
                "user_uri": rail.result('get_all_user_specific_billing_rates'),
                "project_uri": dag_run.conf['projecturi'],
                "project_name": dag_run.conf['projectname'],
                "log_table": dag_run.conf['lookup_table'],
                "user_rate": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_user_specific_billing_rates'), 'useruri', item['useruri'], 'amount', ''),
                "currency_symbol": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_user_specific_billing_rates'), 'useruri', item['useruri'], 'currencysymbol', ''),
                "job_id": dag_run.conf['jobid'],
                "user_lookuptable": dag_run.conf['user_logtable']


            }
        )

        wait_for_process_supplied_users = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_supplied_users',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_supplied_users") }}'
        )

        query_list_usersnotassignedto_project = rail.QueryCollectionOperator(
            task_id='query_list_usersnotassignedto_project',
            query="""SELECT * FROM  userdata WHERE  userdata.useruri NOT IN (SELECT  teammembers.useruri FROM  teammembers)""",
        )

        process_unassigned_users_to_project = rail.TriggerDagRunForEachItemOperator(
            task_id='process_unassigned_users_to_project',
            retries=0,
            items="{{result('query_list_usersnotassignedto_project')}}",
            trigger_dag_id=f'allenphilip_project_rate_update_unassigned_users_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "unassigned_users": item,
                "project_uri": dag_run.conf['projecturi'],
                "project_name": dag_run.conf['projectname'],
                "job_id": dag_run.conf['jobid'],
                "log_table": dag_run.conf['lookup_table'],
                "user_lookuptable": dag_run.conf['user_logtable']
            }
        )

        wait_for_process_unassigned_users_to_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_unassigned_users_to_project',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_unassigned_users_to_project") }}'
        )

        process_usedata = rail.TriggerDagRunForEachItemOperator(
            task_id='process_usedata',
            retries=0,
            items=lambda dag_run: dag_run.conf['userdata'],
            trigger_dag_id=f'allenphilip_project_rate_update_userdata_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "userdata_items": item,
                "project_uri": dag_run.conf['projecturi'],
                "project_name": dag_run.conf['projectname'],
                "job_id": dag_run.conf['jobid'],
                "log_table": dag_run.conf['lookup_table'],
                "user_lookuptable": dag_run.conf['user_logtable']
            }
        )

        wait_for_process_usedata = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_usedata',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_usedata") }}'
        )

        get_success_logs = rail.FilterLogEntriesOperator(
            task_id='get_success_logs',
            log="{{dag_run.conf.user_logtable}}",
            severity='Success'
        )

        if_userdata_present = rail.IfOperator(
            task_id='if_userdata_present',
            test="{{result('get_success_logs','length') > 0}}",
            yes_task='log_resourcestobeadded',
            no_task='log_to_sumo'
        )

        log_resourcestobeadded = rail.PythonOperator(
            task_id='log_resourcestobeadded',
            python_callable=python_callable.get_useruri
        )

        log_loginname = rail.PythonOperator(
            task_id='log_loginname',
            python_callable=python_callable.get_loginname
        )

        get_all_tasks_for_project = rail.RepliconServiceOperator(
            task_id='get_all_tasks_for_project',
            endpoint="/services/TaskService1.svc/GetDescendantTaskDetails",
            data={
                "parentUri": "{{ dag_run.conf.projecturi }}",
            }

        )

        if_parenturi_is_present = rail.IfOperator(
            task_id='if_parenturi_is_present',
            test=lambda: python_callable.get_task_uri(
                    rail.result('get_all_tasks_for_project')),
            yes_task='bulk_update_resource_assignments',
            no_task='on_error'
        )

        bulk_update_resource_assignments = rail.RepliconServiceCallForEachItemOperator(
            task_id='bulk_update_resource_assignments',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            items="{{ result('get_all_tasks_for_project') | to_json}}",
            data=lambda item: {
                "taskUri": item['task']['uri'],
                "resourceUris": rail.result('log_resourcestobeadded'),
                "isAssigned": "true"
            }
        )

        add_success_entries = rail.WriteLogOperator(
            task_id='add_success_entries',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.jobid}}",
                "loginname": "{{ result('log_loginname')}}",
                "projectname": "{{ dag_run.conf.projectname }}",
                "status": "Success",
                "defaultbillingrate": "",
                "details": "Users assigned to all Open and Closed tasks of the Project",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        def get_error_message():
            error_message = rail.render_template("{{get_error_message()}}")
            if rail.get_current_context()['dag_run'].get_task_instance('update_billing_rate').current_state() == 'failed':
                result = "Error Applying User Rate to Project -" + error_message
            elif rail.get_current_context()['dag_run'].get_task_instance('get_project_team_members').current_state() == 'failed':
                result = "Error fetching Assigned Project resources from Project -" + error_message
            elif rail.get_current_context()['dag_run'].get_task_instance('get_all_user_specific_billing_rates').current_state() == 'failed':
                result = "Error fetching Assigned Project resources from Project" + error_message
            elif rail.get_current_context()['dag_run'].get_task_instance('put_project_for_team_member_billing_rates').current_state() == 'failed':
                result = "Error assigning User rate to resource -" + error_message
            elif rail.get_current_context()['dag_run'].get_task_instance('put_project_to_unassign_other_billing_rates').current_state() == 'failed':
                result = "Error assigning User rate to resource -" + error_message
            elif rail.get_current_context()['dag_run'].get_task_instance('put_project_team_member_billing_rates_allowed_for_billing_time').current_state() == 'failed':
                result = "Error assigning user to Project -" + error_message
            elif rail.get_current_context()['dag_run'].get_task_instance('put_project_to_teammembers_for_billing_rates').current_state() == 'failed':
                result = "Error assigning user to Project -" + error_message
            elif rail.get_current_context()['dag_run'].get_task_instance('get_all_tasks_for_project').current_state() == 'failed':
                result = "Error assigning Users to all Open and Closed tasks of Project -" + error_message
            elif rail.get_current_context()['dag_run'].get_task_instance('bulk_update_resource_assignments').current_state() == 'failed':
                result = "Error assigning Users to all Open and Closed tasks of Project -" + error_message
            else:
                result = None
            return result

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.lookup_table }}",
            message='{{ get_error_message() }}',
            severity="Error",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['jobid'],
                "loginname": rail.result('log_loginname'),
                "projectname": dag_run.conf['projectname'],
                "defaultbillingrate": "",
                "status": "Error",
                "childjobid": rail.render_template("{{dag_run_ecid()}}"),
                "details": get_error_message()

            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> update_billing_rate
        update_billing_rate >> create_userdata_list_from_csv
        create_userdata_list_from_csv >> parse_userdata_csv >> get_project_team_members
        get_project_team_members >> if_first_useruri_present >> rail.Label(
            'Yes') >> create_team_members_list >> query_list_supplied_users >> if_create_team_members_list_greater_than
        if_create_team_members_list_greater_than >> rail.Label(
            'Yes') >> get_all_user_specific_billing_rates >> process_supplied_users
        process_supplied_users >> wait_for_process_supplied_users
        wait_for_process_supplied_users >> query_list_usersnotassignedto_project
        if_create_team_members_list_greater_than >> rail.Label(
            'No') >> query_list_usersnotassignedto_project >> process_unassigned_users_to_project
        process_unassigned_users_to_project >> wait_for_process_unassigned_users_to_project >> get_success_logs
        if_first_useruri_present >> rail.Label(
            'No') >> process_usedata >> wait_for_process_usedata >> get_success_logs >> if_userdata_present
        if_userdata_present >> rail.Label(
            'No') >> log_to_sumo
        if_userdata_present >> rail.Label(
            'Yes') >> log_resourcestobeadded >> log_loginname >> get_all_tasks_for_project
        get_all_tasks_for_project >> if_parenturi_is_present >> rail.Label(
            'Yes') >> bulk_update_resource_assignments >> add_success_entries >> on_error >> catch_and_log_error >> log_to_sumo
        if_parenturi_is_present >> rail.Label(
            'No') >> on_error
        return dag


rail.for_each_instance(create_dag)
