from datetime import datetime, timedelta
import rail
from airflow.models import Variable
from pwcglobal.absense_data_pre_population_v1.utils import request_payload
from pwcglobal.absense_data_pre_population_v1.utils import response_filter
from pwcglobal.absense_data_pre_population_v1.utils import python_callable_method
from pwcglobal.absense_data_pre_population_v1.task.hours_quantity_check import get_hours_quantity_check
from pwcglobal.absense_data_pre_population_v1.task.build_oef_values import get_build_oef_values
from pwcglobal.absense_data_pre_population_v1.task.put_timeentry import get_put_time_entry
from pwcglobal.absense_data_pre_population_v1.task.update_or_delete_timeentry import get_update_or_delete_time_entry

null = None


def create_child_pre_population_project_dag(config):
    # pylint: disable=too-many-statements

    add_dags = []

    for idx in range(0, config.TIME_ENTRY_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f'pwc_timesheetprepopulation_child_{config.instance}{get_postfix}_v1',
            description='PwC_Time Prepopulatation_child_v2.0',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.child_dag_max_active_runs,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='create_child_log'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                start_task='create_child_log',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                end_task='catch_and_log_errors',
            )

            create_child_log = rail.CreateLogOperator(
                task_id="create_child_log"
            )

            is_userloginname_present = rail.IfOperator(
                task_id='is_userloginname_present',
                test='{{ dag_run.conf.userloginname | is_truthy }}',
                yes_task='search_users',
                no_task='is_transactiondate_present',
            )

            search_users = rail.RepliconServiceOperator(
                task_id='search_users',
                endpoint='/services/UserListService1.svc/GetData',
                data=request_payload.get_search_user_param,
                response_filter=response_filter.get_user_details
            )

            is_useruri_present = rail.IfOperator(
                task_id='is_useruri_present',
                test=lambda: bool(rail.result('search_users') and
                                rail.result('search_users')[0]['useruri']),
                yes_task='get_timesheet_info',
                no_task='log_user_uri_not_present',
            )

            log_user_uri_not_present = rail.WriteLogOperator(
                task_id='log_user_uri_not_present',
                log='{{ result("create_child_log") }}',
                message='User with ID: {{ dag_run.conf.userloginname }} is not available in Replicon',
                severity='Exception',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Pre-check', status='Exception')
            )

            get_timesheet_info = rail.RepliconServiceOperator(
                task_id='get_timesheet_info',
                endpoint='/services/TimesheetListService1.svc/GetData',
                data=request_payload.get_timesheet_info_payload,
                response_filter=response_filter.get_timesheet_approval_status
            )

            is_timesheet_status_open = rail.IfOperator(
                task_id='is_timesheet_status_open',
                test=lambda: bool(rail.result('get_timesheet_info') and rail.result('get_timesheet_info')[0]['approval_status']
                                and (rail.result('get_timesheet_info')[0]['approval_uri'] == 'urn:replicon:approval-status:open' 
                                     or rail.result('get_timesheet_info')[0]['timesheet_status_2_uri'] == "urn:replicon:timesheet-status-2:submission-failed")),
                yes_task='is_transactiondate_present',
                no_task='log_timesheet_status_not_open'
            )

            log_timesheet_status_not_open = rail.WriteLogOperator(
                task_id='log_timesheet_status_not_open',
                log='{{ result("create_child_log") }}',
                message='Time not populated as timesheet is not in open or submission failed status',
                severity='Exception',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Pre-check', status='Exception')
            )

            is_transactiondate_present = rail.IfOperator(
                task_id='is_transactiondate_present',
                test='{{ dag_run.conf.TransactionDate | is_truthy }}',
                yes_task='search_project_with_code',
                no_task='log_transactiondate_not_present',
            )

            search_project_with_code = rail.RepliconServiceOperator(
                task_id='search_project_with_code',
                endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
                data={
                    'projects': [
                        {
                            'uri': null,
                            'name': null,
                            'code': '{{ dag_run.conf.ChargeCode.ChargeCode }}',
                            'parameterCorrelationId': null
                        }
                    ]
                },
                response_filter=response_filter.get_projectdetails
            )

            is_project_uri_present = rail.IfOperator(
                task_id='is_project_uri_present',
                test=lambda: bool(rail.result('search_project_with_code') and
                                rail.result('search_project_with_code')[0]['projecturi']),
                yes_task='is_project_status_open',
                no_task='log_project_id_not_present',
            )

            log_project_id_not_present = rail.WriteLogOperator(
                task_id='log_project_id_not_present',
                log='{{ result("create_child_log") }}',
                message='Project {{ dag_run.conf.ChargeCode.ChargeCode }} is not found in Replicon',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Pre-check', status='Exception')
            )

            is_project_status_open = rail.IfOperator(
                task_id='is_project_status_open',
                test='{{ result("search_project_with_code") | filter_by_attr("projectstatus", "does-not-equal", "Completed") | length > 0 }}',
                yes_task='get_all_project_tasks',
                no_task='log_project_status_is_closed',
            )

            log_project_status_is_closed = rail.WriteLogOperator(
                task_id='log_project_status_is_closed',
                log='{{ result("create_child_log") }}',
                message='Project {{ dag_run.conf.ChargeCode.ChargeCode }} is closed for time entry',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Pre-check', status='Exception')
            )

            get_all_project_tasks = rail.RepliconServiceOperator(
                task_id='get_all_project_tasks',
                endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
                data={
                    "parentUri": '{{ result("search_project_with_code")[0]["projecturi"] }}'
                },
                response_filter=response_filter.get_filtered_tasks
            )

            get_task_uri = rail.PythonOperator(
                task_id='get_task_uri',
                python_callable=python_callable_method.get_task_uri
            )

            is_user_id_present = rail.IfOperator(
                task_id='is_user_id_present',
                test=lambda dag_run: bool(dag_run.conf['userloginname'] and
                                        rail.result('search_users') and
                                        rail.result('search_users')[0].get('useruri')),
                yes_task='is_user_id_enabled',
                no_task='log_user_id_not_present',
            )

            is_user_id_enabled = rail.IfOperator(
                task_id='is_user_id_enabled',
                test='{{ result("search_users") | filter_by_attr("userstatus", "equals", "True") | length > 0 }}',
                yes_task='check_project_type',
                no_task='log_user_id_not_enabled',
            )

            check_project_type = rail.IfOperator(
                task_id='check_project_type',
                test=lambda: bool(rail.result('search_project_with_code') and
                                rail.result('search_project_with_code')[0]['extensionfieldvalues'] != 'Statistical' and
                                rail.result('search_project_with_code')[0]['extensionfieldvalues'] != 'Leave'),
                yes_task='get_all_project_team_members',
                no_task='update_task_project_metadata',
            )

            log_user_id_not_present = rail.WriteLogOperator(
                task_id='log_user_id_not_present',
                log='{{ result("create_child_log") }}',
                message='User with ID: {{ dag_run.conf.userloginname }} is not available in Replicon',
                severity='Exception',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Pre-check', status='Exception')
            )

            log_user_id_not_enabled = rail.WriteLogOperator(
                task_id='log_user_id_not_enabled',
                log='{{ result("create_child_log") }}',
                message='User with ID: {{ dag_run.conf.userloginname }} is not enabled in Replicon',
                severity='Exception',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Pre-check', status='Exception')
            )

            get_all_project_team_members = rail.RepliconServiceOperator(
                task_id='get_all_project_team_members',
                endpoint='/services/ProjectService1.svc/BulkGetAllProjectTeamMembers2',
                data={
                    'projectUris': [
                        '{{ result("search_project_with_code")[0]["projecturi"] }}'
                    ]
                }
            )

            add_item_to_team_members_list = rail.PythonOperator(
                task_id='add_item_to_team_members_list',
                python_callable=python_callable_method.add_item_to_team_members_list
            )

            update_task_project_metadata = rail.PythonOperator(
                task_id='update_task_project_metadata',
                python_callable=python_callable_method.update_task_projectmetadata
            )

            is_time_entry_only_allowed_against_task = rail.IfOperator(
                task_id='is_time_entry_only_allowed_against_task',
                test=lambda: bool(rail.result("update_task_project_metadata")),
                yes_task='is_time_entry_id_present',
                no_task='log_time_entry_not_added'
            )

            log_time_entry_not_added = rail.WriteLogOperator(
                task_id='log_time_entry_not_added',
                log='{{ result("create_child_log") }}',
                # pylint: disable=line-too-long
                message='Time entry {{ dag_run.conf.TimeEntryID }} not added since project doesnot have any task associated and the time entry allowed is set only against Task',
                severity='Exception',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Pre-check', status='Exception')
            )

            is_time_entry_id_present = rail.IfOperator(
                task_id='is_time_entry_id_present',
                test='{{ dag_run.conf.TimeEntryID | is_truthy }}',
                yes_task='search_time_entry_by_id',
                no_task='add_only_as_no_time_entry_id',
            )

            search_time_entry_by_id = rail.RepliconServiceOperator(
                task_id='search_time_entry_by_id',
                endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetData',
                data=request_payload.get_timeentry_id_payload,
                response_filter=response_filter.get_timeentries_list
            )

            add_only_as_no_time_entry_id = rail.EmptyOperator(
                task_id='add_only_as_no_time_entry_id',
            )

            (is_hours_quantity_zero_add_only, log_time_entry_hours_zero_add_only,
            end_of_hours_quantity_check_add_only) = get_hours_quantity_check('add_only')

            process_oef_for_timeentry_add = get_build_oef_values('add_only', config.WORKTYPE_MAPPER)

            process_put_for_timeentry_add = get_put_time_entry('add_only')

            check_time_entry_revision_group = rail.IfOperator(
                task_id='check_time_entry_revision_group',
                test=lambda: bool(rail.result('search_time_entry_by_id')),
                yes_task='time_entry_revision_group_present',
                no_task='time_entry_revision_group_not_present'
            )

            time_entry_revision_group_not_present = rail.EmptyOperator(
                task_id='time_entry_revision_group_not_present',
            )

            (is_hours_quantity_zero_new_entry, log_time_entry_hours_zero_new_entry,
            end_of_hours_quantity_check_new_entry) = get_hours_quantity_check('new_entry')

            process_oef_for_timeentry_new = get_build_oef_values('new_entry', config.WORKTYPE_MAPPER)

            process_put_for_timeentry_new = get_put_time_entry('new_entry')

            time_entry_revision_group_present = rail.IfOperator(
                task_id='time_entry_revision_group_present',
                test=lambda: len(rail.result("search_time_entry_by_id")) > 1,
                yes_task='log_multiple_time_entries',
                no_task='get_project_details'
            )

            log_multiple_time_entries = rail.WriteLogOperator(
                task_id='log_multiple_time_entries',
                log='{{ result("create_child_log") }}',
                message='Time entry {{ dag_run.conf.TimeEntryID }} not updated since multiple entries found with the same ID',
                severity='Exception',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Update', status='Exception')
            )

            get_project_details = rail.RepliconServiceOperator(
                task_id='get_project_details',
                endpoint='/services/ProjectService1.svc/GetProjectDetails',
                data={
                        "projectUri": '{{ result("search_time_entry_by_id")[0] |  attr_or_default("projecturi") }}'
                },
            )

            check_time_revision_group_in_time_entries_list = rail.IfOperator(
                task_id='check_time_revision_group_in_time_entries_list',
                test='{{ result("search_time_entry_by_id") | is_falsy }}',
                yes_task='time_entry_revision_group_not_present_existing_entry',
                no_task='check_approval_status'
            )

            time_entry_revision_group_not_present_existing_entry = rail.EmptyOperator(
                task_id='time_entry_revision_group_not_present_existing_entry',
            )

            (is_hours_quantity_zero_up_del_entry, log_time_entry_hours_zero_up_del_entry, end_of_hours_quantity_check_up_del_entry) = get_hours_quantity_check(
                'update_delete_entry')

            process_oef_for_timeentry_update_delete = get_build_oef_values(
                'update_delete_entry', config.WORKTYPE_MAPPER)

            process_put_for_timeentry_update_delete = get_put_time_entry(
                'update_delete_entry')

            check_approval_status = rail.IfOperator(
                task_id='check_approval_status',
                test='{{ result("search_time_entry_by_id") | filter_by_attr("approvalstatus", "equals", "Approved") | is_truthy }}',
                yes_task='log_time_entry_is_approved',
                no_task='is_time_entry_already_exist'
            )

            log_time_entry_is_approved = rail.WriteLogOperator(
                task_id='log_time_entry_is_approved',
                log='{{ result("create_child_log") }}',
                message='Time entry {{ dag_run.conf.TimeEntryID }}  not updated since hours is approved',
                severity='Exception',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Update', status='Exception')
            )

            is_time_entry_already_exist = rail.IfOperator(
                task_id='is_time_entry_already_exist',
                test=lambda dag_run: not bool(
                    rail.result("search_time_entry_by_id")[0]['entrydate'] != datetime.strptime(
                        dag_run.conf['TransactionDate'], '%Y%m%d').strftime('%d/%m/%Y')
                    or rail.result("get_project_details")['code'] != dag_run.conf['ChargeCode']['ChargeCode']
                    or rail.result("search_time_entry_by_id")[0]['taskname'] != dag_run.conf['ChargeCode']['WorkItem']['WorkItemType']
                    or rail.result("search_time_entry_by_id")[0]['duration'] != float(dag_run.conf['HoursQuantity'])
                    or rail.result("search_time_entry_by_id")[0]['comments'] != dag_run.conf['Comments']
                ),
                yes_task='log_time_entry_already_available',
                no_task='update_or_delete_time_entry'
            )

            log_time_entry_already_available = rail.WriteLogOperator(
                task_id='log_time_entry_already_available',
                log='{{ result("create_child_log") }}',
                message='Time entry {{ dag_run.conf.TimeEntryID }} already available, no change received',
                severity='Success',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Add', status='Skipped')
            )

            update_or_delete_time_entry = rail.EmptyOperator(
                task_id='update_or_delete_time_entry',
            )

            process_oef_for_timeentry_update_or_delete = get_build_oef_values(
                'update_or_delete_entry', config.WORKTYPE_MAPPER)

            process_update_or_delete_time_entry = get_update_or_delete_time_entry(
                'update_or_delete_entry')

            log_transactiondate_not_present = rail.WriteLogOperator(
                task_id='log_transactiondate_not_present',
                log='{{ result("create_child_log") }}',
                message='Transactiondate is not present',
                severity='Exception',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Pre-check', status='Exception')
            )

            finish = rail.EmptyOperator(
                task_id='finish'
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{ result("create_child_log") }}',
                trigger_rule='one_failed',
                # pylint: disable=line-too-long
                message='{{ get_error_message() }}',
                properties=lambda: python_callable_method.get_log_properties(
                    action='Pre-check', status='Error')
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done',
                extra_info={
                    'Timeentryid': '{{ dag_run.conf.TimeEntryID }}',
                    'User': '{{ dag_run.conf.userloginname }}',
                    'Project': '{{ dag_run.conf.ChargeCode.ChargeCode }}',
                    'Task': '{{ dag_run.conf.ChargeCode.WorkItem.WorkItemType }}',
                    'Date': '{{ dag_run.conf.TransactionDate }}',
                    'Hours': '{{ dag_run.conf.HoursQuantity }}'
                }
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> create_child_log

            create_child_log >> is_userloginname_present >> rail.Label(
                'Yes') >> search_users >> is_useruri_present
            is_userloginname_present >> rail.Label(
                'No') >> is_transactiondate_present

            is_useruri_present >> rail.Label(
                'Yes') >> get_timesheet_info >> is_timesheet_status_open
            is_useruri_present >> rail.Label(
                'No') >> log_user_uri_not_present >> finish

            is_timesheet_status_open >> rail.Label(
                'Yes') >> is_transactiondate_present
            is_timesheet_status_open >> rail.Label(
                'No') >> log_timesheet_status_not_open >> finish

            is_transactiondate_present >> rail.Label(
                'Yes') >> search_project_with_code >> is_project_uri_present
            is_transactiondate_present >> rail.Label(
                'No') >> log_transactiondate_not_present >> finish

            is_project_uri_present >> rail.Label(
                'Yes') >> is_project_status_open
            is_project_uri_present >> rail.Label(
                'No') >> log_project_id_not_present >> finish

            is_project_status_open >> rail.Label(
                'Yes') >> get_all_project_tasks >> get_task_uri >> is_user_id_present
            is_project_status_open >> rail.Label(
                'No') >> log_project_status_is_closed >> finish

            is_user_id_present >> rail.Label(
                'Yes') >> is_user_id_enabled
            is_user_id_present >> rail.Label(
                'No') >> log_user_id_not_present >> finish

            is_user_id_enabled >> rail.Label(
                'Yes') >> check_project_type
            is_user_id_enabled >> rail.Label(
                'No') >> log_user_id_not_enabled >> finish

            check_project_type >> rail.Label(
                'Yes') >> get_all_project_team_members >> add_item_to_team_members_list >> \
                update_task_project_metadata >> is_time_entry_only_allowed_against_task
            check_project_type >> rail.Label(
                'Yes') >> update_task_project_metadata >> is_time_entry_only_allowed_against_task

            is_time_entry_only_allowed_against_task >> rail.Label(
                'Yes') >> is_time_entry_id_present
            is_time_entry_only_allowed_against_task >> rail.Label(
                'No') >> log_time_entry_not_added >> finish

            is_time_entry_id_present >> rail.Label(
                'Yes') >> search_time_entry_by_id >> check_time_entry_revision_group
            is_time_entry_id_present >> rail.Label(
                'No') >> add_only_as_no_time_entry_id >> is_hours_quantity_zero_add_only
            end_of_hours_quantity_check_add_only >> process_oef_for_timeentry_add >> process_put_for_timeentry_add >> finish
            log_time_entry_hours_zero_add_only >> finish

            check_time_entry_revision_group >> rail.Label(
                'Yes') >> time_entry_revision_group_present >> get_project_details
            check_time_entry_revision_group >> rail.Label(
                'No') >> time_entry_revision_group_not_present >> is_hours_quantity_zero_new_entry
            end_of_hours_quantity_check_new_entry >> process_oef_for_timeentry_new >> process_put_for_timeentry_new >> finish
            log_time_entry_hours_zero_new_entry >> finish

            time_entry_revision_group_present >> rail.Label(
                'Yes') >> log_multiple_time_entries >> finish
            time_entry_revision_group_present >> rail.Label(
                'No') >> get_project_details >> check_time_revision_group_in_time_entries_list

            check_time_revision_group_in_time_entries_list >> rail.Label(
                'Yes') >> time_entry_revision_group_not_present_existing_entry >> is_hours_quantity_zero_up_del_entry
            end_of_hours_quantity_check_up_del_entry >> process_oef_for_timeentry_update_delete >> process_put_for_timeentry_update_delete >> finish
            check_time_revision_group_in_time_entries_list >> rail.Label(
                'No') >> check_approval_status

            log_time_entry_hours_zero_up_del_entry >> finish

            check_approval_status >> rail.Label(
                'Yes') >> log_time_entry_is_approved >> finish
            check_approval_status >> rail.Label(
                'No') >> is_time_entry_already_exist

            is_time_entry_already_exist >> rail.Label(
                'Yes') >> log_time_entry_already_available >> finish
            is_time_entry_already_exist >> rail.Label(
                'No') >> update_or_delete_time_entry >> process_oef_for_timeentry_update_or_delete >> \
                process_update_or_delete_time_entry >> finish

            finish >> catch_and_log_errors >> log_to_sumo

        add_dags.append(dag)

    return dag


rail.for_each_instance(create_child_pre_population_project_dag)
