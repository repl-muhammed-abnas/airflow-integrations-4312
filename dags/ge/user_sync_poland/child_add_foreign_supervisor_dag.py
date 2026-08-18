from datetime import timedelta
from airflow.models import Variable
from ge.user_sync_poland.utils import custom_methods
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_foreign_supervisor_dag_id,
        description=f'GE POLAND User Import Add Foreign Supervisor Child',
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
            no_task='search_users_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_3',
            end_task='add_foreign_super_logs_28',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_users_3 = rail.RepliconServicePageOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled',
                    'urn:replicon:user-list-column:employee-type'
                ],
                "sort": [],
                "filterExpression": {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisorloginname'],
                        }
                    }
                }
            },
            page_handler=custom_methods.page_handler,
            all_result_data_handler=lambda response, dag_run: custom_methods.compose_user_details(
                response, dag_run.conf['supervisorloginname'])
        )

        if_log_getsupervisor_uri_4_blank_5 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_blank_5',
            test=lambda: not (rail.result('search_users_3')),
            yes_task="log_supervisor_details_6_8",
            no_task="finish",
        )

        log_supervisor_details_6_8 = rail.PythonOperator(
            task_id='log_supervisor_details_6_8',
            python_callable=custom_methods.get_supervisor_details
        )

        ge_poland_master_mapper_search_entries_foreign_supervisors_9 = rail.PythonOperator(
            task_id='ge_poland_master_mapper_search_entries_foreign_supervisors_9',
            python_callable=lambda:  list(filter(
                lambda x: x["legal_entity"] == "Foreign Supervisors", config.POLAND_MASTER_MAPPER))
        )

        log_get_all_values_to_create_foreign_supervisors_10_20 = rail.PythonOperator(
            task_id='log_get_all_values_to_create_foreign_supervisors_10_20',
            python_callable=lambda dag_run: custom_methods.get_all_values_to_create_foreign_supervisors(rail.result(
                'ge_poland_master_mapper_search_entries_foreign_supervisors_9'), dag_run)
        )

        create_foreign_supervisor_21 = rail.RepliconServiceOperator(
            task_id='create_foreign_supervisor_21',
            endpoint="/services/importservice1.svc/PutUser3",
            data=custom_methods.create_foreign_supervisor_conf
        )

        remove_timeoff_assignments_22 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_22',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_foreign_supervisor_21').uri }}",
                "timeOffTypeUris": []
            }
        )

        remove_start_date_23 = rail.RepliconServiceOperator(
            task_id='remove_start_date_23',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ result('create_foreign_supervisor_21').uri }}",
                "dateRange": null
            }
        )

        put_product_assignments_for_user_24 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_24',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_foreign_supervisor_21')['uri'],
                "productUris": rail.result('log_get_all_values_to_create_foreign_supervisors_10_20')['required_licences']
            }
        )

        update_language_25 = rail.RepliconServiceOperator(
            task_id='update_language_25',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data={
                "userUri": "{{ result('create_foreign_supervisor_21').uri }}",
                "languageUri": "{{ result('log_get_all_values_to_create_foreign_supervisors_10_20').language }}"
            }
        )

        update_holiday_calendar_for_user_26 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_26',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_foreign_supervisor_21').uri }}",
                "holidayCalendarUri": null
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('No') >> search_users_3
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish

        search_users_3 >> if_log_getsupervisor_uri_4_blank_5

        if_log_getsupervisor_uri_4_blank_5 >> rail.Label('No') >> finish
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label('Yes') >> log_supervisor_details_6_8 >>\
            ge_poland_master_mapper_search_entries_foreign_supervisors_9 >> log_get_all_values_to_create_foreign_supervisors_10_20 >>\
            create_foreign_supervisor_21 >> remove_timeoff_assignments_22 >> remove_start_date_23 >> put_product_assignments_for_user_24 >>\
            update_language_25 >> update_holiday_calendar_for_user_26 >> finish

        return dag


rail.for_each_instance(create_dag)
