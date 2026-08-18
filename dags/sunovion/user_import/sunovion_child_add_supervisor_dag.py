
from datetime import timedelta
from airflow.models import Variable
import rail
from sunovion.user_import.utils import request_payload
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sunovion_user_import_add_supervisor_child_{config.instance}',
        description=f'Sunovion_Child_Add Supervisor {config.instance}',
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
            no_task='search_users_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_user_uri(response, dag_run, identifier):
            matching_user = list(filter(
                lambda user: user['cells'][0]['textValue'] == dag_run.conf[identifier], response['rows']))
            return {
                'uri': matching_user[0]['cells'][0]['uri'] if matching_user else ''
            }

        search_users_3 = rail.RepliconServiceOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{dag_run.conf.loginname}}"
                        }
                    }
                }
            },
            data_handler=lambda response, dag_run: get_user_uri(
                response, dag_run, 'loginname')
        )

        if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_5 = rail.IfOperator(
            task_id='if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_5',
            test='''{{ dag_run.conf.supervisorid != dag_run.conf.loginname }}''',
            yes_task="search_users_6",
            no_task="sunovion_user_logs_file_add_entry_16",
        )

        search_users_6 = rail.RepliconServiceOperator(
            task_id='search_users_6', endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{dag_run.conf.supervisorid}}"
                        }
                    }
                }
            },
            data_handler=lambda response, dag_run: get_user_uri(
                response, dag_run, 'supervisorid')
        )

        if_log_getsupervisor_uri_7_present_8 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_7_present_8',
            test='''{{ result('search_users_6').uri | is_truthy }}''',
            yes_task="log_today_day_9",
            no_task="if_log_getsupervisor_uri_7_blank_13",
        )

        log_today_day_9 = rail.PythonOperator(
            task_id='log_today_day_9',
            python_callable= request_payload.get_todays_date
        )

        update_initial_supervisorwithtodayaseffectivedate_12 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisorwithtodayaseffectivedate_12',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('search_users_3').uri }}",
                "supervisorUri": "{{ result('search_users_6').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_today_day_9').year }}",
                        "month": "{{ result('log_today_day_9').month }}",
                        "day": "{{ result('log_today_day_9').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_log_getsupervisor_uri_7_blank_13 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_7_blank_13',
            test='''{{ result('search_users_6').uri | is_falsy }}''',
            yes_task="sunovion_user_logs_file_add_entry_14",
            no_task="catch_error",
        )

        sunovion_user_logs_file_add_entry_14 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_14',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''User "{{ dag_run.conf.username }}" is created, however supervisor is not updated as user with login name "{{ dag_run.conf.supervisorid }}" is not available in Replicon''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        sunovion_user_logs_file_add_entry_16 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_16',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''User "{{ dag_run.conf.username }}" is created, however supervisor is not updated as the "Login name" for user and supervisor same on the input file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_5
        if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_5 >> rail.Label(
            'Yes') >> search_users_6 >> if_log_getsupervisor_uri_7_present_8
        if_log_getsupervisor_uri_7_present_8 >> rail.Label(
            'Yes') >> log_today_day_9 >> update_initial_supervisorwithtodayaseffectivedate_12 >> if_log_getsupervisor_uri_7_blank_13
        if_log_getsupervisor_uri_7_present_8 >> rail.Label(
            'No') >> if_log_getsupervisor_uri_7_blank_13
        if_log_getsupervisor_uri_7_blank_13 >> rail.Label(
            'Yes') >> sunovion_user_logs_file_add_entry_14 >> catch_error
        if_log_getsupervisor_uri_7_blank_13 >> rail.Label('No') >> catch_error
        if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_5 >> rail.Label(
            'No') >> sunovion_user_logs_file_add_entry_16 >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
