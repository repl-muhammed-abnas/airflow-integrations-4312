import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_user_child,
        description=f'gee_user_import_disable_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        get_user_details = rail.RepliconServiceOperator(
            task_id = "get_user_details",
            endpoint = "/services/ImportService1.svc/BulkGetUsers3",
            data =lambda dag_run: {
                "users": [
                    {
                    "uri": dag_run.conf['useruri']
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        disable_user_login = rail.RepliconServiceOperator(
            task_id='disable_user_login',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            }
        )

        is_end_date_present = rail.IfOperator(
            task_id="is_end_date_present",
            test=lambda dag_run: bool(dag_run.conf['EndDate']),
            yes_task="update_employment_daterange",
            no_task="add_to_lookup_table"
        )

        def split_enddate(dag_run):
            return {
                "year" : dag_run.conf['EndDate'].split('/')[2],
                "month" : dag_run.conf['EndDate'].split('/')[1],
                "day" : dag_run.conf['EndDate'].split('/')[0],
            }

        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                "startDate": {
                    "year": rail.result('get_user_details')[0]['userDetails']['employmentDateRange']['startDate']['year'],
                    "month": rail.result('get_user_details')[0]['userDetails']['employmentDateRange']['startDate']['month'],
                    "day": rail.result('get_user_details')[0]['userDetails']['employmentDateRange']['startDate']['day']
                },
                "endDate": split_enddate(dag_run),
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
                }
            }
        )

        add_to_lookup_table = rail.WriteLogOperator(
            task_id='add_to_lookup_table',
            log = "{{ dag_run.conf.gee_user_import_lookup_table }}",
            message="na",
            severity="Sucess",
            properties={
                "loginname": "{{ dag_run.conf.LoginName }}",
                "emplid" : "{{ dag_run.conf.EmployeeId }}",
                "action" : "Disable",
                "status": "Success",
                "details": "User profile disabled successfully",
                "jobid": "{{ dag_run.conf.calling_dag_id }}",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log = "{{ dag_run.conf.gee_user_import_lookup_table }}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "loginname": "{{ dag_run.conf.LoginName }}",
                "emplid" : "{{ dag_run.conf.EmployeeId }}",
                "action" : "Disable",
                "status": "Error",
                "details": '{{ get_error_message() }}',
                "jobid": "{{ dag_run.conf.calling_dag_id }}",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            trigger_rule='all_done'
        )

        get_user_details >> disable_user_login >> is_end_date_present >> rail.Label(
            'Yes') >> update_employment_daterange >> add_to_lookup_table >> catch_and_log_errors
        is_end_date_present >> rail.Label(
            'No') >> add_to_lookup_table >> catch_and_log_errors
        catch_and_log_errors >> log_dagrun_to_sumo


    return dag

rail.for_each_instance(create_dag)
