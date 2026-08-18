from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.user_processor_dag_id,
        description=f'Ascend Child - User processor (search/add/update per user) {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config',
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_required_fields_blank'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_required_fields_blank',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        # ── Required field check ─────────────────────────────────────────
        if_required_fields_blank = rail.IfOperator(
            task_id='if_required_fields_blank',
            test='{{ dag_run.conf["enabled"] | is_falsy or \
                dag_run.conf["location"] | is_falsy or \
                dag_run.conf["employeeid"] | is_falsy }}',
            yes_task='log_skip_required_fields',
            no_task='search_user',
        )

        log_skip_required_fields = rail.WriteLogOperator(
            task_id='log_skip_required_fields',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf["loginname"],
                "action": "Add",
                "status": "Skipped",
                "username": dag_run.conf.get('employeefirstname', '') + " " + dag_run.conf.get('employeelastname', ''),
                "details": rail.smartjoin_by_delim([
                    '' if str(dag_run.conf.get('enabled', '')).lower().strip() == 'yes' else 'Enabled (User Status) is not set to yes',
                    '' if dag_run.conf.get('location') else 'Location is blank',
                    '' if dag_run.conf.get('employeeid') else 'Employee id is blank',
                ], ',')
            }
        )

        # ── Search user by employee id ───────────────────────────────────
        def get_search_payload_data(dag_run):
            return {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": dag_run.conf["employeeid"]
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }

        def get_filtered_user_data(response, dag_run):
            data = response.json()['d']
            return list(filter(lambda x: bool(x['name'] and x['employeeid'] == dag_run.conf["employeeid"]), map(lambda row: {
                "name": row['cells'][0]['textValue'] if row['cells'][0]['dataType'] != 'urn:replicon:list-type:null' else None,
                "uri": row['cells'][0]['uri'],
                "employeeid": row['cells'][1]['textValue']
            }, data['rows'])))

        search_user = rail.RepliconServiceOperator(
            task_id='search_user',
            endpoint="/services/UserListService1.svc/GetData",
            data=get_search_payload_data,
            response_filter=get_filtered_user_data
        )

        if_multiple_users_found = rail.IfOperator(
            task_id='if_multiple_users_found',
            test=lambda: bool(len(rail.result('search_user')) > 1),
            yes_task='log_multiple_users',
            no_task='if_user_found',
        )

        log_multiple_users = rail.WriteLogOperator(
            task_id='log_multiple_users',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "userloginname": dag_run.conf["loginname"],
                "username": dag_run.conf["employeefirstname"] + " " + dag_run.conf["employeelastname"],
                "action": "NA",
                "details": "Multiple users " + ','.join(list(map(lambda i: i['name'], rail.result('search_user')))) + " found with employee id-" + str(dag_run.conf["employeeid"]),
                "status": "Skipped"
            }
        )

        if_user_found = rail.IfOperator(
            task_id='if_user_found',
            test="{{ result('search_user') | is_truthy }}",
            yes_task='trigger_update_user',
            no_task='if_new_profile_enabled_ne_yes',
        )

        if_new_profile_enabled_ne_yes = rail.IfOperator(
            task_id='if_new_profile_enabled_ne_yes',
            test='''{{ dag_run.conf["enabled"].lower() != 'yes' or dag_run.conf["loginname"] | is_falsy }}''',
            yes_task='log_skip_new_user',
            no_task='trigger_add_user',
        )

        log_skip_new_user = rail.WriteLogOperator(
            task_id='log_skip_new_user',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf.get('loginname', ''),
                "username": dag_run.conf.get('employeefirstname', '') + " " + dag_run.conf.get('employeelastname', ''),
                "action": "Add",
                "status": "Skipped",
                "details": rail.smartjoin_by_delim([
                    '' if str(dag_run.conf.get('enabled', '')).lower().strip() == 'yes' else 'Enabled (User Status) is not set to yes for new user',
                    '' if dag_run.conf.get('loginname') else 'Employee loginname is not present',
                ], ',')
            }
        )

        # ── Trigger add user ─────────────────────────────────────────────
        trigger_add_user = rail.TriggerDagRunOperator(
            task_id='trigger_add_user',
            retries=3,
            trigger_dag_id=config.add_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "startdate": dag_run.conf["startdate"],
                "loginname": dag_run.conf["loginname"],
                "department": dag_run.conf["department"],
                "employeetype": dag_run.conf["employeetype"],
                "location": dag_run.conf["location"],
                "employeefirstname": dag_run.conf["employeefirstname"],
                "employeelastname": dag_run.conf["employeelastname"],
                "enabled": dag_run.conf["enabled"],
                "terminationdate": dag_run.conf["terminationdate"],
                "continuousservicedate": dag_run.conf["continuousservicedate"],
                "emailaddress": dag_run.conf["emailaddress"],
                "manager": dag_run.conf["manager"],
                "homecountry": dag_run.conf["homecountry"],
                "homestateprovince": dag_run.conf["homestateprovince"],
                "homecity": dag_run.conf["homecity"],
                "hourlypayrollrate": dag_run.conf["hourlypayrollrate"],
                "hourlypayrollcurrency": dag_run.conf["hourlypayrollcurrency"],
                "timetype": dag_run.conf["timetype"],
                "departmenturi": dag_run.conf["departmenturi"],
                "costcenter": dag_run.conf["costcenter"],
                "udf": dag_run.conf["udf"] if dag_run.conf.get('udf') else null,
                "ascend_user_import_logs_lookuptable": dag_run.conf["ascend_user_import_logs_lookuptable"],
                "ascend_supervisor_assignments_logs_lookuptable": dag_run.conf["ascend_supervisor_assignments_logs_lookuptable"]
            }
        )

        wait_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_add_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_add_user") }}'
        )

        # ── Trigger update user ──────────────────────────────────────────
        trigger_update_user = rail.TriggerDagRunOperator(
            task_id='trigger_update_user',
            retries=3,
            trigger_dag_id=config.update_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "startdate": dag_run.conf["startdate"],
                "loginname": dag_run.conf["loginname"],
                "department": dag_run.conf["department"],
                "employeetype": dag_run.conf["employeetype"],
                "location": dag_run.conf["location"],
                "employeefirstname": dag_run.conf["employeefirstname"],
                "employeelastname": dag_run.conf["employeelastname"],
                "enabled": dag_run.conf["enabled"],
                "terminationdate": dag_run.conf["terminationdate"],
                "continuousservicedate": dag_run.conf["continuousservicedate"],
                "emailaddress": dag_run.conf["emailaddress"],
                "manager": dag_run.conf["manager"],
                "homecountry": dag_run.conf["homecountry"],
                "homestateprovince": dag_run.conf["homestateprovince"],
                "homecity": dag_run.conf["homecity"],
                "hourlypayrollrate": dag_run.conf["hourlypayrollrate"],
                "hourlypayrollcurrency": dag_run.conf["hourlypayrollcurrency"],
                "timetype": dag_run.conf["timetype"],
                "departmenturi": dag_run.conf["departmenturi"],
                "costcenter": dag_run.conf["costcenter"],
                "udf": dag_run.conf["udf"] if dag_run.conf.get('udf') else null,
                "ascend_user_import_logs_lookuptable": dag_run.conf["ascend_user_import_logs_lookuptable"],
                "ascend_supervisor_assignments_logs_lookuptable": dag_run.conf["ascend_supervisor_assignments_logs_lookuptable"],
                "parentjobid": dag_run.conf.get('parentjobid', ''),
                "useruri": rail.result('search_user')[0]['uri'] if rail.result('search_user') and len(rail.result('search_user')) > 0 else '',
            }
        )

        wait_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_update_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_update_user") }}'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties={
                "jobid": "{{ dag_run_ecid() }}",
                "userloginname": '{{ dag_run.conf["loginname"] }}',
                "username": '{{ dag_run.conf["employeefirstname"] }} {{ dag_run.conf["employeelastname"] }}',
                "action": "NA",
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        # ── Wiring ──────────────────────────────────────────────────────
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> if_required_fields_blank
        if_required_fields_blank >> rail.Label('Yes') >> log_skip_required_fields >> catch_and_log_errors
        if_required_fields_blank >> rail.Label('No') >> search_user >> if_multiple_users_found
        if_multiple_users_found >> rail.Label('Yes') >> log_multiple_users >> if_user_found
        if_multiple_users_found >> rail.Label('No') >> if_user_found
        if_user_found >> rail.Label('Yes') >> trigger_update_user >> wait_update_user >> catch_and_log_errors
        if_user_found >> rail.Label('No') >> if_new_profile_enabled_ne_yes
        if_new_profile_enabled_ne_yes >> rail.Label('Yes') >> log_skip_new_user >> catch_and_log_errors
        if_new_profile_enabled_ne_yes >> rail.Label('No') >> trigger_add_user >> wait_add_user >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
