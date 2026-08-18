
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from sunovion.user_import.utils import request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sunovion_user_import_child_for_timeoff_policy_update_on_each_time_off_type_{config.instance}',
        description=f'Sunovion_Child for timeoff policy update on each time off type {config.instance}',
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
            no_task='parse_json_3_3_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='parse_json_3_3_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        parse_json_3_3_3 = rail.PythonOperator(
            task_id='parse_json_3_3_3',
            python_callable=lambda dag_run: json.loads(
                dag_run.conf['policyset'])
        )

        def get_date_string(dateobj):
            return (datetime.strptime(str(dateobj['month']) + '/' + str(dateobj['day']) + '/' + str(dateobj['year']),'%m/%d/%Y')).strftime('%m/%d/%Y')

        log_4 = rail.PythonOperator(
            task_id='log_4',
            python_callable=lambda: get_date_string(
                rail.result('parse_json_3_3_3')[0]['effectiveDate'])
        )

        def get_existingpolicyschedule():
            policysets = rail.result('parse_json_3_3_3')
            today_date = datetime.strptime(datetime.now().strftime("%m/%d/%Y"), "%m/%d/%Y")
            policyset_details = [{
                'effectiveDate': get_date_string(policyset['effectiveDate']),
                'description': policyset['description'],
                'policySet': policyset['policySet'],
                'daydiff': ( today_date - datetime.strptime(get_date_string(
                    policyset['effectiveDate']), "%m/%d/%Y")).days
            } for policyset in policysets]
            existing_policyschedule = [{
                'effectiveDate': {
                    'day': (schedule['effectiveDate'].split('/'))[1],
                    'month': (schedule['effectiveDate'].split('/'))[0],
                    'year': (schedule['effectiveDate'].split('/'))[2]
                },
                'description': schedule['description'],
                'policySet': schedule['policySet']
            } for schedule in policyset_details if schedule['daydiff'] > -1]
            return {
                'schedulelist': policyset_details,
                'existingschedulelist': existing_policyschedule
            }

        get_existing_policy_schedule = rail.PythonOperator(
            task_id='get_existing_policy_schedule',
            python_callable=get_existingpolicyschedule
        )

        log_todays_date_11 = rail.PythonOperator(
            task_id='log_todays_date_11',
            python_callable= request_payload.get_todays_date
        )

        log_checkiftodayisalreadyavailableasaneffectivedate_17 = rail.PythonOperator(
            task_id='log_checkiftodayisalreadyavailableasaneffectivedate_17',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_existing_policy_schedule')['schedulelist'], 'effectiveDate', rail.result(
                'log_todays_date_11')['date'], 'effectiveDate', '') if rail.result('get_existing_policy_schedule')['schedulelist'] else ''
        )

        if_log_9_present_18 = rail.IfOperator(
            task_id='if_log_9_present_18',
            test='''{{ result('log_checkiftodayisalreadyavailableasaneffectivedate_17') | is_truthy }}''',
            yes_task="log_tomorrowsdate_19",
            no_task="log_effective_yeartobeused_25",
        )

        def get_tomorrows_date():
            tomorrow_date = datetime.now() + timedelta(days=1)
            return {
                'description': tomorrow_date.strftime("%m/%d/%Y"),
                'day': tomorrow_date.day,
                'month': tomorrow_date.month,
                'year': tomorrow_date.year
            }

        log_tomorrowsdate_19 = rail.PythonOperator(
            task_id='log_tomorrowsdate_19',
            python_callable= get_tomorrows_date
        )

        def get_effectivedate_and_description():
            return {
                'description': str(rail.result('log_tomorrowsdate_19')['description']) if rail.result(
                    'log_checkiftodayisalreadyavailableasaneffectivedate_17') else str(rail.result('log_todays_date_11')['description']),
                'day': str(rail.result('log_tomorrowsdate_19')['day']) if rail.result(
                    'log_checkiftodayisalreadyavailableasaneffectivedate_17') else str(rail.result('log_todays_date_11')['day']),
                'month': str(rail.result('log_tomorrowsdate_19')['month']) if rail.result(
                    'log_checkiftodayisalreadyavailableasaneffectivedate_17') else str(rail.result('log_todays_date_11')['month']),
                'year': str(rail.result('log_tomorrowsdate_19')['year']) if rail.result(
                    'log_checkiftodayisalreadyavailableasaneffectivedate_17') else str(rail.result('log_todays_date_11')['year'])
            }

        log_effective_yeartobeused_25 = rail.PythonOperator(
            task_id='log_effective_yeartobeused_25',
            python_callable=get_effectivedate_and_description
        )

        get_new_schedule = rail.PythonOperator(
            task_id='get_new_schedule',
            python_callable=lambda dag_run: [{
                "effectiveDate": {
                    "day": rail.result('log_effective_yeartobeused_25')['day'],
                    "month": rail.result('log_effective_yeartobeused_25')['month'],
                    "year": rail.result('log_effective_yeartobeused_25')['year']
                },
                "description": rail.result('log_effective_yeartobeused_25')['description'],
                "policySet": {
                    "timeOffBalanceEventScripts": {
                        "additionalParameters": {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "number": dag_run.conf['newschedulebalance']
                            }
                        },
                        "script": {
                            "description": "Set initial balance for the first day of a policy",
                            "name": "Starting Balance Set To",
                            "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:script:455ffb4b-2daf-4ca0-ae88-73e39b3b17e2"
                        }
                    },
                    "timeOffValidationScripts": {
                        "additionalParameters": {
                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                            "value": {
                                "number": "0"
                            }
                        },
                        "script": {
                            "description": "Do not allow the user's time off balance to go below the overdraw threshold.",
                            "name": "Prevent balance overdraw",
                            "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:script:97274f0c-2cdc-4dce-af52-750144fa09fb"
                        }
                    }
                }
            }]
        )

        def get_newschedule_in_requiredformat():
            return (rail.smartjoin_by_delim((rail.smartjoin_by_delim((rail.smartjoin_by_delim((rail.smartjoin_by_delim((
                (json.dumps(rail.result('get_new_schedule'))).split('{"additionalParameters')),'[{"additionalParameters')).split(
                '}, "script'),'}],"script')).split(
                'additionalParameters": {"keyUri'),'additionalParameters":[{"keyUri')).split(
                '}, "timeOffValidationScripts'),'}],"timeOffValidationScripts')).replace("}}}]", "}]}}]")

        get_new_schedule_in_required_format = rail.PythonOperator(
            task_id='get_new_schedule_in_required_format',
            python_callable=get_newschedule_in_requiredformat
        )

        def flatten(nested_list):
            flat_list = []
            for item in nested_list:
                if isinstance(item, list):
                    flat_list.extend(flatten(item))
                else:
                    flat_list.append(item)
            return flat_list

        get_existing_and_new_schedule_combined = rail.PythonOperator(
            task_id='get_existing_and_new_schedule_combined',
            python_callable=lambda: flatten(json.loads('[' + json.dumps(rail.result('get_existing_policy_schedule')[
                'existingschedulelist']) + ',' +
                rail.result('get_new_schedule_in_required_format') + ']'))
        )

        def get_finalschedule():
            final_schedule_in_json = (json.dumps(rail.result('get_existing_and_new_schedule_combined'))).replace("=>", ":").replace('\"', '"').replace(
                '"scriptTarget"', '{"scriptTarget"').replace('\n', '').replace(
                '[[{', '[{').replace('}]]', '}]').replace('"{"value', '{"value').replace(
                '"{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace('}"}', "}}").replace('}"', "}").replace(
                '"{', "{").replace('{   effectiveDate:', '{   "effectiveDate":')
            final_schedule_in_json = (rail.smartjoin_by_delim((rail.smartjoin_by_delim((rail.smartjoin_by_delim(
                final_schedule_in_json.split('\"'), '"')).split('"['), '')).split(']"'), '')).replace("script", "scriptTarget").replace(
                "descriptTargetion", "description").replace("scriptTarget-key", "script-key").replace(":scriptTarget:", ":script:").replace(
                "scriptTargetTarget", "scriptTarget").replace("] ]", "]").replace('{"scriptTarget"', '"scriptTarget"')
            return json.loads(final_schedule_in_json)

        get_final_schedule = rail.PythonOperator(
            task_id='get_final_schedule',
            python_callable=get_finalschedule
        )

        put_user_time_off_account_policy_set_schedule_34 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_34',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_final_schedule')
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
        can_run_batch_task >> rail.Label('No') >> parse_json_3_3_3
        parse_json_3_3_3 >> log_4 >> get_existing_policy_schedule >> log_todays_date_11 >> log_checkiftodayisalreadyavailableasaneffectivedate_17
        log_checkiftodayisalreadyavailableasaneffectivedate_17 >> if_log_9_present_18
        if_log_9_present_18 >> rail.Label(
            'Yes') >> log_tomorrowsdate_19 >> log_effective_yeartobeused_25
        if_log_9_present_18 >> rail.Label(
            'No') >> log_effective_yeartobeused_25 >> get_new_schedule >> get_new_schedule_in_required_format >> get_existing_and_new_schedule_combined
        get_existing_and_new_schedule_combined >> get_final_schedule >> put_user_time_off_account_policy_set_schedule_34 >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
