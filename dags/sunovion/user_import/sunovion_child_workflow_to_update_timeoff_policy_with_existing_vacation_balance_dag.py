
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sunovion_user_import_child_workflow_to_update_timeoff_policy_with_existing_vacation_balance_{config.instance}',
        description=f'Sunovion_Child Workflow to update timeoff policy with existing vacation balance {config.instance}',
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
            no_task='getdefaultpolicyforrequiredvacationtimeofftype_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='getdefaultpolicyforrequiredvacationtimeofftype_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        getdefaultpolicyforrequiredvacationtimeofftype_3 = rail.RepliconServiceOperator(
            task_id='getdefaultpolicyforrequiredvacationtimeofftype_3',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.newtimeofftypeuri }}"
                }
            }
        )

        get_defaultpolicy_in_required_format = rail.PythonOperator(
            task_id='get_defaultpolicy_in_required_format',
            python_callable=lambda:  json.loads((json.dumps(rail.result('getdefaultpolicyforrequiredvacationtimeofftype_3'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
        )

        def get_date_string(dateobj):
            return str(dateobj['day']) + '/' + str(dateobj['month']) + '/' + str(dateobj['year'])

        def get_futureandold_existingpolicy_list():
            policylist = rail.result('get_defaultpolicy_in_required_format')
            future_list = []
            old_list = []
            for policy in policylist:
                effectiveDate = get_date_string(policy['effectiveDate'])
                effectivedatecheck = '{"day"=>' + policy['effectiveDate']['day'] + ', "month"=>' + \
                    policy['effectiveDate']['month'] + ', "year"=>' + \
                    policy['effectiveDate']['year'] + '}'
                if (datetime.strptime(datetime.now().strftime("%m/%d/%Y"), '%m/%d/%Y') - datetime.strptime(effectiveDate, "%d/%m/%Y")).days < 0:
                    future_list.append({
                        "effectiveDate": {
                            "day": rail.result('foreach_document_7')['effectiveDate']['day'],
                            "month": rail.result('foreach_document_7')['effectiveDate']['month'],
                            "year": rail.result('foreach_document_7')['effectiveDate']['year']
                        },
                        "description": rail.result('foreach_document_7')['description'],
                        "policySet": rail.result('foreach_document_7')['policySet']
                    })
                elif (datetime.strptime(datetime.now().strftime("%m/%d/%Y"), '%m/%d/%Y') - datetime.strptime(effectiveDate, "%d/%m/%Y")).days > -1:
                    old_list.append({
                        "effectiveDate": {
                            "day": rail.result('foreach_document_7')['effectiveDate']['day'],
                            "month": rail.result('foreach_document_7')['effectiveDate']['month'],
                            "year": rail.result('foreach_document_7')['effectiveDate']['year']
                        },
                        "description": rail.result('foreach_document_7')['description'],
                        "policySet": rail.result('foreach_document_7')['policySet'],
                        "daydiff": (datetime.strptime(datetime.now().strftime("%m/%d/%Y"), '%m/%d/%Y') - datetime.strptime(effectiveDate, "%d/%m/%Y")).days,
                        "effectivedatecheck": effectivedatecheck
                    })
            return {
                'future_list': future_list,
                'old_list': old_list
            }

        get_future_and_old_existing_policy_list = rail.PythonOperator(
            task_id='get_future_and_old_existing_policy_list',
            python_callable=get_futureandold_existingpolicy_list
        )

        get_schedule_with_least_daydiff = rail.PythonOperator(
            task_id='get_schedule_with_least_daydiff',
            python_callable=lambda: min(rail.result('get_future_and_old_existing_policy_list')[
                                        'old_list'], key=lambda x: x['daydiff'])
        )

        if_log_16_present_17 = rail.IfOperator(
            task_id='if_log_16_present_17',
            test='''{{ result('get_schedule_with_least_daydiff') | is_truthy }}''',
            yes_task="accumulate_list_items_18",
            no_task="log_19",
        )

        accumulate_list_items_18 = rail.PythonOperator(
            task_id='accumulate_list_items_18',
            python_callable=lambda: {
                'effectiveDate': {
                    'day': datetime.now().day,
                    'month': datetime.now().month,
                    'year': datetime.now().year
                },
                'description': rail.smartjoin_by_delim((rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_future_and_old_existing_policy_list')['old_list'], 'daydiff', rail.result(
                    'get_schedule_with_least_daydiff'), 'description', '').split(',')), ''),
                'policySet': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_future_and_old_existing_policy_list')['old_list'], 'daydiff', rail.result('get_schedule_with_least_daydiff'), 'policySet', '')
            }
        )

        log_19 = rail.PythonOperator(
            task_id='log_19',
            python_callable=lambda: rail.result(
                'accumulate_list_items_18')['policySet']
        )

        log_20 = rail.PythonOperator(
            task_id='log_20',
            python_callable=lambda: rail.result('accumulate_list_items_18')[
                'policySet']['timeOffBalanceEventScripts']
        )

        accumulate_list_items_21 = rail.PythonOperator(
            task_id='accumulate_list_items_21',
            python_callable=lambda dag_run: [{
                "additionalParameters": {
                    "keyUri": "urn:replicon:script-key:parameter:amount",
                    "value": {
                        "number": dag_run.conf['timeofftypebalance']
                    }
                },
                "script": {
                    "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:script:455ffb4b-2daf-4ca0-ae88-73e39b3b17e2",
                    "description": "Set initial balance for the first day of a policy",
                    "name": "Starting Balance Set To"
                }
            }]
        )

        log_22 = rail.PythonOperator(
            task_id='log_22',
            python_callable=lambda: rail.result(
                'accumulate_list_items_21') + rail.result('log_20')
        )

        log_23 = rail.PythonOperator(
            task_id='log_23',
            python_callable=lambda: rail.result('accumulate_list_items_18')[
                'policySet']['timeOffValidationScripts']
        )

        log_24 = rail.PythonOperator(
            task_id='log_24',
            python_callable=lambda: {
                "effectiveDate":  {
                    "day": rail.result('accumulate_list_items_18')[0]['effectiveDate']['day'],
                    "month": rail.result('accumulate_list_items_18')[0]['effectiveDate']['month'],
                    "year": rail.result('accumulate_list_items_18')[0]['effectiveDate']['year']
                },
                "description": rail.result('accumulate_list_items_18')[0]['description'],
                "policySet": {
                    rail.result('log_22') + rail.result('log_23')
                }
            }
        )

        log_25 = rail.PythonOperator(
            task_id='log_25',
            python_callable=lambda: json.loads((json.dumps(rail.result('log_24'))).replace("=>", ":").replace(
                'effectiveDate: [[{', '"effectiveDate": {').replace('}}]] }', '}}]}').replace('}]]', '}').replace(
                'description: effective', '"description": "effective"').replace(
                ' policySet: {[{"additionalParameters"', '"policySet":{"timeOffBalanceEventScripts":[{"additionalParameters"').replace(
                '[[{"additionalParameters"', '"timeOffValidationScripts":[{"additionalParameters"').replace(
                '{"timeOffBalanceEventScripts":[{"additionalParameters":{', '{"timeOffBalanceEventScripts":[{"additionalParameters":[{').replace(
                '}}, "script"', '}}], "script"'))
        )

        log_26 = rail.PythonOperator(
            task_id='log_26',
            python_callable=lambda: '{"day"=>' + rail.result('accumulate_list_items_18')['effectiveDate']['day'] + ',"month"=>' + rail.result(
                'accumulate_list_items_18')['effectiveDate']['month'] + ',"year"=>' + rail.result('accumulate_list_items_18')['effectiveDate']['year'] + '}'
        )

        def get_existingpolicy_list():
            old_list = rail.result('get_future_and_old_existing_policy_list')[
                'old_list']
            past_existing_policy_list = [{
                'effectiveDate': {
                    'day': int(item['effectiveDate']['day']),
                    'month': int(item['effectiveDate']['month']),
                    'year': int(item['effectiveDate']['year'])
                },
                'description': item['description'],
                'policySet': item['policySet']
            } for item in old_list if item['effectivedatecheck'] != rail.result('log_26')]
            return past_existing_policy_list

        get_existing_policy_list = rail.PythonOperator(
            task_id='get_existing_policy_list',
            python_callable=lambda: get_existingpolicy_list
        )

        log_31 = rail.PythonOperator(
            task_id='log_31',
            python_callable=lambda: rail.result('get_existing_policy_list') + rail.result('log_25') + (rail.result(
                'get_future_and_old_existing_policy_list')['future_list'] if rail.result('get_future_and_old_existing_policy_list')['future_list'] else [])
        )

        log_32 = rail.PythonOperator(
            task_id='log_32',
            python_callable=lambda: '[' +
            json.dumps(rail.result('log_31')) + ']'
        )

        log_33 = rail.PythonOperator(
            task_id='log_33',
            python_callable=lambda: json.loads((rail.smartjoin_by_delim((rail.smartjoin_by_delim((rail.smartjoin_by_delim((rail.result('log_32').replace(
                '"=>", ":"').replace('\"', '"').replace('"scriptTarget"', '{"scriptTarget"').replace('\n', '').replace('[[{', '[{').replace(
                '}]]', '}]').replace('"{"value', '{"value').replace('"{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace(
                '}"}', "}}").replace('}"', "}").replace('"{', "{").replace(
                '{   effectiveDate:', '{   "effectiveDate":')).split('\"'), '"')).split('"['), '')).split(']"'), '')).replace(
                "script", "scriptTarget").replace("descriptTargetion", "description").replace("scriptTarget-key", "script-key").replace(
                ":scriptTarget:", ":script:").replace("scriptTargetTarget", "scriptTarget").replace("] ]", "]").replace(
                '{"scriptTarget"', '"scriptTarget"').replace("[nil,", "").replace(", nil", "").replace(",[nil] ", ""))
        )

        put_user_time_off_account_policy_set_schedule_34 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_34',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.newtimeofftypeuri }}"
                },
                "policySetScheduleEntries": "{{ result('log_33') }}"
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> getdefaultpolicyforrequiredvacationtimeofftype_3
        getdefaultpolicyforrequiredvacationtimeofftype_3 >> get_defaultpolicy_in_required_format >> get_future_and_old_existing_policy_list
        get_future_and_old_existing_policy_list >> get_schedule_with_least_daydiff >> if_log_16_present_17
        if_log_16_present_17 >> rail.Label(
            'Yes') >> accumulate_list_items_18 >> log_19
        if_log_16_present_17 >> rail.Label(
            'No') >> log_19 >> log_20 >> accumulate_list_items_21 >> log_22 >> log_23 >> log_24 >> log_25 >> log_26 >> get_existing_policy_list
        get_existing_policy_list >> log_31 >> log_32 >> log_33 >> put_user_time_off_account_policy_set_schedule_34 >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
