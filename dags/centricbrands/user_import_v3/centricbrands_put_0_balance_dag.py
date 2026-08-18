
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_put_0_balance_dag_id,
        description=f'CentricBrands User Import - put 0 balance Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
            no_task='get_effectivedate_object'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_effectivedate_object',
            end_task='catch_error_and_return_response',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year,
                'datestring': datestring
            }

        get_effectivedate_object = rail.PythonOperator(
            task_id='get_effectivedate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['terminationdate'])
        )

        get_assignedpolicy_forthe_timeofftype = rail.RepliconServiceOperator(
            task_id='get_assignedpolicy_forthe_timeofftype',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: list(filter(
                lambda policy: policy['timeOffType']['uri'] == dag_run.conf['timeoffuri'], response['policiesByTimeOffType']))
        )

        if_policyschedule_present = rail.IfOperator(
            task_id='if_policyschedule_present',
            test=lambda: rail.result('get_assignedpolicy_forthe_timeofftype') and rail.result(
                'get_assignedpolicy_forthe_timeofftype')[0]['policySetSchedule'] and rail.result(
                'get_assignedpolicy_forthe_timeofftype')[0]['policySetSchedule'][0]['description'],
            yes_task="get_past_policy_set",
            no_task="catch_error_and_return_response",
        )

        def get_date_string(dateobj, dateformat=True):
            return str(dateobj['month']) + "/" + str(dateobj['day']) + "/" + str(dateobj['year']) if dateformat else (
                str(dateobj['year']) + "-" + str(dateobj['month']) + "-" + str(dateobj['day']))

        def get_pastpolicy_set(dag_run):
            policy_schedule = rail.result('get_assignedpolicy_forthe_timeofftype')[
                0]['policySetSchedule']
            past_schedule = list(filter(lambda policy: datetime.strptime(get_date_string(
                policy['effectiveDate']), "%m/%d/%Y") < datetime.strptime(dag_run.conf['terminationdate'], "%m/%d/%Y"), policy_schedule))
            past_schedule = (json.dumps(past_schedule)).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"')
            past_schedule = '[' + past_schedule + ']'
            past_schedule = past_schedule.replace('[[', '').replace(']]', '')
            return json.loads('[' + past_schedule + ']')

        get_past_policy_set = rail.PythonOperator(
            task_id='get_past_policy_set',
            python_callable=get_pastpolicy_set
        )

        if_past_policyset_present = rail.IfOperator(
            task_id='if_past_policyset_present',
            test=lambda: len(rail.result('get_past_policy_set')) > 0,
            yes_task="put_time_offpolicywith_initialbalanceas0",
            no_task="catch_error_and_return_response",
        )

        put_time_offpolicywith_initialbalanceas0 = rail.RepliconServiceOperator(
            task_id='put_time_offpolicywith_initialbalanceas0',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_past_policy_set') + [
                    {
                        "effectiveDate": {
                            "year": rail.result('get_effectivedate_object')['year'],
                            "month": rail.result('get_effectivedate_object')['month'],
                            "day": rail.result('get_effectivedate_object')['day']
                        },
                        "description": "Effective on " + rail.result('get_effectivedate_object')['datestring'],
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": dag_run.conf['startingbalancesettouri'],
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "0",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "20",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": [
                                {
                                    "scriptTarget": {"uri": dag_run.conf['preventbalanceoverdrawuri']},
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                            "value": {"number": "0"}
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            }
        )

        catch_error_and_return_response = rail.PythonOperator(
            task_id='catch_error_and_return_response',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error_and_return_response
        can_run_batch_task >> rail.Label('No') >> get_effectivedate_object
        get_effectivedate_object >> get_assignedpolicy_forthe_timeofftype >> if_policyschedule_present
        if_policyschedule_present >> rail.Label(
            'Yes') >> get_past_policy_set >> if_past_policyset_present
        if_past_policyset_present >> rail.Label(
            'Yes') >> put_time_offpolicywith_initialbalanceas0 >> catch_error_and_return_response
        if_past_policyset_present >> rail.Label(
            'No') >> catch_error_and_return_response
        if_policyschedule_present >> rail.Label(
            'No') >> catch_error_and_return_response

    return dag


rail.for_each_instance(create_dag)
