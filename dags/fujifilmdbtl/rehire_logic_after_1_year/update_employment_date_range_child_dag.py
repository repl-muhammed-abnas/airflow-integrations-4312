import rail
from datetime import timedelta

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_dag,
        description=f'Fujifilmdbtl | Rehire Logic | Update Employment Date Range Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",
            extra_config=config)


        update_employment_date_range=rail.RepliconServiceOperator(
            task_id='update_employment_date_range',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run:{
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf['adjusted_start_date'], config.date_format),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

    
        get_user_timeofftype_policy_summary=rail.RepliconServiceOperator(
            task_id='get_user_timeofftype_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
            "userUri": "{{ dag_run.conf.useruri }}"
            }
        )


        trigger_timeofftype_policy_updates=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeofftype_policy_updates',
            items=lambda: [item for item in rail.result('get_user_timeofftype_policy_summary')['policiesByTimeOffType'] if item['isTimeOffAllowedAgainstThisTimeOffType']],
            trigger_dag_id=config.subchild_dag,
            conf={
                "userloginname": "{{ dag_run.conf.login_name }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "ftpt": "{{ dag_run.conf.fullparttime }}",
                "regulartemp": "{{ dag_run.conf.regular_temporary }}",
                "adjustedstartdate": "{{ dag_run.conf.adjusted_start_date}}",
                "startdate": "{{ dag_run.conf.user_start_date }}",
                "timeofftypename": "{{ item.timeOffType.name }}",
                "policyset": "{{ item.policySetSchedule | to_json}}",
                "timeoffuri": "{{ item.timeOffType.uri }}",
                "log": "{{ dag_run.conf.log }}",
                "is_timeoff_allowed": "{{ item.isTimeOffAllowedAgainstThisTimeOffType }}"
            },
        )

        wait_for_completion_timeoff_policy_update = rail.WaitForDagRunsSensor(
            task_id="wait_for_completion_timeoff_policy_update",
            dag_runs="{{result('trigger_timeofftype_policy_updates')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'user': "{{dag_run.conf.login_name}}",
                'timeofftype':'',
                'details': '{{ get_error_message() }}',
                "status": "Error"
            }
        )


        update_employment_date_range >> get_user_timeofftype_policy_summary >> trigger_timeofftype_policy_updates >> \
            wait_for_completion_timeoff_policy_update >> catch_and_log_errors


    return dag

rail.for_each_instance(create_dag)


