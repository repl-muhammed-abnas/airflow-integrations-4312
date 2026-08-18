from datetime import timedelta
from datetime import datetime
from airflow.models import Variable
import rail


null = None

# pylint: disable=too-many-statements


def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_process_each_user_timeoff_records_child_{config.instance}',
        description=f'deltek_costpoint_process_each_user_timeoff_records_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
        },
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        def get_to_details_request(dag_run):
            to_start_date = dag_run.conf['user_timeoffs'][0]['timeoffdate']
            to_end_date = dag_run.conf['user_timeoffs'][-1]['timeoffdate']
            timeoff_start_date = datetime.strptime(
                to_start_date, config.costpoint_to_date_format)
            timeoff_end_date = datetime.strptime(
                to_end_date, config.costpoint_to_date_format)
            return {
                "userUri": dag_run.conf['user_timeoffs'][0]['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": timeoff_start_date.year,
                        "month": timeoff_start_date.month,
                        "day": timeoff_start_date.day,
                    },
                    "endDate": {
                        "year": timeoff_end_date.year,
                        "month": timeoff_end_date.month,
                        "day": timeoff_end_date.day,
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
            
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_timeoff_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_timeoff_details',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_details',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda dag_run: get_to_details_request(dag_run)
        )

        def get_user_timeoff_to_process(dag_run):
            processing_timeoffs = []
            costpoint_timeoffs = dag_run.conf['user_timeoffs']
            polaris_existing_timeoffs = rail.result('get_timeoff_details')
            for toinfo in costpoint_timeoffs:
                timeoff_start_date = datetime.strptime(
                    toinfo['timeoffdate'], config.costpoint_to_date_format)
                timeoff_end_date = datetime.strptime(
                    toinfo['timeoffdate'], config.costpoint_to_date_format)
                existing_timeoff = list(filter(lambda x:
                                               datetime.strptime(str(x['startDateDetails']['date']['month'])+'/'+str(x['startDateDetails']['date']['day'])+'/'+str(
                                                   x['startDateDetails']['date']['year']), config.replicon_timeoff_date_format).date()
                                               == timeoff_start_date.date() and
                                               datetime.strptime(str(x['endDateDetails']['date']['month'])+'/'+str(x['endDateDetails']['date']['day'])+'/'+str(
                                                   x['endDateDetails']['date']['year']), config.replicon_timeoff_date_format).date()
                                               == timeoff_end_date.date(), polaris_existing_timeoffs))
                existing_timeoff_uri = None
                to_status = None
                if existing_timeoff:
                    existing_timeoff_uri = existing_timeoff[0]['uri']
                    to_status = existing_timeoff[0]['approvalStatus']['uri']
                processing_timeoffs.append({
                    "timeoffhours": toinfo['timeoffhours'],
                    "useruri": toinfo['useruri'],
                    "empid": toinfo['emp_id'],
                    "timeoffdate": toinfo['timeoffdate'],
                    "existingtimeoff": True if existing_timeoff_uri else False,
                    "timeoffuri": toinfo['timeoffuri'],
                    "bookinguri": existing_timeoff_uri,
                    "deletedtimeoff": False,
                    "timeoff_type": toinfo["timeoff_type"],
                    "istimeoffinopenstatus": True if to_status and to_status == 'urn:replicon:approval-status:open' else False
                })

            return processing_timeoffs

        process_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoff',
            retries=0,
            items=get_user_timeoff_to_process,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_process_timeoff_records_child_{config.instance}',
            conf=lambda item: item
        )

        wait_for_completion_trigger_process_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_timeoff',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_timeoff") }}'
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_timeoff") }}',
            dagrun_task_id='create_log',
            flatten=True
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_timeoff_details
        get_timeoff_details >> process_timeoff >> wait_for_completion_trigger_process_timeoff >> \
            gather_child_logs >> finish

    return dag


rail.for_each_instance(create_dag)
