
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from nttdata.shift_automation.utils.python_callable_methods import get_schedule_assignment_list
import pendulum
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'nttdata_existing_user_default_shift_assignment_master_{config.instance}',
        description=f'NTTData Existing user default shift assignment master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.existing_user_schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.shift_schedule_report_name,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_enabled_user_schedule_data',
            no_task= 'finish'
        )

        load_enabled_user_schedule_data = rail.LoadCSVFileOperator(
            task_id='load_enabled_user_schedule_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_enabled_user_schedule_data = rail.CreateCollectionOperator(
            task_id='create_enabled_user_schedule_data',
            source = "{{ result('load_enabled_user_schedule_data') }}",
            name = "enabled_user_schedule_data",
            columns = {
                'User Name':'username',
                'Login Name':'loginname',
                'useruri':'useruri',
                'Schedule Name (Current)':'schedulename',
                'Country':'country',
                'User Status':'status',
                'User Start Date':'userstartdate'
            }
        )

        query_get_all_users_with_shift_schedules = rail.QueryCollectionOperator(
            task_id='query_get_all_users_with_shift_schedules',
            query="""SELECT * FROM enabled_user_schedule_data WHERE schedulename='Shift Schedule'""",
        )

        start_date = (pendulum.now()+relativedelta(months=+13, day=1)).date()
        end_date = (pendulum.now()+relativedelta(months=+13, day=31)).date()

        trigger_shift_assignment_per_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_shift_assignment_per_user',
            retries=0,
            items="{{ result('query_get_all_users_with_shift_schedules') }}",
            trigger_dag_id=f'nttdata_default_shift_assignment_per_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item : {
                "useruri": item["useruri"],
                "startdate": str(start_date),
                "enddate": str(end_date),
                "country": item["country"],
                "shiftname": get_schedule_assignment_list(item),
                "startdateday": str(start_date.strftime("%d")),
                "startdatemonth": str(start_date.strftime("%m")),
                "startdateyear": str(start_date.strftime("%Y")),
                "enddateday": str(end_date.strftime("%d")),
                "enddatemonth": str(end_date.strftime("%m")),
                "enddateyear": str(end_date.strftime("%Y")),
                "username": item["username"],
                "loginname": item["loginname"]
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_details >> run_my_report_entry
        run_my_report_exit >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> load_enabled_user_schedule_data >> create_enabled_user_schedule_data \
            >> query_get_all_users_with_shift_schedules >> trigger_shift_assignment_per_user

        report_has_data >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_dag)
