from datetime import timedelta
from airflow.models import Variable
import rail


null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdatabc_seniority_udf_update_process_distinct_timesheets_{config.instance}_v3',
        description=f'NTTDATABC Seniority UDF Update Process Timesheets {config.instance}',
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
            no_task='query_details_of_timesheet_from_employee_approved_timesheets'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_details_of_timesheet_from_employee_approved_timesheets',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_details_of_timesheet_from_employee_approved_timesheets=rail.QueryCollectionOperator(
            task_id='query_details_of_timesheet_from_employee_approved_timesheets',
            name='approvedtimesheetdata',
            query="""SELECT * FROM  inputdatatimesheetapproved WHERE
                    inputdatatimesheetapproved.timesheeturi = '{{ dag_run.conf.timesheeturi }}' AND
                    inputdatatimesheetapproved.activity = 'Null' AND  inputdatatimesheetapproved.timeofftype LIKE '%*%' AND
                    inputdatatimesheetapproved.timeoffhours > 0 AND (inputdatatimesheetapproved.employeetype = 'Hourly' OR 
                    inputdatatimesheetapproved.employeetype = 'Auxiliary Hourly' OR 
                    inputdatatimesheetapproved.employeetype = 'Benefited Auxiliary')""",
        )

        query_details_of_timesheet_from_employee_pay_code=rail.QueryCollectionOperator(
            task_id='query_details_of_timesheet_from_employee_pay_code',
            name='paycodehoursdata',
            query="""SELECT * FROM  inputdatapaycodehours WHERE
                    inputdatapaycodehours.timesheeturi = '{{ dag_run.conf.timesheeturi }}' AND
                    ( inputdatapaycodehours.paycodecode IN ('REG','WKDHOL_2.0', 'WKDHOL_2.5'))""",
        )

        create_logs_lookuptable = rail.CreateLogOperator(
            task_id = 'create_logs_lookuptable'
        )

        trigger_child_dag=rail.TriggerDagRunOperator(
            task_id='trigger_child_dag',
            retries=0,
            trigger_dag_id=f'nttdatabc_seniority_udf_update_child_{config.instance}_v3',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "senorityudfuri": "{{ dag_run.conf.udfuri }}",
                "inputdata1": "{{ result('query_details_of_timesheet_from_employee_approved_timesheets') }}",
                "inputdata2": "{{ result('query_details_of_timesheet_from_employee_pay_code') }}",
                "timesheeturi": "{{ dag_run.conf.timesheeturi }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "lookuptable": "{{ result('create_logs_lookuptable')}}",
                "callerjobid": "{{ dag_run.conf.masterdagid }}"
            }
        )

        wait_for_timesheet_processing = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_timesheet_processing',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_dag") }}'
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> query_details_of_timesheet_from_employee_approved_timesheets
        query_details_of_timesheet_from_employee_approved_timesheets >> query_details_of_timesheet_from_employee_pay_code
        query_details_of_timesheet_from_employee_pay_code >> create_logs_lookuptable >> trigger_child_dag >> wait_for_timesheet_processing >> finish

    return dag

rail.for_each_instance(create_dag)
