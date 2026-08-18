
from datetime import timedelta, datetime
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdatabc_seniority_udf_update_subchild_{config.instance}',
        description=f'NTTDATABC Seniority UDF Update Sub Child {config.instance}',
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
            no_task='search_entries_in_timesheet_mapper'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_entries_in_timesheet_mapper',
            end_task='catch_error_and_log_entry',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def do_filter_log(log,dag_run):
            return log['properties']['loginname'] == dag_run.conf['loginname'] and log['properties']['username'] == dag_run.conf['user']

        search_entries_in_timesheet_mapper=rail.FilterLogEntriesOperator(
            task_id='search_entries_in_timesheet_mapper',
            log="{{ dag_run.conf.ntttimesheetmapper }}",
            filter_callable=do_filter_log
        )

        compose_csv=rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source="{{ result('search_entries_in_timesheet_mapper') }}",
            header=['jobid',
                    'loginname',
                    'username',
                    'timesheetperiod',
                    'earlierudfvalue',
                    'totalduration',
                    'finalvalue',
                    'date',
                    'check'],
            row=lambda item: [
                item['properties']['jobid'],
                item['properties']['loginname'],
                item['properties']['username'],
                item['properties']['timesheetperiod'],
                item['properties']['earlierudfvalue'],
                item['properties']['totalduration'],
                item['properties']['finalvalue'],
                item['properties']['date'],
            ],
        )

        create_lookuptable_collection = rail.CreateCollectionOperator(
            task_id='create_lookuptable_collection',
            source = "{{ result('compose_csv') }}",
            name = "lookuptabledata",
            columns = {
                'jobid':'jobid', 
                'loginname':'loginname', 
                'username':'username', 
                'timesheetperiod':'timesheetperiod', 
                'earlierudfvalue':'earlierudfvalue', 
                'totalduration':'totalduration', 
                'finalvalue':'finalvalue', 
                'date':'date'
            }
        )

        query_data_arranged_by_desc=rail.QueryCollectionOperator(
            task_id='query_data_arranged_by_desc',
            query="""SELECT * FROM  lookuptabledata ORDER BY  lookuptabledata.date DESC""",
        )

        load_records_of_arranged_data= rail.PythonOperator(
            task_id ='load_records_of_arranged_data',
            python_callable= lambda: rail.load_all_records(rail.result('query_data_arranged_by_desc'))
        )

        log_final_seniority_value=rail.PythonOperator(
            task_id='log_final_seniority_value',
            python_callable= lambda dag_run: float(rail.result('load_records_of_arranged_data')[0]['finalvalue']) - float(dag_run.conf['previoustotalhours']) +
                                float(dag_run.conf['totalhours'])
        )

        update_seniority_custom_field_value=rail.RepliconServiceOperator(
            task_id='update_seniority_custom_field_value',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.customfielduri }}",
                "value": "{{ result('log_final_seniority_value') }}"
            }
        )

        accumulate_udf_data_for_debugging=rail.SetVariableOperator(
            task_id='accumulate_udf_data_for_debugging',
            name='Updated UDF For debugging',
            append=True,
            value=lambda dag_run:{
                "timesheeturi": dag_run.conf['timesheeturi'],
                "approvalstatus":  dag_run.conf['approvalstatus'],
                "user":  dag_run.conf['user'],
                "useruri":  dag_run.conf['useruri'],
                "timesheetperiod":  dag_run.conf['timesheetperiod'],
                "totalhours":  dag_run.conf['totalhours'],
                "finalvalue": rail.result('log_final_seniority_value'),
                "existingvalue": rail.result('load_records_of_arranged_data')[0]['earlierudfvalue'],
                "status": "Updated Successfully"
            }
        )

        delete_entry_to_update=rail.FilterLogEntriesOperator(
            task_id='delete_entry_to_update',
            log="{{ dag_run.conf.ntttimesheetmapper }}",
            properties= {
                "timesheetperiod": "{{dag_run.conf.timesheetperiod}}",
                "username": "{{dag_run.conf.user}}",
                'loginname': "{{dag_run.conf.loginname}}"
            },
            remove_filtered_entries=True
        )

        update_entry_in_mapper=rail.WriteLogOperator(
            task_id='update_entry_in_mapper',
            log="{{ dag_run.conf.ntttimesheetmapper }}",
            message='na',
            properties=lambda dag_run: {
                "jobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
                "loginname": dag_run.conf['loginname'],
                "username": dag_run.conf['user'],
                "timesheetperiod": dag_run.conf['timesheetperiod'],
                "earlierudfvalue": rail.result('load_records_of_arranged_data')[0]['earlierudfvalue'],
                "totalduration": dag_run.conf['totalhours'],
                "finalvalue": "{{ result('log_final_seniority_value') }}",
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "check": dag_run.conf['check']
            }
        )

        add_success_log_entry = rail.WriteLogOperator(
            task_id='add_success_log_entry',
            log="{{ dag_run.conf.lookuptable }}",
            message='na',
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "loginname": dag_run.conf['loginname'],
                "timesheetperiod": dag_run.conf['timesheetperiod'],
                "totalhours": dag_run.conf['totalhours'],
                "finalvalue": rail.result('log_final_seniority_value'),
                "approvalstatus": dag_run.conf['approvalstatus'],
                "status": "Success",
                "details": "Updated Successfully",
                "childjob": get_dagrun_ecid(rail.get_current_context()['dag_run']),
                "retrycount": 0
            }
        )

        catch_error_and_log_entry=rail.WriteLogOperator(
            task_id='catch_error_and_log_entry',
            log="{{ dag_run.conf.lookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "loginname": dag_run.conf['loginname'],
                "timesheetperiod": dag_run.conf['timesheetperiod'],
                "totalhours": dag_run.conf['totalhours'],
                "finalvalue": rail.result('log_final_seniority_value'),
                "approvalstatus": dag_run.conf['approvalstatus'],
                "status": "Error",
                "details": rail.render_template('{{get_error_message()}}'),
                "childjob": get_dagrun_ecid(rail.get_current_context()['dag_run']),
                "retrycount": 0
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error_and_log_entry
        can_run_batch_task >> rail.Label('No') >> search_entries_in_timesheet_mapper
        search_entries_in_timesheet_mapper >> compose_csv >> create_lookuptable_collection >> query_data_arranged_by_desc
        query_data_arranged_by_desc >> load_records_of_arranged_data >> log_final_seniority_value
        log_final_seniority_value >> update_seniority_custom_field_value >> accumulate_udf_data_for_debugging >> delete_entry_to_update
        delete_entry_to_update >> update_entry_in_mapper >> add_success_log_entry >> catch_error_and_log_entry >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_dag)
