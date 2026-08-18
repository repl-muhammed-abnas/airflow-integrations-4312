from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'wolverinepipeline_custom_paychex_child_{config.instance}',
        description=f'Wolverinepipeline_custom_paychex_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_data_per_timesheet'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_data_per_timesheet',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_data_per_timesheet = rail.QueryCollectionOperator(
            task_id='query_data_per_timesheet',
            query="""SELECT * FROM reportdata WHERE reportdata.timesheeturi = '{{dag_run.conf.timesheeturi }}'""",
        )

        declare_list = rail.SetVariableOperator(
            task_id='declare_list',
            append=False,
            name='finalreportdata',
            value=[]
        )

        create_datapertimesheet_collection = rail.CreateCollectionOperator(
            task_id='create_datapertimesheet_collection',
            source=lambda: rail.result('query_data_per_timesheet'),
            name="datapertimesheet",
        )

        query_distinct_data_timesheets = rail.QueryCollectionOperator(
            task_id='query_distinct_data_timesheets',
            query="""SELECT DISTINCT datapertimesheet.paycode,datapertimesheet.activitycode,datapertimesheet.payrate FROM datapertimesheet WHERE datapertimesheet.hourstype ='workhours' AND datapertimesheet.paycode IS NOT NULL AND datapertimesheet.payrate IS NOT NULL AND datapertimesheet.paycode != 'None' AND datapertimesheet.payrate != 'None' AND datapertimesheet.paycode != 'nil' AND datapertimesheet.payrate != 'nil'""",
        )

        for_each_distinct_data_timesheets_do = rail.ForEachOperator(
            task_id='for_each_distinct_data_timesheets_do',
            items="{{result('query_distinct_data_timesheets')}}",
            start_task='query_workhours',
            end_task='for_each_distinct_data_timesheets_do_end'
        )

        query_workhours = rail.QueryCollectionOperator(
            task_id='query_workhours',
            query="""SELECT datapertimesheet.empid,datapertimesheet.paycode,datapertimesheet.activitycode,ROUND(SUM(datapertimesheet.hours), 2) AS sumhours,datapertimesheet.amount,datapertimesheet.payrate FROM datapertimesheet WHERE NULLIF(hours,'') IS NOT NULL AND datapertimesheet.hourstype == 'workhours' AND datapertimesheet.paycode == '{{result('for_each_distinct_data_timesheets_do').paycode}}' AND '{{result('for_each_distinct_data_timesheets_do').activitycode}}' == datapertimesheet.activitycode AND '{{result('for_each_distinct_data_timesheets_do').payrate}}' == datapertimesheet.payrate""",
        )

        for_each_workhours_query_do = rail.ForEachOperator(
            task_id='for_each_workhours_query_do',
            items=lambda: rail.load_all_records(
                rail.result('query_workhours')),
            start_task='if_sumhours_present',
            end_task='for_each_workhours_query_do_end'
        )

        if_sumhours_present = rail.IfOperator(
            task_id='if_sumhours_present',
            test=lambda: float(rail.result('for_each_workhours_query_do')[
                               'sumhours']) > 0 if rail.result('for_each_workhours_query_do')['sumhours'] else False,
            yes_task="log_get_decimal_places",
            no_task='for_each_workhours_query_do_end'
        )

        log_get_decimal_places = rail.PythonOperator(
            task_id='log_get_decimal_places',
            python_callable=lambda: ((rail.result('for_each_workhours_query_do')['sumhours']) + "0") if len(str(rail.result(
                'for_each_workhours_query_do')['sumhours']).split('.')[1]) == 1 else rail.result('for_each_workhours_query_do')['sumhours']
        )

        add_entry1 = rail.WriteLogOperator(
            task_id='add_entry1',
            log="{{dag_run.conf.custom_paychex_lookuptable}}",
            severity='',
            message='na',
            properties=lambda dag_run: {
                'jobid': dag_run.conf['jobid'],
                'empid': rail.result('for_each_workhours_query_do')['empid'],
                'paycode': rail.result('for_each_workhours_query_do')['paycode'] if rail.result('for_each_workhours_query_do')['paycode'] else "",
                'activitycode': rail.result('for_each_workhours_query_do')['activitycode'] if rail.result('for_each_workhours_query_do')['activitycode'] else "",
                'sumofhours': rail.result('log_get_decimal_places'),
                'amount': rail.result('for_each_workhours_query_do')['amount'] if rail.result('for_each_workhours_query_do')['amount'] else "",
                'payrate': rail.result('for_each_workhours_query_do')['payrate'] if rail.result('for_each_workhours_query_do')['payrate'] else "",
                'childjobid': rail.render_template("{{ dag_run_ecid() }}"),


            },
        )

        accumulate_paycode_list = rail.SetVariableOperator(
            task_id='accumulate_paycode_list',
            name="{{result('declare_list').name}}",
            append=True,
            value=lambda: {
                'empid': rail.result('for_each_workhours_query_do')['empid'],
                'paycode': rail.result('for_each_workhours_query_do')['paycode'] if rail.result('for_each_workhours_query_do')['paycode'] else "",
                'activitycode': rail.result('for_each_workhours_query_do')['activitycode'] if rail.result('for_each_workhours_query_do')['activitycode'] else "",
                'sumofhours': rail.result('log_get_decimal_places'),
                'amount': rail.result('for_each_workhours_query_do')['amount'] if rail.result('for_each_workhours_query_do')['amount'] else "",
                'payrate': rail.result('for_each_workhours_query_do')['payrate'] if rail.result('for_each_workhours_query_do')['payrate'] else "",
            }
        )

        for_each_workhours_query_do_end = rail.EmptyOperator(
            task_id='for_each_workhours_query_do_end'
        )

        for_each_distinct_data_timesheets_do_end = rail.EmptyOperator(
            task_id='for_each_distinct_data_timesheets_do_end'
        )

        query_othours = rail.QueryCollectionOperator(
            task_id='query_othours',
            query="""SELECT datapertimesheet.empid,datapertimesheet.paycode,datapertimesheet.activitycode,datapertimesheet.hours,ROUND(SUM(datapertimesheet.amount),2) AS amount,datapertimesheet.payrate FROM datapertimesheet WHERE NULLIF(amount,'') IS NOT NULL AND datapertimesheet.hourstype == 'otmeal' AND datapertimesheet.paycode IS NOT NULL AND datapertimesheet.payrate IS NOT NULL AND datapertimesheet.paycode != 'None' AND datapertimesheet.payrate != 'None' AND datapertimesheet.paycode != 'nil' AND datapertimesheet.payrate != 'nil'""",
        )

        for_each_othours_query_do = rail.ForEachOperator(
            task_id='for_each_othours_query_do',
            items="{{result('query_othours')}}",
            start_task='if_amount_present',
            end_task='for_each_othours_query_do_end'
        )

        if_amount_present = rail.IfOperator(
            task_id='if_amount_present',
            test=lambda: float(rail.result('for_each_othours_query_do')[
                               'amount']) > 0 if rail.result('for_each_othours_query_do')['amount'] else False,
            yes_task="log_decimal_places",
            no_task='for_each_othours_query_do_end'
        )

        log_decimal_places = rail.PythonOperator(
            task_id='log_decimal_places',
            python_callable=lambda: ((rail.result('for_each_othours_query_do')['amount']) + "0") if len(str(rail.result(
                'for_each_othours_query_do')['amount']).split('.')[1]) == 1 else rail.result('for_each_othours_query_do')['amount']
        )

        add_entry2 = rail.WriteLogOperator(
            task_id='add_entry2',
            log="{{dag_run.conf.custom_paychex_lookuptable}}",
            severity='',
            message='na',
            properties=lambda dag_run: {
                'jobid': dag_run.conf['jobid'],
                'empid': rail.result('for_each_othours_query_do')['empid'],
                'paycode': rail.result('for_each_othours_query_do')['paycode'] if rail.result('for_each_othours_query_do')['paycode'] else "",
                'activitycode': "",
                'sumofhours': "",
                'amount': rail.result('log_decimal_places'),
                'payrate': "",
                'childjobid': rail.render_template("{{ dag_run_ecid() }}"),


            },
        )

        accumulate_amount_list = rail.SetVariableOperator(
            task_id='accumulate_amount_list',
            name="{{result('declare_list').name}}",
            append=True,
            value=lambda: {
                'empid':  rail.result('for_each_othours_query_do')['empid'],
                'paycode': rail.result('for_each_othours_query_do')['paycode'] if rail.result('for_each_othours_query_do')['paycode'] else "",
                'activitycode': "",
                'sumofhours': "",
                'amount': rail.result('log_decimal_places'),
                'payrate': "",
            }
        )

        for_each_othours_query_do_end = rail.EmptyOperator(
            task_id='for_each_othours_query_do_end'
        )

        query_distinct_timeoff_timesheets = rail.QueryCollectionOperator(
            task_id='query_distinct_timeoff_timesheets',
            query="""SELECT DISTINCT datapertimesheet.paycode,datapertimesheet.payrate FROM datapertimesheet WHERE datapertimesheet.hourstype == 'timeoffhours' AND datapertimesheet.paycode IS NOT NULL AND datapertimesheet.payrate IS NOT NULL AND datapertimesheet.paycode != 'None' AND datapertimesheet.payrate != 'None'""",
        )

        for_each_distinct_timeoff_timesheets_do = rail.ForEachOperator(
            task_id='for_each_distinct_timeoff_timesheets_do',
            items="{{result('query_distinct_timeoff_timesheets')}}",
            start_task='query_timeoff_hours',
            end_task='for_each_distinct_timeoff_timesheets_do_end'
        )

        query_timeoff_hours = rail.QueryCollectionOperator(
            task_id='query_timeoff_hours',
            query="""SELECT datapertimesheet.empid,datapertimesheet.paycode,datapertimesheet.activitycode,ROUND(SUM(datapertimesheet.hours), 2) AS sumhours,datapertimesheet.amount,datapertimesheet.payrate FROM datapertimesheet WHERE NULLIF(hours,'') IS NOT NULL AND datapertimesheet.hourstype == 'timeoffhours' AND datapertimesheet.paycode == '{{result('for_each_distinct_timeoff_timesheets_do').paycode}}' AND datapertimesheet.payrate == '{{result('for_each_distinct_timeoff_timesheets_do').payrate}}' AND datapertimesheet.paycode != 'nil' AND datapertimesheet.payrate != 'nil'""",
        )

        for_each_timeoff_hours_do = rail.ForEachOperator(
            task_id='for_each_timeoff_hours_do',
            items=lambda: rail.load_all_records(
                rail.result('query_timeoff_hours')),
            start_task='if_sumofhours_present',
            end_task='for_each_timeoff_hours_do_end'
        )

        if_sumofhours_present = rail.IfOperator(
            task_id='if_sumofhours_present',
            test=lambda: float(rail.result('for_each_timeoff_hours_do')[
                               'sumhours']) > 0 if rail.result('for_each_timeoff_hours_do')['sumhours'] else False,
            yes_task="decimal_places",
            no_task='for_each_timeoff_hours_do_end'
        )

        decimal_places = rail.PythonOperator(
            task_id='decimal_places',
            python_callable=lambda: ((rail.result('for_each_timeoff_hours_do')['sumhours']) + "0") if len(str(rail.result(
                'for_each_timeoff_hours_do')['sumhours']).split('.')[1]) == 1 else rail.result('for_each_timeoff_hours_do')['sumhours']
        )

        add_entry3 = rail.WriteLogOperator(
            task_id='add_entry3',
            log="{{dag_run.conf.custom_paychex_lookuptable}}",
            severity='',
            message='na',
            properties=lambda dag_run: {
                'jobid': dag_run.conf['jobid'],
                'empid': rail.result('for_each_timeoff_hours_do')['empid'],
                'paycode': rail.result('for_each_timeoff_hours_do')['paycode'] if rail.result('for_each_timeoff_hours_do')['paycode'] else "",
                'activitycode': "",
                'sumofhours': rail.result('decimal_places'),
                'amount': "",
                'payrate': rail.result('for_each_timeoff_hours_do')['payrate'] if rail.result('for_each_timeoff_hours_do')['payrate'] else "",
                'childjobid': rail.render_template("{{ dag_run_ecid() }}"),
            },
        )

        accumulate_sumofhours_list = rail.SetVariableOperator(
            task_id='accumulate_sumofhours_list',
            name="{{result('declare_list').name}}",
            append=True,
            value=lambda: {
                'empid': rail.result('for_each_timeoff_hours_do')['empid'],
                'paycode': rail.result('for_each_timeoff_hours_do')['paycode'] if rail.result('for_each_timeoff_hours_do')['paycode'] else "",
                'activitycode': "",
                'sumofhours': rail.result('decimal_places'),
                'amount': "",
                'payrate': rail.result('for_each_timeoff_hours_do')['payrate'] if rail.result('for_each_timeoff_hours_do')['payrate'] else "",
            }
        )

        for_each_timeoff_hours_do_end = rail.EmptyOperator(
            task_id='for_each_timeoff_hours_do_end'
        )

        for_each_distinct_timeoff_timesheets_do_end = rail.EmptyOperator(
            task_id='for_each_distinct_timeoff_timesheets_do_end'
        )

        if_islastitem_present = rail.IfOperator(
            task_id='if_islastitem_present',
            test="{{dag_run.conf.Islastitem == 'yes'}}",
            yes_task="search_entries_in_log_table",
            no_task='log_to_sumo'
        )

        search_entries_in_log_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_log_table',
            log="{{dag_run.conf.custom_paychex_lookuptable}}",
            properties={
                'jobid': "{{dag_run.conf.jobid}}",
            }
        )

        if_entry_has_data_present = rail.IfOperator(
            task_id='if_entry_has_data_present',
            test="{{result('search_entries_in_log_table','length') > 0 }}",
            yes_task="compose_csv",
            no_task="if_entry_has_data_not_present",
        )

        if_entry_has_data_not_present = rail.IfOperator(
            task_id='if_entry_has_data_not_present',
            test="{{result('search_entries_in_log_table','length') == 0 }}",
            yes_task="stop_job_with_error_message",
            no_task="log_to_sumo",
        )

        stop_job_with_error_message = rail.FailOperator(
            task_id='stop_job_with_error_message',
            message='Error generating logs from lookup table'
        )

        compose_csv = rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source="{{dag_run.conf.custom_paychex_lookuptable}}",
            header=None,
            delimiter=' ',
            row=lambda item: [
                item['properties']['empid'].rjust(6),
                ' '.ljust(42, ' '),
                item['properties']['paycode'].rjust(3),
                item['properties']['activitycode'].rjust(8),
                str(item['properties']['sumofhours']).rjust(
                    7) if item['properties']['sumofhours'] else ' '.rjust(6),
                ' '.rjust(11, ' '),
                str(item['properties']['amount']).rjust(
                    9) if item['properties']['amount'] else ' '.rjust(9),
                ' '.rjust(23, ' '),
                item['properties']['payrate'].rjust(1),
                ' '.rjust(11, ' '),
            ],
        )

        def remove_quotes():
            file_in_string = rail.read_artifact(rail.result('compose_csv'))
            return file_in_string.replace('"', '')

        read_gb_csv_artifact = rail.PythonOperator(
            task_id='read_gb_csv_artifact',
            python_callable=remove_quotes
        )

        write_gb_csv_file = rail.PythonOperator(
            task_id='write_gb_csv_file',
            python_callable=lambda: rail.write_artifact(
                rail.result('read_gb_csv_artifact'))
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_gb_csv_file')}}",
            output_file_name='Paychex.txt',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to="{{dag_run.conf.emailid}}",
            subject='{{ get_company_key()}} |  Paychex Export has completed {{current_time("%Y-%m-%eT%H:%M%S.%f")}} ',
            html_content="templates/emails/complete_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> query_data_per_timesheet
        query_data_per_timesheet >> declare_list >> create_datapertimesheet_collection
        create_datapertimesheet_collection >> query_distinct_data_timesheets >> for_each_distinct_data_timesheets_do
        for_each_distinct_data_timesheets_do >> query_workhours >> for_each_workhours_query_do
        for_each_workhours_query_do >> if_sumhours_present >> rail.Label(
            'Yes') >> log_get_decimal_places >> add_entry1 >> accumulate_paycode_list
        accumulate_paycode_list >> for_each_workhours_query_do_end
        if_sumhours_present >> rail.Label(
            'No') >> for_each_workhours_query_do_end
        for_each_workhours_query_do >> for_each_workhours_query_do_end >> for_each_distinct_data_timesheets_do_end
        for_each_distinct_data_timesheets_do >> for_each_distinct_data_timesheets_do_end >> query_othours
        query_othours >> for_each_othours_query_do >> if_amount_present >> rail.Label(
            'Yes') >> log_decimal_places >> add_entry2 >> accumulate_amount_list >> for_each_othours_query_do_end
        if_amount_present >> rail.Label(
            'No') >> for_each_othours_query_do_end
        for_each_othours_query_do >> for_each_othours_query_do_end >> query_distinct_timeoff_timesheets
        query_distinct_timeoff_timesheets >> for_each_distinct_timeoff_timesheets_do
        for_each_distinct_timeoff_timesheets_do >> query_timeoff_hours >> for_each_timeoff_hours_do
        for_each_timeoff_hours_do >> if_sumofhours_present
        if_sumofhours_present >> rail.Label(
            'Yes') >> decimal_places >> add_entry3 >> accumulate_sumofhours_list
        accumulate_sumofhours_list >> for_each_timeoff_hours_do_end
        if_sumofhours_present >> rail.Label(
            'No') >> for_each_timeoff_hours_do_end >> for_each_distinct_timeoff_timesheets_do_end
        for_each_timeoff_hours_do >> for_each_timeoff_hours_do_end >> for_each_distinct_timeoff_timesheets_do_end
        for_each_distinct_timeoff_timesheets_do_end >> if_islastitem_present
        if_islastitem_present >> rail.Label(
            'Yes') >> search_entries_in_log_table >> if_entry_has_data_present
        if_entry_has_data_present >> rail.Label(
            'Yes') >> compose_csv >> read_gb_csv_artifact >> write_gb_csv_file
        write_gb_csv_file >> generate_download_link >> send_import_complete_email
        send_import_complete_email >> log_to_sumo
        if_entry_has_data_present >> rail.Label(
            'No') >> if_entry_has_data_not_present
        if_entry_has_data_not_present >> rail.Label(
            'Yes') >> stop_job_with_error_message >> log_to_sumo
        if_entry_has_data_not_present >> rail.Label('Yes') >> log_to_sumo
        if_islastitem_present >> rail.Label(
            'No') >> log_to_sumo
        for_each_distinct_timeoff_timesheets_do >> for_each_distinct_timeoff_timesheets_do_end

        return dag


rail.for_each_instance(create_dag)
