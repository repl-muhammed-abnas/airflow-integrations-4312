from datetime import timedelta
import rail
from crjg3.payroll_export.utils import python_callable
from airflow.models import Variable

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f"crj_custom_payroll_report_extract_data_child_{config.instance}",
        description=f"CRJ_Custom Payroll Report V2.0 child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_data_related_to_weeks'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_data_related_to_weeks',
            end_task='finish',
        )

        get_data_related_to_weeks = rail.QueryCollectionOperator(
            task_id='get_data_related_to_weeks',
            query="""SELECT projectname as project,taskname as task,taskcode,timesheetperiod,replace,hoursworked,worklocation, \
                labormetrics,weekentrydate,normalizationrequired,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND loginname == :loginname""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='datarelatedtoweeks'
        )

        is_employeecategory_equals_non_exempt = rail.IfOperator(
            task_id='is_employeecategory_equals_non_exempt',
            test=lambda dag_run: dag_run.conf['employeecategory'] == 'Non-Exempt',
            yes_task="get_data_related_to_distinct_weeks",
            no_task="get_total_hours_for_distinct_weeks",
        )

        get_data_related_to_distinct_weeks = rail.QueryCollectionOperator(
            task_id='get_data_related_to_distinct_weeks',
            query="""SELECT DISTINCT taskcode,replace,worklocation, \
                labormetrics,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND loginname == :loginname""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='datarelatedtodistinctweeks'
        )

        add_final_data_per_week_lookup_table_27 = rail.WriteLogOperator(
            task_id='add_final_data_per_week_lookup_table_27',
            log="{{dag_run.conf.lookup_table}}",
            items="{{ result('get_data_related_to_distinct_weeks') | load_all_records() | to_json }}",
            message="na",
            severity="Success",
            properties=lambda item, dag_run: python_callable.get_final_data_per_week_data(
                item, dag_run.conf['week'])
        )

        get_total_hours_for_distinct_weeks = rail.QueryCollectionOperator(
            task_id='get_total_hours_for_distinct_weeks',
            query="""SELECT COALESCE(SUM(hoursworked), 0.0) as totalhoursinweek FROM payrolldata WHERE weekentrydate == :week \
                    AND loginname == :loginname""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='totalhrsfordistinctweeks'
        )

        is_totalhrsfordistinctweeks_greater_than_40 = rail.IfOperator(
            task_id='is_totalhrsfordistinctweeks_greater_than_40',
            test=lambda: float(rail.load_all_records(rail.result(
                "get_total_hours_for_distinct_weeks"))[0]['totalhoursinweek']) > 40,
            yes_task="if_normalizationrequired",
            no_task="get_data_related_to_distinct_weeks_else",
        )

        if_normalizationrequired = rail.IfOperator(
            task_id='if_normalizationrequired',
            test=python_callable.get_if_normalizationrequired,
            yes_task="total_data_for_distinct_week_with_normalization",
            no_task="total_data_for_distinct_week_with_excluded_timetypes_else",
        )

        total_data_for_distinct_week_with_normalization = rail.QueryCollectionOperator(
            task_id='total_data_for_distinct_week_with_normalization',
            query="""SELECT COALESCE(SUM(hoursworked), 0.0) as totalhoursinweek FROM payrolldata WHERE weekentrydate == :week \
                    AND normalizationrequired == 'Yes' AND loginname == :loginname AND taskcode != '02 Floating Holiday' \
                    AND taskcode != '04 Sick' AND taskcode != '7H Holiday' AND taskcode != '03 Vacation'""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='totaldatafordistinctweekwithnormalization'
        )

        total_data_for_distinct_week_with_excluded_timetypes = rail.QueryCollectionOperator(
            task_id='total_data_for_distinct_week_with_excluded_timetypes',
            query="""SELECT COALESCE(SUM(hoursworked), 0.0) as totalhoursinweek FROM payrolldata WHERE weekentrydate == :week \
                    AND loginname == :loginname AND (taskcode == '02 Floating Holiday' \
                    OR taskcode == '04 Sick' OR taskcode == '7H Holiday' OR taskcode == '03 Vacation')""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='totaldatafordistinctweekwithexcludedtimetypes'
        )

        data_related_to_distinct_week_with_excluded_timetypes = rail.QueryCollectionOperator(
            task_id='data_related_to_distinct_week_with_excluded_timetypes',
            query="""SELECT projectname as project,taskname as task,taskcode,timesheetperiod,replace,hoursworked,worklocation, \
                    labormetrics,weekentrydate,normalizationrequired,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND loginname == :loginname AND (taskcode == '02 Floating Holiday' \
                    OR taskcode == '04 Sick' OR taskcode == '7H Holiday' OR taskcode == '03 Vacation')""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='datarelatedtodistinctweekwithexcludedtimetypes'
        )

        add_final_data_per_week_lookup_table_37 = rail.WriteLogOperator(
            task_id='add_final_data_per_week_lookup_table_37',
            log="{{dag_run.conf.lookup_table}}",
            items="{{ result('data_related_to_distinct_week_with_excluded_timetypes') | load_all_records() | to_json }}",
            message="na",
            severity="Success",
            properties=lambda item, dag_run: python_callable.get_final_data_per_week_data_37_60(
                item, dag_run.conf['week'])
        )

        data_related_to_distinct_week_with_normalization = rail.QueryCollectionOperator(
            task_id='data_related_to_distinct_week_with_normalization',
            query="""SELECT projectname as project,taskname as task,taskcode,timesheetperiod,replace,hoursworked,worklocation, \
                    labormetrics,weekentrydate,normalizationrequired,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND normalizationrequired == 'Yes' AND loginname == :loginname AND taskcode != '02 Floating Holiday' \
                    AND taskcode != '04 Sick' AND taskcode != '7H Holiday' AND taskcode != '03 Vacation'""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='datarelatedtodistinctweekwithnormalization'
        )

        exhaustive_normalization = rail.PythonOperator(
            task_id="exhaustive_normalization",
            python_callable=python_callable.exhaustive_normalization
        )

        final_data_related_to_distinct_week_with_normalization = rail.QueryCollectionOperator(
            task_id='final_data_related_to_distinct_week_with_normalization',
            query="""SELECT DISTINCT taskcode,replace,worklocation, \
                    labormetrics,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND normalizationrequired == 'Yes' AND loginname == :loginname AND taskcode != '02 Floating Holiday' \
                    AND taskcode != '04 Sick' AND taskcode != '7H Holiday' AND taskcode != '03 Vacation'""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='finaldatarelatedtodistinctweekwithnormalization'
        )

        exhaustive_normalization_per_project = rail.PythonOperator(
            task_id="exhaustive_normalization_per_project",
            python_callable=python_callable.exhaustive_normalization_per_project
        )

        total_data_for_distinct_week_without_normalization = rail.QueryCollectionOperator(
            task_id='total_data_for_distinct_week_without_normalization',
            query="""SELECT COALESCE(SUM(hoursworked), 0.0) as totalhoursinweek FROM payrolldata WHERE weekentrydate == :week \
                    AND normalizationrequired == 'No' AND loginname == :loginname AND taskcode != '02 Floating Holiday' \
                    AND taskcode != '04 Sick' AND taskcode != '7H Holiday' AND taskcode != '03 Vacation'""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='totaldatafordistinctweekwithoutnormalization'
        )

        data_related_to_distinct_week_without_normalization = rail.QueryCollectionOperator(
            task_id='data_related_to_distinct_week_without_normalization',
            query="""SELECT projectname as project,taskname as task,taskcode,timesheetperiod,replace,hoursworked,worklocation, \
                    labormetrics,weekentrydate,normalizationrequired,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND normalizationrequired == 'No' AND loginname == :loginname AND taskcode != '02 Floating Holiday' \
                    AND taskcode != '04 Sick' AND taskcode != '7H Holiday' AND taskcode != '03 Vacation'""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='datarelatedtodistinctweekwithoutnormalization'
        )

        add_final_data_per_week_lookup_table_45 = rail.WriteLogOperator(
            task_id='add_final_data_per_week_lookup_table_45',
            log="{{dag_run.conf.lookup_table}}",
            items="{{ result('final_data_related_to_distinct_week_with_normalization') | load_all_records() | to_json }}",
            message="na",
            severity="Success",
            properties=lambda item, dag_run: python_callable.get_final_data_per_week_data_45(
                item, dag_run.conf['week'])
        )

        if_totalhoursinweek_is_greater_than_40 = rail.IfOperator(
            task_id='if_totalhoursinweek_is_greater_than_40',
            test=python_callable.if_totalhoursinweek_is_greater_than_40,
            yes_task="normal_normalization",
            no_task="final_data_related_to_distinct_week_without_normalization_else",
        )

        normal_normalization = rail.PythonOperator(
            task_id="normal_normalization",
            python_callable=python_callable.normal_normalization
        )

        normal_normalization_per_project = rail.PythonOperator(
            task_id="normal_normalization_per_project",
            python_callable=python_callable.normal_normalization_per_project
        )

        final_data_related_to_distinct_week_without_normalization = rail.QueryCollectionOperator(
            task_id='final_data_related_to_distinct_week_without_normalization',
            query="""SELECT DISTINCT taskcode,replace,worklocation, \
                    labormetrics,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND normalizationrequired == 'No' AND loginname == :loginname AND taskcode != '02 Floating Holiday' \
                    AND taskcode != '04 Sick' AND taskcode != '7H Holiday' AND taskcode != '03 Vacation'""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='finaldatarelatedtodistinctweekwithoutnormalization'
        )

        add_final_data_per_week_lookup_table_51 = rail.WriteLogOperator(
            task_id='add_final_data_per_week_lookup_table_51',
            log="{{dag_run.conf.lookup_table}}",
            items="{{ result('final_data_related_to_distinct_week_without_normalization') | load_all_records() | to_json }}",
            message="na",
            severity="Success",
            properties=lambda item, dag_run: python_callable.add_final_data_per_week_lookup_table_51(
                item, dag_run.conf['week'])
        )

        final_data_related_to_distinct_week_without_normalization_else = rail.QueryCollectionOperator(
            task_id='final_data_related_to_distinct_week_without_normalization_else',
            query="""SELECT DISTINCT taskcode,replace,worklocation, \
                    labormetrics,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND normalizationrequired == 'No' AND loginname == :loginname AND taskcode != '02 Floating Holiday' \
                    AND taskcode != '04 Sick' AND taskcode != '7H Holiday' AND taskcode != '03 Vacation'""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='datarelatedtodistinctweekwithoutnormalizationelse'
        )

        add_final_data_per_week_lookup_table_55 = rail.WriteLogOperator(
            task_id='add_final_data_per_week_lookup_table_55',
            log="{{dag_run.conf.lookup_table}}",
            items="{{ result('final_data_related_to_distinct_week_without_normalization_else') | load_all_records() | to_json }}",
            message="na",
            severity="Success",
            properties=lambda item, dag_run: python_callable.add_final_data_per_week_lookup_table_55(
                item, dag_run.conf['week'])
        )

        total_data_for_distinct_week_with_excluded_timetypes_else = rail.QueryCollectionOperator(
            task_id='total_data_for_distinct_week_with_excluded_timetypes_else',
            query="""SELECT COALESCE(SUM(hoursworked), 0.0) as totalhoursinweek FROM payrolldata WHERE weekentrydate == :week \
                    AND loginname == :loginname AND (taskcode == '02 Floating Holiday' \
                    OR taskcode == '04 Sick' OR taskcode == '7H Holiday' OR taskcode == '03 Vacation')""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='totaldatafordistinctweekwithexcludedtimetypeselse'
        )

        data_related_to_distinct_week_with_excluded_timetypes_else = rail.QueryCollectionOperator(
            task_id='data_related_to_distinct_week_with_excluded_timetypes_else',
            query="""SELECT projectname as project,taskname as task,taskcode,timesheetperiod,replace,hoursworked,worklocation, \
                    labormetrics,weekentrydate,normalizationrequired,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND loginname == :loginname AND (taskcode == '02 Floating Holiday' \
                    OR taskcode == '04 Sick' OR taskcode == '7H Holiday' OR taskcode == '03 Vacation')""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='datarelatedtodistinctweekwithexcludedtimetypeselse'
        )

        add_final_data_per_week_lookup_table_60 = rail.WriteLogOperator(
            task_id='add_final_data_per_week_lookup_table_60',
            log="{{dag_run.conf.lookup_table}}",
            items="{{ result('data_related_to_distinct_week_with_excluded_timetypes_else') | load_all_records() | to_json }}",
            message="na",
            severity="Success",
            properties=lambda item, dag_run: python_callable.get_final_data_per_week_data_37_60(
                item, dag_run.conf['week'])
        )

        normal_normalization_61 = rail.PythonOperator(
            task_id="normal_normalization_61",
            python_callable=python_callable.normal_normalization_61
        )

        final_data_related_to_distinct_week_without_normalization_else_62 = rail.QueryCollectionOperator(
            task_id='final_data_related_to_distinct_week_without_normalization_else_62',
            query="""SELECT DISTINCT taskcode,replace,worklocation, \
                    labormetrics,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND loginname == :loginname AND loginname == :loginname AND taskcode != '02 Floating Holiday' \
                    AND taskcode != '04 Sick' AND taskcode != '7H Holiday' AND taskcode != '03 Vacation'""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='finaldatarelatedtodistinctweekwithoutnormalizationelse'
        )

        normal_normalization_per_project_63 = rail.PythonOperator(
            task_id="normal_normalization_per_project_63",
            python_callable=python_callable.normal_normalization_per_project_63
        )

        add_final_data_per_week_lookup_table_65 = rail.WriteLogOperator(
            task_id='add_final_data_per_week_lookup_table_65',
            log="{{dag_run.conf.lookup_table}}",
            items="{{ result('final_data_related_to_distinct_week_without_normalization_else_62') | load_all_records() | to_json }}",
            message="na",
            severity="Success",
            properties=lambda item, dag_run: python_callable.add_final_data_per_week_lookup_table_65(
                item, dag_run.conf['week'])
        )

        get_data_related_to_distinct_weeks_else = rail.QueryCollectionOperator(
            task_id='get_data_related_to_distinct_weeks_else',
            query="""SELECT DISTINCT taskcode,replace,worklocation, \
                labormetrics,employeeno FROM payrolldata WHERE weekentrydate == :week \
                    AND loginname == :loginname""",
            query_params={
                "week": "{{dag_run.conf.week}}",
                "loginname": "{{dag_run.conf.loginname}}"
            },
            name='datarelatedtodistinctweekselse'
        )

        add_final_data_per_week_lookup_table_else = rail.WriteLogOperator(
            task_id='add_final_data_per_week_lookup_table_else',
            log="{{dag_run.conf.lookup_table}}",
            items="{{ result('get_data_related_to_distinct_weeks_else') | load_all_records() | to_json }}",
            message="na",
            severity="Success",
            properties=lambda item, dag_run: python_callable.get_final_data_per_week_data(
                item, dag_run.conf['week'])
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_data_related_to_weeks

        get_data_related_to_weeks >> is_employeecategory_equals_non_exempt
        is_employeecategory_equals_non_exempt >> rail.Label(
            "Yes") >> get_data_related_to_distinct_weeks >> add_final_data_per_week_lookup_table_27 >> finish
        is_employeecategory_equals_non_exempt >> rail.Label(
            "No") >> get_total_hours_for_distinct_weeks
        get_total_hours_for_distinct_weeks >> is_totalhrsfordistinctweeks_greater_than_40
        is_totalhrsfordistinctweeks_greater_than_40 >> rail.Label(
            "Yes") >> if_normalizationrequired
        is_totalhrsfordistinctweeks_greater_than_40 >> rail.Label(
            "No") >> get_data_related_to_distinct_weeks_else
        if_normalizationrequired >> rail.Label(
            "Yes") >> total_data_for_distinct_week_with_normalization >> total_data_for_distinct_week_with_excluded_timetypes
        if_normalizationrequired >> rail.Label(
            "No") >> total_data_for_distinct_week_with_excluded_timetypes_else
        total_data_for_distinct_week_with_excluded_timetypes >> data_related_to_distinct_week_with_excluded_timetypes >> \
            add_final_data_per_week_lookup_table_37 >> data_related_to_distinct_week_with_normalization
        data_related_to_distinct_week_with_normalization >> \
            exhaustive_normalization >> final_data_related_to_distinct_week_with_normalization >> exhaustive_normalization_per_project >> \
            total_data_for_distinct_week_without_normalization >> data_related_to_distinct_week_without_normalization >> \
            add_final_data_per_week_lookup_table_45 >> if_totalhoursinweek_is_greater_than_40
        if_totalhoursinweek_is_greater_than_40 >> rail.Label(
            "Yes") >> normal_normalization >> normal_normalization_per_project
        if_totalhoursinweek_is_greater_than_40 >> rail.Label(
            "No") >> final_data_related_to_distinct_week_without_normalization_else
        normal_normalization_per_project >> final_data_related_to_distinct_week_without_normalization >> add_final_data_per_week_lookup_table_51
        add_final_data_per_week_lookup_table_51 >> finish
        final_data_related_to_distinct_week_without_normalization_else >> \
            add_final_data_per_week_lookup_table_55 >> finish
        total_data_for_distinct_week_with_excluded_timetypes_else >> data_related_to_distinct_week_with_excluded_timetypes_else
        data_related_to_distinct_week_with_excluded_timetypes_else >> add_final_data_per_week_lookup_table_60 >> normal_normalization_61
        normal_normalization_61 >> final_data_related_to_distinct_week_without_normalization_else_62 >> \
            normal_normalization_per_project_63 >> add_final_data_per_week_lookup_table_65 >> finish
        get_data_related_to_distinct_weeks_else >> add_final_data_per_week_lookup_table_else >> finish

    return dag


rail.for_each_instance(create_child_dag)
