from datetime import timedelta
from pendulum import datetime
import rail
from cbrefcg.user_rate_assignment_new_hire.utils import response_filter

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'cbrefcg_newhire_master_{config.instance}',
        description=f'cbrefcg_Newhire_Master {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2022, 1, 1, tz=config.pacific_timezone),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        get_webhook_log = rail.CreateLogOperator(
            task_id="get_webhook_log",
            tenant_wide_name="cbre_webhook_data",
            existing_log_mode="truncate",
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test=lambda: bool(rail.load_all_records(rail.result('get_webhook_log', 'truncated_data'))),
            yes_task='write_csv_user_data',
            no_task='delete_this_dagrun'
        )

        write_csv_user_data = rail.WriteCSVFileOperator(
            task_id="write_csv_user_data",
            source="{{ result('get_webhook_log', 'truncated_data') }}",
            header=[
                'execution-correlation-id',
                'useruri',
                'loginname',
                'eventdatetime',
                'eventdate',
                'eventtype'],
            row=['{{ item.ecid }}', '{{ item.properties.useruri }}', '{{ item.properties.loginname }}', '{{ item.properties.eventdatetime }}',
                 '{{ item.properties.eventdate }}', '{{ item.properties.eventtype }}'],
        )

        delete_this_dagrun= rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun',
        )

        create_users_collection = rail.CreateCollectionOperator(
            task_id='create_users_collection',
            source="{{ result('write_csv_user_data') }}",
        )

        query_users_data = rail.QueryCollectionOperator(
            task_id='query_users_data',
            query='SELECT * FROM create_users_collection',
        )

        user_group_details = rail.SetVariableOperator(
            task_id='user_group_details',
            append=False,
            name='usergroupdetails',
            value=[]
        )

        for_each_entries= rail.ForEachOperator(
            task_id='for_each_entries',
            items="{{ result('query_users_data') }}",
            start_task = 'get_effective_user_group_membership',
            end_task = 'for_each_entries_end'
        )

        get_effective_user_group_membership= rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ result('for_each_entries').useruri }}",
                "dateRange": None
            }
        )

        add_items_to_user_group_details=rail.SetVariableOperator(
            task_id='add_items_to_user_group_details',
            append=True,
            name='{{ result("user_group_details").name }}',
            value=response_filter.add_user_group_details
        )

        for_each_entries_end= rail.EmptyOperator(
            task_id='for_each_entries_end',
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        load_projects_data_from_report = rail.run_report2(
            group_id='load_projects_data_from_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )


        has_report_data = rail.IfOperator(
            task_id='has_report_data',
            test='{{ result("load_projects_data_from_report.get_report_result", "has_data") }}',
            yes_task='has_report_have_expected_columns',
            no_task='finish'
        )

        expected_report_columns = "Project Name,ProjectUri,Location,Department,All NP Analysts,Managing Offices,Employee Type"

        has_report_have_expected_columns= rail.IfOperator(
            task_id='has_report_have_expected_columns',
            # pylint: disable=consider-using-f-string line-too-long
            test="{{ result('load_projects_data_from_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            yes_task="is_user_loginname_present",
            no_task="fail_dag",
        )

        fail_dag= rail.FailOperator(
            task_id='fail_dag',
            message='''Base report column order does not match'''
        )

        is_user_loginname_present= rail.IfOperator(
            task_id='is_user_loginname_present',
            test="{{ result('add_items_to_user_group_details').value[0].userloginname | is_truthy }}",
            yes_task="load_report_data_to_csv",
            no_task="finish",
        )

        load_report_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_report_data_to_csv",
            document='{{ result("load_projects_data_from_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        create_active_projects_collection = rail.CreateCollectionOperator(
            task_id='create_active_projects_collection',
            source = "{{ result('load_report_data_to_csv') }}",
            name = "activeprojects",
            columns = {
                'Project Name':'projectname',
                'ProjectUri':'projecturi',
                'Location':'locationteam',
                'Department':'Department',
                'All NP Analysts':'allnpanalysts',
                'Managing Offices':'managingOffices',
                'Employee Type':'employeetype'
            }
        )

        foreach_user_group_details= rail.ForEachOperator(
            task_id='foreach_user_group_details',
            items="{{ result('add_items_to_user_group_details').value | to_json }}",
            start_task = 'query_projects_by_users_location',
            end_task = 'foreach_user_group_details_end'
        )

        query_projects_by_users_location= rail.QueryCollectionOperator(
            task_id='query_projects_by_users_location',
            query="""SELECT * FROM  activeprojects WHERE
                    activeprojects.locationteam = '{{ result('foreach_user_group_details').locationname }}' OR
                    activeprojects.Department ='{{ result('foreach_user_group_details').departmentgroupname }}' OR
                    activeprojects.allnpanalysts ='{{ result('foreach_user_group_details').costcentrename }}' OR
                    activeprojects.managingOffices ='{{ result('foreach_user_group_details').servicecentername }}' OR
                    activeprojects.employeetype ='{{ result('foreach_user_group_details').employeetypegroupname }}' """,
        )

        is_project_uri_present= rail.IfOperator(
            task_id='is_project_uri_present',
            test="{{ result('query_projects_by_users_location', 'length') > 0 }}",
            yes_task="process_new_hire_child",
            no_task="foreach_user_group_details_end",
        )

        process_new_hire_child= rail.TriggerDagRunOperator(
            task_id='process_new_hire_child',
            retries=0,
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= response_filter.child_dag_conf
        )

        foreach_user_group_details_end=rail.EmptyOperator(
            task_id='foreach_user_group_details_end',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        get_webhook_log >> has_any_data >> rail.Label(
            'No')  >> delete_this_dagrun

        has_any_data >> rail.Label(
            'Yes') >> write_csv_user_data >> create_users_collection >> query_users_data >> user_group_details >> \
                for_each_entries >> get_effective_user_group_membership >> add_items_to_user_group_details >> for_each_entries_end

        for_each_entries >> for_each_entries_end

        for_each_entries_end >> get_report_details >> load_projects_data_from_report >> has_report_data

        has_report_data >> rail.Label(
            'Yes')  >> has_report_have_expected_columns

        has_report_have_expected_columns >> rail.Label(
            'Yes')  >> is_user_loginname_present

        has_report_have_expected_columns >> rail.Label(
            'No') >> fail_dag

        is_user_loginname_present >> rail.Label(
            'Yes')  >> load_report_data_to_csv >> create_active_projects_collection >> foreach_user_group_details

        foreach_user_group_details >> query_projects_by_users_location >> is_project_uri_present

        is_project_uri_present >> rail.Label(
            'Yes')  >> process_new_hire_child >> foreach_user_group_details_end >> finish

        is_project_uri_present >> rail.Label(
            'No') >> foreach_user_group_details_end

        foreach_user_group_details >> foreach_user_group_details_end

        is_user_loginname_present >> rail.Label(
            'No') >> finish

        has_report_data >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
