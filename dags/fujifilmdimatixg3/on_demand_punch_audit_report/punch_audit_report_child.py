import rail
from fujifilmdimatixg3.on_demand_punch_audit_report.utils import python_callable


def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f"fujifilmdimatixg3_on_demand_punch_audit_report_child_{config.instance}",
        description=f"FUJIFILMDimatixG3 punch audit report child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        check_if_punch_entry_policy_name_present = rail.IfOperator(
            task_id="check_if_punch_entry_policy_name_present",
            test='''{{ dag_run.conf.punch_entry_policy_name | is_truthy }}''',
            yes_task='get_time_punch_audit_details_for_user_and_date_range',
            no_task="finish",
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        get_time_punch_audit_details_for_user_and_date_range = rail.RepliconServiceOperator(
            task_id='get_time_punch_audit_details_for_user_and_date_range',
            endpoint='/services/TimePunchService1.svc/GetTimePunchAuditDetailsForUserAndDateRange2',
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['user_uri'],
                },
                "dateRange": {
                    "startDate": dag_run.conf['start_date'],
                    "endDate": dag_run.conf['end_date']
                }
            },
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/UserService1.svc/GetUserDetails',
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        for_each_time_punch_audit_details = rail.ForEachOperator(
            task_id="for_each_time_punch_audit_details",
            items="{{ result('get_time_punch_audit_details_for_user_and_date_range') | to_json }}",
            start_task="check_length_and_user_uri",
            end_task="punch_audit_details_for_each_end"
        )

        punch_audit_details_for_each_end = rail.EmptyOperator(
            task_id="punch_audit_details_for_each_end"
        )

        check_length_and_user_uri = rail.IfOperator(
            task_id="check_length_and_user_uri",
            test=python_callable.check_if_length_is_1_and_actual_user_uri_does_not_equal_new_user_uri,
            yes_task='log_success_entries',
            no_task="check_length_if_greater",
        )

        log_success_entries = rail.WriteLogOperator(
            task_id='log_success_entries',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="Success",
            properties=lambda: python_callable.get_writelog_properties_if_length_is_1_and_actual_user_uri_does_not_equal_new_user_uri
        )

        check_length_if_greater = rail.IfOperator(
            task_id="check_length_if_greater",
            test=python_callable.check_if_length_is_greater,
            yes_task='for_each_audit_records',
            no_task="punch_audit_details_for_each_end",
        )

        for_each_audit_records = rail.ForEachOperator(
            task_id="for_each_audit_records",
            items=lambda: rail.result(
                'for_each_time_punch_audit_details').get('auditRecords'),
            start_task="check_if_edited",
            end_task="punch_audit_for_each_end"
        )
        check_if_edited = rail.IfOperator(
            task_id="check_if_edited",
            test=python_callable.check_if_audit_record_edited,
            yes_task='log_success_entries_if_edited',
            no_task="check_if_deleted",
        )

        log_success_entries_if_edited = rail.WriteLogOperator(
            task_id='log_success_entries_if_edited',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="Success",
            properties=lambda: python_callable.get_writelog_properties_if_audit_record_edited
        )

        punch_audit_for_each_end = rail.EmptyOperator(
            task_id="punch_audit_for_each_end"
        )

        check_if_deleted = rail.IfOperator(
            task_id="check_if_deleted",
            test=python_callable.check_if_audit_record_deleted,
            yes_task='log_success_entries_if_deleted',
            no_task="punch_audit_for_each_end",
        )

        log_success_entries_if_deleted = rail.WriteLogOperator(
            task_id='log_success_entries_if_deleted',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="Success",
            properties=lambda: python_callable.get_writelog_properties_if_audit_record_deleted
        )

        check_if_punch_entry_policy_name_present >> rail.Label("Yes") >> get_time_punch_audit_details_for_user_and_date_range >> get_user_details >> \
            for_each_time_punch_audit_details >> check_length_and_user_uri
        check_if_punch_entry_policy_name_present >> rail.Label("No") >> finish
        check_length_and_user_uri >> rail.Label(
            "Yes") >> log_success_entries >> punch_audit_details_for_each_end
        check_length_and_user_uri >> rail.Label(
            "No") >> check_length_if_greater
        check_length_if_greater >> rail.Label("Yes") >> for_each_audit_records >> check_if_edited >> rail.Label("Yes") >> \
            log_success_entries_if_edited >> punch_audit_for_each_end
        check_if_edited >> rail.Label("No") >> check_if_deleted
        check_length_if_greater >> rail.Label(
            "No") >> punch_audit_details_for_each_end
        check_if_deleted >> rail.Label(
            "Yes") >> log_success_entries_if_deleted >> punch_audit_for_each_end
        check_if_deleted >> rail.Label("No") >> punch_audit_for_each_end
        for_each_time_punch_audit_details >> punch_audit_details_for_each_end
        punch_audit_for_each_end >> punch_audit_details_for_each_end
        for_each_audit_records >> punch_audit_for_each_end
        punch_audit_details_for_each_end >> finish
    return dag


rail.for_each_instance(create_child_dag)
