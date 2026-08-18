import rail
from dxctechnology.wf39_psa_planned_leave.utils import request_payload


def report_batch(config):
    with rail.TaskGroup(group_id='generate_report_batch', prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.wf39_psa_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_wf39_psa_report',
            report_params=request_payload.get_filter_params,
            replicon_conn_id=config.replicon_conn_id,
        )

    is_report_failed = rail.IfOperator(
        task_id='is_report_failed',
        test="{{ result('run_wf39_psa_report.get_report_result').reportGenerationResults[0].error | is_truthy }}",
        yes_task='fail_report_generation',
        no_task='report_has_data'
    )

    fail_report_generation = rail.FailOperator(
        task_id='fail_report_generation',
        message="{{ result('run_wf39_psa_report.get_report_result').reportGenerationResults[0].error }}"
    )

    report_has_data = rail.IfOperator(
        task_id="report_has_data",
        test="{{ result('run_wf39_psa_report.get_report_result', 'has_data') }}",
        yes_task='report_has_expected_columns',
        no_task='empty_export_mail'
    )

    expected_report_columns = "EmployeeNumber,LeaveDate,LeaveHours,LeaveTypeName,TimeOffTypeUri,TimeOffTypeDescription,HomeERP"

    report_has_expected_columns = rail.IfOperator(
        task_id="report_has_expected_columns",
        #pylint: disable=consider-using-f-string
        test="{{ result('run_wf39_psa_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
        yes_task='load_report_data',
        no_task='fail_invalid_report_columns',
    )

    fail_invalid_report_columns = rail.FailOperator(
        task_id="fail_invalid_report_columns",
        message="Base report columns do not match",
    )

    empty_export_mail = rail.EmailOperator(
        task_id='empty_export_mail',
        to=config.tenant_email,
        bcc=config.internal_email,
        subject='{{ get_company_key() }} | WF39 PSA Planned Leave Export - No records to export - {{ current_time_in_specified_tz() }}',
        html_content="templates/emails/empty_export.html"
    )

    load_report_data = rail.LoadCSVFileOperator(
        task_id='load_report_data',
        document="{{ result('run_wf39_psa_report.get_report_result').reportGenerationResults[0].payload }}",
    )

    create_report_collection = rail.CreateCollectionOperator(
        task_id='create_report_collection',
        name='reportdatacollection',
        source="{{ result('load_report_data') }}",
    )

    get_report_details >> run_report_group_entry
    run_report_group_exit >> is_report_failed >> rail.Label('Yes') >> fail_report_generation
    is_report_failed >> rail.Label('No') >> report_has_data
    report_has_data >> rail.Label(
        "Yes") >> report_has_expected_columns >> rail.Label('Yes') >> load_report_data >> create_report_collection
    report_has_expected_columns >> rail.Label(
        'No') >> fail_invalid_report_columns
    report_has_data >> rail.Label("No") >> empty_export_mail

    return get_report_details, fail_report_generation, fail_invalid_report_columns, empty_export_mail, create_report_collection
