import rail
from dxctechnology.psa_planned_leave.utils import request_payload


def report_batch(config):
    with rail.TaskGroup(group_id='generate_report_batch', prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.psa_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_psa_report',
            report_params=request_payload.get_filter_params,
            replicon_conn_id=config.replicon_conn_id,
        )

    report_has_data = rail.IfOperator(
        task_id="report_has_data",
        test=lambda: rail.result('run_psa_report.get_report_result')[
            'reportGenerationResults'][0]['payload'] != 'No Data\r\n',
        yes_task='report_has_expected_columns',
        no_task='empty_export_mail'
    )

    expected_report_columns = "EmployeeNumber,LeaveDate,LeaveHours,LeaveTypeName,TimeOffTypeUri"

    report_has_expected_columns = rail.IfOperator(
        task_id="report_has_expected_columns",
        #pylint: disable=consider-using-f-string
        test="{{ result('run_psa_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
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
        subject='{{ get_company_key() }} | PSA Planned Leave Export - No records to export - {{ current_time_in_specified_tz() }}',
        html_content="templates/emails/empty_export.html"
    )

    load_report_data = rail.LoadCSVFileOperator(
        task_id='load_report_data',
        document="{{ result('run_psa_report.get_report_result').reportGenerationResults[0].payload }}",
    )

    create_report_collection = rail.CreateCollectionOperator(
        task_id='create_report_collection',
        name='reportdatacollection',
        source="{{ result('load_report_data') }}",
    )

    get_report_details >> run_report_group_entry
    run_report_group_exit >> report_has_data >> rail.Label(
        "Yes") >> report_has_expected_columns >> rail.Label('Yes') >> load_report_data >> create_report_collection
    report_has_expected_columns >> rail.Label(
        'No') >> fail_invalid_report_columns
    report_has_data >> rail.Label("No") >> empty_export_mail

    return get_report_details, fail_invalid_report_columns, empty_export_mail, create_report_collection
