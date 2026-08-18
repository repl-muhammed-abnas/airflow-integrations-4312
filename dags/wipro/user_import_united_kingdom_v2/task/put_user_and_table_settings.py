import rail
from wipro.user_import_united_kingdom_v2.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_united_kingdom_v2.utils import custom_methods, request_payload


def get_put_table_view_setting(user_uri, country, caller='supervisor'):
    with rail.TaskGroup(group_id=f'put_table_view_setting_{caller}', prefix_group_id=False) as put_table_view_setting:

        impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
            task_id=f'impersonate_and_create_interactive_session_{caller}',
            endpoint='/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession',
            data={
                "impersonatedUserUri": user_uri
            },
            response_filter=custom_methods.map_impersonate_and_create_interactive_session
        )

        put_columnsettings_for_user_timesheets = rail.RepliconServiceOperator(
            task_id=f'put_columnsettings_for_user_timesheets_{caller}',
            endpoint='/services/ListSettingsService1.svc/PutColumnSettingsForUser',
            data=request_payload.get_put_column_settings_for_user_timesheet_tab_data(
                user_uri),
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}'),
        )

        has_language = rail.IfOperator(
            task_id=f"has_language_{caller}",
            test=lambda: "language_api_value" in user_default_settings[country],
            yes_task=f"update_language_{caller}",
            no_task=f"has_date_format_{caller}"
        )

        update_language = rail.RepliconServiceOperator(
            task_id=f"update_language_{caller}",
            endpoint='/services/InternationalizationService1.svc/UpdateLanguageForUser',
            data={
                "userUri": user_uri,
                "languageUri": user_default_settings[country]["language_api_value"]
            },
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}')
        )

        has_date_format = rail.IfOperator(
            task_id=f"has_date_format_{caller}",
            test=lambda: "date_format_api_value" in user_default_settings[country],
            yes_task=f"update_user_date_format_{caller}",
            no_task=f"has_clock_format_{caller}"
        )

        update_user_date_format = rail.RepliconServiceOperator(
            task_id=f"update_user_date_format_{caller}",
            endpoint="/services/InternationalizationService1.svc/UpdateDateFormatForUser",
            data=lambda: {
                    "userUri": rail.render_template(user_uri),
                    "dateFormatUri": user_default_settings[country]["date_format_api_value"]
            },
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}')
        )

        has_clock_format = rail.IfOperator(
            task_id=f"has_clock_format_{caller}",
            test="clock_format_api_value" in user_default_settings[country],
            yes_task=f"update_clock_format_{caller}",
            no_task=f"has_report_excel_format_{caller}"
        )

        update_clock_format = rail.RepliconServiceOperator(
            task_id=f"update_clock_format_{caller}",
            endpoint="/services/InternationalizationService1.svc/UpdateClockFormatForUser",
            data={
                "userUri": user_uri,
                "clockFormatUri": user_default_settings[country]["clock_format_api_value"]
            },
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}')
        )

        has_report_excel_format = rail.IfOperator(
            task_id=f"has_report_excel_format_{caller}",
            test=lambda: "report_excel_export_format_api_value" in user_default_settings[
                country],
            yes_task=f"update_report_excel_format_{caller}",
            no_task=f"has_comma_separator_{caller}"
        )

        update_report_excel_format = rail.RepliconServiceOperator(
            task_id=f"update_report_excel_format_{caller}",
            endpoint="/services/LegacyUIService1.svc/UpdateMyExcelReportFormat",
            data={
                    "excelReportFormatUri": user_default_settings[country]["report_excel_export_format_api_value"]
            },
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}'),
        )

        has_comma_separator = rail.IfOperator(
            task_id=f"has_comma_separator_{caller}",
            test=lambda: "report_csv_separator" in user_default_settings[country],
            yes_task=f"update_comma_separator_{caller}",
            no_task=f"has_default_timeoff_type_{caller}"
        )

        update_comma_separator = rail.RepliconServiceOperator(
            task_id=f"update_comma_separator_{caller}",
            endpoint="/services/LegacyUIService1.svc/UpdateMyCsvReportColumnSeparator",
            data={
                    "csvColumnSeparator": user_default_settings[country]["report_csv_separator"]
            },
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}'),
        )

        has_default_timeoff_type = rail.IfOperator(
            task_id=f"has_default_timeoff_type_{caller}",
            test=lambda dag_run: "default_time_off_type_for_bookings" in user_default_settings[country] and
            dag_run.conf["default_timeoff_annual_leave_uri"],
            yes_task=f"update_default_timeoff_type_{caller}",
            no_task=f"user_settings_complete_{caller}"
        )

        update_default_timeoff_type = rail.RepliconServiceOperator(
            task_id=f"update_default_timeoff_type_{caller}",
            endpoint="/services/LegacyUIService1.svc/UpdateMyDefaultTimeOffTypeForBookings",
            data=lambda dag_run: {
                "timeOffTypeUri": dag_run.conf["default_timeoff_annual_leave_uri"]
            },
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}'),
        )

        user_settings_complete = rail.EmptyOperator(
            task_id=f"user_settings_complete_{caller}")

        impersonate_and_create_interactive_session >>\
            put_columnsettings_for_user_timesheets >>\
            has_language >> rail.Label(
                "Yes") >> update_language >> has_date_format
        has_language >> rail.Label("No") >>\
            has_date_format >> rail.Label(
                "Yes") >> update_user_date_format >> has_clock_format
        has_date_format >> rail.Label("No") >>\
            has_clock_format >> rail.Label(
                "Yes") >> update_clock_format >> has_report_excel_format
        has_clock_format >> rail.Label("No") >>\
            has_report_excel_format >> rail.Label(
                "Yes") >> update_report_excel_format >> has_comma_separator
        has_report_excel_format >> rail.Label("No") >>\
            has_comma_separator >> rail.Label(
                "Yes") >> update_comma_separator >> has_default_timeoff_type
        has_comma_separator >> rail.Label("No") >>\
            has_default_timeoff_type >> rail.Label(
                "Yes") >> update_default_timeoff_type >> user_settings_complete
        has_default_timeoff_type >> rail.Label("No") >> user_settings_complete

        return put_table_view_setting
