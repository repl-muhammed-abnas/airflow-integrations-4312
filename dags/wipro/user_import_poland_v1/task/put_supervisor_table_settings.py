import rail
from wipro.user_import_poland_v1.utils import custom_methods, request_payload


def get_put_table_view_setting_supervisor(user_uri, caller='supervisor', project_manager_flag="N"):
    with rail.TaskGroup(group_id=f'put_table_view_setting_{caller}', prefix_group_id=False) as put_table_view_setting_supervisor:

        impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
            task_id=f'impersonate_and_create_interactive_session_{caller}',
            endpoint='/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession',
            data={
                "impersonatedUserUri": user_uri
            },
            response_filter=custom_methods.map_impersonate_and_create_interactive_session
        )

        if_supervisor_type_pm = rail.IfOperator(
            task_id=f"if_supervisor_type_pm_{caller}",
            test=lambda: project_manager_flag == "Y",
            yes_task=f'put_column_settings_for_pm_timesheet_tab_{caller}',
            no_task=f'put_column_settings_for_supervisor_timesheet_tab_{caller}'
        )

        put_column_settings_for_pm_timesheet_tab = rail.RepliconServiceOperator(
            task_id=f'put_column_settings_for_pm_timesheet_tab_{caller}',
            endpoint='/services/ListSettingsService1.svc/PutColumnSettingsForUser',
            data=request_payload.get_put_column_settings_for_pm_timesheets_data(
                user_uri),
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}'),
        )


        put_column_settings_for_supervisor_timesheet_tab = rail.RepliconServiceOperator(
            task_id=f'put_column_settings_for_supervisor_timesheet_tab_{caller}',
            endpoint='/services/ListSettingsService1.svc/PutColumnSettingsForUser',
            data=request_payload.get_put_column_settings_for_user_timesheets_data(
                user_uri),
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}'),
        )

        put_columnsettings_for_user_approvals = rail.RepliconServiceOperator(
            task_id=f'put_columnsettings_for_user_approvals_{caller}',
            endpoint='/services/ListSettingsService1.svc/PutColumnSettingsForUser',
            data=request_payload.get_put_column_settings_for_user_approvals_data(
                user_uri),
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}'),
        )

        supervisor_process_complete = rail.EmptyOperator(
            task_id=f'supervisor_process_complete_{caller}'
        )

        impersonate_and_create_interactive_session >>\
        if_supervisor_type_pm >> rail.Label("Yes") >> put_column_settings_for_pm_timesheet_tab >>\
        put_columnsettings_for_user_approvals
        if_supervisor_type_pm >> rail.Label("No")>>\
            put_column_settings_for_supervisor_timesheet_tab >>\
            put_columnsettings_for_user_approvals >> supervisor_process_complete

        return put_table_view_setting_supervisor
