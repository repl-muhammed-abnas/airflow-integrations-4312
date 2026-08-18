import rail
from pwcglobal.user_import_v7.utils import request_payload, custom_method


def get_put_table_view_setting(user_uri, caller='supervisor'):
    with rail.TaskGroup(group_id=f'put_table_view_setting_{caller}', prefix_group_id=False) as put_table_view_setting:

        impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
            task_id=f'impersonate_and_create_interactive_session_{caller}',
            endpoint='/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession',
            data={
                "impersonatedUserUri": user_uri
            },
            response_filter=custom_method.map_impersonate_and_create_interactive_session
        )

        put_column_settings_for_user_timesheet_tab = rail.RepliconServiceOperator(
            task_id=f'put_column_settings_for_user_timesheet_tab_{caller}',
            endpoint='/services/ListSettingsService1.svc/PutColumnSettingsForUser',
            data=request_payload.get_put_column_settings_for_user_timesheet_tab_data(
                user_uri),
            headers=lambda: rail.result(
                f'impersonate_and_create_interactive_session_{caller}'),
        )

        is_supervisor_type = rail.IfOperator(
            task_id=f'is_supervisor_type_{caller}',
            test=lambda: caller == 'supervisor',
            yes_task=f'put_columnsettings_for_user_team_tab_{caller}',
            no_task=f'supervisor_process_complete_{caller}',
        )

        put_columnsettings_for_user_team_tab = rail.RepliconServiceOperator(
            task_id=f'put_columnsettings_for_user_team_tab_{caller}',
            endpoint='/services/ListSettingsService1.svc/PutColumnSettingsForUser',
            data=request_payload.get_put_column_settings_for_user_team_tab_data(
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

        impersonate_and_create_interactive_session >> put_column_settings_for_user_timesheet_tab >> is_supervisor_type
        is_supervisor_type >> rail.Label(
            'yes') >> put_columnsettings_for_user_team_tab >> put_columnsettings_for_user_approvals >> supervisor_process_complete
        is_supervisor_type >> rail.Label(
            'no') >> supervisor_process_complete

        return put_table_view_setting
