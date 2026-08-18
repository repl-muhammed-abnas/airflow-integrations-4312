import rail
from pwcglobal.user_import_v5.utils import custom_method, request_payload
from pwcglobal.user_import_v5.mapper.user_attribute import user_attribute_mapper

# pylint: disable=too-many-statements


def get_update_user_setting(user_uri):
    with rail.TaskGroup(group_id='update_user_setting', prefix_group_id=False) as update_user_setting:
        impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session',
            endpoint='/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession',
            data={
                "impersonatedUserUri": user_uri
            },
            response_filter=custom_method.map_impersonate_and_create_interactive_session
        )

        def get_language_uri_if_not_default(dag_run):
            if dag_run.conf['language_uri_if_not_default']:
                return dag_run.conf['language_uri_if_not_default']
            return None

        def do_map_user_setting():
            mapper = list(
                filter(lambda x: x['location'] == 'Europe', user_attribute_mapper))
            return {
                "dateformatforuser": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'DateFormatForUser', 'Apivalue'),
                "clockformatforuser": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'ClockFormatForUser', 'Apivalue'),
                "hoursformatforuser": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'HoursFormatForUser', 'Apivalue'),
                "timepunchtimezone": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'TimePunchTimeZoneDisplayOptionForUser', 'Apivalue'),
                "language": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'LanguagesAvailableForUsers', 'Apivalue'),
                "activitynameformat": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'ActivityNameFormatForUser', 'Apivalue'),
                "defaulttimesheettodisplay": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'DefaultTimesheetToDisplayForUser', 'Apivalue'),
                "excelreportformat": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'ExcelReportFormat', 'Apivalue'),
                "excelreportcompressionls": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'CsvReportColumnSeparator', 'Apivalue'),
                "csvreportcolumnseperator": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'CsvReportColumnSeparator', 'Apivalue'),
                "csvreportcurrneyamount": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'CsvReportsCurrencyAmountSeparationPreference', 'Apivalue'),
                "clientnameformat": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'Client Name Format', 'Apivalue'),
                "projectnameformat": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'Project Name Format', 'Apivalue'),
                "tasknameformat": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'Task Name Format', 'Apivalue'),
            }
        map_user_setting = rail.PythonOperator(
            task_id='map_user_setting',
            python_callable=do_map_user_setting
        )

        has_dateformat = rail.IfOperator(
            task_id='has_dateformat',
            test="{{ result('map_user_setting').dateformatforuser | is_truthy }}",
            yes_task='update_dateformat',
            no_task='has_clockformat'
        )

        update_dateformat = rail.RepliconServiceOperator(
            task_id='update_dateformat',
            endpoint='/services/InternationalizationService1.svc/UpdateDateFormatForUser',
            data={
                "userUri": user_uri,
                "dateFormatUri": "{{ result('map_user_setting').dateformatforuser }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_clockformat = rail.IfOperator(
            task_id='has_clockformat',
            test="{{ result('map_user_setting').clockformatforuser | is_truthy }}",
            yes_task='update_clockformat',
            no_task='has_hoursformat'
        )

        update_clockformat = rail.RepliconServiceOperator(
            task_id='update_clockformat',
            endpoint='/services/InternationalizationService1.svc/UpdateClockFormatForUser',
            data={
                "userUri": user_uri,
                "clockFormatUri": "{{ result('map_user_setting').clockformatforuser }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_hoursformat = rail.IfOperator(
            task_id='has_hoursformat',
            test="{{ result('map_user_setting').hoursformatforuser | is_truthy }}",
            yes_task='update_hoursformat',
            no_task='has_timepunchtimezone'
        )

        update_hoursformat = rail.RepliconServiceOperator(
            task_id='update_hoursformat',
            endpoint='/services/InternationalizationService1.svc/UpdateHoursFormatForUser',
            data={
                "userUri": user_uri,
                "hoursFormatUri": "{{ result('map_user_setting').hoursformatforuser }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_timepunchtimezone = rail.IfOperator(
            task_id='has_timepunchtimezone',
            test="{{ result('map_user_setting').timepunchtimezone | is_truthy }}",
            yes_task='update_timepunchtimezone',
            no_task='has_language'
        )

        update_timepunchtimezone = rail.RepliconServiceOperator(
            task_id='update_timepunchtimezone',
            endpoint='/services/TimePunchService1.svc/UpdateTimePunchTimeZoneDisplayOptionForUser',
            data={
                "userUri": user_uri,
                "timePunchTimeZoneDisplayOptionUri": "{{ result('map_user_setting').timepunchtimezone }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_language = rail.IfOperator(
            task_id='has_language',
            test=lambda dag_run: bool(get_language_uri_if_not_default(
                dag_run)) or rail.result('map_user_setting')['language'],
            yes_task='update_language',
            no_task='has_activitynameformat'
        )

        update_language = rail.RepliconServiceOperator(
            task_id='update_language',
            endpoint='/services/InternationalizationService1.svc/UpdateLanguageForUser',
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "languageUri": get_language_uri_if_not_default(dag_run) or rail.result('map_user_setting')['language']
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_activitynameformat = rail.IfOperator(
            task_id='has_activitynameformat',
            test="{{ result('map_user_setting').activitynameformat | is_truthy }}",
            yes_task='update_activitynameformat',
            no_task='has_defaulttimesheettodisplay'
        )

        update_activitynameformat = rail.RepliconServiceOperator(
            task_id='update_activitynameformat',
            endpoint='/services/ActivityService1.svc/UpdateActivityNameFormatForUser',
            data={
                "userUri": user_uri,
                "activityNameFormatUri": "{{ result('map_user_setting').activitynameformat }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_defaulttimesheettodisplay = rail.IfOperator(
            task_id='has_defaulttimesheettodisplay',
            test="{{ result('map_user_setting').defaulttimesheettodisplay | is_truthy }}",
            yes_task='update_defaulttimesheettodisplay',
            no_task='has_excelreportformat'
        )

        update_defaulttimesheettodisplay = rail.RepliconServiceOperator(
            task_id='update_defaulttimesheettodisplay',
            endpoint='/services/LegacyUIService1.svc/UpdateDefaultTimesheetToDisplayForUser',
            data={
                "userUri": user_uri,
                "defaultTimesheetToDisplayUri": "{{ result('map_user_setting').defaulttimesheettodisplay }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_excelreportformat = rail.IfOperator(
            task_id='has_excelreportformat',
            test="{{ result('map_user_setting').excelreportformat | is_truthy }}",
            yes_task='update_excelreportformat',
            no_task='has_csvreportcolumnseperator'
        )

        update_excelreportformat = rail.RepliconServiceOperator(
            task_id='update_excelreportformat',
            endpoint='/services/LegacyUIService1.svc/UpdateMyExcelReportFormat',
            data={
                "excelReportFormatUri": "{{ result('map_user_setting').excelreportformat }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_csvreportcolumnseperator = rail.IfOperator(
            task_id='has_csvreportcolumnseperator',
            test="{{ result('map_user_setting').csvreportcolumnseperator | is_truthy }}",
            yes_task='update_csvreportcolumnseperator',
            no_task='has_csvreportcurrneyamount'
        )

        update_csvreportcolumnseperator = rail.RepliconServiceOperator(
            task_id='update_csvreportcolumnseperator',
            endpoint='/services/LegacyUIService1.svc/UpdateMyCsvReportColumnSeparator',
            data={
                "csvColumnSeparator": "{{ result('map_user_setting').csvreportcolumnseperator }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_csvreportcurrneyamount = rail.IfOperator(
            task_id='has_csvreportcurrneyamount',
            test="{{ result('map_user_setting').csvreportcurrneyamount | is_truthy }}",
            yes_task='update_csvreportcurrneyamount',
            no_task='has_clientnameformat'
        )

        update_csvreportcurrneyamount = rail.RepliconServiceOperator(
            task_id='update_csvreportcurrneyamount',
            endpoint='/services/LegacyUIService1.svc/UpdateMyCsvReportsCurrencyAmountSeparationPreference',
            data={
                "csvReportsCurrencyColumnsPreferenceUri": "{{ result('map_user_setting').csvreportcurrneyamount }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_clientnameformat = rail.IfOperator(
            task_id='has_clientnameformat',
            test="{{ result('map_user_setting').clientnameformat | is_truthy }}",
            yes_task='update_clientnameformat',
            no_task='has_projectnameformat'
        )

        update_clientnameformat = rail.RepliconServiceOperator(
            task_id='update_clientnameformat',
            endpoint='/services/ClientService1.svc/UpdateClientNameFormatForUser',
            data={
                "userUri": user_uri,
                "clientNameFormatUri": "{{ result('map_user_setting').clientnameformat }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_projectnameformat = rail.IfOperator(
            task_id='has_projectnameformat',
            test="{{ result('map_user_setting').projectnameformat | is_truthy }}",
            yes_task='update_projectnameformat',
            no_task='has_tasknameformat'
        )

        update_projectnameformat = rail.RepliconServiceOperator(
            task_id='update_projectnameformat',
            endpoint='/services/ProjectService1.svc/UpdateProjectNameFormatForUser',
            data={
                "userUri": user_uri,
                "projectNameFormatUri": "{{ result('map_user_setting').projectnameformat }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_tasknameformat = rail.IfOperator(
            task_id='has_tasknameformat',
            test="{{ result('map_user_setting').tasknameformat | is_truthy }}",
            yes_task='update_tasknameformat',
            no_task='user_setting_update_complete'
        )

        update_tasknameformat = rail.RepliconServiceOperator(
            task_id='update_tasknameformat',
            endpoint='/services/TaskService1.svc/UpdateTaskNameFormatForUser',
            data={
                "userUri": user_uri,
                "taskNameFormatUri": "{{ result('map_user_setting').tasknameformat }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        user_setting_update_complete = rail.EmptyOperator(
            task_id='user_setting_update_complete'
        )

        impersonate_and_create_interactive_session >> map_user_setting >> has_dateformat
        has_dateformat >> rail.Label(
            'yes') >> update_dateformat >> has_clockformat
        has_dateformat >> rail.Label('no') >> has_clockformat
        has_clockformat >> rail.Label(
            'yes') >> update_clockformat >> has_hoursformat
        has_clockformat >> rail.Label('no') >> has_hoursformat
        has_hoursformat >> rail.Label(
            'yes') >> update_hoursformat >> has_timepunchtimezone
        has_hoursformat >> rail.Label('no') >> has_timepunchtimezone
        has_timepunchtimezone >> rail.Label(
            'yes') >> update_timepunchtimezone >> has_language
        has_timepunchtimezone >> rail.Label('no') >> has_language
        has_language >> rail.Label(
            'yes') >> update_language >> has_activitynameformat
        has_language >> rail.Label('no') >> has_activitynameformat
        has_activitynameformat >> rail.Label(
            'yes') >> update_activitynameformat >> has_defaulttimesheettodisplay
        has_activitynameformat >> rail.Label(
            'no') >> has_defaulttimesheettodisplay
        has_defaulttimesheettodisplay >> rail.Label(
            'yes') >> update_defaulttimesheettodisplay >> has_excelreportformat
        has_defaulttimesheettodisplay >> rail.Label(
            'no') >> has_excelreportformat
        has_excelreportformat >> rail.Label(
            'yes') >> update_excelreportformat >> has_csvreportcolumnseperator
        has_excelreportformat >> rail.Label(
            'no') >> has_csvreportcolumnseperator

        has_csvreportcolumnseperator >> rail.Label(
            'yes') >> update_csvreportcolumnseperator >> has_csvreportcurrneyamount
        has_csvreportcolumnseperator >> rail.Label(
            'no') >> has_csvreportcurrneyamount
        has_csvreportcurrneyamount >> rail.Label(
            'yes') >> update_csvreportcurrneyamount >> has_clientnameformat
        has_csvreportcurrneyamount >> rail.Label('no') >> has_clientnameformat
        has_clientnameformat >> rail.Label(
            'yes') >> update_clientnameformat >> has_projectnameformat
        has_clientnameformat >> rail.Label('no') >> has_projectnameformat
        has_projectnameformat >> rail.Label(
            'yes') >> update_projectnameformat >> has_tasknameformat
        has_projectnameformat >> rail.Label('no') >> has_tasknameformat
        has_tasknameformat >> rail.Label(
            'yes') >> update_tasknameformat >> user_setting_update_complete
        has_tasknameformat >> rail.Label('no') >> user_setting_update_complete

    return update_user_setting
