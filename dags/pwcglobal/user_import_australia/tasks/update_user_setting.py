import rail
from pwcglobal.user_import import custom_method
from pwcglobal.user_import_australia.mappers.user_attribute_mapper import user_attribute_mapper

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
                "excelreportscompressionisrequired": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'ExcelReportsCompressionIsRequired', 'Apivalue'),
                "csvreportcolumnseperator": rail.find_first_by_attr_and_get_attr(mapper, 'Attribute', 'CsvReportColumnSeparator', 'Apivalue'),
            }
        map_user_setting = rail.PythonOperator(
            task_id='map_user_setting',
            python_callable=do_map_user_setting
        )

        has_dateformat = rail.IfOperator(
            task_id='has_dateformat',
            test="{{ result('map_user_setting').dateformatforuser | is_truthy }}",
            yes_task='update_dateformat',
            no_task='user_setting_update_complete'
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
            no_task='user_setting_update_complete'
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
            no_task='user_setting_update_complete'
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
            no_task='user_setting_update_complete'
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
            test="{{ result('map_user_setting').language | is_truthy }}",
            yes_task='update_language',
            no_task='user_setting_update_complete'
        )

        update_language = rail.RepliconServiceOperator(
            task_id='update_language',
            endpoint='/services/InternationalizationService1.svc/UpdateLanguageForUser',
            data={
                "userUri": user_uri,
                "languageUri": "{{ result('map_user_setting').language }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        has_activitynameformat = rail.IfOperator(
            task_id='has_activitynameformat',
            test="{{ result('map_user_setting').activitynameformat | is_truthy }}",
            yes_task='update_activitynameformat',
            no_task='user_setting_update_complete'
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
            no_task='user_setting_update_complete'
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
            no_task='user_setting_update_complete'
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
            no_task='user_setting_update_complete'
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

        user_setting_update_complete = rail.EmptyOperator(
            task_id='user_setting_update_complete'
        )

        impersonate_and_create_interactive_session >> map_user_setting >> [has_dateformat, has_activitynameformat, has_clockformat,
                    has_csvreportcolumnseperator, has_defaulttimesheettodisplay, has_excelreportformat, has_hoursformat, has_language, has_timepunchtimezone]

        has_dateformat >> rail.Label(
            'yes') >> update_dateformat >> user_setting_update_complete
        has_dateformat >> rail.Label("No") >> user_setting_update_complete

        has_clockformat >> rail.Label(
            'yes') >> update_clockformat >> user_setting_update_complete
        has_clockformat >> rail.Label("No") >> user_setting_update_complete

        has_hoursformat >> rail.Label(
            'yes') >> update_hoursformat >> user_setting_update_complete
        has_hoursformat >> rail.Label("No") >> user_setting_update_complete

        has_timepunchtimezone >> rail.Label(
            'yes') >> update_timepunchtimezone >> user_setting_update_complete
        has_timepunchtimezone >> rail.Label(
            "No") >> user_setting_update_complete

        has_language >> rail.Label(
            'yes') >> update_language >> user_setting_update_complete
        has_language >> rail.Label("No") >> user_setting_update_complete

        has_activitynameformat >> rail.Label(
            'yes') >> update_activitynameformat >> user_setting_update_complete
        has_activitynameformat >> rail.Label(
            "No") >> user_setting_update_complete

        has_defaulttimesheettodisplay >> rail.Label(
            'yes') >> update_defaulttimesheettodisplay >> user_setting_update_complete
        has_defaulttimesheettodisplay >> rail.Label(
            "No") >> user_setting_update_complete

        has_excelreportformat >> rail.Label(
            'yes') >> update_excelreportformat >> user_setting_update_complete
        has_excelreportformat >> rail.Label(
            "No") >> user_setting_update_complete

        has_csvreportcolumnseperator >> rail.Label(
            'yes') >> update_csvreportcolumnseperator >> user_setting_update_complete
        has_csvreportcolumnseperator >> rail.Label(
            'no') >> user_setting_update_complete

    return update_user_setting
