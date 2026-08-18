from datetime import timedelta
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_file_format_creation_child_{config.instance}',
        description='Creates the File Format for exporting Time Data from Vantagepoint to Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_time_data_columns'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_time_data_columns',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_time_data_columns = rail.RepliconServiceOperator(
            task_id = 'get_time_data_columns',
            endpoint="/services/TimeDataExportService1.svc/GetAllColumns",
            data_handler=lambda response: {
                "userproperties": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'User Properties', 'columns'),
                "timeentryoef": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Time Entry Object Extension Field ', 'columns'),
                "useroef": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'User Object Extension Field', 'columns')
            }
        )

        get_my_identity = rail.RepliconServiceOperator(
            task_id = 'get_my_identity',
            endpoint="/services/UserAccessControlService1.svc/GetMyIdentity",
            data_handler=lambda response: response['actualUser']['uri']
        )

        def add_column(uri, label = False):
            collection = [
                {
                    "collection": [{
                        "text": "uri"
                    }, {
                        "uri": uri
                    }],
                    "uri": "urn:replicon:script-type:parameter:time-data-download:column-uri"
                }
            ]
            if ('entry-date' in uri) or ('timesheet-period' in uri):
                collection.append({
                    "collection": [{
                        "text": "text"
                    }, {
                        "text": config.date_format_for_file_format
                    }],
                    "uri": "urn:replicon:script-type:parameter:time-data-download:column-date-format"
                })
            if label:
                collection.append({
                    "collection": [{
                        "text": "text"
                    }, {
                        "text": label
                    }],
                    "uri": "urn:replicon:script-type:parameter:time-data-download:column-name"
                })
            return {
                "collection": [{
                    "text": "dictionary"
                },{
                    "collection": collection
                }]
            }


        def get_columns_collection():
            columns_collection = []
            timeentryoefcolumns = rail.result('get_time_data_columns').get('timeentryoef')
            useroefcolumns = rail.result('get_time_data_columns').get('useroef')
            userpropertiescolumns = rail.result('get_time_data_columns').get('userproperties')
            laborcodeoefs = [rail.find_first_by_attr_and_get_attr(timeentryoefcolumns, 'displayText', (oef.get('name') + ' (Code)'), 'uri') for oef in config.oefs if 'laborcodelevel' in oef.get('id')]
            default_laborcodeoefs = [{
                'uri': rail.find_first_by_attr_and_get_attr(useroefcolumns, 'displayText', (oef.get('name') + ' (Code)'), 'uri'),
                'label': 'Default ' + oef.get('name') + ' (Code)'
            } for oef in config.oefs if 'laborcodelevel' in oef.get('id')]
            homecompany_config = rail.find_first_by_attr_and_get_attr(config.groups, 'id', 'homecompany')
            homecompany_code_uri = rail.find_first_by_attr_and_get_attr(userpropertiescolumns, 'displayText', (homecompany_config.get('name') + ' Code'), 'uri') if homecompany_config.get('type') != 'department' else 'urn:replicon:time-data-export-column:department-group-code'
            allow_lc_update_oef_uri = rail.find_first_by_attr_and_get_attr(useroefcolumns, 'displayText', rail.find_first_by_attr_and_get_attr(config.oefs, 'id', 'allowlcupdate', 'name'), 'uri')
            laborcategory_oef_uri = rail.find_first_by_attr_and_get_attr(timeentryoefcolumns, 'displayText', (rail.find_first_by_attr_and_get_attr(config.oefs, 'id', 'laborcategory', 'name') + ' (Code)'), 'uri')
            workdistribution_oef_uri = rail.find_first_by_attr_and_get_attr(timeentryoefcolumns, 'displayText', (rail.find_first_by_attr_and_get_attr(config.oefs, 'id', 'workdistribution', 'name')), 'uri')
            timesheet_field_lc_oef_name = getattr(config, 'timesheet_field_oef_name_for_lc', None)
            timesheet_field_lc_oef_uri = rail.find_first_by_attr_and_get_attr(
                timeentryoefcolumns, 'displayText', (timesheet_field_lc_oef_name + ' (Code)'), 'uri') if timesheet_field_lc_oef_name else None
            for uri in [
                "urn:replicon:time-data-export-column:user-login-name",
                "urn:replicon:time-data-export-column:entry-date",
                "urn:replicon:time-data-export-totals-column:hours",
                "urn:replicon:time-data-export-column:time-entry-id",
                "urn:replicon:time-data-export-column:comments",
                "urn:replicon:time-data-export-column:timesheet-period",
                "urn:replicon:time-data-export-column:project-name",
                "urn:replicon:time-data-export-column:project-code",
                "urn:replicon:time-data-export-column:task-name",
                "urn:replicon:time-data-export-column:task-hierarchy-name",
                "urn:replicon:time-data-export-column:task-code",
                "urn:replicon:time-data-export-column:task-hierarchy-code",
                "urn:replicon:time-data-export-column:time-off-code-name",
                "urn:replicon:time-data-export-column:time-off-code-description"
                ]:
                columns_collection.append(add_column(uri))
            for uri in laborcodeoefs:
                columns_collection.append(add_column(uri))
            for item in default_laborcodeoefs:
                columns_collection.append(add_column(item.get('uri'), item.get('label')))
            columns_collection.append(add_column(allow_lc_update_oef_uri))
            columns_collection.append(add_column(laborcategory_oef_uri))
            columns_collection.append(add_column(workdistribution_oef_uri))
            columns_collection.append(add_column(homecompany_code_uri))
            if timesheet_field_lc_oef_uri:
                columns_collection.append(add_column(timesheet_field_lc_oef_uri))
            return columns_collection

        def get_file_format_key_values():
            key_values = [
                {
                    "keyUri": "urn:replicon:csv-file-settings:custom-delimiter",
                    "value": {
                        "text": ","
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:basis-script-identifier",
                    "value": {
                        "text": "CsvGeneration"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:creator",
                    "value": {
                        "uri": rail.result('get_my_identity')
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:custom-script",
                    "value": {
                        "bool": "false"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:name",
                    "value": {
                        "text": config.replicon_export_file_format_name
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:owner",
                    "value": {
                        "uri": "urn:replicon:script-key:owner:tenant"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:parameter:time-data-download:columns",
                    "value": {
                        "collection": [{
                            "text": "collection"
                        },
                        {
                            "collection": get_columns_collection()
                        }]
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:script-filename",
                    "value": {
                        "text": "@replicon/script-library/TimeDataDownload/CsvGeneration.py"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:script-version",
                    "value": {
                        "text": "0.1"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:system-format",
                    "value": {
                        "text": "csv"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:time-data-download-script:transport-method",
                    "value": {
                        "uri": "urn:replicon:time-data-download-transport-method:file"
                    }
                }
            ]

            return key_values

        put_file_format = rail.RepliconServiceOperator(
            task_id = 'put_file_format',
            endpoint="/services/TimeDataExportService1.svc/PutFileFormat",
            data=lambda: {
                "fileFormat": {
                    "target": {
                    "uri": null,
                    "slug": null,
                    "name": config.replicon_export_file_format_name
                    },
                    "name": config.replicon_export_file_format_name,
                    "description": null,
                    "detailedDescription": null,
                    "isActive": "true",
                    "keyValues": get_file_format_key_values()
                }
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_time_data_columns >> get_my_identity >> put_file_format >> log_to_sumo
        return dag

rail.for_each_instance(create_dag)
