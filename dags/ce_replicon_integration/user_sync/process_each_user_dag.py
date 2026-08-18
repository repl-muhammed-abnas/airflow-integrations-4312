from datetime import timedelta
from uuid import uuid4
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_child_dag_id,
        description='Handles Sync of One Computerease User to Replicon',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_or_update_user',
            end_task='catch_user_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_extensionfields_list(user_details):
            oefs = []

            def add_tag_oef(definition, tag):
                oefs.append({
                    "value": {
                        "definition": {
                            "uri": definition,
                        },
                        "tag": {
                            "uri": tag
                        } if tag else None,
                    }
                })

            def add_text_oef(definition, textValue):
                oefs.append({
                    "value": {
                        "definition": {
                            "uri": definition,
                        },
                        "textValue": textValue
                    }
                })

            for oef in user_details['oefs']:
                if oef['def']:
                    if oef['type'] == 'text':
                        add_text_oef(oef['def'], oef['value'])
                    elif oef['type'] == 'dropdown':
                        add_tag_oef(oef['def'], oef['value'])
            return oefs
        
        def get_group_schedules(user_details):
            group_schedules = {}
            current_details = user_details['currentdetails']
            effective_date = None
            for group in config.groups:
                current_value = current_details and current_details.get(
                    group['type'])
                group_schedules[group['grouptypevariable'] + 'Schedule'] = [
                    {
                        "dateRange": {
                            "startDate": effective_date if current_value else null
                        },
                        "item": {
                            "uri": user_details[group['type']],
                        }
                    }
                ] if (user_details.get(group['type']) and user_details[group['type']] != current_value) else null
            return group_schedules

        def get_userdetails_payload(dag_run):
            user_details = dag_run.conf
            current_details = user_details['currentdetails']
            user_uri = current_details and current_details['uri']
            current_timesheetperiod = current_details and current_details.get(
                'timesheetperiod')
            effective_date = None
            payload = {
                "target": {
                    "uri": user_uri
                } if user_uri else None,
                "modifications": {
                    "firstName": {
                        "value": user_details['firstname'] or user_details['lastname']
                    },
                    "lastName": {
                        "value": user_details['lastname']
                    },
                    "loginName": {
                        "value": user_details['loginname']
                    },
                    "displayName": {
                        "value": user_details['displayname']
                    },
                    "securitySettings": {
                        "value": {
                            "loginEnabled": {
                                "value": user_details['loginenabled']
                            },
                            "forcePasswordChange": {
                                "value": True
                            },
                            "password": {
                                "value": config.initial_password
                            }
                        }
                    },
                    "timesheetApprovalPath": {
                        "value": {
                            "name": user_details['timesheetapprovalpath']
                        }
                    },
                    "timeZone": {
                        "value": {
                            "uri": user_details['timezone']
                        }
                    },
                    "workWeekStartDay": {
                        "value": {
                            "uri": user_details['workweek']
                        }
                    },
                    "timesheetTemplate": {
                        "value": {
                            "name": user_details['timesheettemplate']
                        }
                    },
                    "extensionFields": get_extensionfields_list(user_details),
                    "permissionSets": [{
                        "modificationOptionUri": "urn:replicon:collection-modification-option:replace",
                        "items": user_details['permissions']
                    }],
                    **get_group_schedules(user_details),
                    "timesheetPeriodSchedule": [
                        {
                            "dateRange": {
                                "startDate": effective_date if current_timesheetperiod else None
                            },
                            "item": {
                                "name": user_details['timesheetperiod'],
                            }
                        }
                    ] if (user_details['timesheetperiod'] and user_details['timesheetperiod'] != current_timesheetperiod) else None,
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }

            return payload

        create_or_update_user = rail.RepliconServiceOperator(
            task_id='create_or_update_user',
            endpoint='services/ImportService2.svc/CreateUserOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_userdetails_payload
        )

        def get_downstreamtasks_error(user_name, error_message):
            return {
                'error': f'Error with {user_name} - {error_message}'
            }

        catch_user_error = rail.PythonOperator(
            task_id='catch_user_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.loginname }}',
                     '{{ get_error_message() }}']
        )

        batch_task >> rail.Label('On Error') >> catch_user_error
        batch_task >> create_or_update_user >> rail.Label('On Error') >> catch_user_error

        return dag


rail.for_each_instance(create_dag)
