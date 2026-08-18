from datetime import timedelta
from uuid import uuid4
from airflow.models import Variable
import rail

from deltek_vantagepoint_v2.user_sync.utils.python_callable_method import get_exception_logs
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_child_dag_id,
        description=f'{config.company_key} Handles Sync of One Vantagepoint User to Replicon',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='check_exceptions_for_logging'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_exceptions_for_logging',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        check_exceptions_for_logging = rail.PythonOperator(
            task_id = 'check_exceptions_for_logging',
            python_callable=lambda dag_run: get_exception_logs(dag_run, config)
        )

        if_supervisor_to_assign = rail.IfOperator(
            task_id='if_supervisor_to_assign',
            test=lambda dag_run: dag_run.conf['supervisoruri'] and (dag_run.conf['supervisoruri'] != (
                dag_run.conf['currentdetails'] and dag_run.conf['currentdetails'].get('supervisor'))) and (not dag_run.conf[
                'is_user_own_supervisor']) and (dag_run.conf['is_supervisor_enabled']),
            yes_task='search_supervisor_permission',
            no_task='create_or_update_user'
        )

        search_supervisor_permission = rail.RepliconServiceOperator(
            task_id='search_supervisor_permission',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                "userUri": "{{ dag_run.conf.supervisoruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision')
        )

        if_supervisor_permission_not_present = rail.IfOperator(
            task_id='if_supervisor_permission_not_present',
            test=lambda: not rail.result(
                'search_supervisor_permission'),
            yes_task='assign_supervisor_permission',
            no_task='create_or_update_user'
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                'userUri': dag_run.conf['supervisoruri'],
                "permissionSetUri": dag_run.conf['supervisorpermissionuri']
            }
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

            def add_number_oef(definition, numericValue):
                oefs.append({
                    "value": {
                        "definition": {
                            "uri": definition,
                        },
                        "numericValue": numericValue
                    }
                })
            for oef in user_details['oefs']:
                if oef['def']:
                    if oef['type'] == 'text':
                        add_text_oef(oef['def'], oef['value'])
                    elif oef['type'] == 'dropdown':
                        add_tag_oef(oef['def'], oef['value'])
                    elif oef['type'] == 'number':
                        add_number_oef(oef['def'], oef['value'])
            return oefs

        def get_group_schedules(user_details):
            group_schedules = {}
            current_details = user_details['currentdetails']
            effective_date = user_details['modDate']
            for group in config.groups:
                current_value = current_details and current_details.get(group['type'])
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
            current_supervisor = current_details and current_details.get(
                'supervisor')
            current_scheduletype = current_details and current_details.get(
                'scheduletype')
            current_timesheetperiod = current_details and current_details.get(
                'timesheetperiod')
            new_supervisor = user_details['supervisoruri']
            effective_date = user_details['modDate']
            return {
                "target": {
                    "uri": user_uri
                } if user_uri else null,
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
                    "emailAddress": {
                        "value": user_details['email']
                    },
                    "employmentDateRange": {
                        "value": {
                            "startDate": user_details['startdate'],
                            "endDate": user_details['enddate'],
                        }
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
                    "holidayCalendar": null,
                    "extensionFields": get_extensionfields_list(user_details),
                    "permissionSets": [{
                        "modificationOptionUri": "urn:replicon:collection-modification-option:replace",
                        "items": user_details['permissions']
                    }],
                    **get_group_schedules(user_details),
                    "supervisorSchedule": [
                        {
                            "dateRange": {
                                "startDate": effective_date if current_supervisor else null
                            },
                            "item": {
                                "uri": new_supervisor,
                            }
                        }
                    ] if (new_supervisor and new_supervisor != current_supervisor and user_details[
                        'is_supervisor_enabled'] and not user_details['is_user_own_supervisor']) else null,
                    "timesheetPeriodSchedule": [
                        {
                            "dateRange": {
                                "startDate": effective_date if current_timesheetperiod else null
                            },
                            "item": {
                                "name": user_details['timesheetperiod'],
                            }
                        }
                    ] if (user_details['timesheetperiod'] and user_details['timesheetperiod'] != current_timesheetperiod) else None,
                    "scheduleTypeSchedule": [
                        {
                            "dateRange": {
                                "startDate": effective_date if current_scheduletype else null,
                            },
                            "item": {
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                                "officeSchedule": {
                                    "name": user_details['scheduletype']
                                }
                            }
                        }
                    ] if (user_details['scheduletype'] and user_details['scheduletype'] != current_scheduletype) else null,
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }

        create_or_update_user = rail.RepliconServiceOperator(
            task_id='create_or_update_user',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='services/ImportService2.svc/CreateUserOrApplyModifications',
            data=get_userdetails_payload
        )

        if_supervisor_to_be_removed = rail.IfOperator(
            task_id='if_supervisor_to_be_removed',
            test=lambda dag_run: ((not dag_run.conf['supervisor']) or dag_run.conf[
                'is_user_own_supervisor']) and dag_run.conf['currentdetails'] and dag_run.conf['currentdetails']['supervisor'],
            yes_task='remove_supervisor',
            no_task='if_supervisor_sent_but_not_found_in_replicon'
        )

        remove_supervisor = rail.RepliconServiceOperator(
            task_id='remove_supervisor',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='services/UserService1.svc/PutSupervisorAssignmentSchedule2',
            data=lambda dag_run: {
                "userUri": dag_run.conf['currentdetails'].get('uri'),
                "scheduleEntries": []
            }
        )

        if_supervisor_sent_but_not_found_in_replicon = rail.IfOperator(
            task_id='if_supervisor_sent_but_not_found_in_replicon',
            test=lambda dag_run: dag_run.conf['supervisor'] and not dag_run.conf['supervisoruri'] and not(dag_run.conf['is_user_own_supervisor']),
            yes_task="add_supervisor_assignment_log",
            no_task='catch_error'
        )

        add_supervisor_assignment_log = rail.WriteLogOperator(
            task_id='add_supervisor_assignment_log',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "useruri": rail.result('create_or_update_user')['user']['uri'],
                "supervisor": dag_run.conf['supervisor'],
                "currentsupervisor": dag_run.conf['currentdetails'] and dag_run.conf['currentdetails']['supervisor'],
                "supervisorpermissionuri": dag_run.conf['supervisorpermissionuri'],
                "effectivedate": dag_run.conf['modDate']
            }
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in process each user sync - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> check_exceptions_for_logging >> if_supervisor_to_assign
        batch_task >> check_exceptions_for_logging
        if_supervisor_to_assign >> rail.Label('Yes') >> search_supervisor_permission >> if_supervisor_permission_not_present
        if_supervisor_permission_not_present >> rail.Label('Yes') >> assign_supervisor_permission >> create_or_update_user
        if_supervisor_permission_not_present >> rail.Label('No') >> create_or_update_user
        if_supervisor_to_assign >> rail.Label('No') >> create_or_update_user >> if_supervisor_to_be_removed
        if_supervisor_to_be_removed >> rail.Label('Yes') >> remove_supervisor >> if_supervisor_sent_but_not_found_in_replicon
        if_supervisor_to_be_removed >> rail.Label('No') >> if_supervisor_sent_but_not_found_in_replicon
        if_supervisor_sent_but_not_found_in_replicon >> rail.Label('Yes') >> add_supervisor_assignment_log >> catch_error
        if_supervisor_sent_but_not_found_in_replicon >> rail.Label('No') >> catch_error

        return dag


rail.for_each_instance(create_dag)
