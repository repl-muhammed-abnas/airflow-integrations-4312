import rail

null = None


def log_mandatory_field_exception_task(import_type):
    with rail.TaskGroup(group_id=f'log_mandatory_field_exception_task_{import_type.lower()}', prefix_group_id=False) as log_mandatory_field_not_present:

        action_type = "created" if import_type == "Add" else "updated"

        def get_log_missing_required_fields_msg(dag_run):
            msg = []
            msg.append(
                "Project code not present" if not dag_run.conf.get('chargecode') else null)
            msg.append(
                "Project name not present" if not dag_run.conf.get('chargecodename') else null)
            msg.append(
                "Project type is not present" if not dag_run.conf.get('chargecodetype') else null)
            msg.append(
                "Project start date is received blank" if not dag_run.conf.get('chargecodestartdate') else null)
            msg.append(
                "Project status not present" if not dag_run.conf.get('openfortime') else null)
            return f"Project not {action_type} because of following reasons: " + ", ".join([m for m in msg if m is not null])

        log_mandatory_field_exception = rail.WriteLogOperator(
            task_id=f"log_mandatory_field_exception_{import_type.lower()}",
            log="{{ result('create_log') }}",
            severity='Exception',
            message=f"Project not {action_type} because one or more of the mandatory fields were not present",
            properties=lambda dag_run: {
                'SenderID': f"{dag_run.conf['sender']} | Project",
                'Project Name|Project Code': f"{dag_run.conf.get('chargecodename', '')} | {dag_run.conf.get('chargecode', '')}",
                'Client Name|Client Code': f"{dag_run.conf['client_name']} | {dag_run.conf['client_code'] }",
                'Task Name|Task Code': 'nil',
                'status': 'Exception',
                'details': get_log_missing_required_fields_msg(dag_run),
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': import_type
            }
        )

        log_mandatory_field_exception

        return log_mandatory_field_not_present
