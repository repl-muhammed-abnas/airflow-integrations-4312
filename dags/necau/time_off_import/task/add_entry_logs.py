import rail
from rail.lib.ecid import get_dagrun_ecid


def add_entry_log_process(caller, severity, status, reason):
    with rail.TaskGroup(group_id=f'add_entry_log_process_{caller}', prefix_group_id=False) as add_entry_log:

        add_log_entry = rail.WriteLogOperator(
            task_id=f'add_log_entry_{caller}',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity=severity,
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': status,
                'reason': reason,
                'Request Key': dag_run.conf['request_key']
            },
            message=status
        )

        add_log_entry

        return add_entry_log
