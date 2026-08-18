from uuid import uuid4
from pendulum import datetime
import rail


OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

#pylint: disable=too-many-statements

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.time_export_process_timesheets_time_entries_dag_id,
        description="Mammoet Time Export process time export",
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.timeentry_process_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        view_dagrun_conf =rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        reopen_timeentry = rail.RepliconServiceOperator(
            task_id = "reopen_timeentry",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/Reopen",
            data=lambda dag_run:{
                "timeEntryRevisionGroupUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:time-entry-revision-group:{dag_run.conf['time_entry_id']}",
                "unitOfWorkId": str(uuid4()),
                "comments": "Time Entry Reopened By Integration (Time Export)"
            }
        )

        update_replicon_id = rail.RepliconServiceOperator(
            task_id = "update_replicon_id",
            endpoint="/services/ObjectExtensionService1.svc/BulkUpdateObjectExtensionFieldValue",
            data=lambda dag_run:{
                "objectUris": [
                    f"urn:replicon-tenant:{rail.get_tenant_slug()}:time-entry-revision-group:{dag_run.conf['time_entry_id']}"
                ],
                "value": {
                    "definition": {
                        "uri": dag_run.conf['replicon_id_oef']['uri']
                    },
                    "tag": None,
                    "numericValue": None,
                    "textValue": dag_run.conf['short_time_entry_id'],
                    "fileValue": None,
                    "jsonValue": None
                }
            }
        )

        force_approve = rail.RepliconServiceOperator(
            task_id = "force_approve",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/ForceApprove",
            # Making sure that the time-entry gets approve back even if the update fails
            trigger_rule = "all_done",
            data=lambda dag_run:{
                "timeEntryRevisionGroupUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:time-entry-revision-group:{dag_run.conf['time_entry_id']}",
                "unitOfWorkId": str(uuid4()),
                "comments": "Time Entry Force Approved By Integration (Time Export)"
            }
        )

        view_dagrun_conf >> reopen_timeentry >> update_replicon_id >> force_approve

    return dag

rail.for_each_instance(create_main_dag)
