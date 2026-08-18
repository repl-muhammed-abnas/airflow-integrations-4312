from datetime import datetime as dt
from pendulum import datetime, now
import rail
from airflow.models import Variable

OPEN_BRACKET = "{{"
CLOSE_BRACKET = "}}"

def create_webhook_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"capgemini_deleted_timeoff_booking_logs_cleanup_{config.instance}",
        description="Cleanup dag for Deleted TO booking in Tenant wide Log",
        schedule_interval="30 5 1 * *",
        start_date=datetime(2023, 9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        for idx, log_name in enumerate(config.tenant_wide_log_list):
            get_tenant_wide_log = rail.CreateLogOperator(
                task_id = f"get_tenant_wide_log_{idx}",
                tenant_wide_name=log_name.split(":")[-1],
                existing_log_mode="append"
            )

            # should delete entries with age more than 30
            def do_filter_log(log):
                current_time = now()
                timestamp = dt.fromisoformat(log['timestamp'])
                return (current_time - timestamp).days > config.MAX_AGE_FOR_RECORD_IN_DAYS

            filter_logs = rail.FilterLogEntriesOperator(
                task_id = f"filter_logs_{idx}",
                log=f"{OPEN_BRACKET}result('get_tenant_wide_log_{idx}'){CLOSE_BRACKET}",
                filter_callable=do_filter_log,
                remove_filtered_entries=True,
            )

            create_deleted_booking_csv = rail.WriteCSVFileOperator(
                task_id = f"create_deleted_booking_csv_{idx}",
                source=f"{OPEN_BRACKET}result('filter_logs_{idx}'){CLOSE_BRACKET}",
                header=['Timestamp', 'Ecid', 'UserLoginName',
                        'UserUri', 'TimeoffTypeName', 'TimeoffTypeUri', 'TimeoffBookingUri', 'TotalWorkingDays (Decimal)', 'TotalWorkingHours (Decimal)'],
                row=lambda item: [
                    item["timestamp"],
                    item["ecid"],
                    item["properties"]["user_login_name"],
                    item["properties"]["user_uri"],
                    item["properties"]["timeoff_type_name"],
                    item["properties"]["timeoff_type_uri"],
                    item["properties"]["timeoff_booking_uri"],
                    item["properties"]["total_working_days"],
                    item["properties"].get("total_working_hours")
                ]
            )

            upload_csv_to_s3 = rail.S3UploadFileOperator(
                task_id = f"upload_csv_to_s3_{idx}",
                source=f"{OPEN_BRACKET}result('create_deleted_booking_csv_{idx}'){CLOSE_BRACKET}",
                bucket_name=lambda: Variable.get(config.bucket_name),
                aws_conn_id=config.aws_conn_id,
                key_name=config.s3_upload_filepath + '{{ecid() | replace(":", "_")}}_{{current_time_in_specified_tz(fmt="%m%d%Y")}}_DeletedTimeoffDetails.csv',
            )

            get_tenant_wide_log >> filter_logs >> create_deleted_booking_csv >> upload_csv_to_s3

    return dag

rail.for_each_instance(create_webhook_dag)
