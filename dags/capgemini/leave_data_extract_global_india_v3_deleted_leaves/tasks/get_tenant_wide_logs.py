import rail
def get_tenant_wide_logs(tenant_wide_log_list):
    with rail.TaskGroup(group_id='get_tenant_wide_logs', prefix_group_id=False) as taskgroup:
        for idx, log_artifact in enumerate(tenant_wide_log_list):

            create_tenant_wide_logs_collection = rail.CreateCollectionOperator(
                task_id=f'create_tenant_wide_logs_collection_{idx}',
                source=log_artifact,
                name=f"raw_tenant_wide_log_data_{idx}"
            )

            query_extract_json_data = rail.QueryCollectionOperator(
                task_id=f"query_extract_json_data_{idx}",
                query=f"""SELECT
                    json_extract(l.properties, '$.user_login_name') as "user_login_name",
                    json_extract(l.properties, '$.user_uri') as "user_uri",
                    json_extract(l.properties, '$.timeoff_type_name') as "timeoff_type_name",
                    json_extract(l.properties, '$.timeoff_type_uri') as "timeoff_type_uri",
                    json_extract(l.properties, '$.timeoff_booking_uri') as "timeoff_booking_uri",
                    json_extract(l.properties, '$.total_working_days') as "total_working_days"
                    from raw_tenant_wide_log_data_{idx} l
                """,
                name = f"tenant_wide_log_data_{idx}"
            )

            create_tenant_wide_logs_collection >> query_extract_json_data

    return taskgroup
