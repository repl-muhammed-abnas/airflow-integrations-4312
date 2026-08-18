region = 'all'
environment = ['pre-production', 'production']
sftp_alert_child_dag_id = 'system_sftp_monitoring_alerts_child'
max_active_runs_child_dag = 5
dag_config_var_name = 'system_sftp_monitoring_alerts_master_dag_config'
# var value should be json string format e.g default_dag_config
default_dag_config = {
    "sftp_monitoring_list": [
        {
            "paths": [
                "/DXC/TimeExport",
                "/DXC/test"
            ],
            "company_key": "dxcsandbox",
            "sftp_conn_id": "sftp_dxctechnology",
            "sftp_file_count_threshold": 5,
            "sftp_file_hours_threshold": 6
        },
        {
            "paths": [
                "/DXC/ppmc/input",
                "/DXC/C1WBS/input"
            ],
            "company_key": "dxcsandbox",
            "sftp_conn_id": "sftp_dxctechnology",
            "sftp_file_count_threshold": 5,
            "sftp_file_hours_threshold": 6
        },
        {
            "paths": [
                "/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Project/OracleDEV6/_logs/_archive"
            ],
            "company_key": "dxcsandbox",
            "sftp_conn_id": "sftp_pwc_sshkeytest",
            "sftp_file_count_threshold": 5,
            "sftp_file_hours_threshold": 6
        }
    ],
    "alert_email": '{{ var.value.dagrun_internal_log_email }}'
}
