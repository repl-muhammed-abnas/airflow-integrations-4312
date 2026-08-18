import rail


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_sender_erp_details():
    return {
        "sender": get_dag_run_conf()['webhook']['data']['sender'],
        "erp": "FTP" if get_dag_run_conf()['webhook']['data']['sender'].lower() == "ftp" else \
            get_dag_run_conf()['webhook']['data']['timeExportID'].split("|")[1],
        "time_export_name": get_dag_run_conf()['webhook']['data']['timeExportID'] if get_dag_run_conf()['webhook']['data']['sender'].lower() == "ftp" \
            else get_dag_run_conf()['webhook']['data']['timeExportID'].split("|")[0]
    }
