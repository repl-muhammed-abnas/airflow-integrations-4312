from datetime import datetime


def _get_export_data():
    return {
        "export_name": 'DL_C1_CP_WORKER_LEAVE_' + datetime.now().strftime("%Y%m%d%H%M%S"),
        "report_start_date": datetime.now().strftime("%m/%d/%Y")
    }
