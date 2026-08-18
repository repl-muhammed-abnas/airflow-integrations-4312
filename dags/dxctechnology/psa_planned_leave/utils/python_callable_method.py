from datetime import datetime


def get_export_data():
    return {
        "export_name": 'DL_WORKER_LEAVE_' + datetime.now().strftime("%Y%m%d-%H%M%S"),
        "report_start_date": str(datetime.now().strftime("%m/%d/%Y"))
    }
