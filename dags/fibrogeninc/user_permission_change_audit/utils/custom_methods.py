import rail
import pendulum
from rail.lib.ecid import get_dagrun_ecid

null = None

def get_logging_details():
    return {
        "dag_run_time": pendulum.now("America/New_York").isoformat(),
        "filename": "userlistdata_" + get_dagrun_ecid(rail.get_current_context()['dag_run']) + "_"
                        + pendulum.now("America/New_York").strftime("%Y%m%dT%H%M%S") + ".csv",
        "changed_date_value": str(pendulum.now("America/New_York").strftime("%m/%d/%Y %I:%M:%S %p")),
        "log_filename": 'Permission_change_audit_logs_'+ pendulum.now().strftime("%Y%m%d%H%M%S")+'.csv'
    }

def get_reference_file_name():
    return " ".join(list(map(lambda filepath: filepath.split("/")[-1],
                filter(lambda filepath: filepath.split("/")[-1].startswith("userlistdata"),
                    rail.result("list_reference_files"))))) if " ".join(list(map(lambda filepath: filepath,
                        filter(lambda filepath: filepath.split("/")[-1].startswith("userlistdata"),
                               rail.result("list_reference_files"))))) else null
