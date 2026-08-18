from datetime import timedelta
import pendulum as pndlum

def logging_details(config):
    return {
        "dag_run_start_time": str(pndlum.now(config.time_zone).strftime("%Y-%m-%d %H:%M:%S %z")),
        "output_filename": '_PwC_replicontimeexport_' + str((pndlum.now(config.time_zone)-timedelta(days=1)).strftime("%m%d%Y")) + '.csv'
    }
