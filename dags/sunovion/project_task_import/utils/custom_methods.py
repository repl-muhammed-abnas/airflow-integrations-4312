import pendulum
import rail


def get_project_records(dag_run):
    return list(filter(lambda x: x['projectcode'] == dag_run.conf['projectcode'], dag_run.conf['data']))

def logging_details():
    current_time = pendulum.now().strftime("%m_%d_%Y_T%H_%M_%S")
    input_filename = rail.result("new_file_sensor").split("/")[-1].split(".")[0]
    return {
        "log_filename": "Logs_SunovionProjectTaskImport_" + current_time + ".csv",
        "input_archive_filename": input_filename + "_" + current_time + ".csv",
        "current_time": current_time
    }
