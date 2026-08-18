import rail
def get_list_of_subtasks(dag_run):
    overhead_investment_tasks = {
        "OVERHEAD": ["Study/Preparation", "Data (Migration,structure...)", 
                     "Roll out/deployment","Training", "Hypercare","Strategize", "RFP Activities",
                     "DA&AI Security & Compliance   (Ops Support Team only)", "Architecture Design",
                        "Architecture Review"],
        "INVESTMENT" : ["Design","Developments/Configuration/ Customisation","Integration/interface",
                        "Equipments  Installation","Release to production","E2E Tests/ UAT", "Architecture Design",
                        "Architecture Review"]
                                }
    return overhead_investment_tasks[dag_run.conf["objectclass"].upper()]


def data_field_errors(dag_run):
    msg = ""
    if not dag_run.conf["projectcode"]:
        msg =  "Project code is mandatory"
    elif not dag_run.conf["projectdescription"]:
        msg =  "Project description is mandatory"
    elif not dag_run.conf["companycode"]:
        msg =  "Company code is mandatory"
    elif not dag_run.conf["projectstatus"]:
        msg =  "Project status is mandatory"
    elif not dag_run.conf["controllingarea"]:
        msg =  "Controlling area is mandatory"
    elif not dag_run.conf["wbscode"]:
        msg =  "WBS code is mandatory"
    elif not  dag_run.conf["wbsdescription"]:
        msg =  "WBS description is mandatory"
    elif not dag_run.conf["wbsstatus"]:
        msg =  "WBS Status is mandatory"
    elif not dag_run.conf["costtype"]:
        msg =  "Cost type is mandatory"
    elif not dag_run.conf["objectclass"]:
        msg =  "Object class is mandatory"
    elif not dag_run.conf["bu"]:
        msg =  "BU is mandatory"
    return {
                "projectcode" : dag_run.conf["projectcode"],
                "projectdescription": dag_run.conf["projectdescription"],
                "JobID":'{{dag_run.conf.parent_ecid}}',
                "Task Code": dag_run.conf["wbscode"],
                "Status": "Exception",
                "Reason":msg,
                "Child jobid": rail.render_template('{{ecid()}}')
            }

def data_field_value_errors(dag_run,config):
    msg = ""
    if dag_run.conf["projectstatus"] not in  ["AVAILABLE", "CLOSED"]:
        msg =  "Invalid project status"
    elif dag_run.conf["wbsstatus"] not in  ["AVAILABLE","CLOSED"]:
        msg =  "Invalid WBS status"
    elif dag_run.conf["costtype"] not in ["OPEX","CAPEX"]:
        msg =  "Invalid cost type"
    elif dag_run.conf["originsystem"] not in (["PF1", "WP1"] if config.instance == "production" else ["PF2", "WP2"]):
        msg =  "Invalid Origin system"
    return {
            "projectcode" : dag_run.conf["projectcode"],
            "projectdescription": dag_run.conf["projectdescription"],
            "JobID":'{{dag_run.conf.parent_ecid}}',
            "Task Code": dag_run.conf["wbscode"],
            "Status": "Exception",
            "Reason":msg,
            "Child jobid": rail.render_template('{{ecid()}}')
        }
