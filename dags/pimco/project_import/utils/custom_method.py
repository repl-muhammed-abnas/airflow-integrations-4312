import rail

def validate_project(dag_run):
    if not dag_run.conf['Projectname'] or not dag_run.conf['Projectcode'] or not dag_run.conf['flag'] or dag_run.conf['flag'] not in ['N', 'Y']:
        return True
    return False

def get_log_message(dag_run):
    msg = []
    if not dag_run.conf['Projectname']:
        msg.append("Project Name not present in feed file" )
    if not dag_run.conf['Projectcode']:
        msg.append("Project Code not present in feed file")
    if not dag_run.conf['flag']:
        msg.append("Flag not present in feed file")
    if dag_run.conf['flag'] not in ['N', 'Y']:
        msg.append("Flag is not 'N' or 'Y'")
    return rail.smartjoin_by_delim(msg, ';')
