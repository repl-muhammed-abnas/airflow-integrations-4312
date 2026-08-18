import uuid


def reopen_timeentry_payload(dag_run):
    return {
        "timeEntryRevisionGroupUri": dag_run.conf['timeentryrevisionid'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Reopening of time entries by Replicon"
    }


def submit_timeentry_payload(dag_run):
    return {
        "timeEntryRevisionGroupUri": dag_run.conf['timeentryrevisionid'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Resubmission of time entries by Replicon"
    }
