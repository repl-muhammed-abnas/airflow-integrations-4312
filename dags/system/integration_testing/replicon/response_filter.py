import itertools
import json
import rail


def get_empid_by_loginname(response, dag_run):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    user_empids = (
        [
            item["cells"][1].get("textValue")
            for item in flatten_rows
            if item["cells"][0]["textValue"] == dag_run.conf["loginname"]
        ]
        if flatten_rows
        else []
    )
    return rail.smartjoin_by_delim(user_empids) if user_empids else ""


def get_filtered_employee_grp(response):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    return list(
        map(
            lambda row: {
                "name": row["cells"][0]["textValue"],
                "uri": row["cells"][0]["uri"],
                "code": row["cells"][1]["textValue"],
            },
            flatten_rows
        )
    )
