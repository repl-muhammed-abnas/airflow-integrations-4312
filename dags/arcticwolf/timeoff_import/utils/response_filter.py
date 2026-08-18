import rail


def get_timeoff_booking_list(response, dag_run):
    data = response.json()["d"]

    filtered_data = list(
        map(
            lambda row: {
                "hours": row["cells"][2]["textValue"],
                "timeoffbookinguri": row["cells"][0]["uri"],
                "timeofftype": row["cells"][3]["uri"],
                "timeoffapprovalstatus": row["cells"][1]["textValue"],
                "timeoffname": row["cells"][3]["textValue"],
            },
            data["rows"],
        )
    )

    return {
        "timeoffbookinguri": rail.find_first_by_attr_and_get_attr(
            filtered_data,
            "timeoffname",
            dag_run.conf["timeofftype"],
            "timeoffbookinguri",
        ),
        "timeofftype": rail.find_first_by_attr_and_get_attr(
            filtered_data, "timeoffname", dag_run.conf["timeofftype"], "timeofftype"
        ),
        "timeoffapprovalstatus": rail.find_first_by_attr_and_get_attr(
            filtered_data,
            "timeoffname",
            dag_run.conf["timeofftype"],
            "timeoffapprovalstatus",
        ),
        "hours": rail.find_first_by_attr_and_get_attr(
            filtered_data, "timeoffname", dag_run.conf["timeofftype"], "hours"
        ),
        "timeoffname": rail.find_first_by_attr_and_get_attr(
            filtered_data, "timeoffname", dag_run.conf["timeofftype"], "timeoffname"
        ),
    }
