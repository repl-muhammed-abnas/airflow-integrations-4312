def filter_timeoff_types(response):

    return list(map(lambda row:
        {
            "timeoff_name": row["cells"][0]["textValue"],
            "status": row["cells"][2]["textValue"],
            "timeoff_uri": row["cells"][0]["uri"]
        }, response.json()["d"]["rows"]))
