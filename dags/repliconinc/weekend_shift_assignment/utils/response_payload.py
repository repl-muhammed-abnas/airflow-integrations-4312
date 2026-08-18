def get_user_details(response):
    if not response:
        return []

    return list(
        filter(
            lambda x: x["employeeid"],
            list(
                map(
                    lambda item: {
                        "uri": item["userDetails"]["uri"],
                        "employeeid": item["userDetails"]["employeeId"],
                        "name": item["userDetails"]["displayText"],
                        "emailaddress": item["userDetails"]["emailAddress"],
                    },
                    response,
                )
            ),
        )
    )


def get_all_shift_assigment(response):
    if not response:
        return None

    formatted_data = []
    for item in response:
        formatted_data.append(item["assignmentUri"])

    return formatted_data
