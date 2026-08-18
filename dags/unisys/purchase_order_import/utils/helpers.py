def page_handler(request, result):
    if len(result["rows"]) > 0:
        request["page"] += 1
        return request
    return None