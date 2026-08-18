import rail


def get_project_update_status():
    if rail.result("check_if_project_name_updated") == "update_project_name" or\
            rail.result("if_project_status_progress") == "update_project_status_progress" or\
            rail.result("if_project_status_completed") == "update_project_status_closed":
        return True
    if rail.result("if_prevailing_wages_present") == "get_prevailing_wage_drop_down_options" or\
            rail.result("if_project_leader_updated") == "update_project_leader" or\
            rail.result("if_comanager_present_in_replicon") == "get_explicit_sharing_assignments":
        return True
    return False
