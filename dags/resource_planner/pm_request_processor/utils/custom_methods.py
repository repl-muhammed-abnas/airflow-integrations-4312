"""
Converts a Replicon 'childTasks' read-tree into the flat 'taskHierarchy'
create/save payload used by the task modification API.

Rule: a node's create-payload `target` is null only if the node itself has no
parent in the source tree (i.e. it's a direct child of the project). Otherwise
`target.parent` is a {name, parent, project} chain built from the node's own
`task['parent']` field -- `project` attaches only at the ancestor that is
itself top-level. This holds regardless of whether the node is the root you
passed in or several levels deep, so picking any task as the starting point
(top-level or not) "just works."
"""

from copy import deepcopy

SAVE_URI = "urn:replicon:task-modification-option:save"


def _project_ref(info):
    return {
        "uri": None,
        "name": info.get("name"),
        "code": None,
        "parameterCorrelationId": None,
    }


def _ancestor_ref(read_parent, project_ref):
    """Recursively turns a read-format parent/parentTask node into the
    create-format {name, parent, project} ancestor descriptor. None in ->
    None out (that ancestor is top-level, nothing further up but the project)."""
    if read_parent is None:
        return None
    parent_ref = _ancestor_ref(read_parent.get("parentTask"), project_ref)
    return {
        "uri": None,
        "name": read_parent["task"]["name"],
        "parent": parent_ref,
        "project": deepcopy(project_ref) if parent_ref is None else None,
    }


def _target(task, project_ref):
    read_parent = task.get("parent")
    if read_parent is None:
        return None  # top-level task, direct child of the project
    return {
        "uri": None,
        "name": None,
        "parent": _ancestor_ref(read_parent, project_ref),
        "project": None,
    }


def _task_modification(task):
    date_range = task.get("timeEntryDateRange") or {}
    tae_uri = (task.get("timeAndExpenseEntryType") or {}).get("uri")
    code, desc = task.get("code"), task.get("description")
    start, end = date_range.get("startDate"), date_range.get("endDate")
    hours, cost = task.get("estimatedHours"), task.get("estimatedCost")

    return {
        "name": task["name"],
        "codeToApply": {"value": code} if code else None,
        "descriptionToApply": {"value": desc} if desc else None,
        "isClosed": "1" if task.get("isClosed") else "0",
        "timeEntryStartDateToApply": {"date": start} if start else None,
        "timeEntryEndDateToApply": {"date": end} if end else None,
        "timeAndExpenseEntryTypeToApply": {"value": tae_uri} if tae_uri else None,
        "isTimeEntryAllowed": "1" if task.get("isTimeEntryAllowed") else "0",
        # NOTE: costType / customFields / keyValues left blank -- no confirmed
        # *ToApply schema for these yet. Wire up here once you have a real example.
        "costTypeToApply": None,
        "estimatedHoursToApply": (
            {"duration": {"hours": str(hours["hours"]), "minutes": 0, "seconds": 0}}
            if hours else None
        ),
        "estimatedCostToApply": {"value": cost} if cost else None,
        "resourceAssignmentModifications": None,
        "resourceTaskAssignmentModifications": None,
        "customFieldsToApply": [],
        "keyValuesToApply": [],
        "objectExtensionFieldsToApply": [],
    }


def _flatten(nodes, project_ref, node_filter):
    entries = []

    def walk(node):
        if node_filter and not node_filter(node):
            return
        task = node["task"]
        entries.append({
            "target": _target(task, project_ref),
            "parameterCorrelationId": None,
            "taskModificationToApply": _task_modification(task),
        })
        for child in node.get("childTasks", []):
            walk(child)

    for n in nodes:
        walk(n)
    return entries


def find_node_by_name(tree_root, task_name):
    """DFS lookup: returns the node whose task['name'] == task_name (tree_root
    itself, or any descendant), or None if not found."""
    if tree_root["task"]["name"] == task_name:
        return tree_root
    for child in tree_root.get("childTasks", []):
        found = find_node_by_name(child, task_name)
        if found is not None:
            return found
    return None


def build_task_hierarchy_payload(root, unit_of_work_id, project_override=None, node_filter=None):
    """
    root: a single read-tree node (has 'task' + 'childTasks'), or a list of
          sibling root nodes. Whatever you pass becomes the top of the
          flattened list.
    unit_of_work_id: string stamped onto the outer payload.
    project_override: optional {'name': ..., 'code': ...} to force the project
          ref instead of reading it off the first root's task['project'].
    node_filter: optional predicate fn(node) -> bool; return False to skip a
          node (and everything under it).

    Returns the full creation payload dict, ready to send to the
    taskModificationOptionUri:save endpoint.
    """
    nodes = root if isinstance(root, list) else [root]
    project_ref = _project_ref(project_override or nodes[0]["task"].get("project") or {})
    return {
        "project": project_ref,
        "taskHierarchy": _flatten(nodes, project_ref, node_filter),
        "taskModificationOptionUri": SAVE_URI,
        "unitOfWorkId": unit_of_work_id,
    }


def _duration(hours):
    return {"hours": hours, "minutes": 0, "seconds": 0}


def _force_open(node):
    """Recursively forces isClosed=False throughout node and every
    descendant. Production template placeholders are closed/disabled by
    design, but every task actually created from them must be open."""
    node["task"]["isClosed"] = False
    for child in node.get("childTasks", []):
        _force_open(child)


def _replace_ancestor(read_parent, old_name, new_ancestor):
    """Recursively substitutes an `old_name`-named ancestor anywhere in a
    read-parent chain with `new_ancestor`, leaving the rest of the chain
    (and everything below the match) untouched."""
    if read_parent is None:
        return None
    if read_parent["task"]["name"] == old_name:
        return new_ancestor
    return {
        "parentTask": _replace_ancestor(read_parent.get("parentTask"), old_name, new_ancestor),
        "task": read_parent["task"],
    }


def _rewrite_ancestor(node, old_name, new_ancestor):
    """Recursively rewrites node's own embedded task['parent'] chain, and
    every descendant's, replacing an `old_name`-named ancestor with
    `new_ancestor` wherever it appears in that chain."""
    node["task"]["parent"] = _replace_ancestor(node["task"].get("parent"), old_name, new_ancestor)
    for child in node.get("childTasks", []):
        _rewrite_ancestor(child, old_name, new_ancestor)


def build_estimation_hierarchy_node(reference_placeholder, conf):
    """
    Builds a brand-new task-tree node named conf['estimation_name'], cloned
    from the shape of `reference_placeholder` (the real reference task found
    under DPS TCoE, e.g. "Custom work - Placeholder 1") WITHOUT ever
    modifying reference_placeholder itself -- everything here is read-only
    with respect to it; only deep copies are mutated.

    Only the "Execution" and/or "Design" branch(es) whose effort hours
    (conf['effort_hours_execution'] / conf['effort_hours_design']) are
    present are kept, each carrying the real requested hours in place of the
    template's own (blank) estimatedHours -- the other branch, if its hours
    are missing, is left out of the result entirely, along with everything
    under it.

    isClosed is forced to False (open) throughout the entire result --
    production template placeholders are closed/disabled by design, but
    every task actually created from them must be usable.

    Task name: ``"<rp_request_number> | <estimation_name>"`` (max 254 chars).
    Falls back to ``estimation_name`` alone if ``rp_request_number`` is absent.
    Task code: ``conf['jira_id']`` if present, else None (no codeToApply sent).

    Raises ValueError if neither effort_hours_execution nor
    effort_hours_design is present in conf, or if a requested branch isn't
    found under reference_placeholder.
    """
    effort_hours_execution = conf.get("effort_hours_execution")
    effort_hours_design = conf.get("effort_hours_design")
    if not effort_hours_execution and not effort_hours_design:
        raise ValueError(
            "Both effort_hours_execution and effort_hours_design are missing "
            "-- nothing to build."
        )

    estimation_name = conf["estimation_name"]
    rp_request_number = conf.get("rp_request_number") or ""
    jira_id = conf.get("jira_id") or None

    # Task name format: "<RP_INTAKE_NUMBER> | <estimation_name>", max 254 chars.
    # Falls back to estimation_name alone if rp_request_number is absent.
    task_name = (
        f"{rp_request_number} | {estimation_name}" if rp_request_number else estimation_name
    )[:254]

    reference_task = reference_placeholder["task"]

    new_task = deepcopy(reference_task)
    new_task["name"] = task_name
    new_task["code"] = jira_id   # codeToApply in the create payload
    new_task["isClosed"] = False

    new_ancestor = {
        "parentTask": deepcopy(reference_task.get("parent")),
        "task": {
            "uri": None, "code": None, "displayText": None,
            "parameterCorrelationId": None, "name": task_name,
        },
    }

    branches = []
    for branch_name, hours in (("Execution", effort_hours_execution), ("Design", effort_hours_design)):
        if not hours:
            continue
        branch_node = find_node_by_name(reference_placeholder, branch_name)
        if branch_node is None:
            raise ValueError(
                f"Reference task {reference_task['name']!r} has no {branch_name!r} "
                f"branch to clone -- cannot apply effort_hours_{branch_name.lower()}."
            )
        branch_copy = deepcopy(branch_node)
        branch_copy["task"]["estimatedHours"] = _duration(hours)
        _force_open(branch_copy)
        _rewrite_ancestor(branch_copy, reference_task["name"], new_ancestor)
        branches.append(branch_copy)

    return {"childTasks": branches, "resourceAssignments": [], "task": new_task}
