
from datetime import timedelta
import itertools
import uuid
from airflow.models import Variable
import rail

null = None


def _group_by_wbs_branch(items, boundary_level):
    """Filter items below the WBS boundary and group them by branch (PROJ_ID prefix at boundary+1 depth)."""
    branch_depth = int(boundary_level or 0) + 1
    groups = {}
    for item in (items or []):
        data = (item.get('row', {}).get('data', {}) or {})
        lvl = int(data.get('LVL_NO', 1) or 1)
        if lvl < branch_depth:
            continue
        branch_key = '.'.join(str(data.get('PROJ_ID', '')).split('.')[:branch_depth])
        groups.setdefault(branch_key, []).append(item)
    return list(groups.values())


def _get_chargeable_branch_paths(items, boundary_level, require_chargeable_leaf=True):
    """For each WBS branch, return the items to sync.

    When require_chargeable_leaf is True (default): a branch is kept only when it
    contains at least one ALLOW_CHARGES_FL='Y' item; only the path from each
    chargeable node up to its boundary ancestor is returned.

    When require_chargeable_leaf is False: a branch is kept when its boundary node
    has ACTIVE_FL='Y'; the full branch is returned without path-trimming.
    """
    result = []
    for branch in _group_by_wbs_branch(items, boundary_level):
        if require_chargeable_leaf:
            charge_ids = [
                str((it.get('row', {}).get('data', {}) or {}).get('PROJ_ID', ''))
                for it in branch
                if (it.get('row', {}).get('data', {}) or {}).get('ALLOW_CHARGES_FL') == 'Y'
            ]
            if not charge_ids:
                continue
            kept = []
            for it in branch:
                pid = str((it.get('row', {}).get('data', {}) or {}).get('PROJ_ID', ''))
                if any(cid == pid or cid.startswith(pid + '.') for cid in charge_ids):
                    kept.append(it)
            if kept:
                result.append(kept)
        else:
            # Gate on boundary-node Active status only; include full branch without path-trimming.
            # The boundary node is the item at LVL_NO == boundary_level + 1; if it is absent
            # from the export the branch is skipped rather than silently mis-gating on a deeper row.
            expected_lvl = boundary_level + 1
            boundary_node = next(
                (it for it in branch
                 if int((it.get('row', {}).get('data', {}) or {}).get('LVL_NO', 0) or 0) == expected_lvl),
                None
            )
            if boundary_node is None:
                continue
            if (boundary_node.get('row', {}).get('data', {}) or {}).get('ACTIVE_FL') == 'Y':
                result.append(branch)
    return result


def _build_wbs_boundary_sync_payload(items, boundary_level, require_chargeable_leaf=True):
    """Build the wbs_boundary_sync payload.

    ``boundary_level`` is the WBS level whose nodes become the top-level Polaris
    projects; items below that level become their tasks.

    When ``require_chargeable_leaf`` is True: items strictly above the boundary
    that are themselves chargeable (ALLOW_CHARGES_FL='Y') are emitted as bare
    items so that rare directly-chargeable root nodes are still synced. Below
    the boundary, only branches with at least one chargeable leaf are included,
    trimmed to the path from each chargeable node up to its boundary ancestor.

    When ``require_chargeable_leaf`` is False: no above-boundary bare items are
    emitted (the boundary branches are the sole sync unit — emitting roots
    separately would create a task-less Polaris project on every run). Below
    the boundary, branches are included whenever their boundary node is Active
    (ACTIVE_FL='Y'); the full branch is returned without path-trimming.
    """
    boundary = max(int(boundary_level or 1) - 1, 1)
    # Above-boundary bare items only apply to the chargeable-leaf mode: in that mode a rare
    # chargeable root (LVL_NO <= boundary) must still be synced on its own. When
    # require_chargeable_leaf is False the branch paths already cover every active
    # boundary-level node; emitting level-1 roots separately would create a task-less
    # top-level Polaris project on every run.
    above_boundary_items = (
        [
            item for item in (items or [])
            if int((item.get('row', {}).get('data', {}) or {}).get('LVL_NO', 1) or 1) <= boundary
            and (item.get('row', {}).get('data', {}) or {}).get('ALLOW_CHARGES_FL') == 'Y'
        ]
        if require_chargeable_leaf else []
    )
    return above_boundary_items + _get_chargeable_branch_paths(items, boundary, require_chargeable_leaf)


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_pick_chose_project_child_{config.instance}',
        description=f'deltek_costpoint_pick_chose_project_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        },
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log",
        )

        get_costpoint_projects = rail.DeltekCostPointServiceOperator(
            task_id='get_costpoint_projects',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company="{{ dag_run.conf.item.data[0] | attr_or_default('_company') | sn }}",
            data={
                "filter": {
                    "id": "polaris_exp_project",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJMBASIC_PROJ",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "like%",
                                                "value": "{{ dag_run.conf.item.root_project_id }}"
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
        )

        enable_wbs_boundary_sync_check = rail.IfOperator(
            task_id='enable_wbs_boundary_sync_check',
            test=lambda: bool(getattr(config, 'enable_wbs_boundary_sync', False)),
            yes_task='wbs_boundary_sync',
            no_task='filter_allow_charges_projects'
        )

        def _require_chargeable_leaf():
            """Single validated read of require_chargeable_leaf_in_hierarchy for this run."""
            val = getattr(config, 'require_chargeable_leaf_in_hierarchy', True)
            if not isinstance(val, bool):
                raise ValueError(
                    f"require_chargeable_leaf_in_hierarchy must be True or False, got: {val!r}"
                )
            return val

        def _do_wbs_boundary_sync():
            return _build_wbs_boundary_sync_payload(
                rail.result('get_costpoint_projects'),
                getattr(config, 'wbs_sync_boundary_level', 0),
                _require_chargeable_leaf(),
            )

        wbs_boundary_sync = rail.PythonOperator(
            task_id='wbs_boundary_sync',
            python_callable=_do_wbs_boundary_sync,
        )

        filter_allow_charges_projects = rail.PythonOperator(
            task_id='filter_allow_charges_projects',
            python_callable=lambda: [
                item for item in (rail.result('get_costpoint_projects') or [])
                if (item.get('row', {}).get('data', {}) or {}).get('ALLOW_CHARGES_FL') == 'Y'
            ],
        )

        trigger_polaris_project_sync_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_polaris_project_sync_child',
            trigger_rule='none_failed_min_one_success',
            retries=0,
            reset_count=10000,
            items=lambda: rail.result('wbs_boundary_sync') or rail.result('filter_allow_charges_projects') or [],
            trigger_dag_id=f'deltek_costpoint_polaris_project_sync_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'item': {
                    'root_project_id': (item[0] if isinstance(item, list) else item)['row']['data']['PROJ_ID'],
                    '_company': rail.get_dag_run_conf()['item']['data'][0].get('_company'),
                    'data': item if isinstance(item, list) else [item],
                },
                'billing_rates': rail.get_dag_run_conf().get('billing_rates'),
                'divisions': rail.get_dag_run_conf().get('divisions'),
                'permission_sets': rail.get_dag_run_conf().get('permission_sets'),
                'project_udfs': rail.get_dag_run_conf().get('project_udfs'),
                'task_udfs': rail.get_dag_run_conf().get('task_udfs'),
            }
        )

        wait_for_polaris_project_sync_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_polaris_project_sync_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_polaris_project_sync_child") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_log >> get_costpoint_projects >> enable_wbs_boundary_sync_check
        enable_wbs_boundary_sync_check >> rail.Label(
            'Yes') >> wbs_boundary_sync >> trigger_polaris_project_sync_child
        enable_wbs_boundary_sync_check >> rail.Label(
            'No') >> filter_allow_charges_projects >> trigger_polaris_project_sync_child
        trigger_polaris_project_sync_child >> wait_for_polaris_project_sync_child >> \
            finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
