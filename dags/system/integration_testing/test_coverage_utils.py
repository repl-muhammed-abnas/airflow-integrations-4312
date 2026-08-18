# pylint:disable=too-many-locals


def collect_dag_run_coverage(session, root_dagrun_ids, max_depth=5):
    """
    Recursively collects task coverage across a full DAG hierarchy.

    Traverses from root DAG runs down through all child/nested DAG runs by
    following XCom return values from trigger operators (TriggerDagRunOperator
    and TriggerDagRunForEachItemOperator both push DagRun.id integers into XCom).

    :param session: SQLAlchemy session (from create_session or provide_session)
    :param root_dagrun_ids: list of DagRun.id integers for the top-level triggered DAGs
    :param max_depth: max recursion depth to guard against cycles
    :returns: dict keyed by dag_id → {'all': set(task_ids), 'executed': set(task_ids)}
    """
    from airflow.models import DagRun, TaskInstance, XCom

    coverage = {}
    seen_ids = set()

    def _process(dagrun_id, depth):
        if depth > max_depth or dagrun_id in seen_ids:
            return
        seen_ids.add(dagrun_id)

        dag_run = session.query(DagRun).get(dagrun_id)
        if not dag_run:
            return

        dag_id = dag_run.dag_id
        run_id = dag_run.run_id

        tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
        ).all()

        if dag_id not in coverage:
            coverage[dag_id] = {'all': set(), 'executed': set()}

        for ti in tis:
            coverage[dag_id]['all'].add(ti.task_id)
            if ti.state == 'success':
                coverage[dag_id]['executed'].add(ti.task_id)

        # Follow child DAG runs stored as DagRun.id integers in XCom by trigger operators
        xcoms = session.query(XCom).filter(
            XCom.dag_id == dag_id,
            XCom.run_id == run_id,
            XCom.key == 'return_value',
        ).all()

        for xcom in xcoms:
            try:
                value = xcom.value
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, int):
                            _process(item, depth + 1)
                elif isinstance(value, int):
                    _process(value, depth + 1)
            except Exception:  # pylint:disable=broad-except
                pass

    for dagrun_id in root_dagrun_ids:
        _process(dagrun_id, 0)

    return coverage


def build_coverage_report(coverage):
    """
    Converts raw coverage dict into a report-ready list and missed task details.

    :param coverage: output of collect_dag_run_coverage
    :returns: (report_rows, missed_task_details)
      report_rows: list of dicts with dag_id, total_tasks, executed_tasks, missed_tasks, coverage_pct
      missed_task_details: sorted list of dicts {dag_id, task_id} for every task never executed
    """
    report = []
    all_missed = []
    for dag_id, data in coverage.items():
        total = len(data['all'])
        executed_count = len(data['executed'])
        missed = sorted(data['all'] - data['executed'])
        pct = round(executed_count / total * 100, 1) if total > 0 else 0.0
        report.append({
            'dag_id': dag_id,
            'total_tasks': total,
            'executed_tasks': executed_count,
            'missed_tasks': len(missed),
            'coverage_pct': pct,
        })
        report[-1]['missed_task_ids'] = ', '.join(missed) if missed else '-'
    return report, []


def collect_scenario_results(session, test_dag_id, scenarios):
    """
    Checks the pass/fail state of each scenario's assert task from Airflow metadata DB.

    :param session: SQLAlchemy session
    :param test_dag_id: DAG ID of the test DAG itself
    :param scenarios: list of dicts, each with keys:
        - name: display name (e.g. 'Scenario 1')
        - description: human-readable description
        - assert_task_id: task_id of the assertion task in the test DAG
        - trigger_task_id: task_id of the trigger operator (used to get dag_run_ids for coverage)
    :returns: (scenario_rows, all_passed, root_dagrun_ids)
      scenario_rows: list of dicts with name, description, status, error
      all_passed: True if all assert tasks were in 'success' state
      root_dagrun_ids: flat list of DagRun.id integers from all trigger tasks (for coverage)
    """
    from airflow.models import TaskInstance, DagRun
    import rail

    scenario_rows = []
    all_passed = True
    root_dagrun_ids = []

    for s in scenarios:
        # Check assert task state — join DagRun to order by execution_date correctly
        ti = (session.query(TaskInstance)
              .join(DagRun, (DagRun.dag_id == TaskInstance.dag_id) & (DagRun.run_id == TaskInstance.run_id))
              .filter(TaskInstance.dag_id == test_dag_id,
                      TaskInstance.task_id == s['assert_task_id'])
              .order_by(DagRun.execution_date.desc())
              .first())

        state = ti.state if ti else 'not_run'
        if state != 'success':
            all_passed = False

        scenario_rows.append({
            'name': s['name'],
            'description': s['description'],
            'status': state.upper() if state else 'NOT RUN',
            'error': f"Task {s['assert_task_id']} {state} - check Airflow logs" if state not in ('success',) else None,
        })

        # Collect root DAG run IDs from the trigger task XCom
        trigger_result = rail.result(s['trigger_task_id']) or []
        if not isinstance(trigger_result, list):
            trigger_result = [trigger_result]
        for item in trigger_result:
            if isinstance(item, int):
                root_dagrun_ids.append(item)

    return scenario_rows, all_passed, root_dagrun_ids
