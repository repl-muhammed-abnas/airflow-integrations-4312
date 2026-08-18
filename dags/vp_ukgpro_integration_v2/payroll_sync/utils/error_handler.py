"""
Central error handler utilities for VP UKG Pro Payroll Sync v2.

Mirrors the `capture_*_error` pattern used in journal_sync v2
(`capture_main_error`) so that all v2 workflows share a consistent
final error-aggregation task.
"""
import rail


def capture_processor_error():
    """
    Final aggregation task for processor_dag.

    Reads the `log_failure` XCom (populated when any upstream task
    fails via trigger_rule='one_failed'). Raises if a failure was
    recorded so the DAG run is marked failed for downstream
    `GatherResultsFromDagRunsOperator` consumers.
    """
    try:
        failure = rail.result('log_failure')
    except Exception:  # pylint: disable=broad-except
        failure = None

    if not failure:
        return None

    reason = (
        failure.get('reason')
        if isinstance(failure, dict)
        else str(failure)
    )
    raise RuntimeError(
        f"Payroll processor v2 failed: {reason or 'unknown error'}"
    )


def capture_webhook_receiver_error():
    """
    Final aggregation task for webhook_receiver_dag.

    Reads `gather_processor_errors` XCom and raises if any errors
    were aggregated from the triggered processor DAG runs.
    """
    try:
        errors = rail.result('gather_processor_errors') or []
    except Exception:  # pylint: disable=broad-except
        errors = []

    if not errors:
        return None

    messages = []
    for err in errors:
        if isinstance(err, dict):
            messages.append(err.get('reason') or str(err))
        else:
            messages.append(str(err))

    raise RuntimeError(
        "Webhook receiver v2 errors: " + "; ".join(messages)
    )
