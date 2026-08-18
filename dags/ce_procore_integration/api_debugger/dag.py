"""
### CE / Procore API Debugger

Ad-hoc utility DAG for making manual API calls to ComputerEase or Procore
using credentials already stored in Airflow connections.

Access is protected by Ed25519 signature verification — only someone with the
private key can trigger a valid run. The signature is verified inside every API
task, so it cannot be bypassed by re-running individual tasks.
Use `sign_conf.py` to sign your conf before triggering.

#### Usage
1. Sign your conf locally:
   ```
   python sign_conf.py --sign '{"api": "ce", "conn_id": "computerease_pmdemo1", "endpoint": "/catalog/job", "method": "GET", "query_params": {}, "body": {}}'
   ```
2. Paste the output JSON into the Airflow trigger UI as the DAG run conf.

**ComputerEase example conf (before signing)**
```json
{
    "api": "ce",
    "conn_id": "computerease_pmdemo1",
    "endpoint": "/catalog/job",
    "method": "GET",
    "query_params": { "status": "active" },
    "body": {}
}
```

**Procore example conf (before signing)**
```json
{
    "api": "procore",
    "conn_id": "procore_pmdemo1",
    "endpoint": "/companies/12345/projects",
    "method": "GET",
    "query_params": {},
    "body": {}
}
```

#### Notes
- `api`: set to `"procore"` to call Procore, anything else routes to ComputerEase.
- `query_params`: optional — used for GET filters.
- `body`: optional — used for POST / PUT / PATCH payloads.
- Pagination is disabled by default so you get a clean raw response.
- The result is visible in the task logs.
"""

import json
import base64
from datetime import datetime, timezone
import rail
from ce_procore_integration.api_debugger import config


def _verify_signature(dag_run):
    """
    Verifies the Ed25519 signature and expiry in dag_run.conf against the public key in config.
    Raises PermissionError if the signature is missing, invalid, or expired.
    Embedded directly in API task callables so it cannot be bypassed by re-running
    individual tasks.
    """
    from cryptography.hazmat.primitives.serialization import load_pem_public_key
    from cryptography.exceptions import InvalidSignature

    conf = dag_run.conf or {}
    signature_b64 = conf.get('signature')
    if not signature_b64:
        raise ValueError(
            "No signature found in dag_run.conf. "
            "Sign your conf using sign_conf.py before triggering."
        )

    expires_at = conf.get('expires_at')
    if not expires_at:
        raise ValueError(
            "No expires_at found in dag_run.conf. "
            "Re-sign your conf using sign_conf.py."
        )
    if datetime.fromtimestamp(expires_at, tz=timezone.utc) < datetime.now(tz=timezone.utc):
        raise PermissionError(
            f"Conf expired at {datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()}. "
            "Re-sign your conf using sign_conf.py before triggering."
        )

    payload_dict = {k: v for k, v in conf.items() if k != 'signature'}
    payload = json.dumps(payload_dict, sort_keys=True, separators=(',', ':')).encode()
    signature = base64.b64decode(signature_b64)

    public_key = load_pem_public_key(config.public_key_pem.strip().encode())
    try:
        public_key.verify(signature, payload)
    except InvalidSignature as exc:
        raise PermissionError("Invalid signature. DAG run rejected.") from exc


def create_dag_instance(cfg):
    with rail.create_airflow_dag(
        dag_id=cfg.dag_id,
        description='Ad-hoc API debugger for ComputerEase and Procore',
        schedule_interval=None,
        integration_type='generic',
        company_key=cfg.instance,
        start_date=datetime(2024, 1, 1),
        max_active_runs=cfg.max_active_runs,
        is_paused_upon_creation=True,
    ) as dag:

        which_api = rail.IfOperator(
            task_id='which_api',
            test=lambda dag_run: (dag_run.conf or {}).get('api') == 'procore',
            yes_task='call_procore_api',
            no_task='call_computerease_api',
        )

        def ce_endpoint(dag_run):
            _verify_signature(dag_run)
            return dag_run.conf.get('endpoint')

        def procore_endpoint(dag_run):
            _verify_signature(dag_run)
            return dag_run.conf.get('endpoint')

        call_computerease_api = rail.ComputereaseAPIOperator(
            task_id='call_computerease_api',
            computerease_conn_id='{{ dag_run.conf.conn_id }}',
            endpoint=ce_endpoint,
            request_method=lambda dag_run: dag_run.conf.get('method', 'GET'),
            query_params=lambda dag_run: dag_run.conf.get('query_params') or {},
            request_body=lambda dag_run: dag_run.conf.get('body') or None,
            paginate=False,
        )

        call_procore_api = rail.ProcoreApiOperator(
            task_id='call_procore_api',
            procore_conn_id='{{ dag_run.conf.conn_id }}',
            endpoint=procore_endpoint,
            method=lambda dag_run: dag_run.conf.get('method', 'GET'),
            query_params=lambda dag_run: dag_run.conf.get('query_params') or {},
            data=lambda dag_run: dag_run.conf.get('body') or None,
            paginate=False,
        )

        which_api >> rail.Label('Procore') >> call_procore_api
        which_api >> rail.Label('CE') >> call_computerease_api

        return dag


rail.for_each_instance(create_dag_instance)
