import json
import os
import pathlib
from airflow.models import DAG
import rail

__version__ = os.environ.get('AIRFLOW_VAR_DAG_BUILD_VERSION', '0+dev')


change_history = pathlib.Path(__file__, '..', 'change_history.json').resolve()
if change_history.exists():
    HISTORY = json.loads(change_history.read_text())
else:
    HISTORY = {}

_VERSION_MARKER = '#### Version Info'


# pylint: disable=line-too-long
def apply_version_info_to_dag(dag):
    """Set the version info + revision/code links footer on a single DAG's doc_md.

    Idempotent — a no-op if the marker is already present. This lets the same
    logic run twice safely: once via the baked-image build-time hook (older
    deployments) and once via RAIL's runtime dag_policy hook (git-sync
    deployments). Whichever runs first wins; the second sees the marker and skips.
    """
    if dag.doc_md and _VERSION_MARKER in dag.doc_md:
        return
    dag.doc_md = f'{dag.doc_md}\n\n&nbsp;\n' if dag.doc_md else ''
    dag.doc_md += f'{_VERSION_MARKER}  \nintegrations: {__version__}  \nrail: {rail.__version__}'
    history = HISTORY.get(dag.dag_id, None)
    if history:
        dag.doc_md += f'\n\n&nbsp;\n{history}'
    else:
        dag.doc_md += f'\n\n#### [Revision History](https://github.com/replicon/airflow-integrations/commits/main/dags/{dag.relative_fileloc})'
        dag.doc_md += f'\n\n#### [Code](https://github.com/replicon/airflow-integrations/blob/main/dags/{dag.relative_fileloc})'


# pylint: disable=line-too-long
def append_version_info(airflow_globals):
    """Legacy build-hook entry point — kept for backward compat with
    baked-image deployments where install_version_doc_hooks.py appends
    a call to this at the bottom of every DAG file."""
    for d in airflow_globals.values():
        if isinstance(d, DAG):
            apply_version_info_to_dag(d)
