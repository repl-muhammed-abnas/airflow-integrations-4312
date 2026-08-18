# This module is intentionally minimal — dynamic config resolution was
# consolidated into IntegrationConfig.get_conn_ids / get_s3_customer in
# common/python_callable_method.py. The per-recipe shims that re-exported
# extract_dynamic_config_from_dag_run have been removed.
