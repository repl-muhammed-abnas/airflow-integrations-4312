region = 'all'
environment = 'all'

dag_id = 'ce_procore_api_debugger'
max_active_runs = 5
execution_timeout_hours = 1

# Ed25519 public key (PEM format) for verifying signed DAG run configs.
# Run sign_conf.py --generate to create your key pair, then paste the public key here.
public_key_pem = """-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAQCu3n7fBLhx0NbAViRp6eIRQINZBxXcmWtBHNiSzLpI=
-----END PUBLIC KEY-----"""
