"""
config.py
---------
Pipeline-level configuration for the Ajera → VantagePoint migration.

Defines CSV_EXTRACTIONS — the list of (sql_key, output_filename) pairs consumed by
the master DAG to fan out one extract_csv child DAG per output file.
sql_key values must match keys in sql/Vantagepoint/csv_extraction/csv_extract_queries.py::SQL_MAP.
"""

region = 'us-east-1'
environment = 'pre-production'


# === CSV Extractions ===
# Each entry maps a sql_key (defined in sql_mapper.SQL_MAP) to an output CSV filename.
csv_extractions = [
    {'sql_key': 'contact',        'output_filename': 'Contact.csv'},
    {'sql_key': 'client',         'output_filename': 'Client.csv'},
    {'sql_key': 'client_address', 'output_filename': 'ClientAddress.csv'},
    {'sql_key': 'vendor',         'output_filename': 'Vendor.csv'},
    {'sql_key': 'vendor_address', 'output_filename': 'VendorAddress.csv'},
]

