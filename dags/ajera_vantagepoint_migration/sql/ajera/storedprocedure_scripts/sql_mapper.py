"""
sql_mapper.py
-------------
Registry mapping string keys to Ajera → VantagePoint SP migration SQL scripts.

SP_SQL_MAP is consumed by process_sp_scripts.py; keys must match op_kwargs['sql_key'] values:
    '001_list_insert'        — VP lookup table inserts (BTLaborCats, CFGClientType, CFGProjectType)
    '002_state_country'      — State/Country standardisation in Ajera source tables
    '003_cnv_client'         — Populates cnvClient staging table
    '004_cnv_client_address' — Populates cnvClientAddress staging table
    '005_cnv_vendor'         — Populates cnvVendor staging table
    '006_cnv_vendor_address' — Populates cnvVendorAddress staging table
    '007_cnv_contact'        — Populates cnvContact staging table
"""

from ajera_vantagepoint_migration.sql.ajera.storedprocedure_scripts.sp_001_list_insert import SP_001_SQL
from ajera_vantagepoint_migration.sql.ajera.storedprocedure_scripts.sp_002_state_country import SP_002_SQL
from ajera_vantagepoint_migration.sql.ajera.storedprocedure_scripts.sp_003_cnv_client import SP_003_SQL
from ajera_vantagepoint_migration.sql.ajera.storedprocedure_scripts.sp_004_cnv_client_address import SP_004_SQL
from ajera_vantagepoint_migration.sql.ajera.storedprocedure_scripts.sp_005_cnv_vendor import SP_005_SQL
from ajera_vantagepoint_migration.sql.ajera.storedprocedure_scripts.sp_006_cnv_vendor_address import SP_006_SQL
from ajera_vantagepoint_migration.sql.ajera.storedprocedure_scripts.sp_007_cnv_contact import SP_007_SQL

sp_sql_map = {
    '001_list_insert':        SP_001_SQL,
    '002_state_country':      SP_002_SQL,
    '003_cnv_client':         SP_003_SQL,
    '004_cnv_client_address': SP_004_SQL,
    '005_cnv_vendor':         SP_005_SQL,
    '006_cnv_vendor_address': SP_006_SQL,
    '007_cnv_contact':        SP_007_SQL,
}
