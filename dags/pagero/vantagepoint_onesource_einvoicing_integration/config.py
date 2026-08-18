
max_active_runs = 1
region = 'us-east-1'
environment = 'pre-production'

initial_sync_time = '2025-12-12T00:00:00.000Z'

# Common settings
time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
execution_timeout_days = 1
flow2_schedule_interval = '@hourly'

# Country code for PUF conversion (optional — auto-detected from BuyerAddrCountryCode).
# Set explicitly when detection is ambiguous (e.g. EUR currency shared by many countries).
# country_code = None

# Country-specific supplier config overrides (optional).
# These fields are merged into the supplier_config from OneSource before PUF conversion.
# Set per-instance for countries that require extra fields beyond basic name/VAT/address.
#
# EXAMPLE — Italy:
#   supplier_config_overrides = {
#       'ufficio': 'MI',                   # Province registration code (2 chars)
#       'numero_rea': 'MI-1234567',        # Company register (REA) number
#       'capitale_sociale': '100000.00',   # Share capital
#       'socio_unico': 'SM',              # SM=Multiple shareholders, SU=Sole
#       'stato_liquidazione': 'LN',       # LN=Not in liquidation, LS=In liquidation
#   }
#
# EXAMPLE — Saudi Arabia:
#   supplier_config_overrides = {
#       'building_number': '1234',         # 4-digit building number (KSA-17)
#       'crn': '1234567890',              # Commercial Registration Number
#   }
#   (uuid is auto-generated per invoice if not provided)
#
# EXAMPLE — France:
#   supplier_config_overrides = {
#       'capital_social': '50000',
#       'rcs_number': 'RCS Paris B 123456789',
#       'ape_code': '6201Z',
#   }
#
# EXAMPLE — India:
#   supplier_config_overrides = {
#       'supply_type': 'B2B',             # B2B, B2C, B2G, SEZWP, SEZWOP, EXPWP, EXPWOP, DEXP
#       'pos_code': '07',                 # Place of supply state code (01-38)
#       'reverse_charge': 'N',            # Y or N
#   }
#
# EXAMPLE — Turkey:
#   supplier_config_overrides = {
#       'scenario': 'TEMELFATURA',        # TEMELFATURA, TICARIFATURA, etc.
#       'tax_office': 'Istanbul VD',
#   }
supplier_config_overrides = None
