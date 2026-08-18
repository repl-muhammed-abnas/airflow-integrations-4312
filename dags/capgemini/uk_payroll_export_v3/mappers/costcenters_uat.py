# costcenters_dev.py - Final version

# cost center hierarchy levels are as per replicon starting with level 0

# Original individual mapping - keep this for backward compatibility
cost_centers = {
    "SBU NCE | SBU-00001": "1",
    "SBU FS | SBU-00002": "1",
    "SBU AMERICAS | SBU-00003": "1",
    "ABL NCE | ABL-00001": "1",
    "GBL BSV | GBL-00001": "1",
    "GBL CIS | GBL-00002": "1",
    "GBL I_D | GBL-00003": "1",
    "GBL INVENT | GBL-00004": "1",
    "GBL ERD | GBL-00005": "1",
    "COMCOR_Invest | GLO-00001": "1",
    "OTHER_UNITS | GLO-00002": "1",
    "OTHER DEL UNIT | GLO-00003": "1",
    "GB01 - Capgemini UK Plc.(FS) | GB01": "2"
}

# New grouped structure for consolidation
cost_center_groups = {
    "NonFS": {
        "hierarchy_level": "1",
        "cost_centers": [
            "SBU NCE | SBU-00001",
            "SBU FS | SBU-00002",
            "SBU AMERICAS | SBU-00003",
            "ABL NCE | ABL-00001",
            "GBL BSV | GBL-00001",
            "GBL CIS | GBL-00002",
            "GBL I_D | GBL-00003",
            "GBL INVENT | GBL-00004",
            "GBL ERD | GBL-00005",
            "COMCOR_Invest | GLO-00001",
            "SBU Shared Services | GLO-00002",
            "OTHER DEL UNIT | GLO-00003"
        ]
    },
    "FS": {
        "hierarchy_level": "2",
        "cost_centers": [
            "GB01 - Capgemini UK Plc.(FS) | GB01"
        ]
    }
}