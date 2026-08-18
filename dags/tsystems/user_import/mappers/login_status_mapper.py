# Usage Guide for login_status_mapper:
# 1. cost_center: [] = Apply to all cost centers
# 2. cost_center: [specific_list] = Apply only to these specific cost centers  
# 3. cost_center_exclude: [specific_list] = Apply to all cost centers EXCEPT these
# 4. cost_center_exclude: [] = No exclusions (default)
#
# Lookup Priority:
# 1. Check specific cost_center matches first (if not empty)
# 2. If cost_center is [], check cost_center_exclude for exclusions
# 3. If both are [], applies to all cost centers

login_status_mapper = [
    # T-Systems ICT India Pvt L (2631/0472) - All Active
    {
        "legal_number": "2631",
        "org_structure_code": "0472",
        "company_description": "T-Systems ICT India Pvt L",
        "employee_type": "Internal",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2631",
        "org_structure_code": "0472",
        "company_description": "T-Systems ICT India Pvt L",
        "employee_type": "Internal worktype",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2631",
        "org_structure_code": "0472",
        "company_description": "T-Systems ICT India Pvt L",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2631",
        "org_structure_code": "0472",
        "company_description": "T-Systems ICT India Pvt L",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2631",
        "org_structure_code": "0472",
        "company_description": "T-Systems ICT India Pvt L",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2631",
        "org_structure_code": "0472",
        "company_description": "T-Systems ICT India Pvt L",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2631",
        "org_structure_code": "0472",
        "company_description": "T-Systems ICT India Pvt L",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2631",
        "org_structure_code": "0472",
        "company_description": "T-Systems ICT India Pvt L",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Active"
    },
    # T-Systems ICT India TC (6242/6242) - All Non Active
    {
        "legal_number": "6242",
        "org_structure_code": "6242",
        "company_description": "T-Systems ICT India TC",
        "employee_type": "Internal",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "6242",
        "org_structure_code": "6242",
        "company_description": "T-Systems ICT India TC",
        "employee_type": "Internal Shift Workers",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "6242",
        "org_structure_code": "6242",
        "company_description": "T-Systems ICT India TC",
        "employee_type": "Internal worktype",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "6242",
        "org_structure_code": "6242",
        "company_description": "T-Systems ICT India TC",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "6242",
        "org_structure_code": "6242",
        "company_description": "T-Systems ICT India TC",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "6242",
        "org_structure_code": "6242",
        "company_description": "T-Systems ICT India TC",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "6242",
        "org_structure_code": "6242",
        "company_description": "T-Systems ICT India TC",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "6242",
        "org_structure_code": "6242",
        "company_description": "T-Systems ICT India TC",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "6242",
        "org_structure_code": "6242",
        "company_description": "T-Systems ICT India TC",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # T-Systems ITC Iberia SAU (2380/0370) - Mixed based on cost center
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "Internal",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "Internal",
        "cost_center": ["119901", "129901", "139901", "139902", "139903", "139904", "139907", "139908", "139970", "139971", "149102", "149901", "149903", "149905", "149906", "149909", "149911", "149912", "149913", "149970", "159901", "169901", "169905", "199019", "199400", "199401", "199405", "199407", "199409", "199450", "199451", "199904", "199905", "199906", "199910", "199913", "199918", "199927", "199930", "199931", "199932", "199941", "199942", "199948", "199951", "199952", "199953", "199954", "199955", "199956", "199957", "199961", "199962", "199971", "210802", "255001", "256001", "258001", "261001", "261002", "280011", "280012", "280031", "280032", "280034", "280051", "280052", "280064", "280071", "290011", "290021", "837001", "837002", "837003", "837004", "7700046", "8370010", "400909", "400971", "400972", "400973", "415220", "417004", "420907", "422902", "422904", "598415", "598802", "598912", "598914", "598915", "598917", "598918", "598923", "598924", "598925", "600021", "600027", "600039"],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "Internal Shift Workers",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "Internal worktype",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0370",
        "company_description": "T-Systems ITC Iberia SAU",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # T-Systems ITC Suc. PT (2380/0377) - Mixed based on cost center
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "Internal",
        "cost_center": ["280074", "280077", "400912", "400990", "415516", "415783", "415810", "425000", "522009", "522011", "598003", "598005", "598107", "598315", "598931", "600009"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "Internal",
        "cost_center": ["119903", "139905", "149904", "199928", "199981", "199991", "199992", "210807", "280013", "415812"],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "Internal Shift Workers",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "Internal worktype",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0377",
        "company_description": "T-Systems ITC Suc. PT",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # TS Iberia Value Centers (2380/1046) - Mixed based on cost center
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "Internal",
        "cost_center": ["7000001", "7000002", "7000003", "7000004", "7000005", "7000006", "7000007", "7000008", "7000009", "7000010", "7000011", "7000012", "7000013", "7000014", "7000015", "7000016", "7000017", "7000018", "7000019", "7000020", "7000021", "7000022", "7000023", "7000024", "7000025", "7000026", "7000027", "7000028", "7000029", "7000030", "7000031", "7000032", "7000033", "7000034", "7000035", "7000036", "7000037", "7000038", "7000039", "7000040", "7000041", "7000042", "7000043", "7000044", "7000045", "7000047", "7000048", "7000049", "7000050", "7000051", "7000052", "7000053", "7000054", "7000056", "7000057", "7000058", "7000059", "7000060"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "Internal",
        "cost_center": ["7000000"],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "Internal Shift Workers",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "Internal worktype",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1046",
        "company_description": "TS Iberia Value Centers",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # DT IT ES (2380/1048) - All Non Active
    {
        "legal_number": "2380",
        "org_structure_code": "1048",
        "company_description": "DT IT ES",
        "employee_type": "Internal",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1048",
        "company_description": "DT IT ES",
        "employee_type": "Internal Shift Workers",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1048",
        "company_description": "DT IT ES",
        "employee_type": "Internal worktype",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1048",
        "company_description": "DT IT ES",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1048",
        "company_description": "DT IT ES",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1048",
        "company_description": "DT IT ES",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1048",
        "company_description": "DT IT ES",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1048",
        "company_description": "DT IT ES",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "1048",
        "company_description": "DT IT ES",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # T-Systems ITC Iberia Chile (2380/0388) - All Non Active
    {
        "legal_number": "2380",
        "org_structure_code": "0388",
        "company_description": "T-Systems ITC Iberia Chile",
        "employee_type": "Internal",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0388",
        "company_description": "T-Systems ITC Iberia Chile",
        "employee_type": "Internal Shift Workers",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0388",
        "company_description": "T-Systems ITC Iberia Chile",
        "employee_type": "Internal worktype",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0388",
        "company_description": "T-Systems ITC Iberia Chile",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0388",
        "company_description": "T-Systems ITC Iberia Chile",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0388",
        "company_description": "T-Systems ITC Iberia Chile",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0388",
        "company_description": "T-Systems ITC Iberia Chile",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0388",
        "company_description": "T-Systems ITC Iberia Chile",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0388",
        "company_description": "T-Systems ITC Iberia Chile",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # UTE T-SYSTEMS - DELOITTE (2380/0382) - All Non Active
    {
        "legal_number": "2380",
        "org_structure_code": "0382",
        "company_description": "UTE T-SYSTEMS - DELOITTE",
        "employee_type": "Internal",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0382",
        "company_description": "UTE T-SYSTEMS - DELOITTE",
        "employee_type": "Internal Shift Workers",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0382",
        "company_description": "UTE T-SYSTEMS - DELOITTE",
        "employee_type": "Internal worktype",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0382",
        "company_description": "UTE T-SYSTEMS - DELOITTE",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0382",
        "company_description": "UTE T-SYSTEMS - DELOITTE",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0382",
        "company_description": "UTE T-SYSTEMS - DELOITTE",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0382",
        "company_description": "UTE T-SYSTEMS - DELOITTE",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0382",
        "company_description": "UTE T-SYSTEMS - DELOITTE",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0382",
        "company_description": "UTE T-SYSTEMS - DELOITTE",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # UTE T-Systems Indra (2380/0384) - All Non Active
    {
        "legal_number": "2380",
        "org_structure_code": "0384",
        "company_description": "UTE T-Systems Indra",
        "employee_type": "Internal",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0384",
        "company_description": "UTE T-Systems Indra",
        "employee_type": "Internal Shift Workers",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0384",
        "company_description": "UTE T-Systems Indra",
        "employee_type": "Internal worktype",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0384",
        "company_description": "UTE T-Systems Indra",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0384",
        "company_description": "UTE T-Systems Indra",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0384",
        "company_description": "UTE T-Systems Indra",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0384",
        "company_description": "UTE T-Systems Indra",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0384",
        "company_description": "UTE T-Systems Indra",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2380",
        "org_structure_code": "0384",
        "company_description": "UTE T-Systems Indra",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # TS PU Public CloudServices (8108/6205) - All Non Active
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6205",
        "company_description": "TS PU Public CloudServices",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # TS PU MIS, Private Cloud (8108/6209) - All Non Active
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6209",
        "company_description": "TS PU MIS, Private Cloud",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # TS Sales, G&A, Cross Delivery (8108/6210) - All Non Active
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6210",
        "company_description": "TS Sales, G&A, Cross Delivery",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # T-Systems on site services GmbH (2804/2804) - All Non Active
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2804",
        "org_structure_code": "2804",
        "company_description": "T-Systems on site services GmbH",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # Deutsche Telekom Clinical Solutions GmbH (1320/1320) - All Non Active
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1320",
        "org_structure_code": "1320",
        "company_description": "Deutsche Telekom Clinical Solutions GmbH",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # Deutsche Telekom Healthcare and Security Solutions GmbH (2654/2654) - All Non Active
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2654",
        "org_structure_code": "2654",
        "company_description": "Deutsche Telekom Healthcare and Security Solutions GmbH",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # GeoMob, Geomobile GmbH (1079/1079) - All Non Active
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "1079",
        "org_structure_code": "1079",
        "company_description": "GeoMob, Geomobile GmbH",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # rola, rola Security Solutions (2599/2599) - All Non Active
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2599",
        "org_structure_code": "2599",
        "company_description": "rola, rola Security Solutions",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # T-Systems Information Services GmbH (9973/9973) - All Non Active
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "9973",
        "org_structure_code": "9973",
        "company_description": "T-Systems Information Services GmbH",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # TS Road User Services (2619/6201) - All Non Active
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    {
        "legal_number": "2619",
        "org_structure_code": "6201",
        "company_description": "TS Road User Services",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": [],
        "status": "Non Active"
    },
    # TS PU SAP (8108/6208) - Mixed based on cost center
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Int HR200 Tarif",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "External Contractors",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "External Manual",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "External Freelancer",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "External Services",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Trainees",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Students",
        "cost_center": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    # TS PU SAP (8108/6208) - Fallback entries for cost centers NOT in Active lists above
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6208",
        "company_description": "TS PU SAP",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": ["T1T0000001", "T1T0000003", "T1T0000018", "T1T0000020", "T1T0000021", "T1T0000023", "T1T0000052", "T1T0000057", "T1T0000058", "T1T0000059", "T1T0000060", "T1T0000061", "T1T0000062", "T1T0000063", "T1T0000064", "T1T0000072", "T1T0000073", "T1T0000076", "T1T0000077", "T1T0000078", "T1T0000079", "T1T0000080", "T1T0000081", "T1T0000082", "T1T0000083", "T1T0000084", "T1T0000085", "T1T0000086", "T1T0000087", "T1T0000092", "T1T0000093", "T1T0000133", "T1T0077775", "T1T0077776", "T1T0077777", "T1T0077778", "T1T0077780", "T1T0077781", "T1T0077782", "T1THR99999"],
        "status": "Non Active"
    },
    # TS PU Digital Solutions (8108/6206) - Mixed based on cost center
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Int HR200 Tarif",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "External Contractors",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "External Manual",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "External Freelancer",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "External Services",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Trainees",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Students",
        "cost_center": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    # TS PU Dedicated SI Solutions (8108/6207) - Mixed based on cost center
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Int HR200 Tarif",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "External Contractors",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "External Manual",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "External Freelancer",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "External Services",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Trainees",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Students",
        "cost_center": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "cost_center_exclude": [],
        "status": "Active"
    },
    
    # TS PU Digital Solutions (8108/6206) - Fallback entries for "Any other different from list"
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6206",
        "company_description": "TS PU Digital Solutions",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": ["T1R0000001", "T1R0000009", "T1R0000011", "T1R0000015", "T1R0000019", "T1R0000020", "T1R0000021", "T1R0000023", "T1R0000025", "T1R0000026", "T1R0000032", "T1R0000038", "T1R0000048", "T1R0000049", "T1R0000051", "T1R0000054", "T1R0000064", "T1R0000065", "T1R0000066", "T1R0000068", "T1R0000071", "T1R0000075", "T1R0000076", "T1R0000077", "T1R0000078", "T1R0000079", "T1R0000080", "T1R0000081", "T1R0000082", "T1R0000083", "T1R0000084", "T1R0000086", "T1R0000087", "T1R0000088", "T1R0000089", "T1R0000090", "T1R0000091", "T1R0000092", "T1R0000093", "T1R0000095", "T1R0077775", "T1R0077776", "T1R0077777", "T1R0077778", "T1R0077780", "T1R0077781", "T1RHR99999"],
        "status": "Non Active"
    },
    
    # TS PU Dedicated SI Solutions (8108/6207) - Fallback entries for "Any other different from list"
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Int HR200 tariffrei",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Int HR200 Tarif",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Int HR200 integr. RZ",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Int HR200 FZ=AZ",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "External Contractors",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "External Manual",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "External Freelancer",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "External Services",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Trainees",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    },
    {
        "legal_number": "8108",
        "org_structure_code": "6207",
        "company_description": "TS PU Dedicated SI Solutions",
        "employee_type": "Students",
        "cost_center": [],
        "cost_center_exclude": ["T1S0000001", "T1S0000002", "T1S0000003", "T1S0000004", "T1S0000005", "T1S0000006", "T1S0000007", "T1S0000008", "T1S0000009", "T1S0000010", "T1S0000011", "T1S0000012", "T1S0000013", "T1S0000015", "T1S0000016", "T1S0000019", "T1S0000021", "T1S0000022", "T1S0000023", "T1S0000025", "T1S0000027", "T1S0000032", "T1S0000033", "T1S0000034", "T1S0000035", "T1S0000037", "T1S0000038", "T1S0000039", "T1S0000041", "T1S0000042", "T1S0000043", "T1S0000044", "T1S0000045", "T1S0000046", "T1S0000047", "T1S0000048", "T1S0000049", "T1S0000050", "T1S0000051", "T1S0000052", "T1S0000053", "T1S0000054", "T1S0000055", "T1S0000056", "T1S0000057", "T1S0000058", "T1S0000059", "T1S0000060", "T1S0000061", "T1S0000062", "T1S0000063", "T1S0000064", "T1S0000065", "T1S0000066", "T1S0000067", "T1S0000068", "T1S0000069", "T1S0000071", "T1S0000073", "T1S0000075", "T1S0000079", "T1S0000081", "T1S0000083", "T1S0000084", "T1S0000086", "T1S0000087", "T1S0000088", "T1S0000090", "T1S0000091", "T1S0000093", "T1S0000094", "T1S0000095", "T1S0000096", "T1S0000097", "T1S0000098", "T1S0000099", "T1S0000100", "T1S0000103", "T1S0000104", "T1S0000105", "T1S0000107", "T1S0000108", "T1S0000109", "T1S0000110", "T1S0000111", "T1S0000112", "T1S0000113", "T1S0000114", "T1S0000115", "T1S0000116", "T1S0000117", "T1S0000118", "T1S0000119", "T1S0000120", "T1S0000121", "T1S0000122", "T1S0000123", "T1S0000124", "T1S0000125", "T1S0000126", "T1S0000127", "T1S0000128", "T1S0000129", "T1S0000130", "T1S0000131", "T1S0000132", "T1S0000133", "T1S0000134", "T1S0000135", "T1S0000136", "T1S0000138", "T1S0000139", "T1S0000140", "T1S0000141", "T1S0000142", "T1S0000143", "T1S0000144", "T1S0077775", "T1S0077776", "T1S0077777", "T1S0077778", "T1S0077780", "T1S0077781", "T1S0077782", "T1SHR99999", "T1SM000020", "T1SM000023", "T1SM000025", "T1SM000068", "T1SM000072", "T1SM000074", "T1SM000075", "T1SM000077", "T1SM000095", "T1SM000096", "T1SM000097", "T1SM000098", "T1SM000099", "T1SM000100", "T1SM000101", "T1SM000102", "T1SM000103", "T1SM000104"],
        "status": "Non Active"
    }
]