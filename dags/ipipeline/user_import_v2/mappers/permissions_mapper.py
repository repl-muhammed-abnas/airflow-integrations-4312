"""
iPipeline User Import - Permissions Mapper

Exact permissions matrix from integration_tech_spec.html (lines 709-723).
Hardcoded as specified in the tech spec - based on Title field.
"""

# Permissions by Role - EXACT from tech spec
PERMISSIONS_MAPPER = {
    "Director": [
        "Project Resource with Reports",
        "Supervisor",
        "Project Manager",
        "Resource Manager"
    ],
    "Project Manager": [
        "Project Resource with Reports",
        "Supervisor",
        "Project Manager",
        "Resource Manager"
    ],
    "Business Analyst": [
        "Project Resource with Reports",
        "Supervisor"
    ],
    "BA Manager": [
        "Project Resource with Reports",
        "Supervisor",
        "Resource Manager"
    ],
    "Developer": [
        "Project Resource with Reports",
        "Supervisor"
    ],
    "Development Manager": [
        "Project Resource with Reports",
        "Supervisor",
        "Resource Manager"
    ],
    "Tester": [
        "Project Resource with Reports",
        "Supervisor"
    ],
    "QA Manager": [
        "Project Resource with Reports",
        "Supervisor",
        "Resource Manager"
    ]
}