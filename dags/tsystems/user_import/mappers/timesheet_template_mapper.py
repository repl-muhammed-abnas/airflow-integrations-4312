# Usage Guide for timesheet_template_mapper:
#
# Field Explanations:
# 1. org_structure_code: Organization structure code (required match)
# 2. work_relationship: "Employee" or "External" (required match)
# 3. employment_type_include: [] = Allow all employment types, [list] = Allow only these types
# 4. employment_type_exclude: [] = No exclusions (default), [list] = Exclude these types
# 5. employment_subtype_include: [] = Allow all subtypes, [list] = Allow only these subtypes
# 6. employment_subtype_exclude: [] = No exclusions (default), [list] = Exclude these subtypes
# 7. manager_flag: [] = Allow any manager flag value (Yes/No), [list] = Allow only these values
# 8. employee_type: Employee type filter (for matching existing employee types)
# 9. timesheet_template: Resulting timesheet template to assign (empty string "" = no template)
#
# Logic Rules:
# - Empty arrays ([]) mean "allow all values" for that field
# - Include lists take precedence over exclude lists
# - If include list is not empty, value must be in the include list
# - If include list is empty, value must NOT be in the exclude list
# - All conditions must match for a rule to apply
# - First matching rule wins (order matters)
# - Empty timesheet_template ("") means no template should be assigned
#
# Sections:
# - "standard": Normal mapping rules for template assignment
# - "exceptions": Special preservation rules for UPDATE scenarios only

timesheet_template_mapper = {
    "standard": [
        # Spain/Portugal Organizations (0370, 0377, 1046)
        # Internal employees
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal",
            "timesheet_template": "0370_Internal only Duration"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal",
            "timesheet_template": "0377_Internal only Duration"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal",
            "timesheet_template": "1046_Internal only Duration"
        },
        
        # Trainees - Trainees template
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees",
            "timesheet_template": "Trainees"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees",
            "timesheet_template": "Trainees"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees",
            "timesheet_template": "Trainees"
        },
        
        # Students - No template (-)
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["F", "A", "J", "L", "M"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students",
            "timesheet_template": ""
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["F", "A", "J", "L", "M"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students",
            "timesheet_template": ""
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["F", "A", "J", "L", "M"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students",
            "timesheet_template": ""
        },
        
        # External Contractors (MA)
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        
        # External Contractors (MC)
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        
        # External Services (MD)
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services",
            "timesheet_template": "External Employee"
        },
        
        # External Freelancer (MF)
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer",
            "timesheet_template": "External Employee"
        },
        
        # External Contractors (catch-all for other M subtypes)
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        
        # T-Systems ICT India (0472)
        # Internal employees
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal",
            "timesheet_template": "0472_Internal only Duration"
        },
        
        # Trainees - External Employee template (correct per tech spec for 0472)
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees",
            "timesheet_template": "External Employee"
        },
        
        # Students - No template
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["F", "A", "M"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students",
            "timesheet_template": ""
        },
        
        # External Contractors (MD)
        {
            "org_structure_code": "0472",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        
        # External Services (MC)
        {
            "org_structure_code": "0472",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services",
            "timesheet_template": "External Employee"
        },
        
        # External Contractors (catch-all)
        {
            "org_structure_code": "0472",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MC", "MD"],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        
        # German Organizations (6206, 6207, 6208)
        # Int HR200 Tarif - No template
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 Tarif",
            "timesheet_template": ""
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 Tarif",
            "timesheet_template": ""
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 Tarif",
            "timesheet_template": ""
        },
        
        # Trainees
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees",
            "timesheet_template": "Trainees"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees",
            "timesheet_template": "Trainees"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees",
            "timesheet_template": "Trainees"
        },
        
        # Students - No template
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["F", "A", "J", "L", "M"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students",
            "timesheet_template": ""
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["F", "A", "J", "L", "M"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students",
            "timesheet_template": ""
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["F", "A", "J", "L", "M"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students",
            "timesheet_template": ""
        },
        
        # External Contractors
        {
            "org_structure_code": "6206",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
            "timesheet_template": "External Employee",
        }
    ],
    
    "exceptions": [
        # Shift workers templates
        {
            "org_structure_code": "0370",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "0370_Shift workers only duration"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "0377_Shift workers only duration"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "1046_Shift workers only duration"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "0472_Shift workers only duration"
        },
        
        # Students template
        {
            "org_structure_code": "0370",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "Students"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "Students"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "Students"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "Students"
        },
        {
            "org_structure_code": "6206",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "Students"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "Students"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "Students"
        },
        
        # German organization special templates
        # HR200 FZ=AZ
        {
            "org_structure_code": "6206",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 FZ=AZ"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 FZ=AZ"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 FZ=AZ"
        },
        
        # HR200 integr. RZ
        {
            "org_structure_code": "6206",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 integr. RZ"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 integr. RZ"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 integr. RZ"
        },
        
        # HR200 Tarif
        {
            "org_structure_code": "6206",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 Tarif"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 Tarif"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 Tarif"
        },
        
        # HR200 tariffrei
        {
            "org_structure_code": "6206",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 tariffrei"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 tariffrei"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": [],
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": [],
            "timesheet_template": "HR200 tariffrei",
        }
    ]
}