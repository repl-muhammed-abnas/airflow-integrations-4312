# CapeFox - Create PLC as Task Requirements Document

**Version:** 1.2  
**Client:** Cape Fox Corporation  
**Customization Name:** Use PLC as Task  
**Replicon Product Version:** Gen 3  
**Requested Date:** April 24, 2025

---

## Overview

- Cape Fox creates users and multi-level projects in Costpoint
- Standard connector imports projects and PLCs from Costpoint to Replicon
- Standard connector imports users from Costpoint to Replicon
- Standard connector exports timesheet data from Replicon to Costpoint
- ~~Standard connector imports PLC as billing rate~~ → PLC to be loaded as a bottom-level subtask instead
- Users punch time on Cloud Clock against Project and Task (Location and PLC)

---

## Requirements

### Core Functional Requirements

1. **PLC as Bottom-Level Task**
   - Load Costpoint PLC as a bottom-level task in the project instead of billing rate
   - Bottom level Task Name = Costpoint PLC Name (v1.2)
   - Bottom level Task Code = Costpoint PLC Code (v1.2)

2. **User Assignment**
   - Assign users to the correct bottom-level task for time capture
   - ~~Assign Billing Rate (PLC) based on bottom-level task selection~~

3. **Project Filtering (v1.2)**
   - Only Costpoint Projects with Owning Organization beginning with `1.02`, `1.02.`, or `3.01` should be included
   - All other projects should be ignored

4. **Data Export to Costpoint**
   - Remove bottom-level task when sending data back to Costpoint timesheets
   - Send bottom-level Task Code to Costpoint as PLC Code (v1.2)
   - ~~Send billing rate as PLC Code~~

### Integration Workflow Requirements

1. **Dual Workflow Architecture**
   - **Workflow A:** Manage Project information, start & end date, and PM info
   - **Workflow B:** Manage assignment against task level, including Task creation & update

2. **Project Date Handling**
   - Project start and end dates updated from Workflow A
   - ~~Current integration logic to determine & extend project dates~~ → Removed

3. **Task Date Handling**
   - ~~Map "TASK_START_DATE" and "TASK_END_DATE" with "ASNMT_START_DATE" and "ASNMT_END_DATE" values for user assignments~~
   - Tasks do not require start and end dates (v1.2)

4. **Task Name Logic**
   - ~~Concatenated name of task ID & name from input~~
   - Bottom level Task Name = Costpoint PLC Name (v1.2)
   - Bottom level Task Code = Costpoint PLC Code (v1.2)

5. **Classification Requirements**
   - Include Classification at project-level
   - Include DIGIQ classification at task level
   - When classification changes: disable existing tasks and add new set of tasks

### API & Endpoint Requirements

- Separate endpoint for Project information workflow
- Customer posts project info data to new endpoint
- Existing API used for Assignment data
- Customer responsible for sequencing (assignment data sent before project creation will be skipped)

---

## Business Process Flow

1. Standard connector imports CP Project levels ~~and PLC as Project/Task/Subtask(s)/Billing Rate~~
2. ~~Standard connector assigns user to Project/Billing Rate~~
3. Customization creates bottom-level task for CP PLC in Project
4. Customization ensures only bottom-level tasks are open for time capture (billable only)
5. Customization assigns user to bottom-level task (PLC) based on CP Project and PLC assignments
6. User captures time against project and task in Cloud Clock
7. After timesheet period ends, Allocate Timesheet Hours automation distributes time against project and task
8. ~~Customization checks if Billing Rate = Task Code~~
9. ~~If not equal, Customization assigns Billing Rate to distribution row based on bottom-level task~~
10. Allocate Timesheet Hours automation submits timesheet
11. Supervisor approves timesheet
12. Standard connector sends time data to CP timesheets:
    - Removes bottom-level task (PLC)
    - Sends Task Code as PLC Code (v1.2)
    - ~~Sends billing rate as PLC Code~~

---

## Field Mapping

### Inbound (Costpoint → Replicon)

| Costpoint | Replicon | Logic |
|-----------|----------|-------|
| Project Level 1 | Project | - |
| Project Levels 2, 3, etc | Task and Subtask(s) | - |
| PLC | Bottom Level Subtask ~~and Billing Rate~~ | Use PLC to create a bottom-level subtask ~~AND a Billing Rate~~ |
| User Project Assignment | Project Team Resource | - |
| User PLC Assignment | Bottom Level Subtask assignment ~~and Billing Rate assignment~~ | Assign user to task matching PLC ~~AND Billing Rate matching PLC~~ |

### Outbound (Replicon → Costpoint)

| Replicon | Costpoint | Logic |
|----------|-----------|-------|
| Project | Project Level 1 | - |
| Task and Subtask(s) | Project Levels 2, 3, etc | Modified to remove bottom-level Subtask (PLC), matching original project hierarchy from Costpoint |
| ~~Billing Rate~~ Bottom-Level Task Code | PLC | - |
| User | User | - |
| Hours | Hours | - |

---

## Detailed Field Mapping

| Costpoint Field | Replicon Field | Logic |
|-----------------|----------------|-------|
| Project Name [PROJ.PROJ_NAME] | Project Name OR Task Name | If Project Level = 1, Project Name = CP Project Name; ELSE IF Allow Charging = FALSE, Task Name = "/"; ELSE Task Name = CP Project Name |
| Project Long Name [PROJ.PROJ_LONG_NAME] | Project Description OR Task Description | Project Description IF Project "Level" = 1; ELSE Task Description |
| Project ID [PROJ.PROJ_ID] | Project Code OR Task Code | Project Code IF Project "Level" = 1; ELSE Task Code |
| - | Project "When Adding Team Members" | Do not assign them to all tasks |
| - | Project Time and Expense Entry | Non-Billable |
| - | Project "Allow Time Entry against Task Only" | Always TRUE |
| PLCs Assigned to Employee Work Force [PROJ_EMPL_LAB_CAT.EMPL_ID] | Project Team – User Employee ID | Costpoint workforce assigned to billable project level; Replicon Team assigned at top project level |
| Project Workforce PLC [PROJ_LAB_CAT.BILL_LAB_CAT_CD] | Subtask Code | - |
| Project Workforce PLC Description [PROJ_LAB_CAT.BILL_LAB_CAT_DESC] | Subtask Name | - |
| Project Employee Work Force – Employee PLC Assignment [PROJ_EMPL_LAB_CAT] | Task Resource Assignment for PLC tasks – Employee ID and Task Code | Costpoint workforce assigned to billable project level; Replicon task resource assignment on new, next lower task level created from assigned Costpoint PLC |
| Project "Allow Charging" [PROJ.ALLOW_CHARGES_FL] | PLC Task "Allow Time Entry" | Inherit from Costpoint project containing this PLC |
| Project "Active" flag (Level 1) [PROJ.ACTIVE_FL] | Project Status | If Active = TRUE → "In-Progress"; If Active = FALSE → "Completed" |
| Project "Active" flag (sub-level) [PROJ.ACTIVE_FL] | Task Status flag "Open" | Keep for each Costpoint Project level; PLC task inherits from Costpoint project containing the PLC |

---

## Schedule & Automation

- **Run Schedule:** Every 16th and 1st of each month at 6:50am Alaska Standard Time
- **Scope:** Previous timesheet period for all unsubmitted timesheets
- **Manual Trigger:** Required for periods where a holiday changes the required run date

---

## System Requirements

- **Browsers:** Internet Explorer, Google Chrome
- **Hardware:** Cloud Clock iPad

---

## Implementation Details

### Licensed Products
- TimeBill Plus
- TimeOff Enterprise
- Workforce Management

### Configuration
- **Timesheet Period:** Semimonthly
- **Timesheet Template:** Time Punches with Distribution
- **Time Off:** Sick Leave
- **Overtime Rules:** Alaska

### Environments
- **UAT Instance:** CapeFoxCorporationSB
- **Production Instance:** CapeFoxCorporation
- **Costpoint Sandbox:** CAPEFOXCORPSBOX

### Data Volume
| Integration/Record | Go-live Volume | Regular Volume (Delta/Post Go-live) |
|--------------------|----------------|-------------------------------------|
| Timesheets | 150 | 150-300 |

---

## Pre-Deployment Requirements

1. For all existing projects & tasks, respective classifications must be updated before change deployment (manual activity)
2. ~~With task name format being updated, names of existing tasks should be updated manually~~

---

## Timeline

- **Original Go-live:** May 1, 2025
- **Revised Go-live:** June 1, 2025
- **Note:** This customization part of Wave 2 with the Costpoint Connector

---

## Version History

| Version | Description | Date | Author |
|---------|-------------|------|--------|
| V0.1 | Draft document | April 24, 2025 | Clint Smith |
| V1.0 | Final Version | June 30, 2025 | Clint Smith |
| V1.1 | Updated to include mapping table | July 11, 2025 | Clint Smith |
| V1.2 | Remove Billing Rate logic, other updates | December 17, 2025 | Clint Smith |
