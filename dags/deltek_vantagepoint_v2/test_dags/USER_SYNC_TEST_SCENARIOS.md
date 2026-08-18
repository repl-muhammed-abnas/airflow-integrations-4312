# VP-Replicon User Sync Integration Test - Scenario Documentation

## Overview

This test DAG (`user_sync_integration_test_dag.py`) validates the end-to-end user synchronization workflow between Deltek Vantagepoint (VP) and Replicon. It tests all critical scenarios with dynamically-created test employees, ensuring isolation between test runs.

**Test Duration:** ~30-45 minutes (depending on main DAG processing time)  
**Test Isolation:** Each run uses unique employee IDs (IT{uuid} and WI{uuid}) to avoid conflicts  
**Cleanup:** Dynamically-created VP employees are deleted after test completion

---

## Test Scenarios (5 Total)

### Scenario 1: Initial Sync
**Purpose:** Validate user creation via initial run (force_initial_run=True)

**Flow:**
1. Create IT{test_id} employee in VP
2. Trigger main DAG with force_initial_run=True (filtered to IT employee)
3. Main DAG fetches IT employee from VP and syncs to Replicon
4. Verify user created in Replicon with correct fields

**Assertions:**
- ✓ User exists in Replicon
- ✓ loginName = IT{test_id}
- ✓ firstName, lastName, displayName, emailAddress match VP
- ✓ loginEnabled = True
- ✓ startDate, timeZone, permissions populated

**Test Data:** IT{random_4_hex}

---

### Scenario 2: Webhook Create with Supervisor Assignment
**Purpose:** Validate webhook create trigger AND supervisor permission assignment

**Flow:**
1. Create WI{test_id} employee in VP **with IT{test_id} as Supervisor**
2. Trigger main DAG via webhook with Action='insert' (employee number WI{test_id})
3. Main DAG:
   - Fetches WI employee from VP
   - Detects IT{test_id} as supervisor
   - Syncs WI to Replicon
   - **Assigns supervision permission to IT{test_id}**
4. Verify WI employee created AND IT employee has supervision permission

**Assertions:**
- ✓ WI employee exists in Replicon with loginEnabled=True
- ✓ WI fields match VP (firstName='WebhookTest', etc.)
- ✓ **IT employee exists in Replicon**
- ✓ **IT employee has `urn:replicon:policy:supervision` permission assigned**

**Test Data:** IT{uuid} (from S1), WI{random_4_hex} with Supervisor=IT{uuid}

---

### Scenario 3: Webhook Update
**Purpose:** Validate webhook update trigger with field changes

**Flow:**
1. Update WI{test_id} employee in VP (change FirstName, LastName, EMail, PreferredName)
2. Trigger main DAG via webhook with Action='update'
3. Main DAG fetches updated WI employee and syncs changes to Replicon
4. Verify updated fields in Replicon

**Assertions:**
- ✓ User exists in Replicon
- ✓ firstName = 'WebhookTestUpdated'
- ✓ lastName = 'WI{test_id}-Updated'
- ✓ displayName = 'Webhook User Updated'
- ✓ emailAddress = 'wi-updated-{test_id}@test.local'
- ✓ loginEnabled = True

**Test Data:** Same WI{uuid} from S2, updated fields

---

### Scenario 4: Webhook Delete (User Exists)
**Purpose:** Validate delete action with existing user in Replicon

**Flow:**
1. Set TerminationDate on WI{test_id} in VP (mark as deleted)
2. Trigger main DAG via webhook with Action='delete'
3. Main DAG:
   - Checks if Action='delete'
   - Searches for user in Replicon
   - Disables user (sets loginEnabled=False)
4. Verify user still exists but is disabled

**Assertions:**
- ✓ User exists in Replicon (uri is not null)
- ✓ loginName = WI{test_id}
- ✓ loginEnabled = False (disabled, NOT deleted)

**Test Data:** Same WI{uuid}, with TerminationDate set

---

### Scenario 5: Webhook Reactivate
**Purpose:** Validate reactivation after deletion

**Flow:**
1. Clear TerminationDate on WI{test_id} in VP (mark as active again)
2. Trigger main DAG via webhook with Action='update'
3. Main DAG fetches reactivated WI employee and syncs to Replicon
4. Verify user is re-enabled

**Assertions:**
- ✓ User exists in Replicon
- ✓ loginEnabled = True (re-enabled)
- ✓ endDate is empty/null (no termination)

**Test Data:** Same WI{uuid}, TerminationDate cleared

---

## Test Data Lifecycle

```
generate_test_id (4-char hex, e.g., "A3F7")
    ↓
create_vp_employee → Creates IT{A3F7}
    ↓
[Scenario 1: Initial Sync with IT{A3F7}]
    ↓
create_webhook_vp_employee → Creates WI{A3F7} with Supervisor=IT{A3F7}
    ↓
[Scenario 2: Create + Supervisor Assignment]
    ↓
update_vp_employee_for_webhook → Changes WI{A3F7} fields
    ↓
[Scenario 3: Update]
    ↓
update_vp_employee_for_delete → Sets TerminationDate on WI{A3F7}
    ↓
[Scenario 4: Delete]
    ↓
update_vp_employee_for_reactivate → Clears TerminationDate on WI{A3F7}
    ↓
[Scenario 5: Reactivate]
    ↓
cleanup_wi_employee → Deletes WI{A3F7} from VP
cleanup_it_employee → Deletes IT{A3F7} from VP
```

---

## API Endpoints Used

### Vantagepoint API
- `POST /employee` - Create employee
- `PUT /employee/{id}` - Update employee fields
- `DELETE /employee/{id}` - Delete employee

### Replicon API (ImportService2)
- `GetUserDetails` - Fetch user by loginName

### Replicon API (PermissionSetService1)
- `GetAssignedPermissionSetsForUser2` - Fetch user permissions

---

## Error Handling

| Scenario | Error | Expected Behavior |
|----------|-------|-------------------|
| S1 | IT employee creation fails in VP | AssertionError in create_vp_employee task |
| S1 | IT employee not found after initial sync | AssertionError: "IT employee not found in Replicon" |
| S2 | WI employee creation fails in VP | AssertionError in create_webhook_vp_employee task |
| S2 | WI not synced after webhook | AssertionError: "WI employee not found in Replicon" |
| S2 | IT doesn't have supervision permission | AssertionError: "supervisor does not have supervision permission assigned" |
| S3 | Updated fields don't match | AssertionError: Field mismatch (firstName, lastName, emailAddress) |
| S4 | User not found in Replicon | AssertionError: "WI employee not found in Replicon after delete webhook" |
| S4 | loginEnabled != False | AssertionError: "expected loginEnabled=False, got X" |
| S5 | User not found in Replicon | AssertionError: "WI employee not found in Replicon after reactivation" |
| S5 | loginEnabled != True | AssertionError: "expected loginEnabled=True, got X" |
| S5 | endDate not cleared | AssertionError: "expected endDate to be empty/null after reactivation" |

---

## Testing Checklist

- [ ] Scenario 1 passes: IT employee synced with all fields
- [ ] Scenario 2 passes: WI employee synced, IT employee has supervision permission
- [ ] Scenario 3 passes: Field updates reflected in Replicon
- [ ] Scenario 4 passes: User disabled (loginEnabled=False), not deleted
- [ ] Scenario 5 passes: User re-enabled (loginEnabled=True), endDate cleared
- [ ] Cleanup succeeds: Both IT and WI employees removed from VP

---

## Notes

- **Supervisor Assignment:** Integrated into Scenario 2 (webhook create). The test validates that when a new employee has a supervisor, the main DAG correctly assigns supervision permission to that supervisor.
- **No Fixtures:** All test data is dynamically created, eliminating the need for pre-existing fixture employees.
- **Idempotent:** Test runs are isolated by unique IDs; multiple runs can execute in parallel without conflicts.
- **Self-Contained:** All scenarios use the same IT and WI employees, testing the full lifecycle in one run.
