# Task 4: Integrity Enhancement

## Objective
Implement an `IntegrityCheck` method for the `DB` struct to verify DB/WAL consistency, ensuring data integrity and aiding in debugging corruption issues.

## Background
The nnut library uses WAL for durability. An integrity check will validate WAL entries, checksums, and replayability to detect corruption early.

## Detailed Implementation Plan

### 1. Design IntegrityCheck Method
- **Method signature**: `func (db *DB) IntegrityCheck() error`
- **Scope**: Check WAL and DB consistency without modifying state.
- **Return value**: `nil` if no issues, detailed error with specifics otherwise.

### 2. WAL Validation
- **Checksum verification**: Recompute CRC32 for each WAL entry and compare.
- **Decoding check**: Attempt to decode all entries without errors.
- **Sequence validation**: Ensure operation sequence is logical.

### 3. DB Consistency Check
- **Replay simulation**: Virtually replay WAL operations to check for conflicts.
- **Data integrity**: Verify DB file structure if possible.
- **Cross-reference**: Ensure WAL and DB are synchronized.

### 4. Error Reporting
- **Detailed errors**: Include operation indices, failure types, checksum mismatches.
- **Multiple issues**: Collect all problems, not just first error.
- **Actionable info**: Provide guidance on fixing issues.

### 5. Implementation Details
- **Read-only**: Ensure no writes or modifications during check.
- **Performance**: Optimize for large WALs/DBs.
- **Concurrency**: Handle checks during active operations.

### 6. Testing
- **Positive tests**: Verify clean DBs pass checks.
- **Negative tests**: Introduce corruption and test detection.
- **Edge cases**: Empty DB, large WAL, concurrent modifications.

### 7. Integration
- **API exposure**: Make method public for user-initiated checks.
- **Automated checks**: Consider periodic or startup integrity checks.
- **Logging**: Add logging for integrity issues.

### 8. Risks and Mitigations
- **False positives**: Careful validation logic.
- **Performance overhead**: Optimize checks, consider sampling for large datasets.
- **Complexity**: Modular design for maintainability.

## Success Criteria
- `IntegrityCheck` accurately detects corruption.
- Detailed error messages aid debugging.
- Method is performant and safe to run on production DBs.
- Comprehensive test coverage.
