# Backup and Recovery

## Objective
Enhance the backup and recovery capabilities of the nnut library by implementing an `Export` method for the `DB` struct, building on the existing basic WAL for crash recovery.

## Background
Currently, nnut provides WAL for crash recovery. This task adds explicit backup functionality to allow users to create point-in-time copies of the database for disaster recovery or migration.

## Current State Analysis
- Basic WAL exists for crash recovery.
- No explicit backup mechanism.
- DB operations are buffered and flushed periodically.

## Detailed Implementation Plan

### 1. Design the Export Method
- **Method signature**: `func (db *DB) Export(destPath string) error`
- **Functionality**:
  - Flush all pending operations to ensure DB is up-to-date.
  - Close the DB temporarily to prevent modifications during copy.
  - Copy the DB file (and WAL if separate) to `destPath`.
  - Reopen the DB to resume operations.
- **Safety considerations**:
  - Validate `destPath`: Check if file exists, prompt for overwrite if needed.
  - Ensure atomic operation: Use temporary files and rename for safety.
  - Handle permissions and disk space issues.

### 2. Implement Flushing and Synchronization
- **Pre-export flush**: Call existing `Flush()` method to write buffered operations.
- **Prevent new flushes**: Acquire locks to block concurrent flushes during export.
- **WAL handling**: Ensure WAL is consistent with DB state before copying.

### 3. File Copying Logic
- **Copy mechanism**: Use efficient file copying (e.g., `io.Copy` with buffers).
- **Error handling**: Gracefully handle I/O errors, disk full, permissions.
- **DB integrity**: Verify DB file is valid after copy if possible.

### 4. Reopening and Recovery
- **Reopen DB**: After successful copy, reopen the DB.
- **Failure recovery**: If export fails, ensure DB remains operational.
- **Rollback**: No changes to original DB, so minimal rollback needed.

### 5. Testing and Validation
- **Unit tests**: Test export with various scenarios (empty DB, large DB, concurrent access).
- **Integration tests**: Simulate failures (disk full, permission denied).
- **Concurrent access**: Ensure export doesn't interfere with ongoing operations.
- **Restore testing**: Verify exported DB can be opened and used.

### 6. Documentation Updates
- **API docs**: Add godoc for `Export` method with examples.
- **README**: Include backup and restore procedures.
- **Best practices**: Recommend regular backups, offsite storage.

### 7. Risks and Mitigations
- **Data corruption**: Extensive testing and validation.
- **Performance impact**: Optimize copy process, consider async options.
- **Concurrency issues**: Proper locking and testing.

## Success Criteria
- `Export` method successfully creates DB backups.
- DB remains operational during and after export.
- Comprehensive tests cover edge cases.
- Documentation includes backup procedures.
