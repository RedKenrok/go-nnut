# Encryption at Rest

## Objective
Implement full encryption of WAL and DB data at rest using AES, providing comprehensive data protection for stored information.

## Background
Building on field-level encryption, this adds encryption for the entire database and WAL files to protect against unauthorized access.

## Detailed Implementation Plan

### 1. Configuration Extension
- **Add option**: `EncryptAtRest bool` to `Config`.
- **Reuse keys**: Use existing `EncryptionKey` and `EncryptionAlgorithm`.
- **Validation**: Require encryption key when enabled.

### 2. Encryption Implementation
- **WAL operations**: Encrypt `operation.Value` before writing to WAL.
- **DB flushing**: Encrypt data before writing to DB.
- **Consistency**: Apply encryption across all operations (normal, replay, flush).

### 3. Decryption Implementation
- **Reading DB**: Decrypt data when loading from DB.
- **WAL replay**: Decrypt operations during replay.
- **Error handling**: Handle decryption failures gracefully.

### 4. Initialization
- **OpenWithConfig**: Initialize encryption context if enabled.
- **Key setup**: Validate and prepare encryption keys.

### 5. Backup Integration
- **Export method**: Ensure backups handle encryption properly (export encrypted or decrypted?).
- **Consistency**: Maintain encryption state in backups.

### 6. Performance and Security
- **Overhead**: Document performance impact.
- **Key security**: Secure key handling, no exposure in logs.
- **Algorithm**: AES-GCM for authenticated encryption.

### 7. Testing
- **Full encryption**: Verify all data is encrypted at rest.
- **Decryption**: Ensure correct data retrieval.
- **Edge cases**: Key changes, corruption.

### 8. Documentation
- **Configuration**: How to enable encryption at rest.
- **Usage**: Implications for backups, performance.
- **Security**: Best practices for key management.

### 9. Risks and Mitigations
- **Performance**: Significant overhead; provide benchmarks.
- **Complexity**: Careful integration to avoid corruption.
- **Backward compatibility**: System is in experimental phase, no backwards compatibility required.

## Success Criteria
- When enabled, all DB and WAL data is encrypted at rest.
- Seamless operation with existing API.
- Secure key handling and authenticated encryption.
- Comprehensive tests and documentation.
