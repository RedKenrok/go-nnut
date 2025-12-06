# Tasks

## 1. Refactor query interface

- Current state: Query functionality is only used for getting data.
- Improvements:
  - Rename `Query` to `GetQuery` and `QueryCount` to `CountQuery`.
    - Update method names in `store_query.go` to `GetQuery` and `CountQuery` for clarity and consistency.
    - Add deprecation notices or aliases for backward compatibility if needed.
    - Update all internal calls and tests to use the new names.
  - Add `Count` function for index length.
    - Implement `Count(ctx context.Context, index string) (int, error)` on `Store[T]` that returns the number of unique values in the specified index.
    - Validate that the index exists using `s.indexFields`.
    - Use `countKeysFromIndexTx` internally to count entries in the index bucket.
    - Handle cases where index is empty or doesn't exist.
    - Add unit tests for various index scenarios.
  - Add `DeleteQuery` function reusing query logic.
    - Implement `DeleteQuery(ctx context.Context, query *Query) (int, error)` that deletes records matching the query conditions and returns the count of deleted records.
    - Reuse candidate key gathering logic from `GetQuery` to identify keys to delete.
    - Perform deletions in a single `Update` transaction for atomicity.
    - Update indexes accordingly during deletion (similar to `Put` with old/new index operations).
    - Ensure proper error handling and return partial success if some deletions fail.
    - Add comprehensive tests including edge cases like deleting with conditions and indexes.

## 2. Code maintainability

Add documentation with godoc examples.
- Add comprehensive godoc comments to all types and methods, including usage examples.
- Update `README.md` take the reader through all the features in the library with example code snippets. Make sure features are introduced in a logical manner one by one.
- Ensure examples cover common use cases, error handling, and advanced features like indexing and querying.

## 3. Backup and recovery

- Current state: Basic WAL for crash recovery.
- Improvements:
  - Add `Export(destPath string) error` method to `DB` that flushes pending operations, closes the DB, copies the DB file to `destPath`, and reopens the DB.
  - Validate `destPath` for safety (e.g., not overwriting existing files without confirmation).
  - Call `Flush()` before copying the database over and prevent new flushes during copying.
  - Handle file system errors gracefully and ensure DB remains operational even if export fails.
  - Add tests for export functionality, including concurrent access scenarios.
  - Update documentation with backup and restore procedures.

## 4. Integrity enhancement

- Add `IntegrityCheck` method to verify DB/WAL consistency.
  - Implement a public `IntegrityCheck() error` method on the `DB` struct.
  - The method should validate all WAL entries by recomputing and comparing CRC32 checksums.
  - Check for WAL file corruption by attempting to decode all entries without errors.
  - Verify that WAL operations can be replayed to the database without conflicts or errors.
  - Return detailed errors for any inconsistencies found, including operation indices and types of failures.
  - Ensure the check is performed without modifying the database state.

## 5. Value encryption

- Allow fields to be tagged `nnut:"encrypt"` for automatic encryption.
  - Extend the tag parsing in `NewStore` to recognize `nnut:"encrypt"` and `nnut:"encrypt:<salt_field>"` tags.
  - Add fields to `Store[T]` to track encrypted fields and their salt references.
  - Add encryption configuration to `Config` struct: `EncryptionKey []byte` (32 bytes for AES-256) and `EncryptionAlgorithm string` (default "AES-GCM").
  - Implement encryption logic: before marshaling in `Put`/`PutBatch`, encrypt tagged fields using AES-GCM with the config key and optional salt from another field.
  - Implement decryption logic: after unmarshaling in `Get`/`GetBatch`, decrypt tagged fields.
  - Handle salt generation automatically if the salt field is empty and referenced.
  - Ensure encrypted fields cannot be indexed (return error if tagged with both encrypt and index).
  - Update documentation with examples of encrypted fields.

## 6. Encryption at rest

- Add config options for AES encryption of WAL/DB at rest.
  - Add `EncryptAtRest bool` to `Config` to enable/disable full data encryption.
  - Reuse the `EncryptionKey` and `EncryptionAlgorithm` from field encryption.
  - Encrypt entire `operation.Value` before writing to WAL and before flushing to DB.
  - Decrypt data when reading from DB or replaying WAL.
  - Ensure encryption is applied consistently across WAL replay, flushing, and normal operations.
  - Add validation to ensure `EncryptionKey` is provided when `EncryptAtRest` is true.
  - Update `OpenWithConfig` to initialize encryption if enabled.
  - Ensure the `Backup` method is handling the encryption properly as well.
