# Task 5: Value Encryption

## Objective
Add support for automatic encryption of specific fields in stored structs using `nnut:"encrypt"` tags, enhancing data security at the field level.

## Background
Users may need to encrypt sensitive data fields while keeping others plaintext. This feature allows selective encryption with optional salt fields.

## Detailed Implementation Plan

### 1. Extend Tag Parsing
- **Recognize new tags**: `nnut:"encrypt"` and `nnut:"encrypt:<salt_field>"`.
- **Validation**: Ensure encrypted fields are not indexed (conflict check).
- **Store metadata**: Add fields to `Store[T]` to track encrypted fields and salts.

### 2. Configuration Updates
- **Add to Config**: `EncryptionKey []byte` (32 bytes for AES-256), `EncryptionAlgorithm string` (default "AES-GCM").
- **Validation**: Require key when encryption is used.

### 3. Encryption Logic
- **In Put/PutBatch**: Before marshaling, encrypt tagged fields using AES-GCM.
- **Salt handling**: Use referenced field as salt, generate if empty.
- **Algorithm**: Support AES-GCM for authenticated encryption.

### 4. Decryption Logic
- **In Get/GetBatch**: After unmarshaling, decrypt tagged fields.
- **Error handling**: Fail gracefully on decryption errors.

### 5. Security Considerations
- **Key management**: Advise users on secure key storage.
- **Salt generation**: Automatic random salt for fields without reference.
- **No indexing**: Prevent indexing of encrypted fields.

### 6. Testing
- **Encryption/decryption**: Verify data integrity.
- **Salt variations**: Test with and without salt fields.
- **Error cases**: Invalid keys, corrupted data.

### 7. Documentation
- **Tag usage**: Examples of encrypt tags.
- **Configuration**: How to set encryption keys.
- **Limitations**: No indexing, performance impact.

### 8. Risks and Mitigations
- **Performance**: Encryption adds overhead; document impact.
- **Security**: Ensure proper key handling; no key logging.
- **Compatibility**: New feature; ensure backward compatibility.

## Success Criteria
- Fields tagged `nnut:"encrypt"` are automatically encrypted/decrypted.
- Supports optional salt fields.
- Secure implementation with AES-GCM.
- Comprehensive documentation and tests.
