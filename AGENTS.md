# Agent guidelines

This document provides guidelines for AI agents working on the nnut Go library, which wraps bbolt for enhanced embedded key-value storage with Write-Ahead Logging (WAL), automatic encoding via msgpack, and indexing.

## Code style

- **Naming conventions**:
  - Packages: lowercase, single word (e.g., `nnut`)
  - Types and Functions: PascalCase (e.g., `Store[T]`, `NewStore`)
  - Variables and Fields: camelCase (e.g., `keyField`, `indexFields`)
  - Constants: PascalCase (e.g., `userCount`)

- **Formatting**:
  - Use 2 spaces for indentation
  - UTF-8 encoding
  - LF line endings
  - Insert final newline
  - Trim trailing whitespace
  - Follow standard Go formatting with `gofmt`

- **Comments**:
  - Comments explain why code exists, not what it does
  - Function comments start with the function name and describe purpose
  - Inline comments justify design decisions or complex logic

- **Generics and types**:
  - Use generics for type-safe operations (e.g., `Store[T any]`)
  - Ensure key fields are strings
  - Validate struct tags at runtime for safety

- **Error handling**:
  - Return descriptive errors with context
  - Use `errors.New` for simple errors
  - Avoid panics; handle edge cases gracefully

- **Concurrency**:
  - Use mutexes for shared resources (e.g., WAL file, operation buffers)
  - Ensure thread-safe operations with proper locking

- **Performance**:
  - Use buffer pools to reduce allocations
  - Minimize reflection usage where possible
  - Batch operations for efficiency

## Testing

- **Test structure**:
  - Use `t.Parallel()` for concurrent test execution
  - Use `t.TempDir()` for temporary test databases
  - Clean up files with `defer os.Remove()` for both DB and WAL files
  - Test both success and failure paths

- **Test data**:
  - Define test structs with appropriate tags (e.g., `TestUser` with `nnut:"key"` and `nnut:"index:email"`)
  - Use realistic data for comprehensive coverage

- **Running tests**:
  - Execute all tests with `go test -v`
  - Run concurrency and race condition tests with `go test -race -run "Test(Concurrency|RaceConditions|Concurrent)"` to detect data races
  - Run fuzz tests with `go test -fuzz=FuzzQueryConditions -fuzztime=30s` for edge case detection
  - Ensure tests pass before committing changes
  - Run tests in parallel where possible

- **Coverage**:
  - Aim for high test coverage on critical paths
  - Include edge cases like empty keys, non-existent records, and concurrent access
  - Test race conditions with `-race` flag on concurrency tests
  - Use fuzzing for query conditions and edge cases

## Benchmarking

- **Benchmark setup**:
  - Use a template database created by `TestSetupBenchmarkDB` to avoid setup overhead
  - Copy the template for each benchmark run
  - Use diverse, realistic data (e.g., 10,000 users with varied names and emails)

- **Benchmark types**:
  - Measure performance of core operations: Get, Put, Delete, Query
  - Include batch operations and complex queries
  - Test sorting, limits, offsets, and conditions

- **Running benchmarks**:
  - Execute benchmarks with `go test -bench=. -benchtime=1s -benchmem` to include memory allocation stats
  - Compare results across changes to detect regressions
  - Profile memory and CPU usage for optimizations

## Linting and quality assurance

- **Linting**:
  - Run `go vet` to detect potential issues
  - Use `gofmt -d .` to check formatting
  - Address all warnings and errors

- **Dependencies**:
  - Use Go modules for dependency management
  - Keep dependencies minimal and up-to-date

- **Code Review**:
  - Ensure changes align with project goals (reliability, performance, simplicity)
  - Verify thread safety and error handling
  - Check for memory leaks or inefficiencies

## Development workflow

- **Before Code Changes**:
  - Run the tests
  - Run only relevant benchmarks to establish baseline
  - Understand the codebase structure and conventions

- **During development**:
  - Write tests and benchmarks first for new features
  - Follow code style guidelines
  - Run tests frequently to catch regressions

- **After code changes**:
  - Lint and format code
  - Update documentation if needed
  - Run full test suite and benchmarks
  - Ensure no performance regressions

## Maintenance

- **Refactoring**:
  - Break large functions into smaller, testable units
  - Extract interfaces for better testability
  - Improve error messages and documentation

- **Performance monitoring**:
  - Regularly run benchmarks to track performance
  - Profile code to identify bottlenecks
  - Optimize based on real usage patterns

This guide ensures consistent, high-quality contributions to the nnut project while maintaining its focus on reliability and performance.
