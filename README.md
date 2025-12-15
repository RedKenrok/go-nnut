# nnut

A Go library that wraps [go.etcd.io/bbolt](https://github.com/etcd-io/bbolt/#readme) to provide enhanced embedded key-value storage capabilities. It adds a Write Ahead Log (WAL) to reduce write frequency and implements automatic encoding using [msgpack](github.com/vmihailenco/msgpack/#readme) and indexing.

## Features

- **bbolt wrapper**: Builds on top of the reliable bbolt database for embedded key-value storage
- **Write Ahead Log (WAL)**: Reduces disk write frequency by buffering changes in a log before committing to the database
- **Automatic indices**: Maintains and updates indices automatically as data is inserted, updated, or deleted
- **Type-safe operations**: Uses Go generics for compile-time type checking
- **Efficient querying**: Supports filtering, sorting, and offsets with indexed fields

## Installation

```sh
go get github.com/redkenrok/go-nnut
```

## Configuration

The library supports various configuration options for customization:

```go
import "github.com/redkenrok/go-nnut"

config := &nnut.Config{
  FlushInterval:    time.Minute * 15, // Flushes every 15 minutes
  MaxBufferBytes:   10 * 1024 * 1024, // 10MB buffer
  WALPath:          "mydata.db.wal",  // Custom WAL path
}

db, err := nnut.OpenWithConfig("mydata.db", config)
if err != nil {
  log.Fatal(err)
}
```

- **FlushInterval**: How often to flush WAL to disk (default: 15 minutes)
- **MaxBufferBytes**: Maximum size of in-memory buffer before forcing flush (default: 10MB)
- **WALPath**: File path for the Write-Ahead Log (default: dbPath + ".wal")
- **FlushChannelSize**: Size of the flush channel buffer (default: 10)
- **Logger**: Custom logger implementing bbolt.Logger interface for integrated logging
- **BoltOptions**: Direct access to underlying bbolt.Options for advanced configuration

## Usage

The library provides fundamental key-value storage operations, directly wrapping `bbolt` for reliable embedded database functionality. It implements advanced typed data storage with automatic features like indexing and encryption, leveraging Go generics and struct tags for metadata-driven behavior.

First you define your Go structs with special tags to specify additional data such as the unique key of the data. The library uses reflection to interpret these tags and apply the appropriate behavior.

```go
type User struct {
   UUID  string `nnut:"key"`
   Name  string `nnut:"index"`
   Email string `nnut:"index"`
}
```

You can then create a type-safe store instance for your data structures. This will handle serialization and automatic feature application.

```go
func main() {
  db, err := nnut.Open("mydata.db")
  if err != nil {
    log.Fatal(err)
  }
  defer db.Close()

  // Create a store for User type
  userStore, err := nnut.NewStore[User](db, "users")
  if err != nil {
    log.Fatal(err)
  }
}
```

You can then perform type-safe create, read, update, and delete operations.

```go
// Create or update a user record
user := User{
  	UUID: "aa0000a0...",
  	Name: "Ron",
  	Email: "ron@example.com",
}
err = userStore.Put(context.Background(), user)
if err != nil {
   log.Fatal(err)
}

// Read a user by primary key
user, err = userStore.Get(context.Background(), "aa0000a0...")
if err != nil {
   log.Fatal(err)
}
log.Printf("User: %+v", user)

// Delete a user by primary key
err = userStore.Delete(context.Background(), "aa0000a0...")
if err != nil {
   log.Fatal(err)
}
```

### Batch operations

For better performance with multiple operations, use batch methods instead.

```go
// Batch put multiple users
users := []User{
   {UUID: "uuid1", Name: "Alice", Email: "user1@example.com"},
   {UUID: "uuid2", Name: "Bob", Email: "user2@example.com"},
}
err = userStore.PutBatch(context.Background(), users)
if err != nil {
   log.Fatal(err)
}

// Batch get by keys
users, err = userStore.GetBatch(context.Background(), []string{"uuid1", "uuid2"})
if err != nil {
   log.Fatal(err)
}
for _, user := range users {
   log.Printf("User: %+v", user)
}

// Batch delete by keys
err = userStore.DeleteBatch(context.Background(), []string{"uuid1", "uuid2"})
if err != nil {
   log.Fatal(err)
}
```

### Query

You can specify indexes on the data structure and the typed container will automatically ensure the indexes are kept up to date. You can then query, sort, and paginate over this index.

```go
type User struct {
   UUID  string `nnut:"key"`
   Name  string `nnut:"index"`
   Email string `nnut:"index"`
}

[...]

// Query users sorted by name
query := &nnut.Query{
   Index: "Name",
   Offset: 0,
   Limit: 48,
   Sort: nnut.Ascending, // or nnut.Descending
}

users, err := userStore.GetQuery(context.Background(), query)
if err != nil {
   log.Fatal(err)
}
for _, user := range users {
   log.Printf("User: %+v", user)
}
```

#### Query logic

Query data using conditions on indexed fields. Multiple conditions are combined with AND logic.

```go
// Get a user by their e-mail
query := &nnut.Query{
  Conditions: []nnut.Condition{
    {Field: "Email", Value: "ron@example.com"},
  },
}
users, err := userStore.GetQuery(context.Background(), query)
if err != nil {
  log.Fatal(err)
}
for _, user := range users {
  log.Printf("User: %+v", user)
}
```

Query data by multiple fields using multiple conditions:

```go
// Get users where email equals "ron@example.com" AND age is greater than 28
query := &nnut.Query{
	Conditions: []nnut.Condition{
	  {Field: "Email", Value: "ron@example.com"},
	  {Field: "Age", Value: 28, Operator: nnut.GreaterThan},
	},
}

users, err := userStore.GetQuery(context.Background(), query)
if err != nil {
  log.Fatal(err)
}
for _, user := range users {
  log.Printf("User: %+v", user)
}
```

Supported operators:
- **Equals**: Exact match (default)
- **GreaterThan**: Value greater than specified
- **LessThan**: Value less than specified
- **GreaterThanOrEqual**: Value greater than or equal to specified
- **LessThanOrEqual**: Value less than or equal to specified

#### Query count

To get the number of records matching a query without retrieving the data:

```go
// Count users with a specific age
query := &nnut.Query{
   Conditions: []nnut.Condition{
      {Field: "Age", Value: 28},
   },
}
count, err := userStore.CountQuery(context.Background(), query)
if err != nil {
  log.Fatal(err)
}
 log.Printf("Found %d users with that email", count)
 ```

### Delete with queries

You can delete records matching query conditions:

```go
// Delete users older than 30
query := &nnut.Query{
   Conditions: []nnut.Condition{
      {Field: "Age", Value: 30, Operator: nnut.GreaterThanOrEqual},
   },
}
deletedCount, err := userStore.DeleteQuery(context.Background(), query)
if err != nil {
   log.Fatal(err)
}
log.Printf("Deleted %d users", deletedCount)
```

## Backup and Recovery

nnut provides built-in backup functionality to create point-in-time copies of your database for disaster recovery or migration.

### Creating a Backup

Use the `Export` method to create a backup of the database:

```go
db, err := nnut.Open("mydata.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Create a backup
err = db.Export("mydata_backup.db")
if err != nil {
    log.Fatal(err)
}
```

This will:
- Flush all pending operations to ensure the database is up-to-date
- Create a consistent copy of the database file

### Restoring from Backup

To restore from a backup, simply open the backup file as a new database:

```go
// Open the backup as the new database
db, err := nnut.Open("mydata_backup.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// The database is now restored with all data from the backup
```

### Best Practices

- **Regular Backups**: Schedule regular exports to prevent data loss
- **Offsite Storage**: Store backups in a separate location from the primary database
- **Test Restores**: Periodically test restoring from backups to ensure they work
- **Naming Convention**: Use timestamps in backup filenames for easy identification

```go
// Example: Backup with timestamp
timestamp := time.Now().Format("20060102_150405")
backupPath := fmt.Sprintf("mydata_backup_%s.db", timestamp)
err = db.Export(backupPath)
```

## Logging

nnut integrates with bbolt's logging system to provide comprehensive logging support. You can configure custom logging to monitor database operations, debug issues, and track performance.

```go
import (
  "fmt"
  "log"
  "github.com/redkenrok/go-nnut"
)

// Create a custom logger (must implement bbolt.Logger interface)
type customLogger struct {
  *log.Logger
}

func (c *customLogger) Debug(v ...interface{}) { c.Printf("DEBUG: "+fmt.Sprint(v...)) }
func (c *customLogger) Debugf(format string, v ...interface{}) { c.Printf("DEBUG: "+format, v...) }
// ... implement other bbolt.Logger methods

logger := &customLogger{log.New(os.Stdout, "nnut: ", log.LstdFlags)}

// Configure nnut with logging
config := &nnut.Config{
  Logger: logger,
  // ... other config options
}

db, err := nnut.OpenWithConfig("mydata.db", config)
```

### bbolt Options

For advanced use cases, you can pass bbolt-specific options directly through the `BoltOptions` field:

```go
import (
  "syscall"
  "time"
  "go.etcd.io/bbolt"
)

config := &nnut.Config{
  Logger: &customLogger{log.New(os.Stdout, "nnut: ", log.LstdFlags)},
  BoltOptions: &bbolt.Options{
    Timeout:    time.Second * 5,
    NoSync:     false,
    MmapFlags:  syscall.MAP_POPULATE,
    // ... other bbolt options
  },
}
```

The logger will receive messages from both nnut operations (database opening, WAL replay, flushing, CRUD operations) and underlying bbolt operations (file operations, transactions).

## Benchmarks

Run benchmarks with:

```sh
go test -bench=. -benchtime=5s -benchmem
```

Example output (results may vary by hardware):

```
goos: darwin
goarch: amd64
pkg: github.com/redkenrok/go-nnut
cpu: Intel(R) Core(TM) i5-1038NG7 CPU @ 2.00GHz
BenchmarkBTreeInsert-8                  	 9396440	       651 ns/op	     162 B/op	       5 allocs/op
BenchmarkBTreeSearch-8                  	16184138	       340 ns/op	      15 B/op	       1 allocs/op
BenchmarkBTreeRange-8                   	 1365384	      4367 ns/op	    4688 B/op	      12 allocs/op
BenchmarkCountQuery-8                   	  503740	     10289 ns/op	   19480 B/op	      29 allocs/op
BenchmarkCountQueryIndex-8              	 7083350	       845 ns/op	     464 B/op	      11 allocs/op
BenchmarkCountQueryNoConditions-8       	  498370	     11238 ns/op	     464 B/op	      11 allocs/op
BenchmarkCountQueryGreaterThan-8        	     759	   7796908 ns/op	 2976666 B/op	   69821 allocs/op
BenchmarkCount-8                        	  482259	     10992 ns/op	     464 B/op	      11 allocs/op
BenchmarkDeleteQuery-8                  	   12747	    498717 ns/op	   25104 B/op	     522 allocs/op
BenchmarkDelete-8                       	  333681	    415810 ns/op	    5410 B/op	     103 allocs/op
BenchmarkDeleteBatch-8                  	  354183	    482015 ns/op	    5660 B/op	      89 allocs/op
BenchmarkGetQuery-8                     	   36595	    158253 ns/op	   59099 B/op	    1549 allocs/op
BenchmarkGetQueryMultipleConditions-8   	   14376	    415235 ns/op	  168608 B/op	    3683 allocs/op
BenchmarkGetQuerySorting-8              	   10564	    553829 ns/op	  720553 B/op	    1728 allocs/op
BenchmarkGetQueryLimitOffset-8          	   75399	     78781 ns/op	   31579 B/op	     734 allocs/op
BenchmarkGetQueryNoConditions-8         	   37364	    157194 ns/op	   59819 B/op	    1542 allocs/op
BenchmarkGetQueryNonIndexedField-8      	   23743	    246155 ns/op	   84400 B/op	    2222 allocs/op
BenchmarkGetQueryComplexOperators-8     	    2444	   2269347 ns/op	 2041322 B/op	    9557 allocs/op
BenchmarkGetQueryLargeLimit-8           	    7261	    819011 ns/op	  281112 B/op	    8203 allocs/op
BenchmarkGetQueryOffsetOnly-8           	    8013	    737024 ns/op	  261789 B/op	    7389 allocs/op
BenchmarkGetQuerySortingAscending-8     	   11192	    522248 ns/op	  718284 B/op	    1542 allocs/op
BenchmarkGet-8                          	 1912294	      3184 ns/op	     952 B/op	      32 allocs/op
BenchmarkGetBatch-8                     	 2768272	      2079 ns/op	     702 B/op	      18 allocs/op
BenchmarkPut-8                          	  221076	     40256 ns/op	    4258 B/op	      66 allocs/op
BenchmarkPutBatch-8                     	  291127	     30955 ns/op	    5020 B/op	      52 allocs/op
BenchmarkHighLoadConcurrent-8           	  780380	      7741 ns/op	    3740 B/op	      89 allocs/op
```
