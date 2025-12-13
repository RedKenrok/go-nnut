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
BenchmarkBTreeInsert-8                  	 9484695	       654 ns/op	     162 B/op	       5 allocs/op
BenchmarkBTreeSearch-8                  	16891801	       358 ns/op	      15 B/op	       1 allocs/op
BenchmarkBTreeRange-8                   	 1333772	      4445 ns/op	    4688 B/op	      12 allocs/op
BenchmarkCountQuery-8                   	  529810	     10917 ns/op	   19480 B/op	      29 allocs/op
BenchmarkCountQueryIndex-8              	 6655213	       860 ns/op	     464 B/op	      11 allocs/op
BenchmarkCountQueryNoConditions-8       	  176835	     32119 ns/op	    7168 B/op	     840 allocs/op
BenchmarkCountQueryGreaterThan-8         	     751	   7927168 ns/op	 2793191 B/op	   69551 allocs/op
BenchmarkCount-8                        	  172503	     31960 ns/op	    7168 B/op	     840 allocs/op
BenchmarkDeleteQuery-8                  	     100	  50631474 ns/op	 1734481 B/op	   42755 allocs/op
BenchmarkDelete-8                       	  579942	     11171 ns/op	    1599 B/op	      32 allocs/op
BenchmarkDeleteBatch-8                  	 1092081	      5024 ns/op	    2178 B/op	      26 allocs/op
BenchmarkGetQuery-8                     	   40581	    139572 ns/op	   58960 B/op	    1531 allocs/op
BenchmarkGetQueryMultipleConditions-8   	   16456	    357479 ns/op	  164020 B/op	    3497 allocs/op
BenchmarkGetQuerySorting-8              	   10297	    521525 ns/op	  720677 B/op	    1729 allocs/op
BenchmarkGetQueryLimitOffset-8          	   72380	     72466 ns/op	   31582 B/op	     734 allocs/op
BenchmarkGetQueryNoConditions-8         	   41551	    141150 ns/op	   59825 B/op	    1542 allocs/op
BenchmarkGetQueryNonIndexedField-8      	   26259	    220491 ns/op	   83542 B/op	    2198 allocs/op
BenchmarkGetQueryComplexOperators-8     	    2884	   2011017 ns/op	 2048237 B/op	    9684 allocs/op
BenchmarkGetQueryLargeLimit-8           	    8156	    713585 ns/op	  279137 B/op	    8011 allocs/op
BenchmarkGetQueryOffsetOnly-8           	    9120	    775723 ns/op	  259961 B/op	    7215 allocs/op
BenchmarkGetQuerySortingAscending-8     	   11458	    518694 ns/op	  718249 B/op	    1525 allocs/op
BenchmarkGet-8                          	 2343332	      2524 ns/op	     948 B/op	      31 allocs/op
BenchmarkGetBatch-8                     	 3435333	      1705 ns/op	     654 B/op	      18 allocs/op
BenchmarkPut-8                          	  211905	     37177 ns/op	    4348 B/op	      65 allocs/op
BenchmarkPutBatch-8                     	  326954	     24985 ns/op	    4991 B/op	      51 allocs/op
BenchmarkHighLoadConcurrent-8           	  868899	      8321 ns/op	    3654 B/op	      86 allocs/op
```
