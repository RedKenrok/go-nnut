# nnut

A Go library that wraps [go.etcd.io/bbolt](https://github.com/etcd-io/bbolt/#readme) to provide enhanced embedded key-value storage capabilities. It adds a Write Ahead Log (WAL) to reduce write frequency and implements automatic encoding using [msgpack](github.com/vmihailenco/msgpack/#readme) and indexing.

## Features

- **bbolt wrapper**: Builds on top of the reliable bbolt database for embedded key-value storage
- **Write Ahead Log (WAL)**: Reduces disk write frequency by buffering changes in a log before committing to the database
- **Automatic indices**: Maintains and updates indices automatically as data is inserted, updated, or deleted
- **Type-safe operations**: Uses Go generics for compile-time type checking
- **Efficient querying**: Supports filtering, sorting, and pagination with indexed fields

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
BenchmarkCount-8                     	   27000	    223429 ns/op	    4712 B/op	     524 allocs/op
BenchmarkGet-8                       	 2468985	      2412 ns/op	     940 B/op	      31 allocs/op
BenchmarkBatchGet-8                  	 3641587	      1648 ns/op	     645 B/op	      17 allocs/op
BenchmarkPut-8                       	  223033	     34535 ns/op	    5061 B/op	      75 allocs/op
BenchmarkBatchPut-8                  	  205912	    156884 ns/op	  228786 B/op	      60 allocs/op
BenchmarkDelete-8                    	  539360	     11011 ns/op	    1666 B/op	      33 allocs/op
BenchmarkBatchDelete-8               	 1114160	      5194 ns/op	    2358 B/op	      27 allocs/op
BenchmarkHighLoadConcurrent-8        	  915861	      6367 ns/op	    3695 B/op	      85 allocs/op
BenchmarkWALTruncation-8             	84286142	        67.07 ns/op	      48 B/op	       1 allocs/os
BenchmarkQuery-8                     	   45436	    130641 ns/op	   58190 B/op	    1435 allocs/op
BenchmarkQueryMultipleConditions-8   	   17932	    367999 ns/op	  160891 B/op	    3263 allocs/op
BenchmarkQuerySorting-8              	   10794	    489874 ns/op	  720485 B/op	    1729 allocs/op
BenchmarkQueryLimitOffset-8          	   85545	     68955 ns/op	   31581 B/op	     734 allocs/op
BenchmarkQueryCount-8                	  609810	     10032 ns/op	   19352 B/op	      23 allocs/op
BenchmarkQueryCountIndex-8           	10909909	       547.5 ns/op	     336 B/op	       5 allocs/op
BenchmarkQueryNoConditions-8         	   43579	    136864 ns/op	   59823 B/op	    1542 allocs/op
BenchmarkQueryNonIndexedField-8      	   27314	    220997 ns/op	   84186 B/op	    2216 allocs/op
BenchmarkQueryComplexOperators-8     	    2504	   2201169 ns/op	 2100744 B/op	   11359 allocs/op
BenchmarkQueryLargeLimit-8           	    7552	    692992 ns/op	  274994 B/op	    7627 allocs/op
BenchmarkQueryOffsetOnly-8           	    9489	    634947 ns/op	  256579 B/op	    6927 allocs/op
BenchmarkQuerySortingAscending-8     	   14042	    426271 ns/op	  717409 B/op	    1429 allocs/op
BenchmarkQueryCountNoConditions-8    	   43670	    139538 ns/op	    4712 B/op	     524 allocs/op
BenchmarkQueryCountNonIndexed-8      	     781	   7767698 ns/op	 2970987 B/op	   69275 allocs/op
BenchmarkDeleteQuery-8               	      55	  97027197 ns/op	13115395 B/op	  122809 allocs/op
```
