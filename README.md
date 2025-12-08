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
BenchmarkGet-8                       	 2954486	      2040 ns/op	     836 B/op	      26 allocs/op
BenchmarkBatchGet-8                  	 4056624	      1488 ns/op	     644 B/op	      17 allocs/op
BenchmarkPut-8                       	  164708	     53121 ns/op	    5920 B/op	      92 allocs/op
BenchmarkBatchPut-8                  	  191046	     46756 ns/op	    6915 B/op	      86 allocs/op
BenchmarkDelete-8                    	  752538	      9482 ns/op	     926 B/op	      16 allocs/op
BenchmarkBatchDelete-8               	 1849153	      3206 ns/op	    1507 B/op	      15 allocs/op
BenchmarkHighLoadConcurrent-8        	  242562	    105732 ns/op	   71889 B/op	    2062 allocs/op
BenchmarkQuery-8                     	    3787	   1503364 ns/op	  483689 B/op	   13311 allocs/op
BenchmarkQueryMultipleConditions-8   	    3289	   1810355 ns/op	  587773 B/op	   16104 allocs/op
BenchmarkQuerySorting-8              	   46336	    130587 ns/op	   67681 B/op	    1946 allocs/op
BenchmarkQueryLimitOffset-8          	    4020	   1444309 ns/op	  457090 B/op	   12610 allocs/op
BenchmarkQueryCount-8                	     849	   6951439 ns/op	 2193283 B/op	   61035 allocs/op
BenchmarkQueryCountIndex-8           	   47655	    125754 ns/op	    4272 B/op	     471 allocs/op
BenchmarkQueryNoConditions-8         	   48792	    121247 ns/op	   59774 B/op	    1540 allocs/op
BenchmarkQueryNonIndexedField-8      	   30085	    200495 ns/op	   85631 B/op	    2256 allocs/op
BenchmarkQueryComplexOperators-8     	   10000	    519348 ns/op	  207675 B/op	    5232 allocs/op
BenchmarkQueryLargeLimit-8           	     776	   7707258 ns/op	 2448874 B/op	   68639 allocs/op
BenchmarkQueryOffsetOnly-8           	     788	   7615560 ns/op	 2430471 B/op	   67939 allocs/op
BenchmarkQuerySortingAscending-8     	   46822	    128125 ns/op	   64625 B/op	    1646 allocs/op
BenchmarkQueryCountNoConditions-8    	   44370	    135773 ns/op	    4680 B/op	     522 allocs/op
BenchmarkQueryCountNonIndexed-8      	     864	   6990600 ns/op	 2790105 B/op	   69244 allocs/op
```
