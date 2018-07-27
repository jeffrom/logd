## writer design

### Goroutine overview

two goroutines:

1.  Caller API

- used by multiple goroutines via Write, Flush, etc
- buffers data which is sent to the other goroutine
  - this is configurable by time interval and batch size

2.  Client interface

- issues flushes to the server
- tries to reconnect when connection is broken
- shares failure with caller api goroutine when connection is broken

#### state

the possible states for a Writer are as follows:

1.  CLOSED

- initial state

2.  CONNECTED

- a connection has been created

3.  FLUSHING

- a BATCH request is in progress

4.  FAILING

- the connection is failing

### Caller API

```go
NewWriter(conf *Config, topic string) *Writer
```

- returns a new instance assigned to a topic

```go
Write(p []byte) (int, error)
```

- adds another message to the batch
- if the batch is full, sends the batch to the server
- if sending the batch fails, returns an error
- other calls while the request is failing return an error
- checks the connection to the server is created, returns an error if not

```go
Flush() error
```

- sends any pending batch data to the server, if any
- returns an error if the request fails

```go
Close() error
```

- flushes any pending batch data to the server, then closes the connection

```go
Reset(topic string)
```

- resets the writer to its initial state so it can be reused
