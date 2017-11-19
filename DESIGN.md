# overview

logd - A networked, sequenced logging system designed to be simple and fast,
and to adhere to the unix philosophy when reasonable. Overall, the design at
first is to put a single server in front, and have replicas . Each message gets an
continuous incremental id. There is an index so consumers can keep checkpoints
and return to them to resume reading. It is possible to tail a log stream and
respond to messages as they come in.

# architecture

Overall, my goal is for all application logic to happen in the `events`
namespace, the main piece of which is a queue. Then, there will be a server
component, which at first is going to be implemented as a socket server with a
text-based wire protocol. Finally, there is the logger component, which handles
reading from and writing to the backing log itself. This will at first be a
local file on disk, but the interface should be flexible enough to handle other
sources.

Currently, the implementation is that the server, upon receiving a command,
will call a method on the event queue that writes the command struct into a
channel which is read by the main loop. From there, the command is dispatched
to a handler function that validates arguments, executes the command (
write/subscribe to the log, check the id of head, and some other things), and
then responds to the consumer by calling methods on the command struct. As a
result of the server initially calling methods on the event queue when
receiving commands, the event q is set as an attribute on the server, however
as more servers are implemented, it makes sense to make the servers attributes
on the event queue.

## commands

- `MSG(*)`: Write a message to the log. Arguments are variadic. Each one will be
  written to the log in sequence.

- `READ(start, limit=0)`: Read the log from `start`. If `limit` is 0, read
  forever.  Otherwise, read until `limit` messages have been read.

- `REPLICATE(start)`: Replicates the log from `start`. Before release, this
  should change to REPLICATE(start, end=0), and `start` and `end` should be
  partition numbers, not message ids.

- `HEAD()`: Return the current id of the head of the log.

- `CLOSE()`: Closes the connection.

- `PING()`: Returns "PONG". Used for diagnostics.

- `SHUTDOWN()`: Shuts down the server. Only used in test.

### these are planned

- `REFUSE(partition=0)`: Refuse write commands, starting at `partition`. This
  effectively changes a server into a replica. Used when changing masters.

- `ACCEPT(partition=0)`: Accept write commands, starting at `partition`. This
  promotes a replica to a master.

- `STATS()`: Returns a bunch of stats.

## event queue

This code ensures that messages are processed in the order received, and
contains most of the application logic. The queue interacts with the server and
log by calling methods on them, however there is a caveat. Currently the server
calls a push method on the queue after parsing a received command.

## log

## server

# maybe later

This will happen later, maybe.

## clustering

It should distribute partitions among different servers. Also, I want to
experiment with other approaches when it's distributed.

