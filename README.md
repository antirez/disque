[![Build Status](https://travis-ci.org/antirez/disque.svg)](https://travis-ci.org/antirez/disque)

Disque, an in-memory, distributed job queue
===

Disque is ongoing experiment to build a distributed, in memory, message broker.
Its goal is to capture the essence of the "Redis as a jobs queue" use case,
which is usually implemented using blocking list operations, and move
it into an ad-hoc, self-contained, scalable, and fault tolerant design, with
simple to understand properties and guarantees, but still resembling Redis
in terms of simplicity, performances, and implementation as a C non-blocking
networked server.

Currently (27 April 2015) the project is just an alpha quality preview, that was developed in roughly 120 hours, 244 different commits performed in 72 different days, often at night and during weekends. In short: don't expect much or rock solid production systems here.

**WARNING: This is alpha code NOT suitable for production. The implementation and API will likely change in significant ways during the next months. The code and algorithms are not tested enough. A lot more work is needed.**

What is a message queue?
---

*Hint: skip this section if you are familiar with message queues.*

Do you know how humans use text messages to communicate right? I could write
my wife "please get the milk at the store", and she maybe will reply "Ok message
received, I'll get two bottles on my way home".

A message queue is the same as human text messages, but for computer programs.
For example a web application, when an user subscribes, may send another
process that handles sending emails "please send the confirmation email
to tom@example.net".

Message systems like Disque allow communication between processes using
different queues. So a process can send a message into a queue with a given
name, and only processes fetching messages from this queue will return those
messages. Moreover multiple processes can listen for messages in a given
queue, and multiple processes can send messages to the same queue.

The important part of a message queue is to be able to provide guarantees so
that messages are eventually delivered even in the face of failures. So even
if in theory implementing a message queue is very easy, to write a very
robust and scalable one is harder than it may appear.

Give me the details!
---

Disque is a distributed and fault tolerant message broker, so it works as
middle layer among processes that want to exchange messages.

Producers add messages that are served to consumers.
Since message queues are often used in order to process delayed
jobs, Disque often uses the term "job" in the API and in the documentation,
however jobs are actually just messages in the form of strings, so Disque
can be used for other use cases. In this documentation "jobs" and "messages"
are used in an interchangeable way.

Job queues with a producer-consumer model are pretty common, so the devil is
in the details. A few details about Disque are:

Disque is a **synchronously replicated job queue**. By default when a new job is added, it is replicated to W nodes before the client gets an acknowledge about the job being added. W-1 nodes can fail and still the message will be delivered.

Disque supports both **at-least-once and at-most-once** delivery semantics. At least once delivery semantics is where most efforts were spent in the design and implementation, while the at most once semantics is a trivial result of using a *retry time* set to 0 (which means, never re-queue the message again) and a replication factor of 1 for the message (not strictly needed, but it is useless to have multiple copies of a message around if it will be delivered at most one time). You can have, at the same time, both at-least-once and at-most-once jobs in the same queues and nodes, since this is a per message setting.

Disque at-least-once delivery is designed to **approximate single delivery** when possible, even during certain kinds of failures. This means that while Disque can only guarantee a number of deliveries equal or greater to one, it will try hard to avoid multiple deliveries whenever possible.

Disque is a distributed system where **all nodes have the same role** (aka, it is multi-master). Producers and consumers can attach to whatever node they like, and there is no need for producers and consumers of the same queue, to stay connected to the same node. Nodes will automatically exchange messages based on load and client requests.

Disque is Available (it is an eventually consistent AP system in CAP terms): producers and consumers can make progresses as long as a single node is reachable.

Disque supports **optional asynchronous commands** that are low latency for the client but provide less guarantees. For example a producer can add a job to a queue with a replication factor of 3, but may want to run away before knowing if the contacted node was really able to replicate it to the specified number of nodes or not. The node will replicate the message in the background in a best effort way.

Disque **automatically re-queue messages that are not acknowledged** as already processed by consumers, after a message-specific retry time. There is no need for consumers to re-queue a message if it was not processed.

Disque uses **explicit acknowledges** in order for a consumer to signal a message as delivered (or, using a different terminology, to signal a job as already processed).

Disque queues only provides **best effort ordering**. Each queue sorts messages based on the job creation time, which is obtained using the *wall clock* of the local node where the message was created (plus an incremental counter for messages created in the same millisecond), so messages created in the same node are normally delivered in the same order they were created. This is not causal ordering since correct ordering is violated in different cases: when messages are re-issued because not acknowledged, because of nodes local clock drifts, and when messages are moved to other nodes for load balancing and federation (in this case you end with queues having jobs originated in different nodes with different wall clocks). However all this also means that normally messages are not delivered in random order and usually messages created first are delivered first.

Note that since Disque does not provide strict FIFO semantics, technically speaking it should not be called a *message queue*, and it could better identified as a message broker. However I believe that at this point in the IT industry a *message queue* is often more lightly used to identify a generic broker that may or may not be able to guarantee order in all the cases. Given that we document very clearly the semantics, I grant myself the right to call Disque a message queue anyway.

Disque provides the user with fine-grained control for each job **using three time related parameters**, and one replication parameter. For each job, the user can control:

1. The replication factor (how many nodes have a copy).
2. The delay time (the min time Disque will wait before putting the message in a queue, making the message deliverable).
3. The retry time (how much time should elapse, since the last time the job was queued, and without an acknowledge about the job delivery, before the job is re-queued again for delivery).
4. The expire time (how much time should elapse for the job to be deleted regardless of the fact it was successfully delivered, i.e. acknowledged, or not).

Finally, Disque supports optional disk persistence, which is not enabled by default, but that can be handy in single data center setups and during restarts.

ACKs and retries
---

Disque implementation of *at least once* delivery semantics is designed in order
to avoid multiple delivery during certain classes of failures. It is not able to guarantee that no multiple deliveries will occur. However there are many *at least once* workloads where duplicated deliveries are acceptable (or explicitly handled), but not desirable either. A trivial example is sending emails to users (it is not terrible if an user gets a duplicated email, but is important to avoid it when possible), or doing idempotent operations that are expensive (all the times where it is critical for performances to avoid multiple deliveries).

In order to avoid multiple deliveries when possible, Disque uses client ACKs. When a consumer processes a message correctly, it should acknowledge this fact to Disque. ACKs are replicated to multiple nodes, and are garbage collected as soon as the system believes it is unlikely that more nodes in the cluster have the job (the ACK refers to) still active. Under memory pressure or under certain failure scenarios, ACKs are eventually discarded.

More explicitly:

1. A job is replicated to multiple nodes, but usually only *queued* in a single node. There is a difference between having a job in memory, and queueing it for delivery.
2. Nodes having a copy of a message, if a certain amount of time has elapsed without getting the ACK for the message, will re-queue it. Nodes will run a best-effort protocol to avoid re-queueing the message multiple times.
3. ACKs are replicated and garbage collected across the cluster so that eventually processed messages are evicted (this happens ASAP if there are no failures nor network partitions).

For example, if a node having a copy of a job gets partitioned away during the time the job gets acknowledged by the consumer, it is likely that when it returns back (in a reasonable amount of time, that is, before the retry time is reached) it will be informed about the ACK and will avoid to re-queue the message. Similarly jobs can be acknowledged during a partition to just a single node available, and when the partition heals the ACK will be propagated to other nodes that may still have a copy of the message.

So an ACK is just a **proof of delivery** that is replicated and retained for
some time in order to make multiple deliveries less likely to happen in practice.

As already mentioned, In order to control replication and retries, a Disque job has the following associated properties: number of replicas, delay, retry and expire.

If a job has a retry time set to 0, it will get queued exactly once (and in this case a replication factor greater than 1 is useless, and signaled as an error to the user), so it will get delivered either a single time or will never get delivered. While jobs can be persisted on disk for safety, queues aren't, so this behavior is guaranteed even when nodes restart after a crash, whatever the persistence configuration is. However when nodes are manually restarted by the sysadmin, for example for upgrades, queues are persisted correctly and reloaded at startup, since the store/load operation is atomic in this case, and there are no race conditions possible (it is not possible that a job was delivered to a client and is persisted on disk as queued at the same time).

Fast acknowledges
---

Disque supports a faster way to acknowledge processed messages, via the
`FASTACK` command. The normal acknowledge is very expensive from the point of
view of messages exchanged between nodes, this is what happens during a normal
acknowledge:

1. The client sends ACKJOB to one node.
2. The node sends a SETACK message to everybody it believes to have a copy.
3. The receivers of SETACK reply with GOTACK to confirm.
4. The node finally sends DELJOB to all the nodes.

*Note: actual garbage collection is more complex in case of failures and is explained in the state machine later. The above is what happens 99% of times.*

If a message is replicated to 3 nodes, acknowledging requires 1+2+2+2 messages,
for the sake of retaining the ack if some nodes may not be reached when the
message is acknowledged. This makes the probability of multiple deliveries of
this message less likely.

However the alternative **fast ack**, while less reliable, is much faster
and invovles exchanging less messages. This is how a fast acknowledge works:

1. The client sends `FASTACK` to one node.
2. The node evicts the job and sends a best effort DELJOB to all the nodes that may have a copy, or to all the cluster if the node was not aware of the job.

If during a fast acknowledge a node having a copy of the message is not
reachable, for example because of a network partition, the node will deliver
the message again, since it has a non acknowledged copy of the message and
there is nobody able to inform it the message was acknowledged when it
returns back available.

If the network you are using is pretty reliable, you are very concerned with
performances, and multiple deliveries in the context of your applications are
a non issue, then `FASTACK` is probably the way to go.


Dead letter queue
---

Many message queues implement a feature called *dead letter queue*. It is
a special queue used in order to accumulate messages that cannot be processed
for some reason. Common causes could be:

1. The message was delivered too many times but never correctly processed.
2. The message time to live reached zero before it was processed.
3. Some worker explicitly asked the system to flag the message as having issues.

The idea is that the administrator of the system checks (usually via automatic
systems) if there is something in the dead letter queue in order to understand
if there is some software error or other kind of error preventing messages
to be processed as expected.

Since Disque is an in memory system the message time to live is an important
property. When it is reached, we want messages to go away, since the TTL should
be choosen so that after such a time it is no longer meaningful to process
the message. In such a system, to use memory and create a queue in response
to an error or to messages timing out looks like a non optimal idea. Moreover
for the distributed nature of Disque dead letters could end spawning multiple
nodes and having duplicated entries inside.

So Disque uses a different approach. Each node message representation has
two counters: a **nacks counter** and an **additional deliveries** counter.
The counters are not consistent among nodes having a copy of the same message,
they are just best effort counters that may not increment in some node during
network partitions.

The idea of these two counters is that one is incremented every time a worker
uses the `NACK` command to tell the queue the message was not processed correctly
and should be put back on the queue ASAP. The other is incremented for every other condition (different than `NACK` call) that requires a message to be put back
on the queue again. This includes messages that get lost and are enqueued again
or messages that are enqueued in one side of the partition since the message
was processed in the other side and so forth.

Using the `GETJOB` command with the `WITHCOUNTERS` option, or using the
`SHOW` command to inspect a job, it is possible to retrieve these two counters
together with the other jobs information, so if a worker, before processing
a message, sees the counters having too high values, it can notify operations
people in multiple ways:

1. It may send an email.
2. Set a flag in a monitoring system.
3. Put the message in a special queue (simulating the dead letter feature).
4. Attempt to process the message and report the stack trace of the error if any.

Basically the exact handling of the feature is up to the application using
Disque. Note that the counters don't need to be consistent in the face of
failures or network partitions: the idea is that eventually if a message has
issues the counters will get incremented enough times to reach the limit
selected by the application as a warning threshold.

The reason for having two distinct counters is that applications may want
to handle the case of explicit negative acknowledges via `NACK` differently
than multiple deliveries because of timeouts or messages lost.

Disque and disk persistence
---

Disque can be operated in-memory only, using the synchronous replication as
durability guarantee, or can be operated using the Append Only File where
jobs creations and evictions are logged on disk (with configurable fsync
policies) and reloaded at restart.

AOF is recommended especially if you run in a single availability zone
where a mass reboot of all your nodes is possible.

Normally Disque only reloads jobs data in memory, without populating queues,
since anyway not acknowledged jobs are requeued eventually. Moreover reloading
queue data is not safe in the case of at-most-once jobs having the retry value
set to 0. However a special option is provided in order to reload the full
state from the AOF. This is used together with an option that allows to
shutdown the server just after the AOF is generated from scratch, in order to
be safe to reload even jobs with retry set to 0, since the AOF is generated
while the server no longer accepts commands from clients, so no race condition
is possible.

Even when running memory-only, Disque is able to dump its memory on disk and reload from disk on controlled restarts, for example in order to upgrade the software.

This is how to perform a controlled restart, that works whether AOF is enabled
or not:

1. CONFIG SET aof-enqueue-jobs-once yes
2. CONFIG REWRITE
3. SHUTDOWN REWRITE-AOF

At this point we have a freshly generated AOF on disk, and the server is
configured in order to load the full state only at the next restart (the
`aof-enqueue-jobs-once` is automatically turned off after the restart).

We can just restart the server with the new software, or in a new server, and
it will restart with the full state. Note that the `aof-enqueue-jobs-once`
implies to load the AOF even if the AOF support is switched off, so there is
no need to enable AOF just for the upgrade of an in-memory only server.

Job IDs
---

Disque jobs are uniquely identified by an ID like the following:

DI0f0c644fd3ccb51c2cedbd47fcb6f312646c993c05a0SQ

Job IDs always start with "DI" and end with "SQ" and are always composed of
exactly 48 characters.

We can split an ID into multiple parts:

DI | 0f0c644f | d3ccb51c2cedbd47fcb6f312646c993c | 05a0 | SQ

1. DI is the prefix
2. 0f0c644f is the first 8 bytes of the node ID where the message was generated.
3. d3ccb51c2cedbd47fcb6f312646c993c is the 128 bit ID pesudo random part in hex.
4. 05a0 is the Job TTL in minutes. Because of it, message IDs can be expired safely even without having the job representation.
5. SQ is the suffix.

IDs are returned by ADDJOB when a job is successfully created, are part of
the GETJOB output, and are used in order to acknowledge that a job was
correctly processed by a worker.

Part of the node ID is included in the message so that a worker processing
messages for a given queue can easily guess what are the nodes where jobs
are created, and move directly to these nodes to increase efficiency instead
of listening for messages in a node that will require to fetch messages from
other nodes.

Only 32 bits of the original node ID is included in the message, however
in a cluster with 100 Disque nodes, the probability of two nodes to have
identical 32 bit ID prefixes is given by the birthday paradox:

    P(100,2^32) = .000001164

In case of collisions, the workers may just do a non-efficient choice.

Collisions in the 128 bits random part are believed to be impossible,
since it is computed as follows.

    128 bit ID = SHA1(seed || counter)

Where:

* **seed** is a seed generated via `/dev/urandom` at startup.
* **counter** is a 64 bit counter incremented at every ID generation.

Setup
===

To play with Disque please do the following:

1. Compile Disque, if you can compile Redis, you can compile Disque, it's the usual no external deps thing. Just type `make`. Binaries (`disque` and `disque-server`) will end up in the `src` directory.
2. Run a few Disque nodes in different ports. Create different `disque.conf` files following the example `disque.conf` in the source distribution.
3. After you have them running, you need to join the cluster. Just select a random node among the nodes you are running, and send the command `CLUSTER MEET <ip> <port>` for every other node in the cluster.

**Please note that you need to open two TCP ports on each node**, the base port of the Disque instance, for example 7711, plus the cluster bus port, which is always at a fixed offset, obtained summing 10000 to the base port, so in the above example, you need to open both 7711 and 17711. Disque uses the base port to communicate with clients and the cluster bus port to communicate with other Disque processes.

To run a node, just call `./disque-server`.

For example if you are running three Disque servers in port 7711, 7712, 7713 in order to join the cluster you should use the `disque` command line tool and run the following commands:

    ./disque -p 7711 cluster meet 127.0.0.1 7712
    ./disque -p 7711 cluster meet 127.0.0.1 7713

Your cluster should now be ready. You can try to add a job and fetch it back
in order to test if everything is working:

    ./disque -p 7711
    127.0.0.1:7711> ADDJOB queue body 0
    DI0f0c644ffca14064ced6f8f997361a5c0af65ca305a0SQ
    127.0.0.1:7711> GETJOB FROM queue
    1) 1) "queue"
       2) "DI0f0c644ffca14064ced6f8f997361a5c0af65ca305a0SQ"
       3) "body"

Remember that you can add and get jobs from different nodes as Disque
is multi master. Also remember that you need to acknowledge jobs otherwise
they'll never go away from the server memory (unless the time to live is
reached).

Main API
===

Disque API is composed of a small set of commands, since the system solves a
single very specific problem. The three main commands are:

## `ADDJOB queue_name job <ms-timeout> [REPLICATE <count>] [DELAY <sec>] [RETRY <sec>] [TTL <sec>] [MAXLEN <count>] [ASYNC]`

Adds a job to the specified queue. Arguments are as follows:

* *queue_name* is the name of the queue, any string, basically. You don't need to create queues, if it does not exist, it gets created automatically. If it has no longer jobs, it gets removed.
* *job* is a string representing the job. Disque is job meaning agnostic, for it a job is just a message to deliver. Job max size is 4GB.
* *ms-timeout* is the command timeout in milliseconds. If no ASYNC is specified, and the replication level specified is not reached in the specified number of milliseconds, the command returns with an error, and the node does a best-effort cleanup, that is, it will try to delete copies of the job across the cluster. However the job may still be delivered later. Note that the actual timeout resolution is 1/10 of second or worse with the default server hz.
* *REPLICATE count* is the number of nodes the job should be replicated to.
* *DELAY sec* is the number of seconds that should elapse before the job is queued by any server.
* *RETRY sec* period after which, if no ACK is received, the job is put again into the queue for delivery. If RETRY is 0, the job has at-most-once delivery semantics.
* *TTL sec* is the max job life in seconds. After this time, the job is deleted even if it was not successfully delivered.
* *MAXLEN count* specifies that if there are already *count* messages queued for the specified queue name, the message is refused and an error reported to the client.
* *ASYNC* asks the server to let the command return ASAP and replicate the job to other nodes in the background. The job gets queued ASAP, while normally the job is put into the queue only when the client gets a positive reply.

The command returns the Job ID of the added job, assuming ASYNC is specified, or if the job was replicated correctly to the specified number of nodes. Otherwise an error is returned.

## `GETJOB [NOHANG] [TIMEOUT <ms-timeout>] [COUNT <count>] [WITHCOUNTERS] FROM queue1 queue2 ... queueN`

Return jobs available in one of the specified queues, or return NULL
if the timeout is reached. A single job per call is returned unless a count greater than 1 is specified. Jobs are returned as a three elements array containing the queue name, the Job ID, and the job body itself. If jobs are available into multiple queues, queues are processed left to right.

If there are no jobs for the specified queues the command blocks, and messages are exchanged with other nodes, in order to move messages about these queues to this node, so that the client can be served.

Options:

* **NOHANG**: Ask the command to don't block even if there are no jobs in all the specified queues. This way the caller can just check if there are available jobs without blocking at all.
* **WITHCOUNTERS**: Return the best-effort count of NACKs (negative acknowledges) received by this job, and the number of additional deliveries performed for this ob. See the *Dead Letters* section for more information.

## `ACKJOB jobid1 jobid2 ... jobidN`

Acknowledges the execution of one or more jobs via job IDs. The node receiving the ACK will replicate it to multiple nodes and will try to garbage collect both the job and the ACKs from the cluster so that memory can be freed.

## `FASTACK jobid1 jobid2 ... jobidN`

Performs a best effort cluster wide deletion of the specified job IDs. When the
network is well connected and there are no node failures, this is equivalent to
`ACKJOB` but much faster (less messages exchanged), however during failures it
is more likely that fast acknowledges will result into multiple deliveries of
the same messages.

## `WORKING jobid`

Claims to be still working with the specified job, and asks Disque to postpone
the next time it will deliver again the job. The next delivery is postponed
for the job retry time, however the command works in a **best effort** way
since there is no way to guarantee during failures that another node in a
different network partition is performing a delivery of the same job.

Another limitation of the `WORKING` command is that it cannot be sent to
nodes not knowing about this particular job. In such a case the command replies
with a `NOJOB` error. Similarly if the job is already acknowledged an error
is returned.

Note that the `WORKING` command is refused by Disque nodes if 50% of the job
time to live has already elapsed. This limitation makes Disque safer since
usually the *retry* time is much smaller than the time to live of a job, so
it can't happen that a set of broken workers monopolize a job with `WORKING`
never processing it. After 50% of the TTL elapsed, the job will be delivered
to other workers anyway.

Note that `WORKING` returns the number of seconds you (likely) postponed the
message visiblity for other workers (the command basically returns the
*retry* time of the job), so the worker should make sure to send the next
`WORKING` command before this time elapses. Moreover a worker that may want
to use such an iterface may fetch the retry value with the `SHOW` command
when starting to process a message, or may simply send a `WORKING` command
ASAP, like in the following example (in pseudo code):

    retry = WORKING(jobid)
    RESET timer
    WHILE ... work with the job still not finished ...
        IF timer reached 80% of the retry time
            WORKING(jobid)
            RESET timer
        END
    END

## `NACK <job-id> ... <job-id>`

The `NACK` command tells Disque to put back the job in the queue ASAP. It
is very similar to `ENQUEUE` but it increments the job `nacks` counter
instead of the `additional-deliveries` counter. The command should be used
when the worker was not able to process a message and wants the message to
be put back into the queue in order to be processed again.

Other commands
===

## `INFO`

Generic server information / stats.

## `HELLO`

Returns hello format version, this node ID, all the nodes IDs, IP addresses,
ports, and priority (lower is better, means node more available).
Clients should use this as an handshake command when connecting with a
Disque node.

## `QLEN <qname>`

Return the length of the queue.

## `QSTAT <qname> (TODO)`

Return produced ... consumed ... idle ... sources [...] ctime ...

## `QPEEK <qname> <count>`

Return, without consuming from queue, *count* jobs. If *count* is positive
the specified number of jobs are returned from the oldest to the newest
(in the same best-effort FIFO order as GETJOB). If *count* is negative the
commands changes behavior and shows the *count* newest jobs, from the newest
from the oldest.

## `ENQUEUE <job-id> ... <job-id>`

Queue jobs if not already queued.

## `DEQUEUE <job-id> ... <job-id>`

Remove the job from the queue.

## `DELJOB <job-id> ... <job-id>`

Completely delete a job from a node.
Note that this is similar to `FASTACK`, but limited to a single node since
no `DELJOB` cluster bus message is sent to other nodes.

## `SHOW <job-id>`

Describe the job.

## `QSCAN [COUNT <count>] [BUSYLOOP] [MINLEN <len>] [MAXLEN <len>] [IMPORTRATE <rate>]`

The command provides an interface to iterate all the existing queues in
the local node, providing a cursor in the form of an integer that is passed
to the next command invocation. During the first call cursor must be 0,
in the next calls the cursor returned in the previous call is used in the
next. The iterator guarantees to return all the elements but may return
duplicated elements.

Options:

* `COUNT <count>` An hit about how much work to do per iteration.
* `BUSYLOOP` Block and return all the elements in a busy loop.
* `MINLEN <count>` Don't return elements with less than count jobs queued.
* `MAXLEN <count>`Don't return elements with more than count jobs queued.
* `IMPORTRATE <rate>` Only return elements with an job import rate (from other nodes) `>=` rate.

The cursor argument can be in any place, the first non matching option
that has valid cursor form of an usigned number will be sensed as a valid
cursor.

## `JSCAN [<cursor>] [COUNT <count>] [BUSYLOOP] [QUEUE <queue>] [STATE <state1> STATE <state2> ... STATE <stateN>] [REPLY all|id]`

The command provides an interface to iterate all the existing jobs in
the local node, providing a cursor in the form of an integer that is passed
to the next command invocation. During the first call cursor must be 0,
in the next calls the cursor returned in the previous call is used in the
next. The iterator guarantees to return all the elements but may return
duplicated elements.

Options:

* `COUNT <count>` An hit about how much work to do per iteration.
* `BUSYLOOP` Block and return all the elements in a busy loop.
* `QUEUE <queue>` Return only jobs in the specified queue.
* `STATE <state>` Return jobs in the specified state. Can be used multiple times for a logic OR.
* `REPLY <type>` Job reply type. Type can be `all` or `id`. Default is to report just the job ID. If `all` is specified the full job state is returned like for the SHOW command.

The cursor argument can be in any place, the first non matching option
that has valid cursor form of an usigned number will be sensed as a valid
cursor.

Client libraries
===

Disque uses the same protocol as Redis itself. To adapt Redis clients, or to use it directly, should be pretty easy. However note that Disque default port is 7711 and not 6379.

While a vanilla Redis client may work well with Disque, clients should optionally use the following protocol in order to connect with a Disque cluster:

1. The client should be given a number of IP addresses and ports where nodes are located. The client should select random nodes and should try to connect until one available is found.
2. On a successful connection the `HELLO` command should be used in order to retrieve the Node ID and other potentially useful information (server version, number of nodes).
3. If a consumer sees a high message rate received from foreign nodes, it may optionally have logic in order to retrieve messages directly from the nodes where producers are producing the messages for a given topic. The consumer can easily check the source of the messages by checking the Node ID prefix in the messages IDs.

This way producers and consumers will eventually try to minimize nodes messages exchanges whenever possible.

So basically you could perform basic usage using just a Redis client, however
there are already specialized client libraries implementing a more specialized
API on top of Disque:

*C++*

- [disque C++ client](https://github.com/zhengshuxin/acl/tree/master/lib_acl_cpp/samples/disque)

*Common Lisp*

- [cl-disque](https://github.com/CodyReichert/cl-disque)

*Elixir*

- [exdisque](https://github.com/mosic/exdisque)

*Erlang*

- [edisque](https://github.com/nacmartin/edisque)

*Go*

- [disque-go](https://github.com/zencoder/disque-go)
- [go-disque](https://github.com/EverythingMe/go-disque)
- [disque](https://github.com/goware/disque)

*Java*

- [jedisque](https://github.com/xetorthio/jedisque)
- [spinach](https://github.com/mp911de/spinach)

*Node.js*

- [disque.js](https://www.npmjs.com/package/disque.js)
- [thunk-disque](https://github.com/thunks/thunk-disque)
- [disqueue-node](https://www.npmjs.com/package/disqueue-node)

*Perl*

- [perl-disque](https://github.com/lovelle/perl-disque)

*PHP*

- [phpque](https://github.com/s12v/phpque) (PHP/HHVM)
- [disque-php](https://github.com/mariano/disque-php) ([Composer/Packagist](https://packagist.org/packages/mariano/disque-php))
- [disque-client-php](https://github.com/mavimo/disque-client-php) ([Composer/Packagist](https://packagist.org/packages/mavimo/disque-client))
- [phloppy](https://github.com/0x20h/phloppy) ([Composer/Packagist](https://packagist.org/packages/0x20h/phloppy))

*Python*

- [disq](https://github.com/ryansb/disq) ([PyPi](https://pypi.python.org/pypi/disq))
- [pydisque](https://github.com/ybrs/pydisque) ([PyPi](https://pypi.python.org/pypi/pydisque))
- [django-q](https://github.com/koed00/django-q) ([PyPi](https://pypi.python.org/pypi/django-q))

*Ruby*

- [disque-rb](https://github.com/soveran/disque-rb)
- [disque_jockey](https://github.com/DevinRiley/disque_jockey)

*Rust*

- [disque-rs](https://github.com/seppo0010/disque-rs)

*.NET*

- [Disque.Net](https://github.com/ziyasal/Disque.Net)

Implementation details
===

Jobs replication strategy
---

1. Disque tries to replicate to W-1 (or W during out of memory) reachable nodes, shuffled.
2. The cluster ADDJOB message is used to replicate a job to multiple nodes, the job is sent together with the list of nodes that may have a copy.
2. If the required replication is not reached promptly, the job is send to one additional node every 50 milliseconds. When this happens, a new ADDJOB message is also re-sent to each node that may have already a copy, in order to refresh the list of nodes that have a copy.
3. If the specified synchronous replication timeout is reached, the node that originally received the ADDJOB command from the client, gives up and returns an error to the client. When this happens the node performs a best-effort procedure to delete the job from nodes that may have already received a copy of the job.

Cluster topology
---

Disque is a full mesh, with each node connected to each other. Disque performs
distributed failure detection via gossip, only in order to adjust the
replication strategy (try reachable nodes first when trying to replicate
a message), and in order to inform clients about non reachable nodes when
they want the list of nodes they can connect to.

Being Disque multi-master the event of nodes failing is not handled in any
special way.

Cluster messages
---

Nodes communicate via a set of messages, using the node-to-node message bus.
A few of the messages are used in order to check that other nodes are
reachable and to mark nodes as failing. Those messages are PING, PONG and
FAIL. Since failure detection is only used to adjust the replication strategy
(talk with reachable nodes first in order to improve latency), the details
are yet not described. Other messages are more important since they are used
in order to replicate jobs, re-issue jobs trying to minimize multiple
deliveries, and in order to auto-federate to serve consumers when messages
are produced in different nodes compared to where consumers are.

The following is a list of messages and what they do, split by category.
Note that this is just an informal description, while in the next sections
describing the Disque state machine, there is a more detailed description
of the behavior caused by messages reception, and in what cases they are
generated.

Cluster messages related to jobs replication and queueing
---

* ADDJOB: ask the receiver to replicate a job, that is, to add a copy of the job among the registered jobs in the target node. When a job is accepted, the receiver replies with GOTJOB to the sender. A job may not be accepted if the receiving node is near out of memory. In this case GOTJOB is not send and the message discarded.
* GOTJOB: The reply to ADDJOB to confirm the job was replicated.
* ENQUEUE: Ask a node to put a given job into its queue. This message is used when a job is created by a node that does not want to take a copy, so it asks another node (among the ones that acknowledged the job replication) to queue it for the first time. If this message is lost, after the retry time some node will try to re-queue the message, unless retry is set to zero.
* WILLQUEUE: This message is sent 500 milliseconds before a job is re-queued to all the nodes that may have a copy of the message, according to the sender table. If some of the receivers already have the job queued, they'll reply with QUEUED in order to prevent the sender to queue the job again (avoid multiple delivery when possible).
* QUEUED: When a node re-queue a job, it sends QUEUED to all the nodes that may have a copy of the message, so that the other nodes will update the time at which they'll retry to queue the job. Moreover every node that already has the same job in queue, but with a node ID which is lexicographically smaller than the sending node, will de-queue the message in order to best-effort de-dup messages that may be queued into multiple nodes at the same time.

Cluster messages related to ACKs propagation and garbage collection
---

* SETACK: This message is sent to force a node to mark a job as successfully delivered (acknowledged by the worker): the job will no longer be considered active, and will never be re-queued by the receiving node. Also SETACK is send to the sender if the receiver of QUEUED or WILLQUEUE message has the same job marked as acknowledged (successfully delivered) already.
* GOTACK: This message is sent in order to acknowledge a SETACK message. The receiver can mark a given node that may have a copy of a job, as informed about the fact that the job was acknowledged by the worker. Nodes delete (garbage collect) a message cluster wide when they believe all the nodes that may have a copy are informed about the fact the job was acknowledged.
* DELJOB: Ask the receiver to remove a job. Is only sent in order to perform garbage collection of jobs by nodes that are sure the job was already delivered correctly. Usually the node sending DELJOB only does that when its sure that all the nodes that may have a copy of the message already marked the message ad delivered, however after some time the job GC may be performed anyway, in order to reclaim memory, and in that case, an otherwise avoidable multiple delivery of a job may happen. The DELJOB message is also used in order to implement *fast acknowledges*.

Cluster messages related to nodes federation
---

* NEEDJOBS(queue,count): The sender asks the receiver to obtain messages for a given queue, possibly *count* messages, but this is only an hit for congestion control and messages optimization, the receiver is free to reply with whatever number of messages. NEEDJOBS messages are delivered in two ways: broadcasted to every node in the cluster from time to time, in order to discover new source nodes for a given queue, or more often, to a set of nodes that recently replies with jobs for a given queue. This latter mechanism is called an *ad hoc* delivery, and is possible since every node remembers for some time the set of nodes that were recent providers of messages for a given queue. In both cases, NEEDJOBS messages are delivered with exponential delays, with the exception of queues that drop to zero-messages and have a positive recent import rate, in this case an ad hoc NEEDJOBS delivery is performed regardless of the last time the message was delivered in order to allow a continuous stream of messages under load.

* YOURJOBS(array of messages): The reply to NEEDJOBS. An array of serialized jobs, usually all about the same queue (but future optimization may allow to send different jobs from different queues). Jobs into YOURJOBS replies are extracted from the local queue, and queued at the receiver node's queue with the same name. So even messages with a retry set to 0 (at most once delivery) still guarantee the safety rule since a given message may be in the source node, on the wire, or already received in the destination node. If a YOURJOBS message is lost, at least once delivery jobs will be re-queued later when the retry time is reached.

Disque state machine
---

This section shows the most interesting (as in less obvious) parts of the state machine each Disque node implements. While practically it is a single state machine, it is split in sections. The state machine description uses a convention that is not standard but should look familiar, since it is event driven, made of actions performed upon: message receptions in the form of commands received from clients, messages received from other cluster nodes, timers, and procedure calls.

Note that: `job` is a job object with the following fields:

1. `job.delivered`: A list of nodes that may have this message. This list does not need to be complete, is used for best-effort algorithms.
2. `job.confirmed`: A list of nodes that confirmed reception of ACK by replying with a GOTJOB message.
3. `job.id`: The job 48 chars ID.
4. `job.state`: The job state among: `wait-repl`, `active`, `queued`, `acknowledged`.
5. `job.replicate`: Replication factor for this job.
5. `job.qtime`: Time at which we need to re-queue the job.

List fields such as `.delivered` and `.confirmed` support methods like `.size` to get the number of elements.

States are as follows:

1. `wait-repl`: the job is waiting to be synchronously replicated.
2. `active`: the job is active, either it reached the replication factor in the originating node, or it was created because the node received an `ADDJOB` message from another node.
3. `queued`: the job is active and also is pending into a queue in this node.
4. `acknowledged`: the job is no longer actived since a client confirmed the reception using the `ACKJOB` command or another Disque node sent a `SETACK` message for the job.

Generic functions
---

PROCEDURE `LOOKUP-JOB(string job-id)`:

1. If job with the specified id is found, returns the corresponding job object.
2. Otherwise returns NULL.

PROCEDURE `UNREGISTER(object job)`:

1. Delete the job from memory, and if queued, from the queue.

PROCEDURE `ENQUEUE(job)`:

1. If `job.state == queued` return ASAP.
2. Add `job` into `job.queue`.
3. Change `job.state` to `queued`.

PROCEDURE `DEQUEUE(job)`:

1. If `job.state != queued` return ASAP.
2. Remove `job` from `job.queue`.
3. Change `job.state` to `active`.

ON RECV cluster message: `DELJOB(string job.id)`:

1. job = Call `LOOKUP-JOB(job-id)`.
2. IF `job != NULL` THEN call `UNREGISTER(job)`.

Job replication state machine
---

This part of the state machine documents how clients add jobs to the cluster
and how the cluster replicates jobs across different Disque nodes.

ON RECV client command `ADDJOB(string queue-name, string body, integer replicate, integer retry, integer ttl, ...):

1. Create a job object in `wait-repl` state, having as body, ttl, retry, queue name, the specified values.
2. Send ADDJOB(job.serialized) cluster message to `replicate-1` nodes.
3. Block the client without replying.

Step 3: We'll reply to the client in step 4 of `GOTJOB` message processing.

ON RECV cluster message `ADDJOB(object serialized-job)`:

1. job = Call `LOOKUP-JOB(serialized-job.id)`.
2. IF `job != NULL` THEN: job.delivered = UNION(job.delivered,serialized-job.delivered). Return ASAP, since we have the job.
3. Create a job from serialized-job information.
4. job.state = `active`.
5. Reply to the sender with `GOTJOB(job.id)`.

Step 1: We may already have the job, since ADDJOB may be duplicated.

Step 2: If we already have the same job, we update the list of jobs that may have a copy of this job, performing the union of the list of nodes we have with the list of nodes in the serialized job.

ON RECV cluster message `GOTJOB(object serialized-job)`:

1. job = Call `LOOKUP-JOB(serialized-job.id)`.
2. IF `job == NULL` OR `job.state != wait-repl` Return ASAP.
3. Add sender node to `job.confirmed`.
4. IF `job.confirmed.size == job.replicate` THEN change `job.state` to `active`, call ENQUEUE(job), and reply to the blocked client with `job.id`.

Step 4: As we receive enough confirmations via `GOTJOB` messages, we finally reach the replication factor required by the user and consider the message active.

TIMER, firing every next 50 milliseconds while a job still did not reached the expected replication factor.

1. Select an additional node not already listed in `job.delivered`, call it `node`.
2. Add `node` to `job.delivered`.
3. Send ADDJOB(job.serialized) cluster message to each node in `job.delivered`.

Step 3: We send the message to every node again, so that each node will have a chance to update `job.delivered` with the new nodes. It is not required for each node to know the full list of nodes that may have a copy, but doing so improves our approximation of single delivery whenever possible.

Job re-queueing state machine
---

This part of the state machine documents how Disque nodes put a given job
back into the queue after the specified retry time elapsed without the
job being acknowledged.

TIMER, firing 500 milliseconds before the retry time elapses:

1. Send `WILLQUEUE(job.id)` to every node in `jobs.delivered`.

TIMER, firing when `job.qtime` time is reached.

1. If `job.retry == 0` THEN return ASAP.
2. Call ENQUEUE(job).
3. Update `job.qtime` to NOW + job.retry.
4. Send `QUEUED(job.id)` message to each node in `job.delivered`.

Step 1: At most once jobs never get enqueued again.

Step 3: We'll retry again after the retry period.

ON RECV cluster message `WILLQUEUE(string job-id)`:

1. job = Call `LOOKUP-JOB(job-id)`.
2. IF `job == NULL` THEN return ASAP.
3. IF `job.state == queued` SEND `QUEUED(job.id)` to `job.delivered`.
4. IF `job.state == acked` SEND `SETACK(job.id)` to the sender.

Step 3: We broadcast the message since likely the other nodes are going to retry as well.

Step 4: SETACK processing is documented below in the acknowledges section of the state machine description.

ON RECV cluster message `QUEUED(string job-id)`:

1. job = Call `LOOKUP-JOB(job-id)`.
2. IF `job == NULL` THEN return ASAP.
3. IF `job.state == acknowledged` THEN return ASAP.
4. IF `job.state == queued` THEN if sender node ID is greater than my node ID call DEQUEUE(job).
5. Update `job.qtime` setting it to NOW + job.retry.

Step 4: If multiple nodes re-queue the job about at the same time because of race conditions or network partitions that make `WILLQUEUE` not effective, then `QUEUED` forces receiving nodes to dequeue the message if the sender has a greater node ID, lowering the probability of unwanted multiple delivery.

Step 5: Now the message is already queued somewhere else, but the node will retry again after the retry time.

Acknowledged jobs garbage collection state machine
---

This part of the state machine is used in order to garbage collect
acknowledged jobs, when a job finally gets acknowledged by a client.

PROCEDURE `ACK-JOB(job)`:

1. If job state is already `acknowledged`, do nothing and return ASAP.
2. Change job state to `acknowledged`, dequeue the job if queued, schedule first call to TIMER.

PROCEDURE `START-GC(job)`:

1. Send `SETACK(job.delivered.size)` to each node that is listed in `job.delivered` but is not listed in `job.confirmed`.
2. IF `job.delivered.size == 0`, THEN send `SETACK(0)` to every node in the cluster.

Step 2: this is an ACK about a job we don’t know. In that case, we can just broadcast the acknowledged hoping somebody knows about the job and replies.

ON RECV client command `ACKJOB(string job-id)`:

1. job = Call `LOOKUP-JOB(job-id)`.
1. if job is `NULL`, ignore the message and return.
2. Call `ACK-JOB(job)`.
3. Call `START-GC(job)`.

ON RECV cluster message `SETACK(string job-id, integer may-have)`:

1. job = Call `LOOKUP-JOB(job-id)`.
2. Call ACK-JOB(job) IF job is not `NULL`.
3. Reply with GOTACK IF `job == NULL OR job.delivered.size <= may-have`.
4. IF `job != NULL` and `jobs.delivered.size > may-have` THEN call `START-GC(job)`.
5. IF `may-have == 0 AND job  != NULL`, reply with `GOTACK(1)` and call `START-GC(job)`.

Steps 3 and 4 makes sure that among the reachalbe nodes that may have a message, garbage collection will be performed by the node that is aware of more nodes that may have a copy.

Step 5 instead is used in order to start a GC attempt if we received a SETACK message from a node just hacking a dummy ACK (an acknowledge about a job it was not aware of).

ON RECV cluster message `GOTACK(string job-id, bool known)`:

1. job = Call `LOOKUP-JOB(job-id)`. Return ASAP IF `job == NULL`.
2. Call `ACK-JOB(job)`.
3. IF `known == true AND job.delivered.size > 0` THEN add the sender node to `job.delivered`.
4. IF `(known == true) OR (known == false AND job.delivered.size > 0) OR (known == false AND sender is an element of job.delivered)` THEN add the sender node to `jobs.confirmed`.
5. IF `job.delivered.size > 0 AND job.delivered.size == job.confirmed.size`, THEN send `DELJOB(job.id)` to every node in the `job.delivered` list and call `UNREGISTER(job)`.
6. IF `job.delivered == 0 AND known == true`, THEN call `UNREGISTER(job)`.
7. IF `job.delivered == 0 AND job.confirmed.size == cluster.size` THEN call `UNREGISTER(job)`.

Step 3: If `job.delivered.size` is zero, it means that the node just holds a *dummy ack* for the job. It means the node has an acknowledged job it created on the fly because a client acknowledged (via ACKJOB command) a job it was not aware of.

Step 6: we don't have to hold a dummy acknowledged jobs if there are nodes that have the job already acknowledged.

Step 7: this happens when nobody knows about a job, like when a client acknowledged a wrong job ID.

TIMER, from time to time (exponential backoff with random error), for every acknowledged job in memory:

1. call `START-GC(job)`.

Limitations
===

* Disque is new code, not tested, and will require quite some time to reach production quality. It is likely very buggy and may contain wrong assumptions or tradeoffs.
* As long as the software is non stable, the API may change in random ways without prior notification.
* It is possible that Disque spends too much effort in approximating single delivery during failures. The **fast acknowledge** concept and command makes the user able to opt-out this efforts, but yet I may change the Disque implementation and internals in the future if I see the user base really not caring about multiple deliveries during partitions.
* There is yet a lot of Redis dead code inside probably that could be removed.
* Disque was designed a bit in *astronaut mode*, not triggered by an actual use case of mine, but more in response to what I was seeing people doing with Redis as a message queue and with other message queues. However I'm not an expert, if I succeeded to ship something useful for most users, this is kinda of an accomplishment. Otherwise it may just be that Disque is pretty useless.
* As Redis, Disque is single threaded. While in Redis there are stronger reasons to do so, in Disque there is no manipulation of complex data structures, so maybe in the future it should be moved into a threaded server. We need to see what happens in real use cases in order to understand if it's worth it or not.
* The number of jobs in a Disque process is limited to the amount of memory available. Again while this in Redis makes sense (IMHO), in Disque there are definitely simple ways in order to circumvent this limitation, like logging messages on disk when the server is out of memory and consuming back the messages when memory pressure is already acceptable. However in general, like in Redis, manipulating data structures in memory is a big advantage from the point of view of the implementation simplicity and the functionality we can provide to users.
* Disque is completely not optimized for speed, was never profiled so far. I'm currently not aware of the fact it's slow, fast, or average, compared to other messaging solutions. For sure it is not going to have Redis-alike numbers because it does a lot more work at each command. For example when a job is added, it is serialized and transmitted to other `N` servers. There is a lot more messaging passing between nodes involved, and so forth. The good news is that being totally unoptimized, there is room for improvements.
* Ability of federation to handle well low and high loads without incurring into congestion or high latency, was not tested well enough. The algorithm is reasonable but may fail short under many load patterns.
* Amount of tested code path and possible states is not enough.

FAQ
===

Is Disque part of Redis?
---

No, it is a standalone project, however a big part of the Redis networking source code, nodes message bus, libraries, and the client protocol, were reused in this new project. In theory it was possible to extract the common code and release it as a framework to write distributed systems in C. However this is not a perfect solution as well, since the projects are expected to diverge more and more in the future, and to rely on a common fundation was hard. Moreover the initial effort to turn Redis into two different layers: an abstract server, networking stack and cluster bus, and the actual Redis implementation, was a huge effort, ways bigger than writing Disque itself.

However while it is a separated project, conceptually Disque is related to Redis, since it tries to solve a Redis use case in a vertical, ad-hoc way.

Who created Disque?
---

Disque is a side project of Salvatore Sanfilippo, aka @antirez.

There are chances for this project to be actively developed?
---

Currently I consider this just a public alpha: If I see people happy to use it for the right reasons (i.e. it is better in some use cases compared to other message queues) I'll continue the development. Otherwise it was anyway cool to develop it, I had much fun, and I definitely learned new things.

What happens when a node runs out of memory?
---

1. Maxmemory setting is mandatory in Disque, and defaults to 1GB.
2. When 75% of maxmemory is reached, Disque starts to replicate the new jobs only to external nodes, without taking a local copy, so basically if there is free RAM into other nodes, adding still works.
3. When 95% of maxmemory is reached, Disque starts to evict data that does not violates the safety guarantees: For instance acknowledged jobs and inactive queues.
4. When 100% of maxmemory is reached, commands that may result into more memory used are not processed at all and the client is informed with an error.

Are there plans to add the ability to hold more jobs than the physical memory of a single node can handle?
---

Yes. In Disque it should be relatively simple to use the disk when memory is not
available, since jobs are immutable and don't need to necessarily exist in
memory at a given time.

There are multiple strategies available. The current idea is that
when an instance is out of memory, jobs are stored into a log file instead
of memory. As more free memory is available in the instance, on disk jobs
are loaded.

However in order to implement this, there is to observe strong evidence of its
general usefulness for the user base.

When I consume and produce from different nodes, sometimes there is a delay in order for the jobs to reach the consumer, why?
---

Disque routing is not static, the cluster automatically tries to provide
messages to nodes where consumers are attached. When there is an high
enough traffic (even one message per second is enough) nodes remember other
nodes that recently were sources for jobs in a given queue, so it is possible
to aggressively send messages asking for more jobs, every time there are
consumers waiting for more messages and the local queue is empty.

However when the traffic is very low, informations about recent sources of
messages are discarded, and nodes rely on a more generic mechanism in order to
discover other nodes that may have messages in the queues we need them (which
is also used in high traffic conditions as well, in order to discover new
sources of messages for a given queue).

For example imagine a setup with two nodes, A and B.

1. A client attaches to node A and asks for jobs in the queue `myqueue`. Node A has no jobs enqueued, so the client is blocked.
2. After a few seconds another client produces messages into `myqueue`, but sending them to node B.

During step `1` if there was no recent traffic of imported messages for this queue, node A has no idea about who may have messages for the queue `myqueue`. Every other node may have, or none may have. So it starts to broadcast `NEEDJOBS` messages to the whole cluster. However we can't spam the cluster with messages, so if no reply is received after the first broadcast, the next will be sent with a larger delay, and so foth. The delay is exponential, with a maximum value of 30 seconds (this parameters will be configurable in the future, likely).

When there is some traffic instead, nodes send `NEEDJOBS` messages ASAP to other nodes that were recent sources of messages. Even when no reply is received, the next `NEEDJOBS` messages will be sent more aggressively to the subset of nodes that had messages in the mast, with a delay that starts at 25 milliseconds and has a maximum value of two seconds.

In order to minimize the latency, `NEEDJOBS` messages are not trottled at all when:

1. A client consumed the last message from a given queue. Source nodes are informed immediately in order to receive messages before the node asks for more.
2. Blocked clients are served the last message available in the queue.

For more information, please refer to the file `queue.c`, especially the function `needJobsForQueue` and its callers.

Are messages re-enqueued in the queue tail or head or what?
---

Messages are put into the queue according to their *creation time* attribute. This means that they are enqueued in a best effort order in the local node queue. Messages that need to be put back into the queue again because their delivery failed are usually (but not always) older than messages already in queue, so they'll likely be among the first to be delivered to workers.

What Disque means?
---

DIStributed QUEue but is also a joke with "dis" as negation (like in *dis*order) of the strict concept of queue, since Disque is not able to guarantee the strict ordering you expect from something called *queue*. And because of this tradeof it gains many other interesting things.

Community: how to get help and how to help
===

Get in touch with us in one of the following ways:

1. Post on [Stack Overflow](http://stackoverflow.com) using the `disque` tag. This is the preferred method to get general help about Disque: other users will easily find previous questions so we can incrementally build a knowledge base.
2. Join the `#disque` IRC channel at **irc.freenode.net**.
3. Create an Issue or Pull request if your question or issue is about the Disque implementation itself.

Thanks
===

I would like to say thank you to the following persons and companies.

* Pivotal, for allowing me to work on Disque, most in my spare time, but sometimes during work hours. Moreover Pivotal agreed to leave the copyright of the code to me. This is very generous. Thanks Pivotal!
* Michel Martens and Damian Janowski for providing early feedbacks about Disque while the project was still private.
* Everybody which is already writing client libraries, sending pull requests, creating issues in order to move this forward from alpha to something actually usable.
