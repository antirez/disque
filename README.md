Disque, an in-memory, distributed job queue
===

Disque is a distributed, in memory, message broker.
Its goal is to capture the essence of the "Redis as a jobs queue" use case,
which is usually implemented using blocking list operations, and move
it into an ad-hoc, self-contained, scalable, and fault tolerant design, with
simple to understand properties and guarantees, but still resembling Redis
in terms of simplicity, performances, and implementation as a C non-blocking
networked server.

Currently the project is just an alpha quality preview, that was developed
in roughly 100 hours (so far, keep me updated!), mostly at night and during
weekends.

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

Disque is Available (as in "A" of CAP): producers and consumers can make progresses as long as a single node is reachable.

Disque supports **optional asynchronous commands** that are low latency for the client but provide less guarantees. For example a producer can add a job to a queue with replication factor of 3 but may want to run away before knowing if the contacted node was really able to replicate it to the specified number of nodes or not.

Disque **automatically re-queue messages that are not acknowledged** as already processed by consumers, after a message-specific retry time. There is no need for consumers to re-queue a message if it was not processed.

Disque uses **explicit acknowledges** in order for a consumer to signal a message as delivered (or, using a different terminology, to signal a job as already processed).

Disque queues only provide **weak ordering**. Each queue sorts messages based on the job creation time, which is obtained using the *wall clock* of the local node where the message was created (plus an incremental counter for messages created in the same millisecond), so messages created in the same node are normally delivered in the same order they were created. This is not causal ordering since correct ordering is violated in different cases: when messages are re-issued because not acknowledged, because of nodes local clock drifts, and when messages are moved to other nodes for load balancing and federation (in this case you end with queues having jobs originated in different nodes with different wall clocks). However all this also means that normally messages are not delivered in random order and usually messages created first are delivered first.

Note that since Disque does not provide strict FIFO semantics, technically speaking it should not be called a *message queue*, and it could better identified as a message broker. However I believe that at this point in the IT industry a *message queue* is often more lightly used to identify a generic broker that may or may not be able to guarantee order in all the cases. Given that we document very clearly the semantics, I grant myself the right to call Disque a message queue anyway.

Disque provides the user with fine-grained control for each job **using three time related parameters**, and one replication parameter. For each job, the user can control:
1. The replication factor (how many nodes have a copy).
2. The delay time (the min time Disque will wait before putting the message in a queue, making the message deliverable).
3. The retry time (how much time should elapse, since the last time the job was queued, and without an acknowledge about the job delivery, before the job is re-queued again for delivery).
4. The expire time (how much time should elapse for the job to be deleted regardless of the fact it was successfully delivered, i.e. acknowledged, or not).

Finally, Disque supports optional on disk persistence, which is not enabled by default, but that can be handy in single data center setups and during restarts.

ACKs and retries
---

Disque implementation of *at least once* delivery semantics is designed in order
to avoid multiple delivery during certain classes of failures. It is not able to guarantee that no multiple deliveries will occur. However there are many at least once workloads where duplicated deliveries are acceptable (or explicitly handled), but not desirable either. A trivial example is sending emails to users (it is not terrible if an user gets a duplicated email, but is important to avoid it when possible), or doing idempotent operations that are expensive (all the times where it is critical for performances to avoid multiple deliveries).

In order to avoid multiple deliveries when possible, Disque uses client ACKs. When a consumer processes a message correctly, it should acknowledge this fact to Disque. ACKs are replicated to multiple nodes, and are garbage collected as soon as the system believes it is unlikely that more nodes in the cluster have the job (the ACK refers to) still active. Under memory pressure or under certain failure scenarios, ACKs are eventually discarded.

More explicitly:

1. A job is replicated to multiple nodes, but only *queued* in a single node. There is a difference between having a job in memory, and queueing it for delivery.
2. Nodes having a copy of a message, if a certain amount of time has elapsed without getting the ACK for the message, will re-queue it. Nodes will run a best-effort protocol to avoid re-queueing the message multiple times.
3. ACKs are replicated and garbage collected across the cluster so that eventually processed messages are evicted (this happens ASAP if there are no failures nor network partitions).

For example, if a node having a copy of a job gets partitioned away during the time the job gets acknowledged by the consumer, it is likely that when it returns back (in a reasonable amount of time, that is, before the retry time is reached) it will be informed about the ACK and will avoid to re-queue the message. Similarly jobs can be acknowledged during a partition to just a single node available, and when the partition heals the ACK will be propagated to other nodes that may still have a copy of the message.

So an ACK is just a **proof of delivery** that is replicated and retained for
some time in order to make multiple deliveries less likely to happen in practice.

As already mentioned, In order to control replication and retires, a Disque job has the following associated properties: number of replicas, delay, retry and expire.

If a job has a retry time set to 0, it will get queued exactly once (and in this case a replication factor greater than 1 is useless, and signaled as an error to the user), so it will get delivered either a single time or will never get delivered. While jobs can be persisted on disk for safety, queues aren't, so this behavior is guaranteed even when nodes restart after a crash, whatever the persistence configuration is. However when nodes are manually restarted by the sysadmin, for example for upgrades, queues are persisted correctly and reloaded at startup, since the store/load operation is atomic in this case, and there are no race conditions possible (it is not possible that a job was delivered to a client and is persisted on disk as queued at the same time).

Disque and disk persistence
---

The default mode of operation is in-memory only, since jobs
are synchronously replicated, and safety is guaranteed by replication.
However because there are single data center setups, this is
too risky in certain environments, so:

1. Optionally you can enable AOF persistence (similar to Redis). In this mode only jobs data is persisted, but content of queues is not. However jobs will be re-queued eventually.
2. Even when running memory-only, Disque is able to dump its memory on disk and reload from disk on controlled restarts, for example in order to upgrade the software. In this case both jobs and queues are persisted, since in this specific case persisting queues is safe. The format used is the same as the AOF format, but with additional commands to put the jobs into the queue.

Job IDs
---

Disque jobs are uniquely identified by an ID like the following:

DI0f0c644fd3ccb51c2cedbd47fcb6f312646c993c05a0SQ

Job IDs always start with "DI" and end with "QS" and are always composed of
exactly 48 characters.

We can split a job into multiple parts:

DI | 0f0c644f | d3ccb51c2cedbd47fcb6f312646c993c | 05a0 | SQ

1. DI is the prefix
2. 0f0c644f is the first 8 bytes of the node ID where the message was generated.
3. d3ccb51c2cedbd47fcb6f312646c993c is the 128 bit ID pesudo random part in hex.
4. 05a0 is the Job TTL in minutes. Because of it, message IDs can be expired safety even without having the job representation.
5. SQ is the suffix.

IDs are returned by ADDJOB when a job is successfully created, are part of
the GETJOB output, and are used in order to acknowledge that a job was
correctly processed by a worker.

Part of the node ID is included in the message so that a worker processing
messages for a given queue can easily guess what are the nodes where jobs
are created, and move directly to these nodes to increase efficiency instead
of listeing for messages in a node that will require to fetch messages from
other nodes.

Only 64 bits of the original node ID is included in the message, however
in a cluster with 1000 Disque nodes, the probability of two nodes to have
identical 64 bit ID prefixes is given by the birthday paradox:

    P(100,2^64) = .000000000000027

In case of collisions, the workers may just do a non-efficient choice.

API
===

Disque API is composed of a small set of commands, since the system solves a
single very specific problem. The three main commands are:

    ADDJOB queue_name job <ms-timeout> [REPLICATE <count>] [DELAY <sec>] [RETRY <sec>] [TTL <sec>] [MAXLEN <count>] [ASYNC]

Adds a job to the specified queue. Arguments are as follows:

* *queue_name* is the name of the queue, any string, basically. You don't need to create queues, if it does not exist, it gets created automatically. If it has no longer jobs, it gets removed.
* *job* is a string representing the job. Disque is job meaning agnostic, for it a job is just a message to deliver. Job max size is 4GB.
* *ms-timeout* is the command timeout in milliseconds. If no ASYNC is specified, and the replication level specified is not reached in the specified number of milliseconds, the command returns with an error, and the node does a best-effort cleanup, that is, it will try to delete copies of the job across the cluster. However the job may still be delivered later. Note that the actual timeout resolution is 1/10 of second or worse with the default server hz.
* *REPLICATE count* is the number of nodes the job should be replicated to.
* *DELAY sec* is the number of seconds that should elapse before the job is queued by any server.
* *RETRY sec* period after which, if no ACK is received, the job is put again into the queue for delivery. If RETRY is 0, the job has an at-least-once delivery semantics.
* *TTL sec* is the max job life in seconds. After this time, the job is deleted even if it was not successfully delivered.
* *ASYNC* asks the server to let the command return ASAP and replicate the job to other nodes in the background. The job gets queued ASAP, while normally the job is put into the queue only when the client gets a positive reply.

The command returns the Job ID of the added job, assuming ASYNC is specified, or if the job was replicated correctly to the specified number of nodes. Otherwise an error is returned.

    GETJOBS [TIMEOUT <ms-timeout>] [COUNT <count>] FROM queue1 queue2 ... queueN

Return jobs available in one of the specified queues, or return NULL
if the timeout is reached. A single job per call is returned unless a count greater than 1 is specified. Jobs are returned as a three elements array containing the queue name, the Job ID, and the job body itself. If jobs are available into multiple queues, queues are processed left to right.

If there are no jobs for the specified queues the command blocks, and messages are exchanged with other nodes, in order to move messages about these queues to this node, so that the client can be served.

    ACKJOB jobid1 jobid2 ... jobidN

Acknowledges the execution of one or more jobs via job IDs. The node receiving the ACK will replicate it to multiple nodes and will try to garbage collect both the job and the ACKs from the cluster so that memory can be freed.

Other commands
===

    INFO
Generic server information / stats.

    HELLO
Returns id ... version ... nodes ...

    QLEN <qname>
Length of queue

    QSTAT <qname>
Return produced ... consumed ... idle ... sources [...] ctime ...

    QPEEK <qname> <count>
Return without consuming <count> jobs.

    QUEUE <job-id> ... <job-id>
Queue jobs if not already queued.

    DEQUEUE <job-id> ... <job-id>
Remove the job from the queue.

    DELJOB [BCAST] [FORCE] <job-id> ... <job-id>
Completely delete a job from a node.
If BCAST is given, a DELJOB cluster message is broadcasted to the set of nodes
that may have the job. If FORCE is given, when the job is not known, instead of
returning an error the node will broadcast a DELJOB cluster message to all the
nodes in the cluster.

    SHOW <job-id>
Describe the job.

    SCANJOBS <cursor> [STATE ...] [QUEUE ...] [COUNT ...]
Iterate job IDs.

    SCANQUEUES <cursor> [COUNT ...] [MAXIDLE ...]
Iterate queue names.

Client libraries
===

Disque uses the same protocol as Redis itself. To adapt Redis clients, or to use it directly, should be pretty easy. However note that Disque default port is 7711 and not 6379.

While a vanilla Redis client may work well with Disque, clients should optionally use the following protocol in order to connect with a Disque cluster:

1. The client should be given a number of IP addresses and ports where nodes are located. The client should select random nodes and should try to connect until one available is found.
2. On a successful connection the `HELLO` command should be used in order to retrieve the Node ID and other potentially useful information (server version, number of nodes).
3. If a consumer sees an high message rate received from foreign nodes, it may optionally have logic in order to retrieve messages directly from the nodes where producers are producing the messages for a given topic. The consumer can easily check the source of the messages by checking the Node ID prefix in the messages IDs.

This way producers and consumers will eventually try to minimize nodes messages exchanges whenever possible.

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

Cluster messages related to jobs replication and queueing
---

* ADDJOB: ask the receiver to replicate a job, that is, to add a copy of the job among the registered jobs in the target node. When a job is accepted, the receiver replies with GOTJOB to the sender. A job may not be accepted if the receiving node is near out of memory. In this case GOTJOB is not send and the message discarded.
* GOTJOB: The reply to ADDJOB to confirm the job was replicated.
* QUEUEJOB: Ask a node to put a given job into its queue. This message is used when a job is created by a node that does not want to take a copy, so it asks another node (among the ones that acknowledged the job replication) to queue it for the first time. If this message is lost, after the retry time some node will try to re-queue the message, unless retry is set to zero.
* WILLQUEUE: This message is send 500 milliseconds before a job is re-queued to all the nodes that may have a copy of the message, according to the sender table. If some of the receivers already have the job queued, they'll reply with QUEUED in order to prevent the sender to queue the job again (avoid multiple delivery when possible).
* QUEUED: When a node re-queue a job, it sends QUEUED to all the nodes that may have a copy of the message, so that the other nodes will update the time at which they'll retry to queue the job. Moreover every node that already has the same job in queue, but with a node ID which is lexicographically smaller than the sending node, will de-queue the message in order to best-effort de-dup messages that may be queued into multiple nodes at the same time.

Cluster messages related to ACKs propagation and garbage collection
---

* SETACK: This message is sent to force a node to mark a job as successfully delivered (acknowledged by the worker): the job will no longer be considered active, and will never be re-queued by the receiving node. Also SETACK is send to the sender if the receiver of QUEUED or WILLQUEUE message has the same job marked as acknowledged (successfully delivered) already.
* GOTACK: This message is sent in order to acknowledge a SETACK message. The receiver can mark a given node that may have a copy of a job, as informed about the fact that the job was acknowledged by the worker. Nodes delete (garbage collect) a message cluster wide when they believe all the jobs are informed about the fact the job was acknowledged.
* DELJOB: Ask the receiver to remove a job. Is only sent in order to perform garbage collection of jobs by nodes that are sure the job was already delivered correctly. Usually the node sending DELJOB only does that when its sure that all the nodes that may have a copy of the message already marked the message ad delivered, however after some time the job GC may be performed anyway, in order to reclaim memory, and in that case, an otherwise avoidable multiple delivery of a job may happen.

Cluster messages related to nodes federation
---

* NEEDJOBS(queue,count): The sender asks the receiver to obtain messages for a given queue, possibly *count* messages, but this is only an hit for congestion control and messages optimization, the receiver is free to reply with whatever number of messages. NEEDJOBS messages are delivered in two ways: broadcasted to every node in the cluster from time to time, in order to discover new source nodes for a given queue, or more often, to a set of nodes that recently replies with jobs for a given queue. This latter mechanism is called an *ad hoc* delivery, and is possible since every node remembers for some time the set of nodes that were recent providers of messages for a given queue. In both cases, NEEDJOBS messages are delivered with exponential delays, with the exception of queues that drop to zero-messages and have a positive recent import rate, in this case an ad hoc NEEDJOBS delivery is performed regardless of the last time the message was delivered in order to allow a continuous stream of messages under load.

* YOURJOBS(array of messages): The reply to NEEDJOBS. An array of serialized jobs, usually all about the same queue (but future optimization may allow to send different jobs from different queues). Jobs into YOURJOBS replies are extracted from the local queue, and queued at the receiver node's queue with the same name. So even messages with a retry set to 0 (at most once delivery) still guarantee the safety rule since a given message may be in the source node, in the wire, or already received in the destination node. If a YOURJOBS message is lost, at least once delivery jobs will be re-queued later when the retry time is reached.

ACKs garbage collection state machine
---

This section shows the state machine used in order to collect acknowledged jobs
in terms of procedures, timers, and actions performed when a given type of
cluster message or client command is received.

Note that: job is a job object with the following fields:

1. job.delivered: A list of nodes that may have this message. This list does not need to be complete, is used for best-effort algorithms.
2. job.confirmed: A list of nodes that confirmed reception of ACK by replying with a GOTJOB message.
3. job.id: The job 48 chars ID.

Both fields support methods like `.size` to get the number of elements.

PROCEDURE `LOOKUP-JOB(job-id)`:

1. If job with the specified id is found, returns the corresponding job object.
2. Otherwise returns NULL.

PROCEDURE `UNREGISTER(job)`:

1. Delete the job from memory, and if queued, from the queue.

PROCEDURE `ACK_JOB(job)`:

1. If job state is already `acknowledged`, do nothing and return ASAP.
2. Change job state to `acknowledged`, dequeue the job if queued, schedule first call to TIMER.

PROCEDURE `START_GC(job)`:

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
4. IF `job != NULL` and `jobs.delivered.size > may-have` THEN call `START_GC(job)`.
5. IF `may_have == 0 AND job  != NULL`, reply with `GOTACK(1)` and call `START_GC(job)`. 

Steps 3 and 4 makes sure that among the reachalbe nodes that may have a message, garbage collection will be performed by the node that is aware of more nodes that may have a copy.

Step 5 instead is used in order to start a GC attempt if we received a SETACK message from a node just hacking a dummy ACK (an acknowledge about a job it was not aware of).

ON RECV cluster message `GOTACK(string job-id, bool known)`:

1. job = Call `LOOKUP-JOB(job-id)`. Return ASAP IF `job == NULL`.
2. Call `ACK_JOB(job)`.
3. IF `known == true AND job.delivered.size > 0` THEN add the sender node to `job.delivered`.
4. IF `(known == true) OR (known == false AND job.delivered.size > 0) OR (known == false AND sender is an element of job.delivered)` THEN add the sender node to `jobs.confirmed`.
5. IF `job.delivered.size > 0 AND job.delivered.size == job.confirmed.size`, THEN send `DELJOB(job.id)` to every node in the `job.delivered` list and call `UNREGISTER(job)`.
6. IF `job.delivered == 0 AND known == true`, THEN call `UNREGISTER(job)`.
7. IF `job.delivered == 0 AND job.confirmed.size == cluster.size` THEN call `UNREGISTER(job)`.

Step 3: If `job.delivered.size` is zero, it means that the node just holds a *dummy ack* for the job. It means the node has an acknowledged job it created on the fly because a client acknowledged (via ACKJOB command) a job it was not aware of.

Step 6: we don't have to hold a dummy acknowledged jobs if there are nodes that have the job already acknowledged.

Step 7: this happens when nobody knows about a job, like when a client acknowledged a wrong job ID.

ON RECV cluster message: `DELJOB(job.id)`:

1. job = Call `LOOKUP-JOB(job-id)`.
2. IF `job != NULL` THEN call `UNREGISTER(job)`.

TIMER, every N seconds (starting with 3 minutes), for every acknowledged job in memory:

1. If job state is `acknowledged`, call `START_GC(job)`. Reschedule TIMER call with an exponential delay.

FAQ
===

Is Disque based on Redis?
---

No, it is a standalone project, however a big part of the Redis networking source code, nodes message bus, libraries, and the client protocol, were reused in this new project. In theory it was possible to extract the common code and release it as a framework to write distributed systems in C. However given that this was a side project coded mostly at night, I went for the simplest route. Sorry, I'm a pragmatic kind of person and it was very important to maximize the probability of me actually being able to release the project soon or later.

Who created Disque?
---

Disque is a project of Salvatore Sanfilippo, aka @antirez.

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

What Disque means?
---

DIStributed QUEue but is also a joke with "dis" as negation (like in *dis*order) of the strict concept of queue, since Disque is not able to guarantee the strict ordering you expect from something called *queue*.
