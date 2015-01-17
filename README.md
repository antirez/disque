Disque, an in-memory, distributed job queue
===

Disque is a distributed, in memory, jobs (messages) queue.
It's goal is to capture the essence of the use case for Redis as a back end
for job queues libraries (mainly using blocking list operations), and move
it into an ad-hoc, self-contained, scalable, and fault tolerant design, with
simple to understand properties and guarantees, but still resembling Redis
in terms of simplicity and implementation as a C non-blocking networked server.

Currently the project is just an alpha quality preview, that was developed
in roughly 100 hours (so far, keep me updated!), mostly at night and during
weekends.

What it does exactly?
---

Disque is a message queue. Producers add messages that are served to
consumers. Since message queues are often used in order to process delayed
jobs, Disque often uses "job" in the API and in the documentation, however
jobs are actually just messages in the form of strings, so Disque can be used
for other use cases. In this documentation "jobs" and "messages" are used
in an interchangeable way.

Job queues with a producer-consumer model are pretty common, so the devil is
in the details. A few details about Disque are:

Disque is a **synchronously replicated job queue**. By default when a new job is added, it is replicated to W nodes before the client gets an acknowledge about the job being added. W-1 nodes can fail and still the message will be delivered.

Disque supports both **at-least-once and at-most-once** delivery semantics. At least once delivery semantics is where most efforts were spent in the design, while the at most once semantics is a trivial result of using a replication factor of 1 for the message, with a retry time set to 0 (which means, never re-queue the message again). You can have, at the same time, both at-least-once and at-most-once jobs in the same queues and nodes.

Disque at-least-once delivery is designed to **approximate single delivery** when possible even during certain kinds of failures. This means that while Disque can only guarantee a number of deliveries equal or greater to one, it will try to avoid multiple deliveries whenever possible.

Disque is a distributed system where **all nodes have the same role** (aka, it is multi-master). Producers and consumers can attach to whatever node they like, and there is no need for producers and consumers of the same queue, to stay connected to the same node. Nodes will automatically exchange messages based on load.

Disque is Available (as in "A" of CAP): producers and consumers can make progresses as long as a single node is reachable.

Disque supports **optional asynchronous commands** that are low latency for the client but provide less guarantees. For example a producer can add a job to a queue with replication factor of 3 but may want to run away before knowing if the contacted node was really able to replicate it to the specified number of nodes or not.

Disque **automatically re-queue messages that are not acknowledged** as already processed by consumers, after a message-specific retry time.

Disque queues only provide **weak ordering**. Each queue sorts messages based on the wall clock of the local node where the message was created (plus an incremental counter for messages created in the same millisecond), so messages created in the same node are normally delivered in the same order they were created. This property is weak because is violated in different cases: when messages are re-issued because not acknowledged, because of nodes local clock drifts, and when messages are moved to other nodes for load balancing. However it means that normally messages are not delivered in random order and usually messages created first are delivered first.

Disque provides the user with fine-grainde control for each job **using three time related parameters**, and one replication parameter. For each job, the user can control:
1. The replication factor (how many nodes have a copy).
2. The delay time (the min time Disque will wait before putting the message in a queue).
3. The retry time (how much time should elapse, since the last time the job was queued, and without an acknowledge about the job delivery, before the job is re-queued again).
4. The expire time (how much time should elapse for the job to be deleted regardless of the fact it was delivered or not).

ACKs and retries
---

Disque implementation of at-least-once delivery semantics is designed in order
to avoid multiple delivery during certain classes of failures. It is not able to guarantee that no multiple deliveries will occur. However there are many at-least-once workloads where duplicated deliveries are acceptable (or explicitly handled), but not desirable either. An example is sending emails to users (it is not terrible if an user gets a duplicated email, but is important to avoid it when possible), or doing idempotent operations that are expensive (all the times where it is critical for performances to avoid multiple deliveries).

In order to avoid multiple deliveries when possible, Disque uses client ACKs. When a consumer processes a message correctly, it should acknowledge this fact to Disque. ACKs are replicated to multiple nodes, and are garbage collected as soon as the system believes it is unlikely that more nodes in the cluster have the job (the ACK refers to) still active. Under memory pressure or under certain failure scenarios, ACKs are eventually discarded.

More explicitly:

1. A job is replicated to multiple nodes, but only *queued* in a single node.
2. Nodes having a copy of a message, if a certain amount of time has elapsed without getting the ACK for the message, will re-queue it. Nodes will run a best-effort protocol to avoid re-queueing the message multiple times.
3. ACKs are replicated and garbage collected across the cluster so that eventually processed messages are evicted (this happens ASAP if there are no failures nor network partitions).

For example, if a node having a copy of a job gets partitioned away during the time the job gets acknowledged by the consumer, it is likely that when it returns back (in a reasonable amount of time, that is, before the retry time is reached) it will be informed about the ACK and will avoid to re-queue the message. Similarly jobs can be acknowledged during a partition to just a single node available, and when the partition heals the ACK will be propagated to other nodes that may still have a copy of the message.

So an ACK is just a **proof of delivery** that is replicated and retained for
some time in order to make multiple delivery less likely in practice.

As already mentioned, In order to control replication and retires, a Disque job has the following associated properties: number of replicas, delay, retry and expire.

If a job has a retry time set to 0, it will get queued exactly once (and in this case a replication factor greater than 1 is useless, and signaled as an error to the user), so it will get delivered either a single time or will never get delivered. While jobs can be persisted on disk for safety, queues aren't, so this behavior is guaranteed even when nodes restart after a crash, whatever the persistence configuration is. However when nodes are manually restarted by the sysadmin, for example for upgrades, queues are persisted correctly and reloaded at startup, since the store/load operation is atomic in this case, and there are no race conditions possible (it is not possible that a job was delivered to a client and is persisted on disk as queued).

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

FAQ
===

Is Disque based on Redis?
---

No, it is a standalone project, however a big part of the Redis networking source code, nodes message bus, libraries, and the client protocol, were reused in this new project. In theory it was possible to extract the common code and release it as a framework to write distributed systems in C. However given that this was a side project coded mostly at night, I went for the simplest route. Sorry, I'm a pragmatic kind of person.

Who created Disque?
---

Disque is a project of Salvatore Sanfilippo, aka @antirez. Most of the code was written at night, so it is just a side project currently. However I would love to work more to it in the future.

There are chances for this project to be actively developed?
---

Currently I consider this just a public alpha: If I see people happy to use it for the right reasons (i.e. it is better in some use cases compared to other message queues) I'll continue the developments. Otherwise it was anyway cool to develop it, I had much fun.

What happens when a node runs out of memory?
---

1. Maxmemory setting is mandatory in Disque, and defaults to 1GB.
2. When 75% of maxmemory is reached, Disque starts to replicate the new jobs only to external nodes, without taking a local copy, so basically if there is free RAM into other nodes, adding still works.
3. When 95% of maxmemory is reached, Disque starts to evict data that does not violates the safety guarantees: For instance acknowledged jobs and expired jobs.
4. When 100% of maxmemory is reached, commands that may result into more memory used are not processed at all and the client is informed with an error.

Are there plans to add the ability to hold more jobs than the physical memory of a single node can handle?
---

Yes. In Disque it should be relatively simple to use the disk when memory is not
available, since jobs are immutable and don't need to necessarely exist in
memory at a given time.

There are multiple strategies available. The current idea is that
when an instance is out of memory, jobs are stored into a log file instead
of memory. As more free memory is available in the instance, on disk jobs
are loaded.

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

Disque uses the same protocl as Redis itself. To adapt Redis clients, or to use it directly, should be pretty easy. However note that Disque default port is 7711 and not 6379.

While a vanilly Redis client may work well with Disque, clients should optionally use the following protocol in order to connect with a Disque cluster:

1. The client should be given a number of ip addresses and ports where nodes are located. The client should select random nodes and should try to connect until one available is found.
2. On a successful connection the `HELLO` command should be used in order to retrieve the Node ID and other potentially useful information (server version, number of nodes).
3. If a consumer sees an high message rate received from foreing nodes, it may optionally have logic in order to retrieve messages directly from the nodes where producers are producing the messages for a given topic. The consumer can easily check the source of the messages by checking the Node ID prefix in the messages IDs.

This way producers and consumers will eventually try to minimize nodes messages exchanges whenever possible.

Implementation details
===

Jobs replication strategy
---

1. Disque tries to replicate to W-1 (or W if OOM) reachable nodes, shuffled.
2. ADDJOB message is used to replicate, the job is sent together with the list of nodes that may have a copy.
2. Send to one new node every 50 milliseconds. A new ADDJOB is sent to each node that may have already a copy, plus the new node. All the nodes should update the list of nodes that may have a copy of the message.
3. If the specified timeout is reached, the node that originally received the ADDJOB command from the client, gives up and returns an error to the client. When this happens the node performs a best-effort procedure to delete the job from nodes that may have a copy.

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
in order to replicate jobs, re-issue jobs trying to minimize multiple delivers,
and in order to auto-federate to serve consumers when messages are produced
in different nodes compared to where consumers are.

The following is a list of messages and what they do:

* ADDJOB: ask the receiver to replicate a job, that is, to add a copy of the job among the registered jobs in the target node. When a job is accepted, the receiver replies to GOTJOB to the sender. A job may not be accepted if the receiving node is near out of memory.
* GOTJOB: The reply to ADDJOB to confirm the job was replicated.
* QUEUEJOB: Ask a node to put a given job into its queue. This message is used when a job is replicated by a node that does not want to take a copy, so it asks another node (among the ones that acknowledged the job replication) to queue it for the first time. If this message is lost, after the retry time some node will try to re-queue the message, unless retry is set to zero.
* QUEUED: When a node re-queue a job, it sends QUEUED to all the nodes that may have a copy of the message, so that the other nodes will update the time at which they'll retry to queue the job.
* WILLQUEUE: This is send 500 milliseconds before a job is re-queued to all the nodes that may have a copy. If some of the receivers already have the job queued, they'll reply with QUEUED in order to prevent the sender to queue the job again (avoid multiple delivery when possible).
* SETACK: This message is sent to force a node to mark a job as delivered: the job will no longer be considered active, will not be re-queued, and so forth. Also SETACK is send to the sender if the receiver of QUEUED or WILLQUEUE message has the same job marked as acknowledged (successfully delivered) already.
* GOTACK: This message is sent in order to acknowledge a SETACK message. The receiver can mark a given node that may have a copy of a job, as informed about the fact that the job was acknowledged by the worker.
* DELJOB: Ask the receiver to remove a job. Is only sent in order to perform garbage collection of jobs by nodes that are sure the job was already delivered correctly. Usually the node sending DELJOB only does that when its sure that all the nodes that may have a copy of the message already marked the message ad delivered, however after some time the Job GC may be performed anyway in order to reclaim memory, and in that case, an otherwise avoidable mutliple delivery of a job may happen.

There are two additional messages that are used for federation, NEEDJOBS and YOURJOBS, but they are covered in the next section.

Auto federation
---

Auto federation is based on two cluster requests, plus metrics and heuristics in order to exchange messages with a good frequency and with the right nodes.

The message NEEDJOBS is at the base of the federation, and asks a node to obtain jobs about a given queue. The receiving node will de-queue jobs from the queue (but will still hold a copy of the job), and send those messages to the node that requested them.

Nodes reply to NEEDJOBS with YOURJOBS, a cluster message that has one or multiple jobs inside.
