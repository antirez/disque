/* Queue implementation. In Disque a given node can have active jobs
 * (not ACKed) that are not queued. Only queued jobs can be retrieved by
 * workers via GETJOB. This file implements the local node data structures
 * and functions to model the queue.
 *
 * ---------------------------------------------------------------------------
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Disque nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "cluster.h"
#include "job.h"
#include "queue.h"
#include "skiplist.h"

#include <math.h>

void signalQueueAsReady(queue *q);

/* ------------------------ Low level queue functions ----------------------- */

/* Job comparision inside a skiplist: by ctime, if ctime is the same by
 * job ID. */
int skiplistCompareJobsInQueue(const void *a, const void *b) {
    const job *ja = a, *jb = b;

    if (ja->ctime > jb->ctime) return 1;
    if (jb->ctime > ja->ctime) return -1;
    return memcmp(ja->id,jb->id,JOB_ID_LEN);
}

/* Crete a new queue, register it in the queue hash tables.
 * On success the pointer to the new queue is returned. If a queue with the
 * same name already NULL is returned. */
queue *createQueue(robj *name) {
    if (dictFind(server.queues,name) != NULL) return NULL;

    queue *q = zmalloc(sizeof(queue));
    q->name = name;
    q->flags = 0;
    incrRefCount(name);
    q->sl = skiplistCreate(skiplistCompareJobsInQueue);
    q->ctime = server.unixtime;
    q->atime = server.unixtime;
    q->needjobs_bcast_time = 0;
    q->needjobs_bcast_attempt = 0;
    q->needjobs_adhoc_time = 0;
    q->needjobs_adhoc_attempt = 0;
    q->needjobs_responders = NULL; /* Created on demand to save memory. */
    q->clients = NULL; /* Created on demand to save memory. */

    q->current_import_jobs_time = server.mstime;
    q->current_import_jobs_count = 0;
    q->prev_import_jobs_time = server.mstime;
    q->prev_import_jobs_count = 0;
    q->jobs_in = 0;
    q->jobs_out = 0;

    incrRefCount(name); /* Another refernce in the hash table key. */
    dictAdd(server.queues,q->name,q);
    return q;
}

/* Return the queue by name, or NULL if it does not exist. */
queue *lookupQueue(robj *name) {
    dictEntry *de = dictFind(server.queues,name);
    if (!de) return NULL;
    queue *q = dictGetVal(de);
    return q;
}

/* Destroy a queue and unregisters it. On success C_OK is returned,
 * otherwise if no queue exists with the specified name, C_ERR is
 * returned. */
int destroyQueue(robj *name) {
    queue *q = lookupQueue(name);
    if (!q) return C_ERR;

    dictDelete(server.queues,name);
    decrRefCount(q->name);
    skiplistFree(q->sl);
    if (q->needjobs_responders) dictRelease(q->needjobs_responders);
    if (q->clients) {
        serverAssert(listLength(q->clients) == 0);
        listRelease(q->clients);
    }
    zfree(q);
    return C_OK;
}

/* Send a job as a return value of a command. This is about jobs but inside
 * queue.c since this is the format used in order to return a job from a
 * queue, as an array [queue_name,id,body]. */
void addReplyJob(client *c, job *j, int flags) {
    int bulklen = 3;

    if (flags & GETJOB_FLAG_WITHCOUNTERS) bulklen += 4;
    addReplyMultiBulkLen(c,bulklen);

    addReplyBulk(c,j->queue);
    addReplyBulkCBuffer(c,j->id,JOB_ID_LEN);
    addReplyBulkCBuffer(c,j->body,sdslen(j->body));
    /* Job additional information is returned as key-value pairs. */
    if (flags & GETJOB_FLAG_WITHCOUNTERS) {
        addReplyBulkCString(c,"nacks");
        addReplyLongLong(c,j->num_nacks);

        addReplyBulkCString(c,"additional-deliveries");
        addReplyLongLong(c,j->num_deliv);
    }
}

/* ------------------------ Queue higher level API -------------------------- */

/* Queue the job and change its state accordingly. If the job is already
 * in QUEUED state, or the job has retry set to 0 and the JOB_FLAG_DELIVERED
 * flat set, C_ERR is returned, otherwise C_OK is returned and the operation
 * succeeds.
 *
 * The nack argument is set to 1 if the enqueue is the result of a client
 * negative acknowledge. */
int enqueueJob(job *job, int nack) {
    if (job->state == JOB_STATE_QUEUED || job->qtime == 0) return C_ERR;
    if (job->retry == 0 && job->flags & JOB_FLAG_DELIVERED) return C_ERR;

    serverLog(LL_VERBOSE,"QUEUED %.*s", JOB_ID_LEN, job->id);

    job->state = JOB_STATE_QUEUED;

    /* Put the job into the queue and update the time we'll queue it again. */
    if (job->retry) {
        job->flags |= JOB_FLAG_BCAST_WILLQUEUE;
        job->qtime = server.mstime +
                     job->retry*1000 +
                     randomTimeError(DISQUE_TIME_ERR);
    } else {
        job->qtime = 0; /* Never re-queue at most once jobs. */
    }

    /* The first time a job is queued we don't need to broadcast a QUEUED
     * message, to save bandwidth. But the next times, when the job is
     * re-queued for lack of acknowledge, this is useful to (best effort)
     * avoid multiple nodes to re-queue the same job. */
    if (job->flags & JOB_FLAG_BCAST_QUEUED || nack) {
        unsigned char flags = nack ? CLUSTERMSG_FLAG0_INCR_NACKS :
                                     CLUSTERMSG_FLAG0_INCR_DELIV;
        clusterBroadcastQueued(job, flags);
        /* Other nodes will increment their NACKs / additional deliveries
         * counters when they'll receive the QUEUED message. We need to
         * do the same for the local copy of the job. */
        if (nack)
            job->num_nacks++;
        else
            job->num_deliv++;
    } else {
        job->flags |= JOB_FLAG_BCAST_QUEUED; /* Next time, broadcast. */
    }

    updateJobAwakeTime(job,0);
    queue *q = lookupQueue(job->queue);
    if (!q) q = createQueue(job->queue);
    serverAssert(skiplistInsert(q->sl,job) != NULL);
    q->atime = server.unixtime;
    q->jobs_in++;
    if (!(q->flags & QUEUE_FLAG_PAUSED_OUT)) signalQueueAsReady(q);
    return C_OK;
}

/* Remove a job from the queue. Returns C_OK if the job was there and
 * is now removed (updating the job state back to ACTIVE), otherwise
 * C_ERR is returned. */
int dequeueJob(job *job) {
    if (job->state != JOB_STATE_QUEUED) return C_ERR;
    queue *q = lookupQueue(job->queue);
    if (!q) return C_ERR;
    serverAssert(skiplistDelete(q->sl,job));
    job->state = JOB_STATE_ACTIVE; /* Up to the caller to override this. */
    serverLog(LL_VERBOSE,"DE-QUEUED %.*s", JOB_ID_LEN, job->id);
    return C_OK;
}

/* Fetch a job from the specified queue if any, updating the job state
 * as it gets fetched back to ACTIVE. If there are no jobs pending in the
 * specified queue, NULL is returned.
 *
 * The returned job is, among the jobs available, the one with lower
 * 'ctime'.
 *
 * If 'qlen' is not NULL, the residual length of the queue is stored
 * at *qlen. */
job *queueFetchJob(queue *q, unsigned long *qlen) {
    if (skiplistLength(q->sl) == 0) return NULL;
    job *j = skiplistPopHead(q->sl);
    j->state = JOB_STATE_ACTIVE;
    j->flags |= JOB_FLAG_DELIVERED;
    q->atime = server.unixtime;
    q->jobs_out++;
    if (qlen) *qlen = skiplistLength(q->sl);
    return j;
}

/* Like queueFetchJob() but take the queue name as input. */
job *queueNameFetchJob(robj *qname, unsigned long *qlen) {
    queue *q = lookupQueue(qname);
    return q ? queueFetchJob(q,qlen) : NULL;
}

/* Return the length of the queue, or zero if NULL is passed here. */
unsigned long queueLength(queue *q) {
    if (!q) return 0;
    return skiplistLength(q->sl);
}

/* Queue length by queue name. The function returns 0 if the queue does
 * not exist. */
unsigned long queueNameLength(robj *qname) {
    return queueLength(lookupQueue(qname));
}

/* Remove a queue that was not accessed for enough time, has no clients
 * blocked, has no jobs inside. If the queue is removed C_OK is
 * returned, otherwise C_ERR is returned. */
#define QUEUE_MAX_IDLE_TIME (60*5)
int GCQueue(queue *q, time_t max_idle_time) {
    time_t idle = server.unixtime - q->atime;
    if (idle < max_idle_time) return C_ERR;
    if (q->clients && listLength(q->clients) != 0) return C_ERR;
    if (skiplistLength(q->sl)) return C_ERR;
    if (q->flags & QUEUE_FLAG_PAUSED_ALL) return C_ERR;
    destroyQueue(q->name);
    return C_OK;
}

/* This function is called from serverCron() in order to incrementally remove
 * from memory queues which are found to be idle and empty. */
int evictIdleQueues(void) {
    mstime_t start = mstime();
    time_t max_idle_time = QUEUE_MAX_IDLE_TIME;
    long sampled = 0, evicted = 0;

    if (getMemoryWarningLevel() > 0) max_idle_time /= 30;
    if (getMemoryWarningLevel() > 1) max_idle_time = 2;
    while (dictSize(server.queues) != 0) {
        dictEntry *de = dictGetRandomKey(server.queues);
        queue *q = dictGetVal(de);

        sampled++;
        if (GCQueue(q,max_idle_time) == C_OK) evicted++;

        /* First exit condition: we are able to expire less than 10% of
         * entries. */
        if (sampled > 10 && (evicted * 10) < sampled) break;

        /* Second exit condition: we are looping for some time and maybe
         * we are using more than one or two milliseconds of time. */
        if (((sampled+1) % 1000) == 0 && mstime()-start > 1) break;
    }
    return evicted;
}

/* -------------------------- Blocking on queues ---------------------------- */

/* Handle blocking if GETJOB fonud no jobs in the specified queues.
 *
 * 1) We set q->clients to the list of clients blocking for this queue.
 * 2) We set client->bpop.queues as well, as a dictionary of queues a client
 *    is blocked for. So we can resolve queues from clients.
 * 3) When elements are added to queues with blocked clients, we call
 *    signalQueueAsReady(), that will populate the dictionary
 *    server.ready_queues with all the queues that may unblock clients.
 * 4) Before returning into the event loop, handleClientsBlockedOnQueues()
 *    is called, that iterate the list of ready queues, and unblock clients
 *    serving elements from the queue. */

void blockForJobs(client *c, robj **queues, int numqueues, mstime_t timeout, uint64_t flags) {
    int j;

    c->bpop.timeout = timeout;
    c->bpop.flags = flags;
    for (j = 0; j < numqueues; j++) {
        queue *q = lookupQueue(queues[j]);
        if (!q) q = createQueue(queues[j]);

        /* Handle duplicated queues in array. */
        if (dictAdd(c->bpop.queues,queues[j],NULL) != DICT_OK) continue;
        incrRefCount(queues[j]);

        /* Add this client to the list of clients in the queue. */
        if (q->clients == NULL) q->clients = listCreate();
        listAddNodeTail(q->clients,c);
    }
    blockClient(c,BLOCKED_GETJOB);
}

/* Unblock client waiting for jobs in queues. Never call this directly,
 * call unblockClient() instead. */
void unblockClientBlockedForJobs(client *c) {
    dictEntry *de;
    dictIterator *di;

    di = dictGetIterator(c->bpop.queues);
    while((de = dictNext(di)) != NULL) {
        robj *qname = dictGetKey(de);
        queue *q = lookupQueue(qname);
        serverAssert(q != NULL);

        listDelNode(q->clients,listSearchKey(q->clients,c));
        if (listLength(q->clients) == 0) {
            listRelease(q->clients);
            q->clients = NULL;
            GCQueue(q,QUEUE_MAX_IDLE_TIME);
        }
    }
    dictReleaseIterator(di);
    dictEmpty(c->bpop.queues,NULL);
}

/* Add the specified queue to server.ready_queues if there is at least
 * one client blocked for this queue. Otherwise no operation is performed. */
void signalQueueAsReady(queue *q) {
    if (q->clients == NULL || listLength(q->clients) == 0) return;
    if (dictAdd(server.ready_queues,q->name,NULL) == DICT_OK)
        incrRefCount(q->name);
}

/* Run the list of queues the received elements in the last event loop
 * iteration, and unblock as many clients is possible. */
void handleClientsBlockedOnQueues(void) {
    dictEntry *de;
    dictIterator *di;

    /* Don't waste time if there is nothing to do. */
    if (dictSize(server.ready_queues) == 0) return;

    di = dictGetIterator(server.ready_queues);
    while((de = dictNext(di)) != NULL) {
        queue *q = lookupQueue(dictGetKey(de));
        if (!q || !q->clients) continue;
        int numclients = listLength(q->clients);
        while(numclients--) {
            unsigned long qlen;
            listNode *ln = listFirst(q->clients);
            client *c = ln->value;
            job *j = queueFetchJob(q,&qlen);

            if (!j) break; /* No longer elements, try next queue. */
            if (qlen == 0) needJobsForQueue(q,NEEDJOBS_REACHED_ZERO);
            addReplyMultiBulkLen(c,1);
            addReplyJob(c,j,c->bpop.flags);
            unblockClient(c); /* This will remove it from q->clients. */
        }
    }
    dictReleaseIterator(di);
    dictEmpty(server.ready_queues,NULL);
}

/* If a client is blocked into one or mutliple queues waiting for
 * data, we should periodically call needJobsForQueue() with the
 * queues name in order to send NEEDJOBS messages from time to time. */
int clientsCronSendNeedJobs(client *c) {
    if (c->flags & CLIENT_BLOCKED && c->btype == BLOCKED_GETJOB) {
        dictForeach(c->bpop.queues,de)
            robj *qname = dictGetKey(de);
            needJobsForQueueName(qname,NEEDJOBS_CLIENTS_WAITING);
        dictEndForeach
    }
    return 0;
}

/* ------------------------------ Federation -------------------------------- */

/* Return a very rough estimate of the current import message rate.
 * Imported messages are messaged received as NEEDJOBS replies. */
#define IMPORT_RATE_WINDOW 5000 /* 5 seconds max window. */
uint32_t getQueueImportRate(queue *q) {
    double elapsed = server.mstime - q->prev_import_jobs_time;
    double messages = (double)q->prev_import_jobs_count +
                              q->current_import_jobs_count;

    /* If we did not received any message in the latest few seconds,
     * consider the import rate zero. */
    if ((server.mstime - q->current_import_jobs_time) > IMPORT_RATE_WINDOW)
        return 0;

    /* Min interval is 50 ms in order to never overestimate. */
    if (elapsed < 50) elapsed = 50;

    return ceil((double)messages*1000/elapsed);
}

/* Called every time we import a job, this will update our counters
 * and state in order to update the import/sec estimate. */
void updateQueueImportRate(queue *q) {
    /* If the current second no longer matches the current counter
     * timestamp, copy the old timestamp/counter into 'prev', and
     * start a new counter with an updated time. */
    if (server.mstime - q->current_import_jobs_time > 1000) {
        q->prev_import_jobs_time = q->current_import_jobs_time;
        q->prev_import_jobs_count = q->current_import_jobs_count;
        q->current_import_jobs_time = server.mstime;
        q->current_import_jobs_count = 0;
    }
    /* Anyway, update the current counter. */
    q->current_import_jobs_count++;
}

/* Check the queue source nodes list (nodes that replied with jobs to our
 * NEEDJOBS request), purge the ones that timed out, and return the number
 * of sources which are still valid. */
unsigned long getQueueValidResponders(queue *q) {
    if (q->needjobs_responders == NULL ||
        dictSize(q->needjobs_responders) == 0) return 0;

    dictSafeForeach(q->needjobs_responders,de)
        int64_t lastmsg = dictGetSignedIntegerVal(de);
        if (server.unixtime-lastmsg > 5) {
            clusterNode *node = dictGetKey(de);
            dictDelete(q->needjobs_responders,node);
        }
    dictEndForeach
    return dictSize(q->needjobs_responders);
}

/* This function is called every time we realize we need jobs for a given
 * queue, because we have clients blocked into this queue which are currently
 * empty.
 *
 * Calling this function may result into NEEDJOBS messages send to specific
 * nodes that are remembered as potential sources for messages in this queue,
 * or more rarely into NEEDJOBS messages to be broadcasted cluster-wide in
 * order to discover new nodes that may be source of messages.
 *
 * This function in called in two different contests:
 *
 * 1) When a client attempts to fetch messages for an empty queue.
 * 2) From time to time for every queue we have clients blocked into.
 * 3) When a queue reaches 0 jobs since the last was fetched.
 *
 * When called in case 1 and 2, type is set to NEEDJOBS_CLIENTS_WAITING,
 * for case 3 instead type is set to NEEDJOBS_REACHED_ZERO, and in this
 * case the node may send a NEEDJOBS message to the set of known sources
 * for this queue, regardless of needjobs_adhoc_time value (that is, without
 * trying to trottle the requests, since there is an active flow of messages
 * between this node and source nodes).
 */

/* Min and max amount of jobs we request to other nodes. */
#define NEEDJOBS_MIN_REQUEST 5
#define NEEDJOBS_MAX_REQUEST 100
#define NEEDJOBS_BCAST_ALL_MIN_DELAY 2000     /* 2 seconds. */
#define NEEDJOBS_BCAST_ALL_MAX_DELAY 30000    /* 30 seconds. */
#define NEEDJOBS_BCAST_ADHOC_MIN_DELAY 25     /* 25 milliseconds. */
#define NEEDJOBS_BCAST_ADHOC_MAX_DELAY 2000   /* 2 seconds. */

void needJobsForQueue(queue *q, int type) {
    uint32_t import_per_sec; /* Jobs import rate in the latest secs. */
    uint32_t to_fetch;       /* Number of jobs we should try to obtain. */
    unsigned long num_responders = 0;
    mstime_t bcast_delay, adhoc_delay;
    mstime_t now = mstime();

    /* Don't ask for jobs if we are leaving the cluster. */
    if (myselfLeaving()) return;

    import_per_sec = getQueueImportRate(q);

    /* When called with NEEDJOBS_REACHED_ZERO, we have to do something only
     * if there is some active traffic, in order to improve latency.
     * Otherwise we wait for the first client to block, that will trigger
     * a new call to this function, but with NEEDJOBS_CLIENTS_WAITING type. */
    if (type == NEEDJOBS_REACHED_ZERO && import_per_sec == 0) return;

    /* Guess how many replies we need from each node. If we already have
     * a list of sources, assume that each source is capable of providing
     * some message. */
    num_responders = getQueueValidResponders(q);
    to_fetch = NEEDJOBS_MIN_REQUEST;
    if (num_responders > 0)
        to_fetch = import_per_sec / num_responders;

    /* Trim number of jobs to request to min/max values. */
    if (to_fetch < NEEDJOBS_MIN_REQUEST) to_fetch = NEEDJOBS_MIN_REQUEST;
    else if (to_fetch > NEEDJOBS_MAX_REQUEST) to_fetch = NEEDJOBS_MAX_REQUEST;

    /* Broadcast the message cluster from time to time.
     * We use exponential intervals (with a max time limit) */
    bcast_delay = NEEDJOBS_BCAST_ALL_MIN_DELAY *
                  (1 << q->needjobs_bcast_attempt);
    if (bcast_delay > NEEDJOBS_BCAST_ALL_MAX_DELAY)
        bcast_delay = NEEDJOBS_BCAST_ALL_MAX_DELAY;

    if (now - q->needjobs_bcast_time > bcast_delay) {
        q->needjobs_bcast_time = now;
        q->needjobs_bcast_attempt++;
        /* Cluster-wide broadcasts are just to discover nodes,
         * ask for a single job in this case. */
        clusterSendNeedJobs(q->name,1,server.cluster->nodes);
    }

    /* If the queue reached zero, or if the delay elapsed and we
     * have at least a source node, send an ad-hoc message to
     * nodes known to be sources for this queue.
     *
     * We use exponential delays here as well (but don't care about
     * the delay if the queue just dropped to zero), however with
     * much shorter times compared to the cluster-wide broadcast. */
    adhoc_delay = NEEDJOBS_BCAST_ADHOC_MIN_DELAY *
                  (1 << q->needjobs_adhoc_attempt);
    if (adhoc_delay > NEEDJOBS_BCAST_ADHOC_MAX_DELAY)
        adhoc_delay = NEEDJOBS_BCAST_ADHOC_MAX_DELAY;

    if ((type == NEEDJOBS_REACHED_ZERO ||
         now - q->needjobs_adhoc_time > adhoc_delay) &&
         num_responders > 0)
    {
        q->needjobs_adhoc_time = now;
        q->needjobs_adhoc_attempt++;
        clusterSendNeedJobs(q->name,to_fetch,q->needjobs_responders);
    }
}

/* needJobsForQueue() wrapper taking a queue name insteead of a queue
 * structure. The queue will be created automatically if non existing. */
void needJobsForQueueName(robj *qname, int type) {
    queue *q = lookupQueue(qname);

    /* Create the queue if it does not exist. We need the queue structure
     * to store meta-data needed to broadcast NEEDJOBS messages anyway. */
    if (!q) q = createQueue(qname);
    needJobsForQueue(q,type);
}

/* Called from cluster.c when a YOURJOBS message is received. */
void receiveYourJobs(clusterNode *node, uint32_t numjobs, unsigned char *serializedjobs, uint32_t serializedlen) {
    dictEntry *de;
    queue *q;
    uint32_t j;
    unsigned char *nextjob = serializedjobs;

    for (j = 0; j < numjobs; j++) {
        uint32_t remlen = serializedlen - (nextjob-serializedjobs);
        job *job, *sj = deserializeJob(nextjob,remlen,&nextjob,SER_MESSAGE);

        if (sj == NULL) {
            serverLog(LL_WARNING,
                "The %d-th job received via YOURJOBS from %.40s is corrupted.",
                (int)j+1, node->name);
            return;
        }

        /* If the job does not exist, we need to add it to our jobs.
         * Otherwise just get a reference to the job we already have
         * in memory and free the deserialized one. */
        job = lookupJob(sj->id);
        if (job) {
            freeJob(sj);
        } else {
            job = sj;
            job->state = JOB_STATE_ACTIVE;
            registerJob(job);
        }
        /* Don't need to send QUEUED when adding this job into the queue,
         * we are just moving from the queue of one node to another. */
        job->flags &= ~JOB_FLAG_BCAST_QUEUED;

        /* If we are receiving a job with retry set to 0, let's set
         * job->qtime to non-zero, to force enqueueJob() to queue the job
         * the first time. As a side effect the function will set the qtime
         * value to 0, preventing a successive enqueue of the job */
        if (job->retry == 0)
            job->qtime = server.mstime; /* Any value will do. */

        if (enqueueJob(job,0) == C_ERR) continue;

        /* Update queue stats needed to optimize nodes federation. */
        q = lookupQueue(job->queue);
        if (!q) q = createQueue(job->queue);
        if (q->needjobs_responders == NULL)
            q->needjobs_responders = dictCreate(&clusterNodesDictType,NULL);

        if (dictAdd(q->needjobs_responders, node, NULL) == DICT_OK) {
            /* We reset the broadcast attempt counter, that will model the
             * delay to wait before every cluster-wide broadcast, every time
             * we receive jobs from a node not already known as a source. */
            q->needjobs_bcast_attempt = 0;
        }

        de = dictFind(q->needjobs_responders, node);
        dictSetSignedIntegerVal(de,server.unixtime);
        updateQueueImportRate(q);
        q->needjobs_adhoc_attempt = 0;
    }
}

/* Called from cluster.c when a NEEDJOBS message is received. */
void receiveNeedJobs(clusterNode *node, robj *qname, uint32_t count) {
    queue *q = lookupQueue(qname);
    unsigned long qlen = queueLength(q);
    uint32_t replyjobs = count; /* Number of jobs we are willing to provide. */
    uint32_t j;

    /* Ignore requests for jobs if:
     * 1) No such queue here, or queue is empty.
     * 2) We are actively importing jobs ourselves for this queue. */
    if (qlen == 0 || getQueueImportRate(q) > 0) return;

    /* Ignore request if queue is paused in output. */
    if (q->flags & QUEUE_FLAG_PAUSED_OUT) return;

    /* To avoid that a single node is able to deplete our queue easily,
     * we provide the number of jobs requested only if we have more than
     * 2 times what it requested. Otherwise we provide at max half the jobs
     * we have, but always at least a single job. */
    if (qlen < count*2) replyjobs = qlen/2;
    if (replyjobs == 0) replyjobs = 1;

    job *jobs[NEEDJOBS_MAX_REQUEST];
    for (j = 0; j < replyjobs; j++) {
        jobs[j] = queueFetchJob(q,NULL);
        serverAssert(jobs[j] != NULL);
    }
    clusterSendYourJobs(node,jobs,replyjobs);

    /* It's possible that we sent jobs with retry=0. Remove them from
     * the local node since to take duplicates does not make sense for
     * jobs having the replication level of 1 by contract. */
    for (j = 0; j < replyjobs; j++) {
        job *job = jobs[j];
        if (job->retry == 0) {
            unregisterJob(job);
            freeJob(job);
        }
    }
}

/* ------------------------------ Queue pausing -----------------------------
 *
 * There is very little here since pausing a queue is basically just changing
 * its flags. Then what changing the PAUSE flags means, is up to the different
 * parts of Disque implementing the behavior of queues. */

/* Changes the paused state of the queue and handles serving again blocked
 * clients if needed.
 *
 * 'flag' must be QUEUE_FLAG_PAUSED_IN or QUEUE_FLAG_PAUSED_OUT
 * 'set' is true if we have to set this state or 0 if we have
 * to clear this state. */
void queueChangePausedState(queue *q, int flag, int set) {
    uint32_t orig_flags = q->flags;

    if (set) q->flags |= flag;
    else     q->flags &= ~flag;

    if ((orig_flags & QUEUE_FLAG_PAUSED_OUT) &&
        !(q->flags & QUEUE_FLAG_PAUSED_OUT))
    {
        signalQueueAsReady(q);
    }
}

/* Called from cluster.c when a PAUSE message is received. */
void receivePauseQueue(robj *qname, uint32_t flags) {
    queue *q = lookupQueue(qname);

    /* If the queue does not exist, and flags are cleared, there is nothing
     * to do. Otherwise we have to create the queue. */
    if (!q) {
        if (flags == 0) return;
        q = createQueue(qname);
    }

    /* Replicate the sender pause flag in our queue. */
    queueChangePausedState(q,QUEUE_FLAG_PAUSED_IN,
        (flags & QUEUE_FLAG_PAUSED_IN) != 0);
    queueChangePausedState(q,QUEUE_FLAG_PAUSED_OUT,
        (flags & QUEUE_FLAG_PAUSED_OUT) != 0);
}

/* Return the string "in", "out", "all" or "none" depending on the paused
 * state of the specified queue flags. */
char *queueGetPausedStateString(uint32_t qflags) {
    qflags &= QUEUE_FLAG_PAUSED_ALL;
    if (qflags == QUEUE_FLAG_PAUSED_ALL) {
        return "all";
    } else if (qflags == QUEUE_FLAG_PAUSED_IN) {
        return "in";
    } else if (qflags == QUEUE_FLAG_PAUSED_OUT) {
        return "out";
    } else {
        return "none";
    }
}

/* ------------------------- Queue related commands ------------------------- */

/* QLEN <qname> -- Return the number of jobs queued. */
void qlenCommand(client *c) {
    addReplyLongLong(c,queueNameLength(c->argv[1]));
}

/* GETJOB [NOHANG] [TIMEOUT <ms>] [COUNT <count>] FROM <qname1>
 *        <qname2> ... <qnameN>.
 *
 * Get jobs from the specified queues. By default COUNT is 1, so just one
 * job will be returned. If there are no jobs in any of the specified queues
 * the command will block.
 *
 * When there are jobs in more than one of the queues, the command guarantees
 * to return jobs in the order the queues are specified. If COUNT allows
 * more jobs to be returned, queues are scanned again and again in the same
 * order popping more elements. */
void getjobCommand(client *c) {
    mstime_t timeout = 0; /* Block forever by default. */
    long long count = 1, emitted_jobs = 0;
    int nohang = 0; /* Don't block even if all the queues are empty. */
    int withcounters = 0; /* Also return NACKs and deliveries counters. */
    robj **queues = NULL;
    int j, numqueues = 0;

    /* Parse args. */
    for (j = 1; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int lastarg = j == c->argc-1;
        if (!strcasecmp(opt,"nohang")) {
            nohang = 1;
        } else if (!strcasecmp(opt,"withcounters")) {
            withcounters = 1;
        } else if (!strcasecmp(opt,"timeout") && !lastarg) {
            if (getTimeoutFromObjectOrReply(c,c->argv[j+1],&timeout,
                UNIT_MILLISECONDS) != C_OK) return;
            j++;
        } else if (!strcasecmp(opt,"count") && !lastarg) {
            int retval = getLongLongFromObject(c->argv[j+1],&count);
            if (retval != C_OK || count <= 0) {
                addReplyError(c,"COUNT must be a number greater than zero");
                return;
            }
            j++;
        } else if (!strcasecmp(opt,"from")) {
            queues = c->argv+j+1;
            numqueues = c->argc - j - 1;
            break; /* Don't process options after this. */
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    /* FROM is mandatory. */
    if (queues == NULL || numqueues == 0) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* First: try to avoid blocking if there is at least one job in at
     * least one queue. */
    void *mbulk = NULL;

    while(1) {
        long old_emitted = emitted_jobs;
        for (j = 0; j < numqueues; j++) {
            unsigned long qlen;
            queue *q = lookupQueue(queues[j]);
            job *job = NULL;
            if (q && !(q->flags & QUEUE_FLAG_PAUSED_OUT))
                job = queueFetchJob(q,&qlen);

            if (!job) {
                if (!q)
                    needJobsForQueueName(queues[j],NEEDJOBS_CLIENTS_WAITING);
                else
                    needJobsForQueue(q,NEEDJOBS_CLIENTS_WAITING);
                continue;
            } else if (job && qlen == 0) {
                needJobsForQueue(q,NEEDJOBS_REACHED_ZERO);
            }
            if (!mbulk) mbulk = addDeferredMultiBulkLength(c);
            addReplyJob(c,job,withcounters ? GETJOB_FLAG_WITHCOUNTERS :
                                             GETJOB_FLAG_NONE);
            count--;
            emitted_jobs++;
            if (count == 0) break;
        }
        /* When we reached count or when we are no longer making
         * progresses (no jobs left in our queues), stop. */
        if (count == 0 || old_emitted == emitted_jobs) break;
    }

    if (mbulk) {
        setDeferredMultiBulkLength(c,mbulk,emitted_jobs);
        return;
    }

    /* If NOHANG was given and there are no jobs, return NULL. */
    if (nohang) {
        addReply(c,shared.nullmultibulk);
        return;
    }

    /* If this node is leaving the cluster, we can't block waiting for
     * jobs: this would trigger the federation with other nodes in order
     * to import jobs here. Just return a -LEAVING error. */
    if (myselfLeaving()) {
        addReply(c,shared.leavingerr);
        return;
    }

    /* If we reached this point, we need to block. */
    blockForJobs(c,queues,numqueues,timeout,
            withcounters ? GETJOB_FLAG_WITHCOUNTERS : GETJOB_FLAG_NONE);
}

/* ENQUEUE job-id-1 job-id-2 ... job-id-N
 * NACK job-id-1 job-id-2 ... job-id-N
 *
 * If the job is active, queue it if job retry != 0.
 * If the job is in any other state, do nothing.
 * If the job is not known, do nothing.
 *
 * NOTE: Even jobs with retry set to 0 are enqueued! Be aware that
 * using this command may violate the at-most-once contract.
 *
 * Return the number of jobs actually move from active to queued state.
 *
 * The difference between ENQUEUE and NACK is that the latter will propagate
 * cluster messages in a way that makes the nacks counter in the receiver
 * to increment. */
void enqueueGenericCommand(client *c, int nack) {
    int j, enqueued = 0;

    if (validateJobIDs(c,c->argv+1,c->argc-1) == C_ERR) return;

    /* Enqueue all the jobs in active state. */
    for (j = 1; j < c->argc; j++) {
        job *job = lookupJob(c->argv[j]->ptr);
        if (job == NULL) continue;

        if (job->state == JOB_STATE_ACTIVE && enqueueJob(job,nack) == C_OK)
            enqueued++;
    }
    addReplyLongLong(c,enqueued);
}

/* See enqueueGenericCommand(). */
void enqueueCommand(client *c) {
    enqueueGenericCommand(c,0);
}

/* See enqueueGenericCommand(). */
void nackCommand(client *c) {
    enqueueGenericCommand(c,1);
}

/* DEQUEUE job-id-1 job-id-2 ... job-id-N
 *
 * If the job is queued, remove it from queue and change state to active.
 * If the job is in any other state, do nothing.
 * If the job is not known, do nothing.
 *
 * Return the number of jobs actually moved from queue to active state. */
void dequeueCommand(client *c) {
    int j, dequeued = 0;

    if (validateJobIDs(c,c->argv+1,c->argc-1) == C_ERR) return;

    /* Enqueue all the jobs in active state. */
    for (j = 1; j < c->argc; j++) {
        job *job = lookupJob(c->argv[j]->ptr);
        if (job == NULL) continue;

        if (job->state == JOB_STATE_QUEUED && dequeueJob(job) == C_OK)
            dequeued++;
    }
    addReplyLongLong(c,dequeued);
}

/* QPEEK <queue> <count>
 *
 * Return an array of at most "count" jobs available inside the queue "queue"
 * without removing the jobs from the queue. This is basically an introspection
 * and debugging command.
 *
 * Normally jobs are returned from the oldest to the newest (according to the
 * job creation time field), however if "count" is negative , jobs are
 * returend from newset to oldest instead.
 *
 * Each job is returned as a two elements array with the Job ID and body. */
void qpeekCommand(client *c) {
    int newjobs = 0; /* Return from newest to oldest if true. */
    long long count, returned = 0;

    if (getLongLongFromObjectOrReply(c,c->argv[2],&count,NULL) != C_OK)
        return;

    if (count < 0) {
        count = -count;
        newjobs = 1;
    }

    skiplistNode *sn = NULL;
    queue *q = lookupQueue(c->argv[1]);

    if (q != NULL)
        sn = newjobs ? q->sl->tail : q->sl->header->level[0].forward;

    if (sn == NULL) {
        addReply(c,shared.emptymultibulk);
        return;
    }

    void *deflen = addDeferredMultiBulkLength(c);
    while(count-- && sn) {
        job *j = sn->obj;
        addReplyJob(c, j, GETJOB_FLAG_NONE);
        returned++;
        if (newjobs)
            sn = sn->backward;
        else
            sn = sn->level[0].forward;
    }
    setDeferredMultiBulkLength(c,deflen,returned);
}

/* QSCAN [<cursor>] [COUNT <count>] [BUSYLOOP] [MINLEN <len>] [MAXLEN <len>]
 * [IMPORTRATE <rate>]
 *
 * The command provides an interface to iterate all the existing queues in
 * the local node, providing a cursor in the form of an integer that is passed
 * to the next command invocation. During the first call cursor must be 0,
 * in the next calls the cursor returned in the previous call is used in the
 * next. The iterator guarantees to return all the elements but may return
 * duplicated elements.
 *
 * Options:
 *
 * COUNT <count>     -- An hit about how much work to do per iteration.
 * BUSYLOOP          -- Block and return all the elements in a busy loop.
 * MINLEN <count>    -- Don't return elements with less than count jobs queued.
 * MAXLEN <count>    -- Don't return elements with more than count jobs queued.
 * IMPORTRATE <rate> -- Only return elements with an job import rate (from
 *                      other nodes) >= rate.
 *
 * The cursor argument can be in any place, the first non matching option
 * that has valid cursor form of an usigned number will be sensed as a valid
 * cursor.
 */

/* The structure to pass the filter options to the callback. */
struct qscanFilter {
    long minlen, maxlen;
    long importrate;
};

/* Callback for the dictionary scan used by QSCAN. */
void qscanCallback(void *privdata, const dictEntry *de) {
    void **pd = (void**)privdata;
    list *list = pd[0];
    struct qscanFilter *filter = pd[1];
    queue *queue = dictGetVal(de);
    long qlen = (filter->minlen != -1 || filter->maxlen != -1) ?
                    (long) queueLength(queue) : 0;

    /* Don't add the item if it does not satisfies our filter. */
    if (filter->minlen != -1 && qlen < filter->minlen) return;
    if (filter->maxlen != -1 && qlen > filter->maxlen) return;
    if (filter->importrate != -1 &&
        getQueueImportRate(queue) < filter->importrate) return;

    /* Otherwise put the queue into the list that will be returned to the
     * client later. */
    incrRefCount(queue->name);
    listAddNodeTail(list,queue->name);
}

#define QSCAN_DEFAULT_COUNT 100
void qscanCommand(client *c) {
    struct qscanFilter filter = {-1,-1,-1};
    int busyloop = 0; /* If true return all the queues in a blocking way. */
    long count = QSCAN_DEFAULT_COUNT;
    long maxiterations;
    unsigned long cursor = 0;
    int cursor_set = 0, j;

    /* Parse arguments and cursor if any. */
    for (j = 1; j < c->argc; j++) {
        int remaining = c->argc - j -1;
        char *opt = c->argv[j]->ptr;

        if (!strcasecmp(opt,"count") && remaining) {
            if (getLongFromObjectOrReply(c, c->argv[j+1], &count, NULL) !=
                C_OK) return;
            j++;
        } else if (!strcasecmp(opt,"busyloop")) {
            busyloop = 1;
        } else if (!strcasecmp(opt,"minlen") && remaining) {
            if (getLongFromObjectOrReply(c, c->argv[j+1],&filter.minlen,NULL) !=
                C_OK) return;
            j++;
        } else if (!strcasecmp(opt,"maxlen") && remaining) {
            if (getLongFromObjectOrReply(c, c->argv[j+1],&filter.maxlen,NULL) !=
                C_OK) return;
            j++;
        } else if (!strcasecmp(opt,"importrate") && remaining) {
            if (getLongFromObjectOrReply(c, c->argv[j+1],
                &filter.importrate,NULL) != C_OK) return;
            j++;
        } else {
            if (cursor_set != 0) {
                addReply(c,shared.syntaxerr);
                return;
            }
            if (parseScanCursorOrReply(c,c->argv[j],&cursor) == C_ERR)
                return;
            cursor_set = 1;
        }
    }

    /* Scan the hash table to retrieve elements. */
    maxiterations = count*10; /* Put a bound in the work we'll do. */

    /* We pass two pointsr to the callback: the list where to append
     * elements and the filter structure so that the callback will refuse
     * to add non matching elements. */
    void *privdata[2];
    list *list = listCreate();
    privdata[0] = list;
    privdata[1] = &filter;
    do {
        cursor = dictScan(server.queues,cursor,qscanCallback,privdata);
    } while (cursor &&
             (busyloop || /* If it's a busyloop, don't check iterations & len */
              (maxiterations-- &&
               listLength(list) < (unsigned long)count)));

    /* Provide the reply to the client. */
    addReplyMultiBulkLen(c, 2);
    addReplyBulkLongLong(c,cursor);

    addReplyMultiBulkLen(c, listLength(list));
    listNode *node;
    while ((node = listFirst(list)) != NULL) {
        robj *kobj = listNodeValue(node);
        addReplyBulk(c, kobj);
        decrRefCount(kobj);
        listDelNode(list, node);
    }
    listRelease(list);
}

/* WORKING job-id
 *
 * If the job is queued, remove it from queue and change state to active.
 * Postpone the job requeue time in the future so that we'll wait the retry
 * time before enqueueing again.
 * Broadcast a WORKING message to all the other nodes that have a copy according
 * to our local info.
 *
 * Return how much time the worker likely have before the next requeue event
 * or an error:
 *
 * -ACKED     The job is already acknowledged, so was processed already.
 * -NOJOB     We don't know about this job. The job was either already
 *            acknowledged and purged, or this node never received a copy.
 * -TOOLATE   50% of the job TTL already elapsed, is no longer possible to
 *            delay it.
 */
void workingCommand(client *c) {
    if (validateJobIDs(c,c->argv+1,1) == C_ERR) return;

    job *job = lookupJob(c->argv[1]->ptr);
    if (job == NULL) {
        addReplySds(c,
            sdsnew("-NOJOB Job not known in the context of this node.\r\n"));
        return;
    }

    /* Don't allow to postpone jobs that have less than 50% of time to live
     * left, in order to prevent a worker from monopolizing a job for all its
     * lifetime. */
    mstime_t ttl = ((mstime_t)job->etime*1000) - (job->ctime/1000000);
    mstime_t elapsed = server.mstime - (job->ctime/1000000);
    if (ttl > 0 && elapsed > ttl/2) {
        addReplySds(c,
            sdsnew("-TOOLATE Half of job TTL already elapsed, "
                   "you are no longer allowed to postpone the "
                   "next delivery.\r\n"));
        return;
    }

    if (job->state == JOB_STATE_QUEUED) dequeueJob(job);
    job->flags |= JOB_FLAG_BCAST_WILLQUEUE;
    updateJobRequeueTime(job,server.mstime+
                         job->retry*1000+
                         randomTimeError(DISQUE_TIME_ERR));
    clusterBroadcastWorking(job);
    addReplyLongLong(c,job->retry);
}

/* QSTAT queue-name
 *
 * Returns statistics and information about the specified queue. */
void qstatCommand(client *c) {
    queue *q = lookupQueue(c->argv[1]);
    if (!q) {
        addReply(c,shared.nullmultibulk);
        return;
    }
    time_t idle = time(NULL) - q->atime;
    time_t age = time(NULL) - q->ctime;
    if (idle < 0) idle = 0;
    if (age < 0) age = 0;

    addReplyMultiBulkLen(c,20);

    addReplyBulkCString(c,"name");
    addReplyBulk(c,q->name);

    addReplyBulkCString(c,"len");
    addReplyLongLong(c,queueLength(q));

    addReplyBulkCString(c,"age");
    addReplyLongLong(c,age);

    addReplyBulkCString(c,"idle");
    addReplyLongLong(c,idle);

    addReplyBulkCString(c,"blocked");
    addReplyLongLong(c,(q->clients == NULL) ? 0 : listLength(q->clients));

    addReplyBulkCString(c,"import-from");
    if (q->needjobs_responders) {
        addReplyMultiBulkLen(c,dictSize(q->needjobs_responders));
        dictForeach(q->needjobs_responders,de)
            clusterNode *n = dictGetKey(de);
            addReplyBulkCBuffer(c,n->name,CLUSTER_NAMELEN);
        dictEndForeach
    } else {
        addReply(c,shared.emptymultibulk);
    }

    addReplyBulkCString(c,"import-rate");
    addReplyLongLong(c,getQueueImportRate(q));

    addReplyBulkCString(c,"jobs-in");
    addReplyLongLong(c,q->jobs_in);

    addReplyBulkCString(c,"jobs-out");
    addReplyLongLong(c,q->jobs_out);

    addReplyBulkCString(c,"pause");
    addReplyBulkCString(c,queueGetPausedStateString(q->flags));
}

/* PAUSE queue option1 [option2 ... optionN]
 *
 * Change queue paused state. */
void pauseCommand(client *c) {
    int j, bcast = 0, update = 0;
    uint32_t old_flags = 0, new_flags = 0;

    queue *q = lookupQueue(c->argv[1]);
    if (q) old_flags = q->flags;

    for (j = 2; j < c->argc; j++) {
        if (!strcasecmp(c->argv[j]->ptr,"none")) {
            new_flags = 0;
            update = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"in")) {
            new_flags |= QUEUE_FLAG_PAUSED_IN; update = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"out")) {
            new_flags |= QUEUE_FLAG_PAUSED_OUT; update = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"all")) {
            new_flags |= QUEUE_FLAG_PAUSED_ALL; update = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"state")) {
            /* Nothing to do, we reply with the state regardless. */
        } else if (!strcasecmp(c->argv[j]->ptr,"bcast")) {
            bcast = 1;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    /* Update the queue pause state, if needed. */
    if (!q && update && old_flags != new_flags) q = createQueue(c->argv[1]);
    if (q && update) {
        queueChangePausedState(q,QUEUE_FLAG_PAUSED_IN,
            (new_flags & QUEUE_FLAG_PAUSED_IN) != 0);
        queueChangePausedState(q,QUEUE_FLAG_PAUSED_OUT,
            (new_flags & QUEUE_FLAG_PAUSED_OUT) != 0);
    }

    /* Get the queue flags after the operation. */
    new_flags = q ? q->flags : 0;
    new_flags &= QUEUE_FLAG_PAUSED_ALL;

    /* Broadcast a PAUSE command if the user specified BCAST. */
    if (bcast) clusterBroadcastPause(c->argv[1],new_flags);

    /* Always reply with the current queue state. */
    sds reply = sdsnewlen("+",1);
    reply = sdscat(reply,queueGetPausedStateString(new_flags));
    reply = sdscatlen(reply,"\r\n",2);
    addReplySds(c,reply);
}
