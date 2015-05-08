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

#include "disque.h"
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

/* Destroy a queue and unregisters it. On success DISQUE_OK is returned,
 * otherwise if no queue exists with the specified name, DISQUE_ERR is
 * returned. */
int destroyQueue(robj *name) {
    queue *q = lookupQueue(name);
    if (!q) return DISQUE_ERR;

    dictDelete(server.queues,name);
    decrRefCount(q->name);
    skiplistFree(q->sl);
    zfree(q);
    return DISQUE_OK;
}

/* Send a job as a return value of a command. This is about jobs but inside
 * queue.c since this is the format used in order to return a job from a
 * queue, as an array [queue_name,id,body]. */
void addReplyJob(client *c, job *j) {
    addReplyMultiBulkLen(c,3);
    addReplyBulk(c,j->queue);
    addReplyBulkCBuffer(c,j->id,JOB_ID_LEN);
    addReplyBulkCBuffer(c,j->body,sdslen(j->body));
}

/* ------------------------ Queue higher level API -------------------------- */

/* Queue the job and change its state accordingly. If the job is already
 * in QUEUED state, DISQUE_ERR is returned, otherwise DISQUE_OK is returned
 * and the operation succeeds. */
int enqueueJob(job *job) {
    if (job->state == JOB_STATE_QUEUED || job->qtime == 0)
        return DISQUE_ERR;

    serverLog(DISQUE_VERBOSE,"QUEUED %.48s", job->id);

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
    if (job->flags & JOB_FLAG_BCAST_QUEUED)
        clusterBroadcastQueued(job);
    else
        job->flags |= JOB_FLAG_BCAST_QUEUED; /* Next time, broadcast. */

    updateJobAwakeTime(job,0);
    queue *q = lookupQueue(job->queue);
    if (!q) q = createQueue(job->queue);
    serverAssert(skiplistInsert(q->sl,job) != NULL);
    q->atime = server.unixtime;
    signalQueueAsReady(q);
    return DISQUE_OK;
}

/* Remove a job from the queue. Returns DISQUE_OK if the job was there and
 * is now removed (updating the job state back to ACTIVE), otherwise
 * DISQUE_ERR is returned. */
int dequeueJob(job *job) {
    if (job->state != JOB_STATE_QUEUED) return DISQUE_ERR;
    queue *q = lookupQueue(job->queue);
    if (!q) return DISQUE_ERR;
    serverAssert(skiplistDelete(q->sl,job));
    job->state = JOB_STATE_ACTIVE; /* Up to the caller to override this. */
    serverLog(DISQUE_VERBOSE,"DE-QUEUED %.48s", job->id);
    return DISQUE_OK;
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
    q->atime = server.unixtime;
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
 * blocked, has no jobs inside. If the queue is removed DISQUE_OK is
 * returned, otherwise DISQUE_ERR is returned. */
#define QUEUE_MAX_IDLE_TIME (60*5)
int GCQueue(queue *q) {
    time_t elapsed = server.unixtime - q->atime;
    if (elapsed < QUEUE_MAX_IDLE_TIME) return DISQUE_ERR;
    if (q->clients && listLength(q->clients) != 0) return DISQUE_ERR;
    if (skiplistLength(q->sl)) return DISQUE_ERR;
    destroyQueue(q->name);
    return DISQUE_OK;
}

/* This function is called form serverCron() in order to incrementally remove
 * from memory queues which are found to be idle and empty. */
void queueCron(void) {
    mstime_t start = mstime();
    long sampled = 0, evicted = 0;

    while (dictSize(server.queues) != 0) {
        dictEntry *de = dictGetRandomKey(server.queues);
        queue *q = dictGetVal(de);

        sampled++;
        if (GCQueue(q) == DISQUE_OK) evicted++;

        /* First exit condition: we are able to expire less than 10% of
         * entries. */
        if (sampled > 10 && (evicted * 10) < sampled) break;

        /* Second exit condition: we are looping for some time and maybe
         * we are using more than one or two milliseconds of time. */
        if (((sampled+1) % 1000) == 0 && mstime()-start > 1) break;
    }
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
void blockForJobs(client *c, robj **queues, int numqueues, mstime_t timeout) {
    int j;

    c->bpop.timeout = timeout;
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
    blockClient(c,DISQUE_BLOCKED_QUEUES);
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
            GCQueue(q);
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
        if (!q) continue;
        int numclients = listLength(q->clients);
        while(numclients--) {
            unsigned long qlen;
            listNode *ln = listFirst(q->clients);
            client *c = ln->value;
            job *j = queueFetchJob(q,&qlen);

            if (!j) break; /* No longer elements, try next queue. */
            if (qlen == 0) needJobsForQueue(q,NEEDJOBS_REACHED_ZERO);
            addReplyMultiBulkLen(c,1);
            addReplyJob(c,j);
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
    if (c->flags & DISQUE_BLOCKED && c->btype == DISQUE_BLOCKED_QUEUES) {
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
            serverLog(DISQUE_WARNING,
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
        if (enqueueJob(job) == DISQUE_ERR) continue;

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
    robj **queues = NULL;
    int j, numqueues = 0;

    /* Parse args. */
    for (j = 1; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int lastarg = j == c->argc-1;
        if (!strcasecmp(opt,"nohang")) {
            nohang = 1;
        } else if (!strcasecmp(opt,"timeout") && !lastarg) {
            if (getTimeoutFromObjectOrReply(c,c->argv[j+1],&timeout,
                UNIT_MILLISECONDS) != DISQUE_OK) return;
            j++;
        } else if (!strcasecmp(opt,"count") && !lastarg) {
            int retval = getLongLongFromObject(c->argv[j+1],&count);
            if (retval != DISQUE_OK || count <= 0) {
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
            if (q) job = queueNameFetchJob(queues[j],&qlen);

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
            addReplyJob(c,job);
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

    /* If we reached this point, we need to block. */
    blockForJobs(c,queues,numqueues,timeout);
}

/* ENQUEUE job-id-1 job-id-2 ... job-id-N
 *
 * If the job is active, queue it if job retry != 0.
 * If the job is in any other state, do nothing.
 * If the job is not knonw, do nothing.
 *
 * NOTE: Even jobs with retry set to 0 are enqueued! Be aware that
 * using this command may violate the at-most-once contract.
 *
 * Return the number of jobs actually move from active to queued state. */
void enqueueCommand(client *c) {
    int j, enqueued = 0;

    if (validateJobIDs(c,c->argv+1,c->argc-1) == DISQUE_ERR) return;

    /* Enqueue all the jobs in active state. */
    for (j = 1; j < c->argc; j++) {
        job *job = lookupJob(c->argv[j]->ptr);
        if (job == NULL) continue;

        if (job->state == JOB_STATE_ACTIVE && enqueueJob(job) == DISQUE_OK)
            enqueued++;
    }
    addReplyLongLong(c,enqueued);
}

/* DEQUEUE job-id-1 job-id-2 ... job-id-N
 *
 * If the job is queued, remove it from queue and change state to active.
 * If the job is in any other state, do nothing.
 * If the job is not knonw, do nothing.
 *
 * Return the number of jobs actually moved from queue to active state. */
void dequeueCommand(client *c) {
    int j, dequeued = 0;

    if (validateJobIDs(c,c->argv+1,c->argc-1) == DISQUE_ERR) return;

    /* Enqueue all the jobs in active state. */
    for (j = 1; j < c->argc; j++) {
        job *job = lookupJob(c->argv[j]->ptr);
        if (job == NULL) continue;

        if (job->state == JOB_STATE_QUEUED && dequeueJob(job) == DISQUE_OK)
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

    if (getLongLongFromObjectOrReply(c,c->argv[2],&count,NULL) != DISQUE_OK)
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
        addReplyMultiBulkLen(c,2);
        addReplyBulkCBuffer(c,j->id,JOB_ID_LEN);
        addReplyBulkCBuffer(c,j->body,sdslen(j->body));
        returned++;
        if (newjobs)
            sn = sn->backward;
        else
            sn = sn->level[0].forward;
    }
    setDeferredMultiBulkLength(c,deflen,returned);
}

/* QUEUES
 *
 * Return an array of the names of all registered queues. */
void queuesCommand(client *c) {
    dictIterator *di;
    dictEntry *de;
    unsigned long numqueues = 0;
    void *replylen = addDeferredMultiBulkLength(c);

    di = dictGetSafeIterator(server.queues);
    while((de = dictNext(di)) != NULL) {
        robj *qname = dictGetKey(de);
        addReplyBulk(c,qname);
        numqueues++;
    }
    dictReleaseIterator(di);
    setDeferredMultiBulkLength(c,replylen,numqueues);
}
