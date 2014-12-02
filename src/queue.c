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

void signalQueueAsReady(queue *q);

/* ------------------------ Low level queue functions ----------------------- */

/* Job comparision inside a skiplist: by ctime, if ctime is the same by
 * job ID. */
int skiplistCompareJobsInQueue(const void *a, const void *b) {
    const job *ja = a, *jb = b;

    if (ja->ctime > jb->ctime) return -1;
    if (jb->ctime > ja->ctime) return 1;
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
    q->atime = 0;
    q->needjobs_bcast_time = 0;
    q->needjobs_adhoc_time = 0;
    q->needjobs_responders = NULL; /* Created on demand to save memory. */
    q->clients = NULL; /* Created on demand to save memory. */

    memset(q->produced_ops_samples,0,sizeof(q->produced_ops_samples));
    memset(q->consumed_ops_samples,0,sizeof(q->consumed_ops_samples));
    q->produced_ops_idx = 0;
    q->consumed_ops_idx = 0;

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

/* ------------------------ Queue higher level API -------------------------- */

/* Queue the job and change its state accordingly. If the job is already
 * in QUEUED state, DISQUE_ERR is returned, otherwise DISQUE_OK is returned
 * and the operation succeeds. */
int queueJob(job *job) {
    if (job->state == JOB_STATE_QUEUED || job->qtime == 0)
        return DISQUE_ERR;

    serverLog(DISQUE_NOTICE,"QUEUED %.48s", job->id);

    /* If set, cleanup nodes_confirmed to free memory. We'll reuse this
     * hash table again for ACKs tracking in order to garbage collect the
     * job once processed. */
    if (job->nodes_confirmed) {
        dictRelease(job->nodes_confirmed);
        job->nodes_confirmed = NULL;
    }
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
        clusterSendQueued(job);
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
    serverLog(DISQUE_NOTICE,"DE-QUEUED %.48s", job->id);
    return DISQUE_ERR;
}

/* Fetch a job from the specified queue if any, updating the job state
 * as it gets fetched back to ACTIVE. If there are no jobs pending in the
 * specified queue, NULL is returned.
 *
 * The returned job is, among the jobs available, the one with lower
 * 'ctime'. */
job *queueFetchJob(queue *q) {
    if (skiplistLength(q->sl) == 0) return NULL;
    job *j = skiplistPopHead(q->sl);
    j->state = JOB_STATE_ACTIVE;
    q->atime = server.unixtime;
    return j;
}

/* Like queueFetchJob() but take the queue name as input. */
job *queueNameFetchJob(robj *qname) {
    queue *q = lookupQueue(qname);
    return q ? queueFetchJob(q) : NULL;
}

/* Return the length of the queue. If the queue does not eixst, zero
 * is returned. */
unsigned long queueLength(robj *qname) {
    queue *q = lookupQueue(qname);
    if (!q) return 0;
    return skiplistLength(q->sl);
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

/* -------------------------- Blocking on queues ---------------------------- */

/* Handle blocking if GETJOBS fonud no jobs in the specified queues.
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
            listNode *ln = listFirst(q->clients);
            client *c = ln->value;
            job *j = queueFetchJob(q);

            if (!j) break; /* No longer elements, try next queue. */
            addReplyMultiBulkLen(c,1);
            addReplyMultiBulkLen(c,2);
            addReplyBulkCBuffer(c,j->id,JOB_ID_LEN);
            addReplyBulkCBuffer(c,j->body,sdslen(j->body));
            unblockClient(c); /* This will remove it from q->clients. */
        }
    }
    dictReleaseIterator(di);
    dictEmpty(server.ready_queues,NULL);
}

/* ------------------------- Queue related commands ------------------------- */

/* QLEN <qname> -- Return the number of jobs queued. */
void qlenCommand(client *c) {
    addReplyLongLong(c,queueLength(c->argv[1]));
}

/* GETJOBS [NOHANG] [TIMEOUT <ms>] [COUNT <count>] FROM <qname1>
 *          <qname2> ... <qnameN>.
 *
 * Get jobs from the specified queues. By default COUNT is 1, so just one
 * job will be returned. If there are no jobs in any of the specified queues
 * the command will block.
 *
 * When there are jobs in more than one of the queues, the command guarantees
 * to return jobs in the order the queues are specified. If COUNT allows
 * more jobs to be returned, queues are scanned again and again in the same
 * order popping more elements. */
void getjobsCommand(client *c) {
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
            if (retval != DISQUE_OK || count < 0) {
                addReplyError(c,"COUNT must be a number > 0");
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
            job *job = queueNameFetchJob(queues[j]);
            if (!job) continue;
            if (!mbulk) mbulk = addDeferredMultiBulkLength(c);
            addReplyMultiBulkLen(c,2);
            addReplyBulkCBuffer(c,job->id,JOB_ID_LEN);
            addReplyBulkCBuffer(c,job->body,sdslen(job->body));
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


