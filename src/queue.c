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
    q->ctime = mstime();
    q->atime = q->ctime;
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
    if (job->state == JOB_STATE_QUEUED) return DISQUE_ERR;

    printf("QUEUED %.48s\n", job->id);

    /* If set, cleanup nodes_confirmed to free memory. We'll reuse this
     * hash table again for ACKs tracking in order to garbage collect the
     * job once processed. */
    if (job->nodes_confirmed) {
        dictRelease(job->nodes_confirmed);
        job->nodes_confirmed = NULL;
    }
    job->state = JOB_STATE_QUEUED;

    /* Put the job into the queue and update the time we'll queue it again. */
    if (job->retry)
        job->qtime = server.unixtime + job->retry;
    else
        job->qtime = 0; /* Never re-queue at most once jobs. */
    updateJobAwakeTime(job,0);
    queue *q = lookupQueue(job->queue);
    if (!q) q = createQueue(job->queue);
    serverAssert(skiplistInsert(q->sl,job) != NULL);
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
    return DISQUE_ERR;
}

/* Fetch a job from the specified queue if any, updating the job state
 * as it gets fetched back to ACTIVE. If there are no jobs pending in the
 * specified queue, NULL is returned.
 *
 * The returned job is, among the jobs available, the one with lower
 * 'ctime'. */
job *queueFetchJob(robj *qname) {
    queue *q = lookupQueue(qname);
    if (!q || skiplistLength(q->sl) == 0) return NULL;
    job *j = skiplistPopHead(q->sl);
    j->state = JOB_STATE_ACTIVE;
    return j;
}

/* Return the length of the queue. If the queue does not eixst, zero
 * is returned. */
unsigned long queueLength(robj *qname) {
    queue *q = lookupQueue(qname);
    if (!q) return 0;
    return skiplistLength(q->sl);
}

/* ------------------------- Queue related commands ------------------------- */

/* QLEN <qname> -- Return the number of jobs queued. */
void qlenCommand(client *c) {
    addReplyLongLong(c,queueLength(c->argv[1]));
}

/* GETJOBS [TIMEOUT <ms>] [COUNT <count>] FROM <qname1> <qname2> ... <qnameN>.
 *
 * Get jobs from the specified queues. By default COUNT is 1, so just one
 * job will be returned. If there are no jobs in any of the specified queues
 * the command will block.
 *
 * When there are jobs in more than one of the queues, the command guarantees
 * to return jobs in the order the queues are specified. If COUNT allows
 * more jobs to be returned, queues are scanned again and again in the same
 * order popping more elements. */
void getJobsCommand(client *c) {
}


