/* Acknowledges handling and commands.
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

/* ------------------------- Low level ack functions ------------------------ */

/* Change job state as acknowledged. If it is already in that state, the
 * function does nothing. */
void acknowledgeJob(job *job) {
    if (job->state == JOB_STATE_ACKED) return;

    dequeueJob(job);
    job->state = JOB_STATE_ACKED;
    /* Remove the nodes_confirmed hash table if it exists.
     * tryJobGC() will take care to create a new one used for the GC
     * process. */
    if (job->nodes_confirmed) {
        dictRelease(job->nodes_confirmed);
        job->nodes_confirmed = NULL;
    }
}

/* ------------------------- Garbage collection ----------------------------- */

/* Try to garbage collect the job. */
void tryJobGC(job *job) {
    if (job->state != JOB_STATE_ACKED) return;
    serverLog(DISQUE_NOTICE,"GC %.48s", job->id);

    /* nodes_confirmed is used in order to store all the nodes that have the
     * job in ACKed state, so that the job can be evicted when we are
     * confident the job will not be reissued. */
    if (job->nodes_confirmed == NULL) {
        job->nodes_confirmed = dictCreate(&clusterNodesDictType,NULL);
        dictAdd(job->nodes_confirmed,myself->name,myself);
    }

    /* Send a SETACK message to all the nodes that may have a message but are
     * still not listed in the nodes_confirmed hash table. However if this
     * is a dumb ACK (created by ACKJOB command acknowledging a job we don't
     * know) we have to broadcast the SETACK to everybody in search of the
     * owner. */
    dict *targets = dictSize(job->nodes_delivered) == 0 ?
                    server.cluster->nodes : job->nodes_delivered;
    dictForeach(targets,de)
        clusterNode *node = dictGetVal(de);
        if (dictFind(job->nodes_confirmed,node->name) == NULL)
            clusterSendSetAck(node,job);
    dictEndForeach
}

/* This function is called by cluster.c every time we receive a GOTACK message
 * about a job we know. */
void gotAckReceived(clusterNode *sender, job *job, int known) {
    /* A dummy ACK is an acknowledged job that we created just becakse a client
     * send us ACKJOB about a job we were not aware. */
    int dummy_ack = dictSize(job->nodes_delivered) == 0;

    serverLog(DISQUE_NOTICE,"RECEIVED GOTACK FROM %.40s FOR JOB %.48s",
        sender->name, job->id);

    /* If this is a dummy ACK, and we reached a node that knows about this job,
     * it's up to it to perform the garbage collection, so we can forget about
     * this job and reclaim memory. */
    if (dummy_ack && known) {
        serverLog(DISQUE_NOTICE,"Deleting %.48s: authoritative node reached",
            job->id);
        unregisterJob(job);
        freeJob(job);
        return;
    }

    /* If the sender knows about the job, or if we have the sender in the list
     * of nodes that may have the job (even if it no longer remembers about
     * the job), we do two things:
     *
     * 1) Add the node to the list of nodes_delivered. It is likely already
     *    there... so this should be useless, but is a good invariant
     *    to enforce.
     * 2) Add the node to the list of nodes that acknowledged the ACK. */
    if (known || dictFind(job->nodes_delivered,sender->name) != NULL) {
        dictAdd(job->nodes_delivered,sender->name,sender);
        dictAdd(job->nodes_confirmed,sender->name,sender);
    }

    /* If our job is actually a dummy ACK, we are still interested to collect
     * all the nodes in the cluster that reported they don't have a clue:
     * eventually if everybody in the cluster has no clue, we can safely remove
     * the dummy ACK. */
    if (!known && dummy_ack) {
        dictAdd(job->nodes_confirmed,sender->name,sender);
        if (dictSize(job->nodes_confirmed) >= dictSize(server.cluster->nodes))
        {
            serverLog(DISQUE_NOTICE,
                "Deleting %.48s: dummy ACK not known cluster-wide",
                job->id);
            unregisterJob(job);
            freeJob(job);
            return;
        }
    }

    /* Finally the common case: our SETACK reached everybody. Broadcast
     * a DELJOB to all the nodes involved, and delete the job. */
    if (!dummy_ack && dictSize(job->nodes_confirmed) >=
                      dictSize(job->nodes_delivered))
    {
        serverLog(DISQUE_NOTICE,
            "Deleting %.48s: All nodes involved acknowledged the job",
            job->id);
        clusterBroadcastDelJob(job);
        unregisterJob(job);
        freeJob(job);
        return;
    }
}

/* --------------------------  Acks related commands ------------------------ */

/* ACKJOB jobid_1 jobid_2 ... jobid_N
 *
 * Set job state as acknowledged, if the job does not exist creates a
 * fake job just to hold the acknowledge.
 *
 * As a result of a job being acknowledged, the system tries to garbage
 * collect it, that is, to remove the job from every node of the system
 * in order to both avoid multiple deliveries of the same job, and to
 * release resources.
 *
 * If a job was already acknowledged, the ACKJOB command still has the
 * effect of forcing a GC attempt ASAP.
 *
 * The command returns the number of jobs already known and that were
 * already not in the ACKED state.
 */
void ackjobCommand(client *c) {
    int j, known = 0;

    /* Mass-validate the Job IDs, so if we have to stop with an error, nothing
     * at all is processed. */
    for (j = 1; j < c->argc; j++) {
        if (validateJobIdOrReply(c,c->argv[j]->ptr,sdslen(c->argv[j]->ptr))
            == DISQUE_ERR) return;
    }

    /* Perform the appropriate action for each job. */
    for (j = 1; j < c->argc; j++) {
        job *job = lookupJob(c->argv[j]->ptr);
        /* Case 1: No such job. Create one just to hold the ACK. */
        if (job == NULL) {
            job = createJob(c->argv[j]->ptr,JOB_STATE_ACKED,0);
            setJobTtlFromId(job);
            serverAssert(registerJob(job) == DISQUE_OK);
        }
        /* Case 2: Job exists and is not acknowledged. Change state. */
        if (job && job->state != JOB_STATE_ACKED) {
            dequeueJob(job); /* Safe to call if job is not queued. */
            acknowledgeJob(job);
            known++;
        }
        /* Anyway... start a GC attempt on the acked job. */
        tryJobGC(job);
    }
    addReplyLongLong(c,known);
}

