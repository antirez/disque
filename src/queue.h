/*
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


#ifndef __DISQUE_QUEUE_H
#define __DISQUE_QUEUE_H

#include "skiplist.h"

#define QUEUE_OPS_SAMPLES 3

typedef struct queue {
    robj *name;     /* Queue name as a string object. */
    skiplist *sl;   /* The skiplist with the queued jobs. */
    time_t ctime;   /* Creation time of this queue object. */
    time_t atime;   /* Last access time. Updated when a new element is
                       queued or when a new client fetches elements or
                       blocks for elements to arrive. */
    list *clients;  /* Clients blocked here. */

    /* Federation related. */
    mstime_t needjobs_bcast_time; /* Last NEEDJOBS cluster broadcast. */
    mstime_t needjobs_adhoc_time; /* Last NEEDJOBS to notable nodes. */
    dict *needjobs_responders;    /* Set of nodes that provided jobs. */

    /* OPS samples is used in order to compute the number of jobs received
     * and served per second. Each time the sample at the current index
     * (*_ops_idx) is still the current unix timestamp and an op is
     * performed, we increment its counter. Otherwise the index is advanced
     * and a new sample is created. By observing the array of samples we can
     * easily compute the approximated amount of ops per second this queue
     * is undergoing.
     *
     * When we see we are consuming much faster than producing, messages
     * can be sent to other nodes to obtain more jobs. */
    struct {
        uint32_t time;
        uint32_t count;
    } produced_ops_samples[QUEUE_OPS_SAMPLES],
      consumed_ops_samples[QUEUE_OPS_SAMPLES];
    int produced_ops_idx; /* Current index in produced array. */
    int consumed_ops_idx; /* Current index in consumed array. */
} queue;

int queueJob(job *job);
int dequeueJob(job *job);
job *queueFetchJob(queue *q);
job *queueNameFetchJob(robj *qname);
unsigned long queueLength(robj *qname);
void unblockClientBlockedForJobs(client *c);
void handleClientsBlockedOnQueues(void);

#endif
