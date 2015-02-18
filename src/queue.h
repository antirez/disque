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

typedef struct queue {
    robj *name;      /* Queue name as a string object. */
    skiplist *sl;    /* The skiplist with the queued jobs. */
    uint32_t ctime;  /* Creation time of this queue object. */
    uint32_t atime;  /* Last access time. Updated when a new element is
                        queued or when a new client fetches elements or
                        blocks for elements to arrive. */
    list *clients;   /* Clients blocked here. */

    /* Federation related. */
    mstime_t needjobs_bcast_time; /* Last NEEDJOBS cluster broadcast. */
    mstime_t needjobs_adhoc_time; /* Last NEEDJOBS to notable nodes. */

    /* Set of nodes that provided jobs. The key is the node, the value
     * is the unix time of the last time we received data from this node
     * about this queue. */
    dict *needjobs_responders;

    /* Tracking of incoming messages rate (messages received from other
     * nodes because of NEEDJOBS).
     *
     * This is not going to be perfect, but we need to be memory
     * efficient, and have some rought ideas to optimize NEEDJOBS messages.
     *
     * As soon as we receive jobs in the current second, we increment
     * the current count. Otherwise we store the current data into the
     * previous data, and set the current time to the current unix time.
     *
     * Instantaneous receives jobs/sec is just:
     *
     *     jobs_sec = (current_count+prev_count) / (now-prev_time)
     */
    uint32_t current_import_jobs_time;
    uint32_t prev_import_jobs_time;
    uint32_t current_import_jobs_count;
    uint32_t prev_import_jobs_count;
    uint32_t needjobs_bcast_attempt; /* Num of tries without reply. */
} queue;

int queueJob(job *job);
int dequeueJob(job *job);
job *queueFetchJob(queue *q, unsigned long *qlen);
job *queueNameFetchJob(robj *qname, unsigned long *qlen);
unsigned long queueLength(robj *qname);
void unblockClientBlockedForJobs(client *c);
void handleClientsBlockedOnQueues(void);

#define NEEDJOBS_CLIENTS_WAITING 0 /* Called because clients are waiting. */
#define NEEDJOBS_REACHED_ZERO 1    /* Called since we just ran out of jobs. */
void needJobsForQueue(queue *q, int type);
void needJobsForQueueName(robj *qname, int type);

#endif
