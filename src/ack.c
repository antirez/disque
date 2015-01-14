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

/* ------------------------- Garbage collection ----------------------------- */

/* Try to garbage collect the job. */
void tryJobGC(job *j) {
    serverLog(DISQUE_NOTICE,"GC %.48s", j->id);
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
        if (j == NULL) {
            job = createJob(c->argv[j]->ptr,JOB_STATE_ACKED,0);
            setJobTtlFromId(job);
            serverAssert(registerJob(job) == DISQUE_OK);
        }
        /* Case 2: Job exists and is not acknowledged. Change state. */
        if (job && job->state != JOB_STATE_ACKED) {
            dequeueJob(job); /* Safe to call if job is not queued. */
            acknowledgeJob(job);
        }
        /* Anyway... start a GC attempt on the acked job. */
        tryJobGC(job);
    }
}

