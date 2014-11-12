/* Jobs handling and commands.
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
#include "sha1.h"

/* ------------------------- Low level jobs functions ----------------------- */

/* Generate a new Job ID and writes it to the string pointed by 'id'
 * (NOT including a null term), that must be JOB_ID_LEN or more.
 *
 * An ID is composed as such:
 *
 * +----+---------------------------+----+
 * |DISQ|40 random chars, hex format| TTL|
 * +----+---------------------------+----+
 *
 * "DISQ" is just a fixed string. All Disque Job IDs start with this
 * for bytes.
 *
 * The TTL is a big endian 16 bit unsigned number ceiled to 2^16-1
 * if greater than that, and is only used in order to expire ACKs
 * when the job is no longer avaialbe. It represents the TTL of the
 * original job in *minutes*, not seconds, and is encoded in as a
 * 4 digits hexadecimal number. */
void generateJobID(char *id, int ttl) {
    char *charset = "0123456789abcdef";
    SHA1_CTX ctx;
    unsigned char hash[22]; /* 20 + 2 bytes for TTL. */
    int j;
    static uint64_t counter;

    counter++;
    SHA1Init(&ctx);
    SHA1Update(&ctx,(unsigned char*)server.jobid_seed,DISQUE_RUN_ID_SIZE);
    SHA1Update(&ctx,(unsigned char*)&counter,sizeof(counter));
    SHA1Final(hash,&ctx);

    ttl /= 60; /* Store TTL in minutes. */
    hash[20] = (ttl&0xff00)>>8;
    hash[21] = ttl&0xff;

    *id++ = 'D';
    *id++ = 'I';
    *id++ = 'S';
    *id++ = 'Q';

    /* Convert 22 bytes (20 pseudorandom + 2 TTL in minutes) to hex. */
    for (j = 0; j < 22; j++) {
        id[0] = charset[(hash[0]&0xf0)>>4];
        id[1] = charset[hash[0]&0xf];
        id += 2;
    }
}

/* Create a new job in a given state. If "ID" is NULL, a new ID will be
 * created as assigned.
 *
 * This function only creates the job without any body, the only populated
 * fields are the ID and the state. */
job *createJob(char *id, int state, int ttl) {
    job *j = zmalloc(sizeof(job));

    j->id = sdsnewlen(NULL,JOB_ID_LEN);
    if (id)
        memcpy(j->id,id,JOB_ID_LEN);
    else
        generateJobID(j->id,ttl);
    j->state = state;
    j->flags = 0;
    j->bodylen = 0;
    j->body = NULL;
    j->nodes_delivered = NULL;
    j->nodes_confirmed = NULL;
    return j;
}

/* Free a job. */
void freeJob(job *j) {
    decrRefCount(j->queue);
    sdsfree(j->id);
    sdsfree(j->body);
    if (j->nodes_delivered) dictRelease(j->nodes_delivered);
    if (j->nodes_confirmed) dictRelease(j->nodes_confirmed);
    zfree(j);
}

/* Add the job in the jobs hash table, so that we can use lookupJob()
 * (by job ID) later. */
int registerJob(job *j) {
    /* TODO */
}

/* Lookup a job by ID. */
job *lookupJob(char *jobid) {
    /* TODO */
}

/* ---------------------------  Jobs serialization -------------------------- */

/* -------------------------  Jobs cluster functions ------------------------ */

/* This function sends a DELJOB message to all the nodes that may have
 * a copy of the job, in order to trigger deletion of the job.
 * It is used when an ADDJOB command time out to unregister (in a best
 * effort way, without gurantees) the job, and in the ACKs grabage
 * collection procedure.
 *
 * This function also unregisters and releases the job from the local
 * node. */
void deleteJobFromCluster(job *j) {
    /* TODO */
    /* Send DELJOB message to the right nodes. */
    /* Unregister the job. */
    /* Free the job. */
}

/* --------------------------  Jobs related commands ------------------------ */

/* This is called by unblockClient() to perform the cleanup of a client
 * blocked by ADDJOB. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingJobRepl(client *c) {
    c->bpop.job = NULL;
}

/* ADDJOB queue job [REPLICATE <n>] [TTL <sec>] [RETRY <sec>] [TIMEOUT <ms>]
 *        [ASYNC]. */
void addjobCommand(client *c) {
    long long replicate = server.cluster->size/2+1;
    long long ttl = 3600*24;
    long long retry = -1;
    mstime_t timeout = 50;
    int j, retval;
    int async = 0;  /* Asynchronous request? */

    /* Parse args. */
    for (j = 3; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int lastarg = j == c->argc-1;
        if (!strcasecmp(opt,"replicate") && !lastarg) {
            retval = getLongLongFromObject(c->argv[j+1],&replicate);
            if (retval != DISQUE_OK || replicate <= 0 || replicate > 65535) {
                addReplyError(c,"REPLICATE must be between 1 and 65535");
                return;
            }
            j++;
        } else if (!strcasecmp(opt,"ttl") && !lastarg) {
            retval = getLongLongFromObject(c->argv[j+1],&ttl);
            if (retval != DISQUE_OK || ttl <= 0) {
                addReplyError(c,"TTL must be a number > 0");
                return;
            }
            j++;
        } else if (!strcasecmp(opt,"retry") && !lastarg) {
            retval = getLongLongFromObject(c->argv[j+1],&retry);
            if (retval != DISQUE_OK || retry < 0) {
                addReplyError(c,"RETRY count must be a non negative number");
                return;
            }
            j++;
        } else if (!strcasecmp(opt,"timeout") && !lastarg) {
            if (getTimeoutFromObjectOrReply(c,c->argv[j+1],&timeout,UNIT_MILLISECONDS) != DISQUE_OK) return;
            j++;
        } else if (!strcasecmp(opt,"async")) {
            async = 1;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    /* REPLICATE > 1 and RETRY set to 0 does not make sense, why to replicate
     * the job if it will never try to be re-queued if case the job processing
     * is not acknowledged? */
    if (replicate > 1 && retry == 0) {
        addReplyError(c,"REPLICATE > 1 and RETRY 0 is invalid. "
                        "For at-most-once semantic (RETRY 0) use REPLICATE 1");
        return;
    }

    /* When retry is not specified, it defaults to 1/10 of the TTL. */
    if (retry == -1) {
        retry = ttl/10;
        if (retry == 0) retry = 1;
    }

    /* Check if REPLICATE can't be honoured at all. */
    if (replicate-1 > server.cluster->reachable_nodes_count) {
        addReplySds(c,
            sdsnew("-NOREPL Not enough reachable nodes "
                   "for the requested replication level\r\n"));
        return;
    }

    /* Create a new job. */
    job *job = createJob(NULL,JOB_STATE_WAIT_REPL,ttl);
    job->queue = c->argv[1];
    incrRefCount(c->argv[1]);
    job->repl = replicate;
    job->ctime = server.unixtime;
    job->etime = job->ctime + ttl;
    job->qtime = 0; /* Will be updated by queueAddjob(). */
    job->rtime = retry;
    registerJob(job);

    /* If the replication factor is > 1, send REPLJOB messages to REPLICATE-1
     * nodes and block the client, since we want synchronous replication of the
     * message. Otherwise if the job is stored just into this node for user
     * request, we don't have anything to wait and can remember ASAP. */
    if (replicate > 1) {
        c->bpop.timeout = timeout;
        c->bpop.job = job;
        job->state = JOB_STATE_WAIT_REPL;
        blockClient(c,DISQUE_BLOCKED_JOB_REPL);
    } else {
        queueAddJob(c->argv[1],job);
        addReply(c,shared.ok);
    }
}
