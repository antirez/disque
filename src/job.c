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

#include "server.h"
#include "cluster.h"
#include "job.h"
#include "queue.h"
#include "ack.h"
#include "sha1.h"
#include "endianconv.h"

#include <ctype.h>

/* ------------------------- Low level jobs functions ----------------------- */

/* Generate a new Job ID and writes it to the string pointed by 'id'
 * (NOT including a null term), that must be JOB_ID_LEN or more.
 *
 * An ID is 40 bytes string composed as such:
 *
 * +--+-----------------+-+--------------------- --------+-+-----+
 * |D-| 8 bytes Node ID |-| 144-bit ID (base64: 24 bytes)|-| TTL |
 * +--+-----------------+-+------------------------------+-+-----+
 *
 * "D-" is just a fixed string. All Disque job IDs start with this
 * two bytes.
 *
 * Node ID is the first 8 bytes of the hexadecimal Node ID where the
 * message was created. The main use for this is that a consumer receiving
 * messages from a given queue can collect stats about where the producers
 * are connected, and switch to improve the cluster efficiency.
 *
 * The 144 bit ID is the unique message ID, encoded in base 64 with
 * the standard charset "A-Za-z0-9+/".
 *
 * The TTL is a big endian 16 bit unsigned number ceiled to 2^16-1
 * if greater than that, and is only used in order to expire ACKs
 * when the job is no longer avaialbe. It represents the TTL of the
 * original job in *minutes*, not seconds, and is encoded in as a
 * 4 digits hexadecimal number.
 *
 * The TTL is even if the job retry value is 0 (at most once jobs),
 * otherwise is odd, so the actual precision of the value is 2 minutes.
 * This is useful since the receiver of an ACKJOB command can avoid
 * creating a "dummy ack" for unknown job IDs for at most once jobs.
 */
void generateJobID(char *id, int ttl, int retry) {
    char *b64cset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "abcdefghijklmnopqrstuvwxyz"
                    "0123456789+/";
    char *hexcset = "0123456789abcdef";
    SHA1_CTX ctx;
    unsigned char ttlbytes[2], hash[20];
    int j;
    static uint64_t counter;

    /* Get the pseudo random bytes using SHA1 in counter mode. */
    counter++;
    SHA1Init(&ctx);
    SHA1Update(&ctx,(unsigned char*)server.jobid_seed,CONFIG_RUN_ID_SIZE);
    SHA1Update(&ctx,(unsigned char*)&counter,sizeof(counter));
    SHA1Final(hash,&ctx);

    ttl /= 60; /* Store TTL in minutes. */
    if (ttl > 65535) ttl = 65535;
    if (ttl < 0) ttl = 1;

    /* Force the TTL to be odd if retry > 0, even if retry == 0. */
    ttl = (retry > 0) ? (ttl|1) : (ttl & ~1);

    ttlbytes[0] = (ttl&0xff00)>>8;
    ttlbytes[1] = ttl&0xff;

    *id++ = 'D';
    *id++ = '-';

    /* 8 bytes from Node ID + separator */
    for (j = 0; j < 8; j++) *id++ = server.cluster->myself->name[j];
    *id++ = '-';

    /* Pseudorandom Message ID + separator. We encode 4 base64 chars
     * per loop (3 digest bytes), and each char encodes 6 bits, so we have
     * to loop 6 times to encode all the 144 bits into 24 destination chars. */
    unsigned char *h = hash;
    for (j = 0; j < 6; j++) {
        id[0] = b64cset[h[0]>>2];
        id[1] = b64cset[(h[0]<<4|h[1]>>4)&63];
        id[2] = b64cset[(h[1]<<2|h[2]>>6)&63];
        id[3] = b64cset[h[3]&63];
        id += 4;
        h += 3;
    }
    *id++ = '-';

    /* 4 TTL bytes in hex. */
    id[0] = hexcset[(ttlbytes[0]&0xf0)>>4];
    id[1] = hexcset[ttlbytes[0]&0xf];
    id[2] = hexcset[(ttlbytes[1]&0xf0)>>4];
    id[3] = hexcset[ttlbytes[1]&0xf];
    id += 4;
}

/* Helper function for setJobTTLFromID() in order to extract the TTL stored
 * as hex big endian number in the Job ID. The function is only used for this
 * but is more generic. 'p' points to the first digit for 'count' hex digits.
 * The number is assumed to be stored in big endian format. For each byte
 * the first hex char is the most significative. If invalid digits are found
 * considered to be zero, however errno is set to EINVAL if this happens. */
uint64_t hexToInt(char *p, size_t count) {
    uint64_t value = 0;
    char *charset = "0123456789abcdef";

    errno = 0;
    while(count--) {
        int c = tolower(*p++);
        char *pos = strchr(charset,c);
        int v;
        if (!pos) {
            errno = EINVAL;
            v = 0;
        } else {
            v = pos-charset;
        }
        value = (value << 4) | v;
    }
    return value;
}

/* Disque aims to avoid to deliver duplicated message whenever possible, so
 * it is always desirable that a given message is not queued by multiple owners
 * at the same time. This cannot be guaranteed because of partitions, but one
 * of the best-effort things we do is that, when a QUEUED message is received
 * by a node about a job, the node IDs of the sender and the receiver are
 * compared: If the sender has a greater node ID, we drop the message from our
 * queue (but retain a copy of the message to retry again later).
 *
 * However comparing nodes just by node ID means that a given node is always
 * greater than the other. So before comparing the node IDs, we mix the IDs
 * with the pseudorandom part of the Job ID, using the XOR function. This way
 * the comparision depends on the job. */
int compareNodeIDsByJob(clusterNode *nodea, clusterNode *nodeb, job *j) {
    int i;
    char ida[CLUSTER_NAMELEN], idb[CLUSTER_NAMELEN];
    memcpy(ida,nodea->name,CLUSTER_NAMELEN);
    memcpy(idb,nodeb->name,CLUSTER_NAMELEN);
    for (i = 0; i < CLUSTER_NAMELEN; i++) {
        /* The Job ID has 24 bytes of pseudo random bits starting at
         * offset 11. */
        ida[i] ^= j->id[11 + i%24];
        idb[i] ^= j->id[11 + i%24];
    }
    return memcmp(ida,idb,CLUSTER_NAMELEN);
}

/* Return the raw TTL (in minutes) from a well-formed Job ID.
 * The caller should do sanity check on the job ID before calling this
 * function. Note that the 'id' field of a a job structure is always valid. */
int getRawTTLFromJobID(char *id) {
    return hexToInt(id+36,4);
}

/* Set the job ttl from the encoded ttl in its ID. This is useful when we
 * create a new job just to store the fact it's acknowledged. Thanks to
 * the TTL encoded in the ID we are able to set the expire time for the job
 * regardless of the fact we have no info about the job. */
void setJobTTLFromID(job *job) {
    int expire_minutes = getRawTTLFromJobID(job->id);
    /* Convert back to absolute unix time. */
    job->etime = server.unixtime + expire_minutes*60;
}

/* Validate the string 'id' as a job ID. 'len' is the number of bytes the
 * string is composed of. The function just checks length and prefix/suffix.
 * It's pretty pointless to use more CPU to validate it better since anyway
 * the lookup will fail. */
int validateJobID(char *id, size_t len) {
    if (len != JOB_ID_LEN) return C_ERR;
    if (id[0] != 'D' ||
        id[1] != '-' ||
        id[10] != '-' ||
        id[35] != '-') return C_ERR;
    return C_OK;
}

/* Like validateJobID() but if the ID is invalid an error message is sent
 * to the client 'c' if not NULL. */
int validateJobIdOrReply(client *c, char *id, size_t len) {
    int retval = validateJobID(id,len);
    if (retval == C_ERR && c)
        addReplySds(c,sdsnew("-BADID Invalid Job ID format.\r\n"));
    return retval;
}

/* Create a new job in a given state. If 'id' is NULL, a new ID will be
 * created as assigned, otherwise the specified ID is used.
 * The 'ttl' and 'retry' arguments are only used if 'id' is not NULL.
 *
 * This function only creates the job without any body, the only populated
 * fields are the ID and the state. */
job *createJob(char *id, int state, int ttl, int retry) {
    job *j = zmalloc(sizeof(job));

    /* Generate a new Job ID if not specified by the caller. */
    if (id == NULL)
        generateJobID(j->id,ttl,retry);
    else
        memcpy(j->id,id,JOB_ID_LEN);

    j->queue = NULL;
    j->state = state;
    j->gc_retry = 0;
    j->flags = 0;
    j->body = NULL;
    j->nodes_delivered = dictCreate(&clusterNodesDictType,NULL);
    j->nodes_confirmed = NULL; /* Only created later on-demand. */
    j->awakeme = 0; /* Not yet registered in awakeme skiplist. */
    /* Number of NACKs and additiona deliveries start at zero and
     * are incremented as QUEUED messages are received or sent. */
    j->num_nacks = 0;
    j->num_deliv = 0;
    return j;
}

/* Free a job. Does not automatically unregister it. */
void freeJob(job *j) {
    if (j == NULL) return;
    if (j->queue) decrRefCount(j->queue);
    sdsfree(j->body);
    if (j->nodes_delivered) dictRelease(j->nodes_delivered);
    if (j->nodes_confirmed) dictRelease(j->nodes_confirmed);
    zfree(j);
}

/* Add the job in the jobs hash table, so that we can use lookupJob()
 * (by job ID) later. If a node knows about a job, the job must be registered
 * and can be retrieved via lookupJob(), regardless of is state.
 *
 * On success C_OK is returned. If there is already a job with the
 * specified ID, no operation is performed and the function returns
 * C_ERR. */
int registerJob(job *j) {
    int retval = dictAdd(server.jobs, j->id, NULL);
    if (retval == DICT_ERR) return C_ERR;

    updateJobAwakeTime(j,0);
    return C_OK;
}

/* Lookup a job by ID. */
job *lookupJob(char *id) {
    struct dictEntry *de = dictFind(server.jobs, id);
    return de ? dictGetKey(de) : NULL;
}

/* Remove job references from the system, without freeing the job itself.
 * If the job was already unregistered, C_ERR is returned, otherwise
 * C_OK is returned. */
int unregisterJob(job *j) {
    j = lookupJob(j->id);
    if (!j) return C_ERR;

    /* Emit a DELJOB command for all the job states but WAITREPL (no
     * ADDJOB emitted yer), and ACKED (DELJOB already emitted). */
    if (j->state >= JOB_STATE_ACTIVE && j->state != JOB_STATE_ACKED)
        AOFDelJob(j);

    /* Remove from awake skip list. */
    if (j->awakeme) serverAssert(skiplistDelete(server.awakeme,j));

    /* If the job is queued, remove from queue. */
    if (j->state == JOB_STATE_QUEUED) dequeueJob(j);

    /* If there is a client blocked for this job, inform it that the job
     * got deleted, and unblock it. This should only happen when the job
     * gets expired before the requested replication level is reached. */
    if (j->state == JOB_STATE_WAIT_REPL) {
        client *c = jobGetAssociatedValue(j);
        setJobAssociatedValue(j,NULL);
        addReplySds(c,
            sdsnew("-NOREPL job removed (expired?) before the requested "
                   "replication level was achieved\r\n"));
        /* Change job state otherwise unblockClientWaitingJobRepl() will
         * try to remove the job itself. */
        j->state = JOB_STATE_ACTIVE;
        clusterBroadcastDelJob(j);
        unblockClient(c);
    }

    /* Remove the job from the jobs hash table. */
    dictDelete(server.jobs, j->id);
    return C_OK;
}

/* We use the server.jobs hash table in a space efficient way by storing the
 * job only at 'key' pointer, so the 'value' pointer is free to be used
 * for state specific associated information.
 *
 * When the job state is JOB_STATE_WAIT_REPL, the value is set to the client
 * that is waiting for synchronous replication of the job. */
void setJobAssociatedValue(job *j, void *val) {
    struct dictEntry *de = dictFind(server.jobs, j->id);
    if (de) dictSetVal(server.jobs,de,val);
}

/* See setJobAssociatedValue() top comment. */
void *jobGetAssociatedValue(job *j) {
    struct dictEntry *de = dictFind(server.jobs, j->id);
    return de ? dictGetVal(de) : NULL;
}

/* Return the job state as a C string pointer. This is mainly useful for
 * reporting / debugign tasks. */
char *jobStateToString(int state) {
    char *states[] = {"wait-repl","active","queued","acked"};
    if (state < 0 || state > JOB_STATE_ACKED) return "unknown";
    return states[state];
}

/* Return the state number for the specified C string, or -1 if
 * there is no match. */
int jobStateFromString(char *state) {
    if (!strcasecmp(state,"wait-repl")) return JOB_STATE_WAIT_REPL;
    else if (!strcasecmp(state,"active")) return JOB_STATE_ACTIVE;
    else if (!strcasecmp(state,"queued")) return JOB_STATE_QUEUED;
    else if (!strcasecmp(state,"acked")) return JOB_STATE_ACKED;
    else return -1;
}

/* ----------------------------- Awakeme list ------------------------------
 * Disque needs to perform periodic tasks on registered jobs, for example
 * we need to remove expired jobs (TTL reached), requeue existing jobs that
 * where not acknowledged in time, schedule the job garbage collection after
 * the job is acknowledged, and so forth.
 *
 * To simplify the handling of periodic operations without adding multiple
 * timers for each job, jobs are put into a skip list that order jobs for
 * the unix time we need to take some action about them.
 *
 * Every registered job is into this list. After we update some job field
 * that is related to scheduled operations on the job, or when it's state
 * is updated, we need to call updateJobAwakeTime() again in order to move
 * the job into the appropriate place in the awakeme skip list.
 *
 * processJobs() takes care of handling the part of the awakeme list which
 * has an awakeme time <= to the current time. As a result of processing a
 * job, we expect it to likely be updated to be processed in the future
 * again, or deleted at all. */

/* Ask the system to update the time the job will be called again as an
 * argument of awakeJob() in order to handle delayed tasks for this job.
 * If 'at' is zero, the function computes the next time we should check
 * the job status based on the next quee time (qtime), expire time, garbage
 * collection if it's an ACK, and so forth.
 *
 * Otherwise if 'at' is non-zero, it's up to the caller to set the time
 * at which the job will be awake again. */
void updateJobAwakeTime(job *j, mstime_t at) {
    if (at == 0) {
        /* Best case is to handle it for eviction. One second more is added
         * in order to make sure when the job is processed we found it to
         * be already expired. */
        at = (mstime_t)j->etime*1000+1000;

        if (j->state == JOB_STATE_ACKED) {
            /* Try to garbage collect this ACKed job again in the future. */
            mstime_t retry_gc_again = getNextGCRetryTime(j);
            if (retry_gc_again < at) at = retry_gc_again;
        } else if ((j->state == JOB_STATE_ACTIVE ||
                    j->state == JOB_STATE_QUEUED) && j->qtime) {
            /* Schedule the job to be queued, and if the job is flagged
             * BCAST_WILLQUEUE, make sure to awake the job a bit earlier
             * to broadcast a WILLQUEUE message. */
            mstime_t qtime = j->qtime;
            if (j->flags & JOB_FLAG_BCAST_WILLQUEUE)
                qtime -= JOB_WILLQUEUE_ADVANCE;
            if (qtime < at) at = qtime;
        }
    }

    /* Only update the job position into the skiplist if needed. */
    if (at != j->awakeme) {
        /* Remove from skip list. */
        if (j->awakeme) {
            serverAssert(skiplistDelete(server.awakeme,j));
        }
        /* Insert it back again in the skip list with the new awake time. */
        j->awakeme = at;
        skiplistInsert(server.awakeme,j);
    }
}

/* Set the specified unix time at which a job will be queued again
 * in the local node. */
void updateJobRequeueTime(job *j, mstime_t qtime) {
    /* Don't violate at-most-once (retry == 0) contract in case of bugs. */
    if (j->retry == 0 || j->qtime == 0) return;
    j->qtime = qtime;
    updateJobAwakeTime(j,0);
}

/* Job comparision inside the awakeme skiplist: by awakeme time. If it is the
 * same jobs are compared by ctime. If the same again, by job ID. */
int skiplistCompareJobsToAwake(const void *a, const void *b) {
    const job *ja = a, *jb = b;

    if (ja->awakeme > jb->awakeme) return 1;
    if (jb->awakeme > ja->awakeme) return -1;
    if (ja->ctime > jb->ctime) return 1;
    if (jb->ctime > ja->ctime) return -1;
    return memcmp(ja->id,jb->id,JOB_ID_LEN);
}

/* Used to show jobs info for debugging or under unexpected conditions. */
void logJobsDebugInfo(int level, char *msg, job *j) {
    serverLog(level,
        "%s %.*s: state=%d retry=%d delay=%d replicate=%d flags=%d now=%lld cached_now=%lld awake=%lld (%lld) qtime=%lld etime=%lld",
        msg,
        JOB_ID_LEN, j->id,
        (int)j->state,
        (int)j->retry,
        (int)j->delay,
        (int)j->repl,
        (int)j->flags,
        (long long)mstime(),
        (long long)server.mstime,
        (long long)j->awakeme-mstime(),
        (long long)j->awakeme,
        (long long)j->qtime-mstime(),
        (long long)j->etime*1000-mstime()
        );
}

/* Process the specified job to perform asynchronous operations on it.
 * Check processJobs() for more info. */
void processJob(job *j) {
    mstime_t old_awakeme = j->awakeme;

    logJobsDebugInfo(LL_VERBOSE,"PROCESSING",j);

    /* Remove expired jobs. */
    if (j->etime <= server.unixtime) {
        serverLog(LL_VERBOSE,"EVICT %.*s", JOB_ID_LEN, j->id);
        unregisterJob(j);
        freeJob(j);
        return;
    }

    /* Broadcast WILLQUEUE to inform other nodes we are going to re-queue
     * the job shortly. */
    if ((j->state == JOB_STATE_ACTIVE ||
         j->state == JOB_STATE_QUEUED) &&
         j->flags & JOB_FLAG_BCAST_WILLQUEUE &&
         j->qtime-JOB_WILLQUEUE_ADVANCE <= server.mstime)
    {
        if (j->state != JOB_STATE_QUEUED) clusterSendWillQueue(j);
        /* Clear the WILLQUEUE flag, so that the job will be rescheduled
         * for when we need to queue it (otherwise it is scheduled
         * JOB_WILLQUEUE_ADVANCE milliseconds before). */
        j->flags &= ~JOB_FLAG_BCAST_WILLQUEUE;
        updateJobAwakeTime(j,0);
    }

    /* Requeue job if needed. This will also care about putting the job
     * into the queue for the first time for delayed jobs, including the
     * ones with retry=0. */
    if (j->state == JOB_STATE_ACTIVE && j->qtime <= server.mstime) {
        queue *q;

        /* We need to check if the queue is paused in input. If that's
         * the case, we do:
         *
         * If retry != 0, postpone the enqueue-time of "retry" time.
         *
         * If retry == 0 (at most once job), this is a job with a delay that
         * will never be queued again, and we are the only owner.
         * In such a case, put it into the queue, or the job will be leaked. */
        if (j->retry != 0 &&
            (q = lookupQueue(j->queue)) != NULL &&
            q->flags & QUEUE_FLAG_PAUSED_IN)
        {
            updateJobRequeueTime(j,server.mstime+
                                 j->retry*1000+
                                 randomTimeError(DISQUE_TIME_ERR));
        } else {
            enqueueJob(j,0);
        }
    }

    /* Update job re-queue time if job is already queued. */
    if (j->state == JOB_STATE_QUEUED && j->qtime <= server.mstime &&
        j->retry)
    {
        j->flags |= JOB_FLAG_BCAST_WILLQUEUE;
        j->qtime = server.mstime +
                   j->retry*1000 +
                   randomTimeError(DISQUE_TIME_ERR);
        updateJobAwakeTime(j,0);
    }

    /* Try a job garbage collection. */
    if (j->state == JOB_STATE_ACKED) {
        tryJobGC(j);
        updateJobAwakeTime(j,0);
    }

    if (old_awakeme == j->awakeme)
        logJobsDebugInfo(LL_WARNING, "~~~WARNING~~~ NOT PROCESSABLE JOB", j);
}

int processJobs(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int period = 100; /* 100 ms default period. */
    int max = 10000; /* 10k jobs * 1000 milliseconds = 10M jobs/sec max. */
    mstime_t now = mstime(), latency;
    skiplistNode *current, *next;
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

#ifdef DEBUG_SCHEDULER
    static time_t last_log = 0;
    int canlog = 0;
    if (server.port == 25000 && time(NULL) != last_log) {
        last_log = time(NULL);
        canlog = 1;
    }

    if (canlog) printf("--- LEN: %d ---\n",
        (int) skiplistLength(server.awakeme));
#endif

    latencyStartMonitor(latency);
    server.mstime = now; /* Update it since it's used by processJob(). */
    current = server.awakeme->header->level[0].forward;
    while(current && max--) {
        job *j = current->obj;

#ifdef DEBUG_SCHEDULER
        if (canlog) {
            printf("%.*s %d (in %d) [%s]\n",
                JOB_ID_LEN, j->id,
                (int) j->awakeme,
                (int) (j->awakeme-server.mstime),
                jobStateToString(j->state));
        }
#endif

        if (j->awakeme > now) break;
        next = current->level[0].forward;
        processJob(j);
        current = next;
    }

    /* Try to block between 1 and 100 millseconds depending on how near
     * in time is the next async event to process. Note that because of
     * received commands or change in state jobs state may be modified so
     * we set a max time of 100 milliseconds to wakeup anyway. */
    current = server.awakeme->header->level[0].forward;
    if (current) {
        job *j = current->obj;
        period = server.mstime-j->awakeme;
        if (period < 1) period = 1;
        else if (period > 100) period = 100;
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("jobs-processing",latency);
#ifdef DEBUG_SCHEDULER
    if (canlog) printf("---\n\n");
#endif
    return period;
}

/* ---------------------------  Jobs serialization -------------------------- */

/* Serialize an SDS string as a little endian 32 bit count followed
 * by the bytes representing the string. The serialized string is
 * written to the memory pointed by 'p'. The return value of the function
 * is the original 'p' advanced of 4 + sdslen(s) bytes, in order to
 * be ready to store the next value to serialize. */
char *serializeSdsString(char *p, sds s) {
    size_t len = s ? sdslen(s) : 0;
    uint32_t count = intrev32ifbe(len);

    memcpy(p,&count,sizeof(count));
    if (s) memcpy(p+sizeof(count),s,len);
    return p + sizeof(count) + len;
}

/* Serialize the job pointed by 'j' appending the serialized version of
 * the job into the passed SDS string 'jobs'.
 *
 * The serialization may be performed in two slightly different ways
 * depending on the 'type' argument:
 *
 * If type is SER_MESSAGE the expire time field is serialized using
 * the relative TTL still remaining for the job. This serialization format
 * is suitable for sending messages to other nodes that may have non
 * synchronized clocks. If instead SER_STORAGE is used as type, the expire
 * time filed is serialized using an absolute unix time (as it is normally
 * in the job structure representation). This makes the job suitable to be
 * loaded at a latter time from disk, and is used in order to emit
 * LOADJOB commands in the AOF file.
 *
 * Moreover if SER_MESSAGE is used, the JOB_FLAG_DELIVERED is cleared before
 * the serialization, since this is a local node flag and should not be
 * propagated.
 *
 * When the job is deserialized with deserializeJob() function call, the
 * appropriate type must be passed, depending on how the job was serialized.
 *
 * Serialization format
 * ---------------------
 *
 * len | struct | queuename | job | nodes
 *
 * len: The first 4 bytes are a little endian 32 bit unsigned
 * integer that announces the full size of the serialized job.
 *
 * struct: JOB_STRUCT_SER_LEN bytes of the 'job' structure
 * with fields fixed to be little endian regardless of the arch of the
 * system.
 *
 * queuename: uint32_t little endian len + actual bytes of the queue
 * name string.
 *
 * job: uint32_t little endian len + actual bytes of the job body.
 *
 * nodes: List of nodes that may have a copy of the message. uint32_t
 * little endian with the count of N node names followig. Then N
 * fixed lenght node names of CLUSTER_NODE_NAMELEN characters each.
 *
 * The message is concatenated to the existing sds string 'jobs'.
 * Just use sdsempty() as first argument to get a single job serialized.
 *
 * ----------------------------------------------------------------------
 *
 * Since each job has a prefixed length it is possible to glue multiple
 * jobs one after the other in a single string. */
sds serializeJob(sds jobs, job *j, int sertype) {
    size_t len;
    struct job *sj;
    char *p, *msg;
    uint32_t count;

    /* Compute the total length of the serialized job. */
    len = 4;                    /* Prefixed length of the serialized bytes. */
    len += JOB_STRUCT_SER_LEN;  /* Structure header directly serializable. */
    len += 4;                   /* Queue name length field. */
    len += j->queue ? sdslen(j->queue->ptr) : 0; /* Queue name bytes. */
    len += 4;                   /* Body length field. */
    len += j->body ? sdslen(j->body) : 0; /* Body bytes. */
    len += 4;                   /* Node IDs (that may have a copy) count. */
    len += dictSize(j->nodes_delivered) * CLUSTER_NAMELEN;

    /* Make room at the end of the SDS buffer to hold our message. */
    jobs = sdsMakeRoomFor(jobs,len);
    msg = jobs + sdslen(jobs); /* Concatenate to the end of buffer. */
    sdsIncrLen(jobs,len); /* Adjust SDS string final length. */

    /* Total serialized length prefix, not including the length itself. */
    count = intrev32ifbe(len-4);
    memcpy(msg,&count,sizeof(count));

    /* The serializable part of the job structure is copied, and fields
     * fixed to be little endian (no op in little endian CPUs). */
    sj = (job*) (msg+4);
    memcpy(sj,j,JOB_STRUCT_SER_LEN);
    memrev16ifbe(&sj->repl);
    memrev64ifbe(&sj->ctime);
    /* Use a relative expire time for serialization, but only for the
     * type SER_MESSAGE. When we want to target storage, it's better to use
     * absolute times in every field. */
    if (sertype == SER_MESSAGE) {
        if (sj->etime >= server.unixtime)
            sj->etime = sj->etime - server.unixtime + 1;
        else
            sj->etime = 1;
        sj->flags &= ~JOB_FLAG_DELIVERED;
    }
    memrev32ifbe(&sj->etime);
    memrev32ifbe(&sj->delay);
    memrev32ifbe(&sj->retry);
    memrev16ifbe(&sj->num_nacks);
    memrev16ifbe(&sj->num_deliv);

    /* p now points to the start of the variable part of the serialization. */
    p = msg + 4 + JOB_STRUCT_SER_LEN;

    /* Queue name is 4 bytes prefixed len in little endian + actual bytes. */
    p = serializeSdsString(p,j->queue ? j->queue->ptr : NULL);

    /* Body is 4 bytes prefixed len in little endian + actual bytes. */
    p = serializeSdsString(p,j->body);

    /* Node IDs that may have a copy of the message: 4 bytes count in little
     * endian plus (count * CLUSTER_NAMELEN) bytes. */
    count = intrev32ifbe(dictSize(j->nodes_delivered));
    memcpy(p,&count,sizeof(count));
    p += sizeof(count);

    dictIterator *di = dictGetSafeIterator(j->nodes_delivered);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        memcpy(p,node->name,CLUSTER_NAMELEN);
        p += CLUSTER_NAMELEN;
    }
    dictReleaseIterator(di);

    /* Make sure we wrote exactly the intented number of bytes. */
    serverAssert(len == (size_t)(p-msg));
    return jobs;
}

/* Deserialize a job serialized with serializeJob. Note that this only
 * deserializes the first job even if the input buffer contains multiple
 * jobs, but it stores the pointer to the next job (if any) into
 * '*next'. If there are no more jobs, '*next' is set to NULL.
 * '*next' is not updated if 'next' is a NULL pointer.
 *
 * The return value is the job structure populated with all the fields
 * present in the serialized structure. On deserialization error (wrong
 * format) NULL is returned.
 *
 * Arguments: 'p' is the pointer to the start of the job (the 4 bytes
 * where the job serialized length is stored). While 'len' is the total
 * number of bytes the buffer contains (that may be larger than the
 * serialized job 'p' is pointing to).
 *
 * The 'sertype' field specifies the serialization type the job was
 * serialized with, by serializeJob() call.
 *
 * When the serialization type is SER_STORAGE, the job state is loaded
 * as it is, otherwise when SER_MESSAGE is used, the job state is set
 * to JOB_STATE_ACTIVE.
 *
 * In both cases the gc retry field is reset to 0. */
job *deserializeJob(unsigned char *p, size_t len, unsigned char **next, int sertype) {
    job *j = zcalloc(sizeof(*j));
    unsigned char *start = p; /* To check total processed bytes later. */
    uint32_t joblen, aux;

    /* Min len is: 4 (joblen) + JOB_STRUCT_SER_LEN + 4 (queue name len) +
     * 4 (body len) + 4 (Node IDs count) */
    if (len < 4+JOB_STRUCT_SER_LEN+4+4+4) goto fmterr;

    /* Get total length. */
    memcpy(&joblen,p,sizeof(joblen));
    p += sizeof(joblen);
    len -= sizeof(joblen);
    joblen = intrev32ifbe(joblen);
    if (len < joblen) goto fmterr;

    /* Deserialize the static part just copying and fixing endianess. */
    memcpy(j,p,JOB_STRUCT_SER_LEN);
    memrev16ifbe(j->repl);
    memrev64ifbe(j->ctime);
    memrev32ifbe(j->etime);
    if (sertype == SER_MESSAGE) {
        /* Convert back to absolute time if needed. */
        j->etime = server.unixtime + j->etime;
    }
    memrev32ifbe(j->delay);
    memrev32ifbe(j->retry);
    memrev16ifbe(&sj->num_nacks);
    memrev16ifbe(&sj->num_deliv);
    p += JOB_STRUCT_SER_LEN;
    len -= JOB_STRUCT_SER_LEN;

    /* GC attempts are always reset, while the state will be likely set to
     * the caller, but otherwise, we assume the job is active if this message
     * is received from another node. When loading a message from disk instead
     * (SER_STORAGE serializaiton type), the state is left untouched. */
    if (sertype == SER_MESSAGE) j->state = JOB_STATE_ACTIVE;
    j->gc_retry = 0;

    /* Compute next queue time from known parameters. */
    if (j->retry) {
        j->flags |= JOB_FLAG_BCAST_WILLQUEUE;
        j->qtime = server.mstime +
                   j->delay*1000 +
                   j->retry*1000 +
                   randomTimeError(DISQUE_TIME_ERR);
    } else {
        j->qtime = 0;
    }

    /* Queue name. */
    memcpy(&aux,p,sizeof(aux));
    p += sizeof(aux);
    len -= sizeof(aux);
    aux = intrev32ifbe(aux);

    if (len < aux) goto fmterr;
    j->queue = createStringObject((char*)p,aux);
    p += aux;
    len -= aux;

    /* Job body. */
    memcpy(&aux,p,sizeof(aux));
    p += sizeof(aux);
    len -= sizeof(aux);
    aux = intrev32ifbe(aux);

    if (len < aux) goto fmterr;
    j->body = sdsnewlen(p,aux);
    p += aux;
    len -= aux;

    /* Nodes IDs. */
    memcpy(&aux,p,sizeof(aux));
    p += sizeof(aux);
    len -= sizeof(aux);
    aux = intrev32ifbe(aux);

    if (len < aux*CLUSTER_NAMELEN) goto fmterr;
    j->nodes_delivered = dictCreate(&clusterNodesDictType,NULL);
    while(aux--) {
        clusterNode *node = clusterLookupNode((char*)p);
        if (node) dictAdd(j->nodes_delivered,node->name,node);
        p += CLUSTER_NAMELEN;
        len -= CLUSTER_NAMELEN;
    }

    if ((uint32_t)(p-start)-sizeof(joblen) != joblen) goto fmterr;
    if (len && next) *next = p;
    return j;

fmterr:
    freeJob(j);
    return NULL;
}

/* This function is called when the job id at 'j' may be duplicated and we
 * likely already have the job, but we want to update the list of nodes
 * that may have the message by taking the union of our list with the
 * job 'j' list. */
void updateJobNodes(job *j) {
    job *old = lookupJob(j->id);
    if (!old) return;

    dictIterator *di = dictGetIterator(j->nodes_delivered);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        dictAdd(old->nodes_delivered,node->name,node);
    }
    dictReleaseIterator(di);
}

/* -------------------------  Jobs cluster functions ------------------------ */

/* This function sends a DELJOB message to all the nodes that may have
 * a copy of the job, in order to trigger deletion of the job.
 * It is used when an ADDJOB command time out to unregister (in a best
 * effort way, without gurantees) the job, and in the ACKs grabage
 * collection procedure.
 *
 * This function also unregisters and releases the job from the local
 * node.
 *
 * The function is best effort, and does not need to *guarantee* that the
 * specific property that after it gets called, no copy of the job is found
 * on the cluster. It just attempts to avoid useless multiple deliveries,
 * and to free memory of jobs that are already processed or that were never
 * confirmed to the producer.
 */
void deleteJobFromCluster(job *j) {
    clusterBroadcastDelJob(j);
    unregisterJob(j);
    freeJob(j);
}

/* ----------------------------  Utility functions -------------------------- */

/* Validate a set of job IDs. Return C_OK if all the IDs are valid,
 * otherwise C_ERR is returned.
 *
 * When C_ERR is returned, an error is send to the client 'c' if not
 * NULL. */
int validateJobIDs(client *c, robj **ids, int count) {
    int j;

    /* Mass-validate the Job IDs, so if we have to stop with an error, nothing
     * at all is processed. */
    for (j = 0; j < count; j++) {
        if (validateJobIdOrReply(c,ids[j]->ptr,sdslen(ids[j]->ptr))
            == C_ERR) return C_ERR;
    }
    return C_OK;
}

/* ----------------------------------  AOF ---------------------------------- */

/* Emit a LOADJOB command into the AOF. which is used explicitly to load
 * serialized jobs form disk: LOADJOB <serialize-job-string>. */
void AOFLoadJob(job *job) {
    if (server.aof_state == AOF_OFF) return;

    sds serialized = serializeJob(sdsempty(),job,SER_STORAGE);
    robj *serobj = createObject(OBJ_STRING,serialized);
    robj *argv[2] = {shared.loadjob, serobj};
    feedAppendOnlyFile(argv,2);
    decrRefCount(serobj);
}

/* Emit a DELJOB command into the AOF. This function is called in the following
 * two cases:
 *
 * 1) As a side effect of the job being acknowledged, when AOFAckJob()
 *    is called.
 * 2) When the server evicts a job from memory, but only if the state is one
 *    of active or queued. Yet not replicated jobs are not written into the
 *    AOF so there is no need to send a DELJOB, while already acknowledged
 *    jobs are handled by point "1". */
void AOFDelJob(job *job) {
    if (server.aof_state == AOF_OFF) return;

    robj *jobid = createStringObject(job->id,JOB_ID_LEN);
    robj *argv[2] = {shared.deljob, jobid};
    feedAppendOnlyFile(argv,2);
    decrRefCount(jobid);
}

/* Emit a DELJOB command, since ths is how we handle acknowledged jobs from
 * the point of view of AOF. We are not interested in loading back acknowledged
 * jobs, nor we include them on AOF rewrites, since ACKs garbage collection
 * works anyway if nodes forget about ACKs and dropping ACKs is not a safety
 * violation, it may just result into multiple deliveries of the same
 * message.
 *
 * However we keep the API separated, so it will be simple if we change our
 * mind or we want to have a feature to persist ACKs. */
void AOFAckJob(job *job) {
    if (server.aof_state == AOF_OFF) return;
    AOFDelJob(job);
}

/* The LOADJOB command is emitted in the AOF to load serialized jobs at
 * restart, and is only processed while loading AOFs. Clients calling this
 * command get an error. */
void loadjobCommand(client *c) {
    if (!(c->flags & CLIENT_AOF_CLIENT)) {
        addReplyError(c,"LOADJOB is a special command only processed from AOF");
        return;
    }
    job *job = deserializeJob(c->argv[1]->ptr,sdslen(c->argv[1]->ptr),NULL,SER_STORAGE);

    /* We expect to be able to read back what we serialized. */
    if (job == NULL) {
        serverLog(LL_WARNING,
            "Unrecoverable error loading AOF: corrupted LOADJOB data.");
        exit(1);
    }

    int enqueue_job = 0;
    if (job->state == JOB_STATE_QUEUED) {
        if (server.aof_enqueue_jobs_once) enqueue_job = 1;
        job->state = JOB_STATE_ACTIVE;
    }

    /* Check if the job expired before registering it. */
    if (job->etime <= server.unixtime) {
        freeJob(job);
        return;
    }

    /* Register the job, and if needed enqueue it: we put jobs back into
     * queues only if enqueue-jobs-at-next-restart option is set, that is,
     * when a controlled restart happens. */
    if (registerJob(job) == C_OK && enqueue_job)
        enqueueJob(job,0);
}

/* --------------------------  Jobs related commands ------------------------ */

/* This is called by unblockClient() to perform the cleanup of a client
 * blocked by ADDJOB. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingJobRepl(client *c) {
    /* If the job is still waiting for synchronous replication, but the client
     * waiting it gets freed or reaches the timeout, we unblock the client and
     * forget about the job. */
    if (c->bpop.job->state == JOB_STATE_WAIT_REPL) {
        /* Set the job as active before calling deleteJobFromCluster() since
         * otherwise unregistering the job will, in turn, unblock the client,
         * which we are already doing here. */
        c->bpop.job->state = JOB_STATE_ACTIVE;
        deleteJobFromCluster(c->bpop.job);
    }
    c->bpop.job = NULL;
}

/* Return a simple string reply with the Job ID. */
void addReplyJobID(client *c, job *j) {
    addReplyStatusLength(c,j->id,JOB_ID_LEN);
}

/* This function is called by cluster.c when the job was replicated
 * and the replication acknowledged at least job->repl times.
 *
 * Here we need to queue the job, and unblock the client waiting for the job
 * if it still exists.
 *
 * This function is only called if the job is in JOB_STATE_WAIT_REPL.
 * The functionc an also assume that there is a client waiting to be
 * unblocked if this function is called, since if the blocked client is
 * released, the job is deleted (and a best effort try is made to remove
 * copies from other nodes), to avoid non acknowledged jobs to be active
 * when possible.
 *
 * Return value: if the job is retained after the function is called
 * (normal replication) then C_OK is returned. Otherwise if the
 * function removes the job from the node, since the job is externally
 * replicated, C_ERR is returned, in order to signal the client further
 * accesses to the job are not allowed. */
int jobReplicationAchieved(job *j) {
    serverLog(LL_VERBOSE,"Replication ACHIEVED %.*s",JOB_ID_LEN,j->id);

    /* Change the job state to active. This is critical to avoid the job
     * will be freed by unblockClient() if found still in the old state. */
    j->state = JOB_STATE_ACTIVE;

    /* Reply to the blocked client with the Job ID and unblock the client. */
    client *c = jobGetAssociatedValue(j);
    setJobAssociatedValue(j,NULL);
    addReplyJobID(c,j);
    unblockClient(c);

    /* If the job was externally replicated, send a QUEUE message to one of
     * the nodes that acknowledged to have a copy, and forget about it ASAP. */
    if (dictFind(j->nodes_delivered,myself->name) == NULL) {
        dictEntry *de = dictGetRandomKey(j->nodes_confirmed);
        if (de) {
            clusterNode *n = dictGetVal(de);
            clusterSendEnqueue(n,j,j->delay);
        }
        unregisterJob(j);
        freeJob(j);
        return C_ERR;
    }

    /* If set, cleanup nodes_confirmed to free memory. We'll reuse this
     * hash table again for ACKs tracking in order to garbage collect the
     * job once processed. */
    if (j->nodes_confirmed) {
        dictRelease(j->nodes_confirmed);
        j->nodes_confirmed = NULL;
    }

    /* Queue the job locally. */
    if (j->delay == 0)
        enqueueJob(j,0); /* Will change the job state. */
    else
        updateJobAwakeTime(j,0); /* Queue with delay. */

    AOFLoadJob(j);
    return C_OK;
}

/* This function is called periodically by clientsCron(). Its goal is to
 * check if a client blocked waiting for a job synchronous replication
 * is taking too time, and add a new node to the set of nodes contacted
 * in order to replicate the job. This way some of the nodes initially
 * contacted are not reachable, are slow, or are out of memory (and are
 * not accepting our job), we have a chance to make the ADDJOB call
 * succeed using other nodes.
 *
 * The function always returns 0 since it never terminates the client. */
#define DELAYED_JOB_ADD_NODE_MIN_PERIOD 50 /* 50 milliseconds. */
int clientsCronHandleDelayedJobReplication(client *c) {
    /* Return ASAP if this client is not blocked for job replication. */
    if (!(c->flags & CLIENT_BLOCKED) || c->btype != BLOCKED_JOB_REPL)
        return 0;

    /* Note that clientsCronHandleDelayedJobReplication() is called after
     * refreshing server.mstime, so no need to call mstime() again here,
     * we can use the cached value. However we use a fresh timestamp if
     * we have to set added_node_time again. */
    mstime_t elapsed = server.mstime - c->bpop.added_node_time;
    if (elapsed >= DELAYED_JOB_ADD_NODE_MIN_PERIOD) {
        if (clusterReplicateJob(c->bpop.job, 1, 0) > 0)
            c->bpop.added_node_time = mstime();
    }
    return 0;
}

/* ADDJOB queue job timeout [REPLICATE <n>] [TTL <sec>] [RETRY <sec>] [ASYNC]
 *
 * The function changes replication strategy if the memory warning level
 * is greater than zero.
 *
 * When there is no memory pressure:
 * 1) A copy of the job is replicated locally.
 * 2) The job is queued locally.
 * 3) W-1 copies of the job are replicated to other nodes, synchronously
 *    or asynchronously if ASYNC is provided.
 *
 * When there is memory pressure:
 * 1) The job is replicated only to W external nodes.
 * 2) The job is queued to a random external node sending a QUEUE message.
 * 3) QUEUE is sent ASAP for asynchronous jobs, for synchronous jobs instead
 *    QUEUE is sent by jobReplicationAchieved to one of the nodes that
 *    acknowledged to have a copy of the job.
 * 4) The job is discareded by the local node ASAP, that is, when the
 *    selected replication level is achieved or before to returning to
 *    the caller for asynchronous jobs. */
void addjobCommand(client *c) {
    long long replicate = server.cluster->size > 3 ? 3 : server.cluster->size;
    long long ttl = 3600*24;
    long long retry = -1;
    long long delay = 0;
    long long maxlen = 0; /* Max queue length for job to be accepted. */
    mstime_t timeout;
    int j, retval;
    int async = 0;  /* Asynchronous request? */
    int extrepl = getMemoryWarningLevel() > 0; /* Replicate externally? */
    int leaving = myselfLeaving();
    static uint64_t prev_ctime = 0;

    /* Another case for external replication, other than memory pressure, is
     * if this node is leaving the cluster. In this case we don't want to create
     * new messages here. */
    if (leaving) extrepl = 1;

    /* Parse args. */
    for (j = 4; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int lastarg = j == c->argc-1;
        if (!strcasecmp(opt,"replicate") && !lastarg) {
            retval = getLongLongFromObject(c->argv[j+1],&replicate);
            if (retval != C_OK || replicate <= 0 || replicate > 65535) {
                addReplyError(c,"REPLICATE must be between 1 and 65535");
                return;
            }
            j++;
        } else if (!strcasecmp(opt,"ttl") && !lastarg) {
            retval = getLongLongFromObject(c->argv[j+1],&ttl);
            if (retval != C_OK || ttl <= 0) {
                addReplyError(c,"TTL must be a number > 0");
                return;
            }
            j++;
        } else if (!strcasecmp(opt,"retry") && !lastarg) {
            retval = getLongLongFromObject(c->argv[j+1],&retry);
            if (retval != C_OK || retry < 0) {
                addReplyError(c,"RETRY time must be a non negative number");
                return;
            }
            j++;
        } else if (!strcasecmp(opt,"delay") && !lastarg) {
            retval = getLongLongFromObject(c->argv[j+1],&delay);
            if (retval != C_OK || delay < 0) {
                addReplyError(c,"DELAY time must be a non negative number");
                return;
            }
            j++;
        } else if (!strcasecmp(opt,"maxlen") && !lastarg) {
            retval = getLongLongFromObject(c->argv[j+1],&maxlen);
            if (retval != C_OK || maxlen <= 0) {
                addReplyError(c,"MAXLEN must be a positive number");
                return;
            }
            j++;
        } else if (!strcasecmp(opt,"async")) {
            async = 1;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    /* Parse the timeout argument. */
    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout,UNIT_MILLISECONDS)
        != C_OK) return;

    /* REPLICATE > 1 and RETRY set to 0 does not make sense, why to replicate
     * the job if it will never try to be re-queued if case the job processing
     * is not acknowledged? */
    if (replicate > 1 && retry == 0) {
        addReplyError(c,"With RETRY set to 0 please explicitly set  "
                        "REPLICATE to 1 (at-most-once delivery)");
        return;
    }

    /* DELAY greater or equal to TTL is silly. */
    if (delay >= ttl) {
        addReplyError(c,"The specified DELAY is greater than TTL. Job refused "
                        "since would never be delivered");
        return;
    }

    /* When retry is not specified, it defaults to 1/10 of the TTL, with
     * an hard limit of JOB_DEFAULT_RETRY_MAX seconds (5 minutes normally). */
    if (retry == -1) {
        retry = ttl/10;
        if (retry > JOB_DEFAULT_RETRY_MAX) retry = JOB_DEFAULT_RETRY_MAX;
        if (retry == 0) retry = 1;
    }

    /* Check if REPLICATE can't be honoured at all. */
    int additional_nodes = extrepl ? replicate : replicate-1;

    if (additional_nodes > server.cluster->reachable_nodes_count) {
        if (extrepl &&
            additional_nodes-1 == server.cluster->reachable_nodes_count)
        {
            addReplySds(c,
                sdscatprintf(sdsempty(),
                       "-NOREPL Not enough reachable nodes "
                       "for the requested replication level, since I'm unable "
                       "to hold a copy of the message for the following "
                       "reason: %s\r\n",
                       leaving ? "I'm leaving the cluster" :
                                 "I'm out of memory"));
        } else {
            addReplySds(c,
                sdsnew("-NOREPL Not enough reachable nodes "
                       "for the requested replication level\r\n"));
        }
        return;
    }

    /* Lookup the queue by the name, in order to perform checks for
     * MAXLEN and to check for paused queue. */
    queue *q = lookupQueue(c->argv[1]);

    /* If maxlen was specified, check that the local queue len is
     * within the requested limits. */
    if (maxlen && q && queueLength(q) >= (unsigned long) maxlen) {
        addReplySds(c,
            sdsnew("-MAXLEN Queue is already longer than "
                   "the specified MAXLEN count\r\n"));
        return;
    }

    /* If the queue is paused in input, refuse the job. */
    if (q && q->flags & QUEUE_FLAG_PAUSED_IN) {
        addReplySds(c,
            sdsnew("-PAUSED Queue paused in input, try later\r\n"));
        return;
    }

    /* Are we going to discard the local copy before to return to the caller?
     * This happens when the job is at the same type asynchronously
     * replicated AND because of memory warning level we are going to
     * replicate externally without taking a copy. */
    int discard_local_copy = async && extrepl;

    /* Create a new job. */
    job *job = createJob(NULL,JOB_STATE_WAIT_REPL,ttl,retry);
    job->queue = c->argv[1];
    incrRefCount(c->argv[1]);
    job->repl = replicate;

    /* If no external replication is used, add myself to the list of nodes
     * that have a copy of the job. */
    if (!extrepl)
        dictAdd(job->nodes_delivered,myself->name,myself);

    /* Job ctime is milliseconds * 1000000. Jobs created in the same
     * millisecond gets an incremental ctime. The ctime is used to sort
     * queues, so we have some weak sorting semantics for jobs: non-requeued
     * jobs are delivered roughly in the order they are added into a given
     * node. */
    job->ctime = mstime()*1000000;
    if (job->ctime <= prev_ctime) job->ctime = prev_ctime+1;
    prev_ctime = job->ctime;

    job->etime = server.unixtime + ttl;
    job->delay = delay;
    job->retry = retry;
    job->body = sdsdup(c->argv[2]->ptr);

    /* Set the next time the job will be queued. Note that once we call
     * enqueueJob() the first time, this will be set to 0 (never queue
     * again) for jobs that have a zero retry value (at most once jobs). */
    if (delay) {
        job->qtime = server.mstime + delay*1000;
    } else {
        /* This will be updated anyway by enqueueJob(). */
        job->qtime = server.mstime + retry*1000;
    }

    /* Register the job locally, unless we are going to remove it locally. */
    if (!discard_local_copy && registerJob(job) == C_ERR) {
        /* A job ID with the same name? Practically impossible but
         * let's handle it to trap possible bugs in a cleaner way. */
        serverLog(LL_WARNING,"ID already existing in ADDJOB command!");
        freeJob(job);
        addReplyError(c,"Internal error creating the job, check server logs");
        return;
    }

    /* For replicated messages where ASYNC option was not asked, block
     * the client, and wait for acks. Otherwise if no synchronous replication
     * is used or if we don't have additional copies to deliver, we just queue
     * the job and return to the client ASAP.
     *
     * Note that for REPLICATE > 1 and ASYNC the replication process is
     * best effort. */
    if ((replicate > 1 || extrepl) && !async) {
        c->bpop.timeout = timeout;
        c->bpop.job = job;
        c->bpop.added_node_time = mstime();
        blockClient(c,BLOCKED_JOB_REPL);
        setJobAssociatedValue(job,c);
        /* Create the nodes_confirmed dictionary only if we actually need
         * it for synchronous replication. It will be released later
         * when we move away from JOB_STATE_WAIT_REPL. */
        job->nodes_confirmed = dictCreate(&clusterNodesDictType,NULL);
        /* Confirm itself as an acknowledged receiver if this node will
         * retain a copy of the job. */
        if (!extrepl) dictAdd(job->nodes_confirmed,myself->name,myself);
    } else {
        if (job->delay == 0) {
            if (!extrepl) enqueueJob(job,0); /* Will change the job state. */
        } else {
            /* Delayed jobs that don't wait for replication can move
             * forward to ACTIVE state ASAP, and get scheduled for
             * queueing. */
            job->state = JOB_STATE_ACTIVE;
            if (!discard_local_copy) updateJobAwakeTime(job,0);
        }
        addReplyJobID(c,job);
        if (!extrepl) AOFLoadJob(job);
    }

    /* If the replication factor is > 1, send REPLJOB messages to REPLICATE-1
     * nodes. */
    if (additional_nodes > 0)
        clusterReplicateJob(job, additional_nodes, async);

    /* If the job is asynchronously and externally replicated at the same time,
     * send an ENQUEUE message ASAP to one random node, and delete the job from
     * this node right now. */
    if (discard_local_copy) {
        dictEntry *de = dictGetRandomKey(job->nodes_delivered);
        if (de) {
            clusterNode *n = dictGetVal(de);
            clusterSendEnqueue(n,job,job->delay);
        }
        /* We don't have to unregister the job since we did not registered
         * it if it's async + extrepl. */
        freeJob(job);
    }
}

/* Client reply function for SHOW and JSCAN. */
void addReplyJobInfo(client *c, job *j) {
    addReplyMultiBulkLen(c,30);

    addReplyBulkCString(c,"id");
    addReplyBulkCBuffer(c,j->id,JOB_ID_LEN);

    addReplyBulkCString(c,"queue");
    if (j->queue)
        addReplyBulk(c,j->queue);
    else
        addReply(c,shared.nullbulk);

    addReplyBulkCString(c,"state");
    addReplyBulkCString(c,jobStateToString(j->state));

    addReplyBulkCString(c,"repl");
    addReplyLongLong(c,j->repl);

    int64_t ttl = j->etime - time(NULL);
    if (ttl < 0) ttl = 0;
    addReplyBulkCString(c,"ttl");
    addReplyLongLong(c,ttl);

    addReplyBulkCString(c,"ctime");
    addReplyLongLong(c,j->ctime);

    addReplyBulkCString(c,"delay");
    addReplyLongLong(c,j->delay);

    addReplyBulkCString(c,"retry");
    addReplyLongLong(c,j->retry);

    addReplyBulkCString(c,"nacks");
    addReplyLongLong(c,j->num_nacks);

    addReplyBulkCString(c,"additional-deliveries");
    addReplyLongLong(c,j->num_deliv);

    addReplyBulkCString(c,"nodes-delivered");
    if (j->nodes_delivered) {
        addReplyMultiBulkLen(c,dictSize(j->nodes_delivered));
        dictForeach(j->nodes_delivered,de)
            addReplyBulkCBuffer(c,dictGetKey(de),CLUSTER_NAMELEN);
        dictEndForeach
    } else {
        addReplyMultiBulkLen(c,0);
    }

    addReplyBulkCString(c,"nodes-confirmed");
    if (j->nodes_confirmed) {
        addReplyMultiBulkLen(c,dictSize(j->nodes_confirmed));
        dictForeach(j->nodes_confirmed,de)
            addReplyBulkCBuffer(c,dictGetKey(de),CLUSTER_NAMELEN);
        dictEndForeach
    } else {
        addReplyMultiBulkLen(c,0);
    }

    mstime_t next_requeue = j->qtime - mstime();
    if (next_requeue < 0) next_requeue = 0;
    addReplyBulkCString(c,"next-requeue-within");
    if (j->qtime == 0)
        addReply(c,shared.nullbulk);
    else
        addReplyLongLong(c,next_requeue);

    mstime_t next_awake = j->awakeme - mstime();
    if (next_awake < 0) next_awake = 0;
    addReplyBulkCString(c,"next-awake-within");
    if (j->awakeme == 0)
        addReply(c,shared.nullbulk);
    else
        addReplyLongLong(c,next_awake);

    addReplyBulkCString(c,"body");
    if (j->body)
        addReplyBulkCBuffer(c,j->body,sdslen(j->body));
    else
        addReply(c,shared.nullbulk);
}

/* SHOW <job-id> */
void showCommand(client *c) {
    if (validateJobIdOrReply(c,c->argv[1]->ptr,sdslen(c->argv[1]->ptr))
        == C_ERR) return;

    job *j = lookupJob(c->argv[1]->ptr);
    if (!j) {
        addReply(c,shared.nullbulk);
        return;
    }
    addReplyJobInfo(c,j);
}

/* DELJOB jobid_1 jobid_2 ... jobid_N
 *
 * Evict (and possibly remove from queue) all the jobs in memeory
 * matching the specified job IDs. Jobs are evicted whatever their state
 * is, since this command is mostly used inside the AOF or for debugging
 * purposes.
 *
 * The return value is the number of jobs evicted.
 */
void deljobCommand(client *c) {
    int j, evicted = 0;

    if (validateJobIDs(c,c->argv+1,c->argc-1) == C_ERR) return;

    /* Perform the appropriate action for each job. */
    for (j = 1; j < c->argc; j++) {
        job *job = lookupJob(c->argv[j]->ptr);
        if (job == NULL) continue;
        unregisterJob(job);
        freeJob(job);
        evicted++;
    }
    addReplyLongLong(c,evicted);
}

/* JSCAN [<cursor>] [COUNT <count>] [BUSYLOOP] [QUEUE <queue>]
 * [STATE <state1> STATE <state2> ... STATE <stateN>]
 * [REPLY all|id]
 *
 * The command provides an interface to iterate all the existing jobs in
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
 * QUEUE <queue>     -- Return only jobs in the specified queue.
 * STATE <state>     -- Return jobs in the specified state.
 *                      Can be used multiple times for a logic OR.
 * REPLY <type>      -- Job reply type. Default is to report just the job
 *                      ID. If "all" is specified the full job state is
 *                      returned like for the SHOW command.
 *
 * The cursor argument can be in any place, the first non matching option
 * that has valid cursor form of an usigned number will be sensed as a valid
 * cursor.
 */

/* JSCAN reply type. */
#define JSCAN_REPLY_ID 0        /* Just report the Job ID. */
#define JSCAN_REPLY_ALL 1       /* Reply full job info like SHOW. */

/* The structure to pass the filter options to the callback. */
struct jscanFilter {
    int state[16];  /* Every state to return is set to non-zero. */
    int numstates;  /* Number of states non-true. 0 = match all. */
    robj *queue;    /* Queue name or NULL to return any queue. */
};

/* Callback for the dictionary scan used by JSCAN. */
void jscanCallback(void *privdata, const dictEntry *de) {
    void **pd = (void**)privdata;
    list *list = pd[0];
    struct jscanFilter *filter = pd[1];
    job *job = dictGetKey(de);

    /* Skip dummy jobs created by ACK command when job ID is unknown. */
    if (dictSize(job->nodes_delivered) == 0) return;

    /* Don't add the item if it does not satisfies our filter. */
    if (filter->queue && !equalStringObjects(job->queue,filter->queue)) return;
    if (filter->numstates && !filter->state[job->state]) return;

    /* Otherwise put the queue into the list that will be returned to the
     * client later. */
    listAddNodeTail(list,job);
}

#define JSCAN_DEFAULT_COUNT 100
void jscanCommand(client *c) {
    struct jscanFilter filter;
    int busyloop = 0; /* If true return all the jobs in a blocking way. */
    long count = JSCAN_DEFAULT_COUNT;
    long maxiterations;
    unsigned long cursor = 0;
    int cursor_set = 0, j;
    int reply_type = JSCAN_REPLY_ID;

    memset(&filter,0,sizeof(filter));

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
        } else if (!strcasecmp(opt,"queue") && remaining) {
            filter.queue = c->argv[j+1];
            j++;
        } else if (!strcasecmp(opt,"state") && remaining) {
            int jobstate = jobStateFromString(c->argv[j+1]->ptr);
            if (jobstate == -1) {
                addReplyError(c,"Invalid job state name");
                return;
            }
            filter.state[jobstate] = 1;
            filter.numstates++;
            j++;
        } else if (!strcasecmp(opt,"reply") && remaining) {
            if (!strcasecmp(c->argv[j+1]->ptr,"id")) {
                reply_type = JSCAN_REPLY_ID;
            } else if (!strcasecmp(c->argv[j+1]->ptr,"all")) {
                reply_type = JSCAN_REPLY_ALL;
            } else {
                addReplyError(c,"Invalid REPLY type, try ID or ALL");
                return;
            }
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
        cursor = dictScan(server.jobs,cursor,jscanCallback,privdata);
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
        job *j = listNodeValue(node);
        if (reply_type == JSCAN_REPLY_ID) addReplyJobID(c,j);
        else if (reply_type == JSCAN_REPLY_ALL) addReplyJobInfo(c,j);
        else serverPanic("Unknown JSCAN reply type");
        listDelNode(list, node);
    }
    listRelease(list);
}

