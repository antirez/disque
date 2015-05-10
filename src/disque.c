/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "slowlog.h"
#include "bio.h"

#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <locale.h>

/* Our shared "common" objects */

struct sharedObjectsStruct shared;

/* Global vars that are actually used as constants. The following double
 * values are used for double on-disk serialization, and are initialized
 * at runtime to avoid strange compiler optimizations. */

double R_Zero, R_PosInf, R_NegInf, R_Nan;

/*================================= Globals ================================= */

/* Global vars */
struct disqueServer server; /* server global state */

/* Our command table.
 *
 * Every entry is composed of the following fields:
 *
 * name: a string representing the command name.
 * function: pointer to the C function implementing the command.
 * arity: number of arguments, it is possible to use -N to say >= N
 * sflags: command flags as string. See below for a table of flags.
 * flags: flags as bitmask. Computed by Disque using the 'sflags' field.
 * get_keys_proc: an optional function to get key arguments from a command.
 *                This is only used when the following three fields are not
 *                enough to specify what arguments are keys.
 * first_key_index: first argument that is a key
 * last_key_index: last argument that is a key
 * key_step: step to get all the keys from first to last argument. For instance
 *           in MSET the step is two since arguments are key,val,key,val,...
 * microseconds: microseconds of total execution time for this command.
 * calls: total number of calls of this command.
 *
 * The flags, microseconds and calls fields are computed by Disque and should
 * always be set to zero.
 *
 * Command flags are expressed using strings where every character represents
 * a flag. Later the populateCommandTable() function will take care of
 * populating the real 'flags' field using this characters.
 *
 * This is the meaning of the flags:
 *
 * w: write command (may modify jobs or queues state).
 * r: read command  (will never modify jobs or queues state).
 * m: may increase memory usage once called. Don't allow if out of memory.
 * a: admin command, like SAVE or SHUTDOWN.
 * l: Allow command while loading the database.
 * M: Do not automatically propagate the command on MONITOR.
 * F: Fast command: O(1) or O(log(N)) command that should never delay
 *    its execution as long as the kernel scheduler is giving us time.
 *    Note that commands that may trigger a DEL as a side effect (like SET)
 *    are not fast commands.
 */
struct serverCommand serverCommandTable[] = {
    /* Server commands. */
    {"auth",authCommand,2,"rlF",0,NULL,0,0,0,0,0},
    {"ping",pingCommand,-1,"rF",0,NULL,0,0,0,0,0},
    {"info",infoCommand,-1,"rl",0,NULL,0,0,0,0,0},
    {"shutdown",shutdownCommand,-1,"arl",0,NULL,0,0,0,0,0},
    {"monitor",monitorCommand,1,"ar",0,NULL,0,0,0,0,0},
    {"debug",debugCommand,-2,"a",0,NULL,0,0,0,0,0},
    {"config",configCommand,-2,"ar",0,NULL,0,0,0,0,0},
    {"cluster",clusterCommand,-2,"ar",0,NULL,0,0,0,0,0},
    {"client",clientCommand,-2,"ar",0,NULL,0,0,0,0,0},
    {"slowlog",slowlogCommand,-2,"r",0,NULL,0,0,0,0,0},
    {"time",timeCommand,1,"rF",0,NULL,0,0,0,0,0},
    {"command",commandCommand,0,"rl",0,NULL,0,0,0,0,0},
    {"latency",latencyCommand,-2,"arl",0,NULL,0,0,0,0,0},
    {"hello",helloCommand,1,"rF",0,NULL,0,0,0,0,0},

    /* Jobs */
    {"addjob",addjobCommand,-4,"wmF",0,NULL,0,0,0,0,0},
    {"getjob",getjobCommand,-2,"wF",0,NULL,0,0,0,0,0},
    {"ackjob",ackjobCommand,-1,"wF",0,NULL,0,0,0,0,0},
    {"fastack",fastackCommand,-1,"wF",0,NULL,0,0,0,0,0},
    {"deljob",deljobCommand,-1,"wF",0,NULL,0,0,0,0,0},
    {"show",showCommand,2,"rF",0,NULL,0,0,0,0,0},
    {"enqueue",enqueueCommand,-1,"mwF",0,NULL,0,0,0,0,0},
    {"dequeue",dequeueCommand,-1,"wF",0,NULL,0,0,0,0,0},

    /* AOF specific. */
    {"loadjob",loadjobCommand,2,"w",0,NULL,0,0,0,0,0},
    {"bgrewriteaof",bgrewriteaofCommand,1,"ar",0,NULL,0,0,0,0,0},

    /* Queues */
    {"qlen",qlenCommand,2,"rF",0,NULL,0,0,0,0,0},
    {"qpeek",qpeekCommand,3,"r",0,NULL,0,0,0,0,0},
    {"qscan",qscanCommand,-1,"r",0,NULL,0,0,0,0,0}
};

/*============================ Utility functions ============================ */

/* Low level logging. To use only for very big messages, otherwise
 * serverLog() is to prefer. */
void serverLogRaw(int level, const char *msg) {
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#";
    FILE *fp;
    char buf[64];
    int rawmode = (level & DISQUE_LOG_RAW);
    int log_to_stdout = server.logfile[0] == '\0';

    level &= 0xff; /* clear flags */
    if (level < server.verbosity) return;

    fp = log_to_stdout ? stdout : fopen(server.logfile,"a");
    if (!fp) return;

    if (rawmode) {
        fprintf(fp,"%s",msg);
    } else {
        int off;
        struct timeval tv;
        int role_char;
        pid_t pid = getpid();

        gettimeofday(&tv,NULL);
        off = strftime(buf,sizeof(buf),"%d %b %H:%M:%S.",localtime(&tv.tv_sec));
        snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
        if (pid != server.pid) {
            role_char = 'C'; /* AOF writing child. */
        } else {
            role_char = 'P'; /* Parent child. */
        }
        fprintf(fp,"%d:%c %s %c %s\n",
            (int)getpid(),role_char, buf,c[level],msg);
    }
    fflush(fp);

    if (!log_to_stdout) fclose(fp);
    if (server.syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
}

/* Like serverLogRaw() but with printf-alike support. This is the function that
 * is used across the code. The raw version is only used in order to dump
 * the INFO output on crash. */
void serverLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[DISQUE_MAX_LOGMSG_LEN];

    if ((level&0xff) < server.verbosity) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level,msg);
}

/* Log a fixed message without printf-alike capabilities, in a way that is
 * safe to call from a signal handler.
 *
 * We actually use this only for signals that are not fatal from the point
 * of view of Disque. Signals that are going to kill the server anyway and
 * where we need printf-alike features are served by serverLog(). */
void serverLogFromHandler(int level, const char *msg) {
    int fd;
    int log_to_stdout = server.logfile[0] == '\0';
    char buf[64];

    if ((level&0xff) < server.verbosity || (log_to_stdout && server.daemonize))
        return;
    fd = log_to_stdout ? STDOUT_FILENO :
                         open(server.logfile, O_APPEND|O_CREAT|O_WRONLY, 0644);
    if (fd == -1) return;
    ll2string(buf,sizeof(buf),getpid());
    if (write(fd,buf,strlen(buf)) == -1) goto err;
    if (write(fd,":signal-handler (",17) == -1) goto err;
    ll2string(buf,sizeof(buf),time(NULL));
    if (write(fd,buf,strlen(buf)) == -1) goto err;
    if (write(fd,") ",2) == -1) goto err;
    if (write(fd,msg,strlen(msg)) == -1) goto err;
    if (write(fd,"\n",1) == -1) goto err;
err:
    if (!log_to_stdout) close(fd);
}

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
mstime_t mstime(void) {
    return ustime()/1000;
}

/* Return a random time error between -(milliseconds/2) and +(milliseconds/2).
 * This is useful in order to desynchronize multiple nodes competing for the
 * same operation in a best-effort way. */
mstime_t randomTimeError(mstime_t milliseconds) {
    return rand()%milliseconds - milliseconds/2;
}

/* After an RDB dump or AOF rewrite we exit from children using _exit() instead of
 * exit(), because the latter may interact with the same file objects used by
 * the parent process. However if we are testing the coverage normal exit() is
 * used in order to obtain the right coverage information. */
void exitFromChild(int retcode) {
#ifdef COVERAGE_TEST
    exit(retcode);
#else
    _exit(retcode);
#endif
}

/*====================== Hash table type implementation  ==================== */

/* This is a hash table type that uses the SDS dynamic strings library as
 * keys and disque objects as values (objects can hold SDS strings,
 * lists, sets). */

void dictVanillaFree(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    zfree(val);
}

void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

void dictDisqueObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */
    decrRefCount(val);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

unsigned int dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

unsigned int dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

unsigned int dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == DISQUE_ENCODING_INT &&
        o2->encoding == DISQUE_ENCODING_INT)
            return o1->ptr == o2->ptr;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

unsigned int dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (sdsEncodedObject(o)) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == DISQUE_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            unsigned int hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Sets type hash table */
dictType setDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictDisqueObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

/* Cluster nodes hash table, mapping nodes addresses 1.2.3.4:7711 to
 * clusterNode structures. */
unsigned int dictClusterNodeHash(const void *key) {
    return dictGenHashFunction(key,DISQUE_CLUSTER_NAMELEN);
}

int dictClusterNodeKeyCompare(void *privdata, const void *key1,
                      const void *key2)
{
    DICT_NOTUSED(privdata);
    return memcmp(key1, key2, DISQUE_CLUSTER_NAMELEN) == 0;
}

dictType clusterNodesDictType = {
    dictClusterNodeHash,        /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictClusterNodeKeyCompare,  /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

/* Cluster re-addition blacklist. This maps node IDs to the time
 * we can re-add this node. The goal is to avoid readding a removed
 * node for some time. */
dictType clusterNodesBlackListDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

/* Jobs dictionary implementation. Here we use a trick in order to save
 * memory: the key of the dictionary is the job->id field itself, that
 * is the initial part of the value in the dictionary. */
unsigned int dictJobHash(const void *key) {
    return dictGenHashFunction(key,JOB_ID_LEN);
}

int dictJobKeyCompare(void *privdata, const void *key1,
                      const void *key2)
{
    DICT_NOTUSED(privdata);
    return memcmp(key1, key2, JOB_ID_LEN) == 0;
}

dictType jobsDictType = {
    dictJobHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictJobKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL                       /* val destructor */
};

/* Queues hash table type. */
dictType queuesDictType = {
    dictObjHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictObjKeyCompare,         /* key compare */
    dictDisqueObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size && used && size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < DISQUE_HT_MINFILL));
}

void tryResizeHashTables(void) {
    if (htNeedsResize(server.jobs)) dictResize(server.jobs);
    if (htNeedsResize(server.queues)) dictResize(server.queues);
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use 1 millisecond
 * of CPU time at every call of this function to perform some rehahsing.
 *
 * The function returns 1 if some rehashing was performed, otherwise 0
 * is returned. */
int incrementallyRehash(void) {
    int workdone = 0;

    if (dictIsRehashing(server.jobs)) {
        dictRehashMilliseconds(server.jobs,1);
        workdone = 1;
    }
    if (dictIsRehashing(server.queues)) {
        dictRehashMilliseconds(server.queues,1);
        workdone = 1;
    }
    return workdone;
}

/* This function is called once a background process of some kind terminates,
 * as we want to avoid resizing the hash tables when there is a child in order
 * to play well with copy-on-write (otherwise when a resize happens lots of
 * memory pages are copied). The goal of this function is to update the ability
 * for dict.c to resize the hash tables accordingly to the fact we have o not
 * running childs. */
void updateDictResizePolicy(void) {
    if (server.aof_child_pid == -1)
        dictEnableResize();
    else
        dictDisableResize();
}

/* Remove all the server state: jobs, queues, and everything else to start
 * like a fresh instance just rebooted. */
void flushServerData(void) {
    dictSafeForeach(server.jobs,de)
        job *job = dictGetKey(de);
        unregisterJob(job);
        freeJob(job);
    dictEndForeach

    dictSafeForeach(server.queues,de)
        queue *q = dictGetVal(de);
        serverAssert(queueLength(q) == 0);
        destroyQueue(q->name);
    dictEndForeach
}

/* ======================= Cron: called every 100 ms ======================== */

/* Add a sample to the operations per second array of samples. */
void trackInstantaneousMetric(int metric, long long current_reading) {
    long long t = mstime() - server.inst_metric[metric].last_sample_time;
    long long ops = current_reading -
                    server.inst_metric[metric].last_sample_count;
    long long ops_sec;

    ops_sec = t > 0 ? (ops*1000/t) : 0;

    server.inst_metric[metric].samples[server.inst_metric[metric].idx] =
        ops_sec;
    server.inst_metric[metric].idx++;
    server.inst_metric[metric].idx %= DISQUE_METRIC_SAMPLES;
    server.inst_metric[metric].last_sample_time = mstime();
    server.inst_metric[metric].last_sample_count = current_reading;
}

/* Return the mean of all the samples. */
long long getInstantaneousMetric(int metric) {
    int j;
    long long sum = 0;

    for (j = 0; j < DISQUE_METRIC_SAMPLES; j++)
        sum += server.inst_metric[metric].samples[j];
    return sum / DISQUE_METRIC_SAMPLES;
}

/* Check for timeouts. Returns non-zero if the client was terminated */
int clientsCronHandleTimeout(client *c) {
    time_t now = server.unixtime;

    if (server.maxidletime &&
        !(c->flags & DISQUE_BLOCKED) &&  /* no timeout for blocked clients. */
        (now - c->lastinteraction > server.maxidletime))
    {
        serverLog(DISQUE_VERBOSE,"Closing idle client");
        freeClient(c);
        return 1;
    } else if (c->flags & DISQUE_BLOCKED) {
        /* Blocked OPS timeout is handled with milliseconds resolution.
         * However note that the actual resolution is limited by
         * server.hz. */
        mstime_t now_ms = mstime();

        if (c->bpop.timeout != 0 && c->bpop.timeout < now_ms) {
            replyToBlockedClientTimedOut(c);
            unblockClient(c);
        }
    }
    return 0;
}

/* The client query buffer is an sds.c string that can end with a lot of
 * free space not used, this function reclaims space if needed.
 *
 * The function always returns 0 as it never terminates the client. */
int clientsCronResizeQueryBuffer(client *c) {
    size_t querybuf_size = sdsAllocSize(c->querybuf);
    time_t idletime = server.unixtime - c->lastinteraction;

    /* There are two conditions to resize the query buffer:
     * 1) Query buffer is > BIG_ARG and too big for latest peak.
     * 2) Client is inactive and the buffer is bigger than 1k. */
    if (((querybuf_size > DISQUE_MBULK_BIG_ARG) &&
         (querybuf_size/(c->querybuf_peak+1)) > 2) ||
         (querybuf_size > 1024 && idletime > 2))
    {
        /* Only resize the query buffer if it is actually wasting space. */
        if (sdsavail(c->querybuf) > 1024) {
            c->querybuf = sdsRemoveFreeSpace(c->querybuf);
        }
    }
    /* Reset the peak again to capture the peak memory usage in the next
     * cycle. */
    c->querybuf_peak = 0;
    return 0;
}

int clientsCronSendNeedJobs(client *c); /* see queue.c */

int clientsCronHandleDelayedJobReplication(client *c); /* see job.c */

void clientsCron(void) {
    /* Make sure to process at least 1/(server.hz*10) of clients per call.
     * Since this function is called server.hz times per second we are sure that
     * in the worst case we process all the clients in 10 seconds.
     * In normal conditions (a reasonable number of clients) we process
     * all the clients in a shorter time. */
    int numclients = listLength(server.clients);
    int iterations = numclients/(server.hz*10);

    if (iterations < 50)
        iterations = (numclients < 50) ? numclients : 50;
    while(listLength(server.clients) && iterations--) {
        client *c;
        listNode *head;

        /* Rotate the list, take the current head, process.
         * This way if the client must be removed from the list it's the
         * first element and we don't incur into O(N) computation. */
        listRotate(server.clients);
        head = listFirst(server.clients);
        c = listNodeValue(head);
        /* The following functions do different service checks on the client.
         * The protocol is that they return non-zero if the client was
         * terminated. */
        if (clientsCronHandleTimeout(c)) continue;
        if (clientsCronResizeQueryBuffer(c)) continue;
        if (clientsCronHandleDelayedJobReplication(c)) continue;
        if (clientsCronSendNeedJobs(c)) continue;
    }
}

/* This function handles 'background' operations we are required to do
 * incrementally in the Disque server, such as active key expiring, resizing,
 * rehashing. */
void databasesCron(void) {
    /* Perform hash tables rehashing if needed, but only if there are no
     * other processes saving the DB on disk. Otherwise rehashing is bad
     * as will cause a lot of copy-on-write of memory pages. */
    if (server.aof_child_pid == -1) {
        /* We use global counters so if we stop the computation at a given
         * DB we'll be able to start from the successive in the next
         * cron loop iteration. */

        tryResizeHashTables();
        incrementallyRehash();
    }
}

/* We take a cached value of the unix time in the global state because with
 * virtual memory and aging there is to store the current time in objects at
 * every object access, and accuracy is not needed. To access a global var is
 * a lot faster than calling time(NULL) */
void updateCachedTime(void) {
    server.unixtime = time(NULL);
    server.mstime = mstime();
}

/* This is our timer interrupt, called server.hz times per second.
 * Here is where we do a number of things that need to be done asynchronously.
 * For instance:
 *
 * - Active expired keys collection (it is also performed in a lazy way on
 *   lookup).
 * - Software watchdog.
 * - Update some statistic.
 * - Incremental rehashing of the DBs hash tables.
 * - Triggering BGSAVE / AOF rewrite, and handling of terminated children.
 * - Clients timeout of different kinds.
 * - Replication reconnection.
 * - Many more...
 *
 * Everything directly called here will be called server.hz times per second,
 * so in order to throttle execution of things we want to do less frequently
 * a macro is used: run_with_period(milliseconds) { .... }
 */

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    DISQUE_NOTUSED(eventLoop);
    DISQUE_NOTUSED(id);
    DISQUE_NOTUSED(clientData);

    /* Software watchdog: deliver the SIGALRM that will reach the signal
     * handler if we don't return here fast enough. */
    if (server.watchdog_period) watchdogScheduleSignal(server.watchdog_period);

    /* Update the time cache. */
    updateCachedTime();

    run_with_period(100) {
        trackInstantaneousMetric(DISQUE_METRIC_COMMAND,server.stat_numcommands);
        trackInstantaneousMetric(DISQUE_METRIC_NET_INPUT,
                server.stat_net_input_bytes);
        trackInstantaneousMetric(DISQUE_METRIC_NET_OUTPUT,
                server.stat_net_output_bytes);
    }

    /* Record the max memory used since the server was started. */
    if (zmalloc_used_memory() > server.stat_peak_memory)
        server.stat_peak_memory = zmalloc_used_memory();

    /* Sample the RSS here since this is a relatively slow call. */
    server.resident_set_size = zmalloc_get_rss();

    /* We received a SIGTERM, shutting down here in a safe way, as it is
     * not ok doing so inside the signal handler. */
    if (server.shutdown_asap) {
        if (prepareForShutdown(DISQUE_SHUTDOWN_NOFLAGS) == DISQUE_OK) exit(0);
        serverLog(DISQUE_WARNING,"SIGTERM received but errors trying to shut down the server, check the logs for more information");
        server.shutdown_asap = 0;
    }

    /* Show information about connected clients */
    run_with_period(5000) {
        serverLog(DISQUE_VERBOSE,
            "%lu clients connected, %zu bytes in use",
            listLength(server.clients),
            zmalloc_used_memory());
    }

    /* We need to do a few operations on clients asynchronously. */
    clientsCron();

    /* Handle background operations on Disque. */
    databasesCron();

    /* Start a scheduled AOF rewrite if this was requested by the user while
     * a BGSAVE was in progress. */
    if (server.aof_child_pid == -1 && server.aof_rewrite_scheduled) {
        rewriteAppendOnlyFileBackground();
    }

    /* Check if a background saving or AOF rewrite in progress terminated. */
    if (server.aof_child_pid != -1) {
        int statloc;
        pid_t pid;

        if ((pid = wait3(&statloc,WNOHANG,NULL)) != 0) {
            int exitcode = WEXITSTATUS(statloc);
            int bysignal = 0;

            if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

            if (pid == server.aof_child_pid) {
                backgroundRewriteDoneHandler(exitcode,bysignal);
            } else {
                serverLog(DISQUE_WARNING,
                    "Warning, detected child with unmatched pid: %ld",
                    (long)pid);
            }
            updateDictResizePolicy();
        }
    } else {
         /* Trigger an AOF rewrite if needed */
         if (server.aof_child_pid == -1 &&
             server.aof_rewrite_perc &&
             server.aof_current_size > server.aof_rewrite_min_size)
         {
            long long base = server.aof_rewrite_base_size ?
                            server.aof_rewrite_base_size : 1;
            long long growth = (server.aof_current_size*100/base) - 100;
            if (growth >= server.aof_rewrite_perc) {
                serverLog(DISQUE_NOTICE,"Starting automatic rewriting of AOF on %lld%% growth",growth);
                rewriteAppendOnlyFileBackground();
            }
         }
    }

    /* Queue cron function: deletes idle empty queues that are just using
     * memory. */
    run_with_period(100) {
        queueCron();
    }

    /* AOF postponed flush: Try at every cron cycle if the slow fsync
     * completed. */
    if (server.aof_flush_postponed_start) flushAppendOnlyFile(0);

    /* AOF write errors: in this case we have a buffer to flush as well and
     * clear the AOF error in case of success to make the DB writable again,
     * however to try every second is enough in case of 'hz' is set to
     * an higher frequency. */
    run_with_period(1000) {
        if (server.aof_last_write_status == DISQUE_ERR)
            flushAppendOnlyFile(0);
    }

    /* Close clients that need to be closed asynchronous */
    freeClientsInAsyncFreeQueue();

    /* Clear the paused clients flag if needed. */
    clientsArePaused(); /* Don't check return value, just use the side effect.*/

    /* Run the Disque Cluster cron. */
    run_with_period(100) {
        clusterCron();
    }

    server.cronloops++;
    return 1000/server.hz;
}

/* This function gets called every time Disque is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void beforeSleep(struct aeEventLoop *eventLoop) {
    DISQUE_NOTUSED(eventLoop);

    /* Unblock clients waiting to receive messages into queues.
     * We do this both on processCommand() and here, since we need to
     * unblock clients when queues are populated asynchronously. */
    handleClientsBlockedOnQueues();

    /* Try to process pending commands for clients that were just unblocked. */
    if (listLength(server.unblocked_clients))
        processUnblockedClients();

    /* Write the AOF buffer on disk */
    flushAppendOnlyFile(0);

    /* Call the Disque Cluster before sleep function. */
    clusterBeforeSleep();
}

/* =========================== Server initialization ======================== */

void createSharedObjects(void) {
    int j;

    shared.crlf = createObject(DISQUE_STRING,sdsnew("\r\n"));
    shared.ok = createObject(DISQUE_STRING,sdsnew("+OK\r\n"));
    shared.err = createObject(DISQUE_STRING,sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(DISQUE_STRING,sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(DISQUE_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(DISQUE_STRING,sdsnew(":1\r\n"));
    shared.cnegone = createObject(DISQUE_STRING,sdsnew(":-1\r\n"));
    shared.nullbulk = createObject(DISQUE_STRING,sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(DISQUE_STRING,sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(DISQUE_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(DISQUE_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(DISQUE_STRING,sdsnew("+QUEUED\r\n"));
    shared.wrongtypeerr = createObject(DISQUE_STRING,sdsnew(
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
    shared.nokeyerr = createObject(DISQUE_STRING,sdsnew(
        "-ERR no such key\r\n"));
    shared.syntaxerr = createObject(DISQUE_STRING,sdsnew(
        "-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(DISQUE_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(DISQUE_STRING,sdsnew(
        "-ERR index out of range\r\n"));
    shared.noscripterr = createObject(DISQUE_STRING,sdsnew(
        "-NOSCRIPT No matching script. Please use EVAL.\r\n"));
    shared.loadingerr = createObject(DISQUE_STRING,sdsnew(
        "-LOADING Disque is loading the dataset in memory\r\n"));
    shared.slowscripterr = createObject(DISQUE_STRING,sdsnew(
        "-BUSY Disque is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
    shared.masterdownerr = createObject(DISQUE_STRING,sdsnew(
        "-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.\r\n"));
    shared.bgsaveerr = createObject(DISQUE_STRING,sdsnew(
        "-MISCONF Disque is configured to save RDB snapshots, but is currently not able to persist on disk. Commands that may modify the data set are disabled. Please check Disque logs for details about the error.\r\n"));
    shared.roslaveerr = createObject(DISQUE_STRING,sdsnew(
        "-READONLY You can't write against a read only slave.\r\n"));
    shared.noautherr = createObject(DISQUE_STRING,sdsnew(
        "-NOAUTH Authentication required.\r\n"));
    shared.oomerr = createObject(DISQUE_STRING,sdsnew(
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
    shared.execaborterr = createObject(DISQUE_STRING,sdsnew(
        "-EXECABORT Transaction discarded because of previous errors.\r\n"));
    shared.noreplicaserr = createObject(DISQUE_STRING,sdsnew(
        "-NOREPLICAS Not enough good slaves to write.\r\n"));
    shared.busykeyerr = createObject(DISQUE_STRING,sdsnew(
        "-BUSYKEY Target key name already exists.\r\n"));
    shared.space = createObject(DISQUE_STRING,sdsnew(" "));
    shared.colon = createObject(DISQUE_STRING,sdsnew(":"));
    shared.plus = createObject(DISQUE_STRING,sdsnew("+"));
    shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
    shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
    shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
    shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
    shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
    shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);
    shared.loadjob = createStringObject("LOADJOB",7);
    shared.deljob = createStringObject("DELJOB",6);
    for (j = 0; j < DISQUE_SHARED_INTEGERS; j++) {
        shared.integers[j] = createObject(DISQUE_STRING,(void*)(long)j);
        shared.integers[j]->encoding = DISQUE_ENCODING_INT;
    }
    for (j = 0; j < DISQUE_SHARED_BULKHDR_LEN; j++) {
        shared.mbulkhdr[j] = createObject(DISQUE_STRING,
            sdscatprintf(sdsempty(),"*%d\r\n",j));
        shared.bulkhdr[j] = createObject(DISQUE_STRING,
            sdscatprintf(sdsempty(),"$%d\r\n",j));
    }
    /* The following two shared objects, minstring and maxstrings, are not
     * actually used for their value but as a special object meaning
     * respectively the minimum possible string and the maximum possible
     * string in string comparisons for the ZRANGEBYLEX command. */
    shared.minstring = createStringObject("minstring",9);
    shared.maxstring = createStringObject("maxstring",9);
}

void initServerConfig(void) {
    int j;

    getRandomHexChars(server.runid,DISQUE_RUN_ID_SIZE);
    getRandomHexChars(server.jobid_seed,DISQUE_RUN_ID_SIZE);
    server.configfile = NULL;
    server.hz = DISQUE_DEFAULT_HZ;
    server.runid[DISQUE_RUN_ID_SIZE] = '\0';
    server.arch_bits = (sizeof(long) == 8) ? 64 : 32;
    server.port = DISQUE_SERVERPORT;
    server.tcp_backlog = DISQUE_TCP_BACKLOG;
    server.bindaddr_count = 0;
    server.unixsocket = NULL;
    server.unixsocketperm = DISQUE_DEFAULT_UNIX_SOCKET_PERM;
    server.ipfd_count = 0;
    server.sofd = -1;
    server.dbnum = DISQUE_DEFAULT_DBNUM;
    server.verbosity = DISQUE_DEFAULT_VERBOSITY;
    server.maxidletime = DISQUE_MAXIDLETIME;
    server.tcpkeepalive = DISQUE_DEFAULT_TCP_KEEPALIVE;
    server.active_expire_enabled = 1;
    server.client_max_querybuf_len = DISQUE_MAX_QUERYBUF_LEN;
    server.loading = 0;
    server.logfile = zstrdup(DISQUE_DEFAULT_LOGFILE);
    server.syslog_enabled = DISQUE_DEFAULT_SYSLOG_ENABLED;
    server.syslog_ident = zstrdup(DISQUE_DEFAULT_SYSLOG_IDENT);
    server.syslog_facility = LOG_LOCAL0;
    server.daemonize = DISQUE_DEFAULT_DAEMONIZE;
    server.aof_state = DISQUE_AOF_OFF;
    server.aof_fsync = DISQUE_DEFAULT_AOF_FSYNC;
    server.aof_no_fsync_on_rewrite = DISQUE_DEFAULT_AOF_NO_FSYNC_ON_REWRITE;
    server.aof_rewrite_perc = DISQUE_AOF_REWRITE_PERC;
    server.aof_rewrite_min_size = DISQUE_AOF_REWRITE_MIN_SIZE;
    server.aof_rewrite_base_size = 0;
    server.aof_rewrite_scheduled = 0;
    server.aof_last_fsync = time(NULL);
    server.aof_rewrite_time_last = -1;
    server.aof_rewrite_time_start = -1;
    server.aof_lastbgrewrite_status = DISQUE_OK;
    server.aof_delayed_fsync = 0;
    server.aof_fd = -1;
    server.aof_selected_db = -1; /* Make sure the first time will not match */
    server.aof_flush_postponed_start = 0;
    server.aof_rewrite_incremental_fsync = DISQUE_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC;
    server.aof_load_truncated = DISQUE_DEFAULT_AOF_LOAD_TRUNCATED;
    server.aof_enqueue_jobs_once = DISQUE_DEFAULT_AOF_ENQUEUE_JOBS_ONCE;
    server.pidfile = zstrdup(DISQUE_DEFAULT_PID_FILE);
    server.aof_filename = zstrdup(DISQUE_DEFAULT_AOF_FILENAME);
    server.requirepass = NULL;
    server.activerehashing = DISQUE_DEFAULT_ACTIVE_REHASHING;
    server.maxclients = DISQUE_MAX_CLIENTS;
    server.bpop_blocked_clients = 0;
    server.maxmemory = DISQUE_DEFAULT_MAXMEMORY;
    server.maxmemory_policy = DISQUE_DEFAULT_MAXMEMORY_POLICY;
    server.maxmemory_samples = DISQUE_DEFAULT_MAXMEMORY_SAMPLES;
    server.shutdown_asap = 0;
    server.cluster_node_timeout = DISQUE_CLUSTER_DEFAULT_NODE_TIMEOUT;
    server.cluster_configfile = zstrdup(DISQUE_DEFAULT_CLUSTER_CONFIG_FILE);
    server.next_client_id = 1; /* Client IDs, start from 1 .*/
    server.loading_process_events_interval_bytes = (1024*1024*2);

    /* Client output buffer limits */
    for (j = 0; j < DISQUE_CLIENT_TYPE_COUNT; j++)
        server.client_obuf_limits[j] = clientBufferLimitsDefaults[j];

    /* Double constants initialization */
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;

    /* Command table -- we initiialize it here as it is part of the
     * initial configuration, since command names may be changed via
     * disque.conf using the rename-command directive. */
    server.commands = dictCreate(&commandTableDictType,NULL);
    server.orig_commands = dictCreate(&commandTableDictType,NULL);
    populateCommandTable();
    server.delCommand = lookupCommandByCString("del");
    server.multiCommand = lookupCommandByCString("multi");
    server.lpushCommand = lookupCommandByCString("lpush");
    server.lpopCommand = lookupCommandByCString("lpop");
    server.rpopCommand = lookupCommandByCString("rpop");

    /* Slow log */
    server.slowlog_log_slower_than = DISQUE_SLOWLOG_LOG_SLOWER_THAN;
    server.slowlog_max_len = DISQUE_SLOWLOG_MAX_LEN;

    /* Latency monitor */
    server.latency_monitor_threshold = DISQUE_DEFAULT_LATENCY_MONITOR_THRESHOLD;

    /* Debugging */
    server.assert_failed = "<no assertion failed>";
    server.assert_file = "<no file>";
    server.assert_line = 0;
    server.bug_report_start = 0;
    server.watchdog_period = 0;
}

/* This function will try to raise the max number of open files accordingly to
 * the configured max number of clients. It also reserves a number of file
 * descriptors (DISQUE_MIN_RESERVED_FDS) for extra operations of
 * persistence, listening sockets, log files and so forth.
 *
 * If it will not be possible to set the limit accordingly to the configured
 * max number of clients, the function will do the reverse setting
 * server.maxclients to the value that we can actually handle. */
void adjustOpenFilesLimit(void) {
    rlim_t maxfiles = server.maxclients+DISQUE_MIN_RESERVED_FDS;
    struct rlimit limit;

    if (getrlimit(RLIMIT_NOFILE,&limit) == -1) {
        serverLog(DISQUE_WARNING,"Unable to obtain the current NOFILE limit (%s), assuming 1024 and setting the max clients configuration accordingly.",
            strerror(errno));
        server.maxclients = 1024-DISQUE_MIN_RESERVED_FDS;
    } else {
        rlim_t oldlimit = limit.rlim_cur;

        /* Set the max number of files if the current limit is not enough
         * for our needs. */
        if (oldlimit < maxfiles) {
            rlim_t f;
            int setrlimit_error = 0;

            /* Try to set the file limit to match 'maxfiles' or at least
             * to the higher value supported less than maxfiles. */
            f = maxfiles;
            while(f > oldlimit) {
                rlim_t decr_step = 16;

                limit.rlim_cur = f;
                limit.rlim_max = f;
                if (setrlimit(RLIMIT_NOFILE,&limit) != -1) break;
                setrlimit_error = errno;

                /* We failed to set file limit to 'f'. Try with a
                 * smaller limit decrementing by a few FDs per iteration. */
                if (f < decr_step) break;
                f -= decr_step;
            }

            /* Assume that the limit we get initially is still valid if
             * our last try was even lower. */
            if (f < oldlimit) f = oldlimit;

            if (f != maxfiles) {
                int old_maxclients = server.maxclients;
                server.maxclients = f-DISQUE_MIN_RESERVED_FDS;
                if (server.maxclients < 1) {
                    serverLog(DISQUE_WARNING,"Your current 'ulimit -n' "
                        "of %llu is not enough for Disque to start. "
                        "Please increase your open file limit to at least "
                        "%llu. Exiting.",
                        (unsigned long long) oldlimit,
                        (unsigned long long) maxfiles);
                    exit(1);
                }
                serverLog(DISQUE_WARNING,"You requested maxclients of %d "
                    "requiring at least %llu max file descriptors.",
                    old_maxclients,
                    (unsigned long long) maxfiles);
                serverLog(DISQUE_WARNING,"Disque can't set maximum open files "
                    "to %llu because of OS error: %s.",
                    (unsigned long long) maxfiles, strerror(setrlimit_error));
                serverLog(DISQUE_WARNING,"Current maximum open files is %llu. "
                    "maxclients has been reduced to %d to compensate for "
                    "low ulimit. "
                    "If you need higher maxclients increase 'ulimit -n'.",
                    (unsigned long long) oldlimit, server.maxclients);
            } else {
                serverLog(DISQUE_NOTICE,"Increased maximum number of open files "
                    "to %llu (it was originally set to %llu).",
                    (unsigned long long) maxfiles,
                    (unsigned long long) oldlimit);
            }
        }
    }
}

/* Check that server.tcp_backlog can be actually enforced in Linux according
 * to the value of /proc/sys/net/core/somaxconn, or warn about it. */
void checkTcpBacklogSettings(void) {
#ifdef HAVE_PROC_SOMAXCONN
    FILE *fp = fopen("/proc/sys/net/core/somaxconn","r");
    char buf[1024];
    if (!fp) return;
    if (fgets(buf,sizeof(buf),fp) != NULL) {
        int somaxconn = atoi(buf);
        if (somaxconn > 0 && somaxconn < server.tcp_backlog) {
            serverLog(DISQUE_WARNING,"WARNING: The TCP backlog setting of %d cannot be enforced because /proc/sys/net/core/somaxconn is set to the lower value of %d.", server.tcp_backlog, somaxconn);
        }
    }
    fclose(fp);
#endif
}

/* Initialize a set of file descriptors to listen to the specified 'port'
 * binding the addresses specified in the Disque server configuration.
 *
 * The listening file descriptors are stored in the integer array 'fds'
 * and their number is set in '*count'.
 *
 * The addresses to bind are specified in the global server.bindaddr array
 * and their number is server.bindaddr_count. If the server configuration
 * contains no specific addresses to bind, this function will try to
 * bind * (all addresses) for both the IPv4 and IPv6 protocols.
 *
 * On success the function returns DISQUE_OK.
 *
 * On error the function returns DISQUE_ERR. For the function to be on
 * error, at least one of the server.bindaddr addresses was
 * impossible to bind, or no bind addresses were specified in the server
 * configuration but the function is not able to bind * for at least
 * one of the IPv4 or IPv6 protocols. */
int listenToPort(int port, int *fds, int *count) {
    int j;

    /* Force binding of 0.0.0.0 if no bind address is specified, always
     * entering the loop if j == 0. */
    if (server.bindaddr_count == 0) server.bindaddr[0] = NULL;
    for (j = 0; j < server.bindaddr_count || j == 0; j++) {
        if (server.bindaddr[j] == NULL) {
            /* Bind * for both IPv6 and IPv4, we enter here only if
             * server.bindaddr_count == 0. */
            fds[*count] = anetTcp6Server(server.neterr,port,NULL,
                server.tcp_backlog);
            if (fds[*count] != ANET_ERR) {
                anetNonBlock(NULL,fds[*count]);
                (*count)++;
            }
            fds[*count] = anetTcpServer(server.neterr,port,NULL,
                server.tcp_backlog);
            if (fds[*count] != ANET_ERR) {
                anetNonBlock(NULL,fds[*count]);
                (*count)++;
            }
            /* Exit the loop if we were able to bind * on IPv4 or IPv6,
             * otherwise fds[*count] will be ANET_ERR and we'll print an
             * error and return to the caller with an error. */
            if (*count) break;
        } else if (strchr(server.bindaddr[j],':')) {
            /* Bind IPv6 address. */
            fds[*count] = anetTcp6Server(server.neterr,port,server.bindaddr[j],
                server.tcp_backlog);
        } else {
            /* Bind IPv4 address. */
            fds[*count] = anetTcpServer(server.neterr,port,server.bindaddr[j],
                server.tcp_backlog);
        }
        if (fds[*count] == ANET_ERR) {
            serverLog(DISQUE_WARNING,
                "Creating Server TCP listening socket %s:%d: %s",
                server.bindaddr[j] ? server.bindaddr[j] : "*",
                port, server.neterr);
            return DISQUE_ERR;
        }
        anetNonBlock(NULL,fds[*count]);
        (*count)++;
    }
    return DISQUE_OK;
}

/* Resets the stats that we expose via INFO or other means that we want
 * to reset via CONFIG RESETSTAT. The function is also used in order to
 * initialize these fields in initServer() at server startup. */
void resetServerStats(void) {
    int j;

    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_fork_time = 0;
    server.stat_fork_rate = 0;
    server.stat_rejected_conn = 0;
    for (j = 0; j < DISQUE_METRIC_COUNT; j++) {
        server.inst_metric[j].idx = 0;
        server.inst_metric[j].last_sample_time = mstime();
        server.inst_metric[j].last_sample_count = 0;
        memset(server.inst_metric[j].samples,0,
            sizeof(server.inst_metric[j].samples));
    }
    server.stat_net_input_bytes = 0;
    server.stat_net_output_bytes = 0;
}

int skiplistCompareJobsToAwake(const void *a, const void *b);
int processJobs(struct aeEventLoop *eventLoop, long long id, void *clientData);

void initServer(void) {
    int j;

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setupSignalHandlers();

    if (server.syslog_enabled) {
        openlog(server.syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT,
            server.syslog_facility);
    }

    server.pid = getpid();
    server.current_client = NULL;
    server.clients = listCreate();
    server.clients_to_close = listCreate();
    server.monitors = listCreate();
    server.unblocked_clients = listCreate();
    server.ready_keys = listCreate();
    server.clients_paused = 0;

    createSharedObjects();
    adjustOpenFilesLimit();
    server.el = aeCreateEventLoop(server.maxclients+DISQUE_EVENTLOOP_FDSET_INCR);
    /* Open the TCP listening socket for the user commands. */
    if (server.port != 0 &&
        listenToPort(server.port,server.ipfd,&server.ipfd_count) == DISQUE_ERR)
        exit(1);

    /* Open the listening Unix domain socket. */
    if (server.unixsocket != NULL) {
        unlink(server.unixsocket); /* don't care if this fails */
        server.sofd = anetUnixServer(server.neterr,server.unixsocket,
            server.unixsocketperm, server.tcp_backlog);
        if (server.sofd == ANET_ERR) {
            serverLog(DISQUE_WARNING, "Opening socket: %s", server.neterr);
            exit(1);
        }
        anetNonBlock(NULL,server.sofd);
    }

    /* Abort if there are no listening sockets at all. */
    if (server.ipfd_count == 0 && server.sofd < 0) {
        serverLog(DISQUE_WARNING, "Configured to not listen anywhere, exiting.");
        exit(1);
    }

    /* Create the Disque data structures, and init other internal state. */
    server.jobs = dictCreate(&jobsDictType,NULL);
    server.queues = dictCreate(&queuesDictType,NULL);
    server.ready_queues = dictCreate(&setDictType,NULL);
    server.awakeme = skiplistCreate(skiplistCompareJobsToAwake);
    server.cronloops = 0;
    server.aof_child_pid = -1;
    aofRewriteBufferReset();
    server.aof_buf = sdsempty();
    resetServerStats();

    /* A few stats we don't want to reset: server startup time, and peak mem. */
    server.stat_starttime = time(NULL);
    server.stat_peak_memory = 0;
    server.resident_set_size = 0;
    server.aof_last_write_status = DISQUE_OK;
    server.aof_last_write_errno = 0;
    updateCachedTime();

    /* Create the serverCron() time event, that's our main way to process
     * background operations. */
    if(aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL) == AE_ERR ||
       aeCreateTimeEvent(server.el, 1, processJobs, NULL, NULL) == AE_ERR)
    {
        serverPanic("Can't create the serverCron or processJobs time event.");
        exit(1);
    }

    /* Create an event handler for accepting new connections in TCP and Unix
     * domain sockets. */
    for (j = 0; j < server.ipfd_count; j++) {
        if (aeCreateFileEvent(server.el, server.ipfd[j], AE_READABLE,
            acceptTcpHandler,NULL) == AE_ERR)
            {
                serverPanic(
                    "Unrecoverable error creating server.ipfd file event.");
            }
    }
    if (server.sofd > 0 && aeCreateFileEvent(server.el,server.sofd,AE_READABLE,
        acceptUnixHandler,NULL) == AE_ERR) serverPanic("Unrecoverable error creating server.sofd file event.");

    /* Open the AOF file if needed. */
    if (server.aof_state == DISQUE_AOF_ON) {
        server.aof_fd = open(server.aof_filename,
                               O_WRONLY|O_APPEND|O_CREAT,0644);
        if (server.aof_fd == -1) {
            serverLog(DISQUE_WARNING, "Can't open the append-only file: %s",
                strerror(errno));
            exit(1);
        }
    }

    /* 32 bit instances are limited to 4GB of address space, so if there is
     * no explicit limit in the user provided configuration we set a limit
     * at 3 GB using maxmemory with 'noeviction' policy'. This avoids
     * useless crashes of the Disque instance for out of memory. */
    if (server.arch_bits == 32 && server.maxmemory == 0) {
        serverLog(DISQUE_WARNING,"Warning: 32 bit instance detected but no memory limit set. Setting 3 GB maxmemory limit with 'noeviction' policy now.");
        server.maxmemory = 3072LL*(1024*1024); /* 3 GB */
        server.maxmemory_policy = DISQUE_MAXMEMORY_NO_EVICTION;
    }

    clusterInit();
    slowlogInit();
    latencyMonitorInit();
    bioInit();
}

/* Populates the Disque Command Table starting from the hard coded list
 * we have on top of disque.c file. */
void populateCommandTable(void) {
    int j;
    int numcommands = sizeof(serverCommandTable)/sizeof(struct serverCommand);

    for (j = 0; j < numcommands; j++) {
        struct serverCommand *c = serverCommandTable+j;
        char *f = c->sflags;
        int retval1, retval2;

        while(*f != '\0') {
            switch(*f) {
            case 'w': c->flags |= DISQUE_CMD_WRITE; break;
            case 'r': c->flags |= DISQUE_CMD_READONLY; break;
            case 'm': c->flags |= DISQUE_CMD_DENYOOM; break;
            case 'a': c->flags |= DISQUE_CMD_ADMIN; break;
            case 'l': c->flags |= DISQUE_CMD_LOADING; break;
            case 'M': c->flags |= DISQUE_CMD_SKIP_MONITOR; break;
            case 'F': c->flags |= DISQUE_CMD_FAST; break;
            default: serverPanic("Unsupported command flag"); break;
            }
            f++;
        }

        retval1 = dictAdd(server.commands, sdsnew(c->name), c);
        /* Populate an additional dictionary that will be unaffected
         * by rename-command statements in disque.conf. */
        retval2 = dictAdd(server.orig_commands, sdsnew(c->name), c);
        serverAssert(retval1 == DICT_OK && retval2 == DICT_OK);
    }
}

void resetCommandTableStats(void) {
    int numcommands = sizeof(serverCommandTable)/sizeof(struct serverCommand);
    int j;

    for (j = 0; j < numcommands; j++) {
        struct serverCommand *c = serverCommandTable+j;

        c->microseconds = 0;
        c->calls = 0;
    }
}

/* ====================== Commands lookup and execution ===================== */

struct serverCommand *lookupCommand(sds name) {
    return dictFetchValue(server.commands, name);
}

struct serverCommand *lookupCommandByCString(char *s) {
    struct serverCommand *cmd;
    sds name = sdsnew(s);

    cmd = dictFetchValue(server.commands, name);
    sdsfree(name);
    return cmd;
}

/* Lookup the command in the current table, if not found also check in
 * the original table containing the original command names unaffected by
 * disque.conf rename-command statement.
 *
 * This is used by functions rewriting the argument vector such as
 * rewriteClientCommandVector() in order to set client->cmd pointer
 * correctly even if the command was renamed. */
struct serverCommand *lookupCommandOrOriginal(sds name) {
    struct serverCommand *cmd = dictFetchValue(server.commands, name);

    if (!cmd) cmd = dictFetchValue(server.orig_commands,name);
    return cmd;
}

/* Send commands to the list of clients in monitor state. */
void replicationFeedMonitors(client *c, list *monitors, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & DISQUE_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[unix:%s] ",server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%s] ",getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == DISQUE_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(DISQUE_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

/* Call() is the core of Disque execution of a command */
void call(client *c, int flags) {
    long long start, duration;

    /* Sent the command to clients in MONITOR mode, only if the commands are
     * not generated from reading an AOF. */
    if (listLength(server.monitors) &&
        !server.loading &&
        !(c->cmd->flags & DISQUE_CMD_SKIP_MONITOR))
    {
        replicationFeedMonitors(c,server.monitors,c->argv,c->argc);
    }

    /* Call the command. */
    start = ustime();
    c->cmd->proc(c);
    duration = ustime()-start;

    /* Log the command into the Slow log if needed, and populate the
     * per-command statistics that we show in INFO commandstats. */
    if (flags & DISQUE_CALL_SLOWLOG) {
        char *latency_event = (c->cmd->flags & DISQUE_CMD_FAST) ?
                              "fast-command" : "command";
        latencyAddSampleIfNeeded(latency_event,duration/1000);
        slowlogPushEntryIfNeeded(c->argv,c->argc,duration);
    }
    if (flags & DISQUE_CALL_STATS) {
        c->cmd->microseconds += duration;
        c->cmd->calls++;
    }
    server.stat_numcommands++;
}

/* If this function gets called we already read a whole
 * command, arguments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If 1 is returned the client is still alive and valid and
 * other operations can be performed by the caller. Otherwise
 * if 0 is returned the client was destroyed (i.e. after QUIT). */
int processCommand(client *c) {
    /* The QUIT command is handled separately. Normal command procs will
     * go through checking for replication and QUIT will cause trouble
     * when FORCE_REPLICATION is enabled and would be implemented in
     * a regular command proc. */
    if (!strcasecmp(c->argv[0]->ptr,"quit")) {
        addReply(c,shared.ok);
        c->flags |= DISQUE_CLOSE_AFTER_REPLY;
        return DISQUE_ERR;
    }

    /* Now lookup the command and check ASAP about trivial error conditions
     * such as wrong arity, bad command name and so forth. */
    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
    if (!c->cmd) {
        addReplyErrorFormat(c,"unknown command '%s'",
            (char*)c->argv[0]->ptr);
        return DISQUE_OK;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return DISQUE_OK;
    }

    /* Check if the user is authenticated */
    if (server.requirepass && !c->authenticated && c->cmd->proc != authCommand)
    {
        addReply(c,shared.noautherr);
        return DISQUE_OK;
    }

    /* Handle the maxmemory directive.
     *
     * First we try to free some memory if possible (if there are volatile
     * keys in the dataset). If there are not the only thing we can do
     * is returning an error. */
    if (server.maxmemory) {
        int retval = freeMemoryIfNeeded();
        if ((c->cmd->flags & DISQUE_CMD_DENYOOM) && retval == DISQUE_ERR) {
            addReply(c, shared.oomerr);
            return DISQUE_OK;
        }
    }

    /* Don't accept write commands if there are problems persisting on disk. */
    if (server.aof_last_write_status == DISQUE_ERR &&
         (c->cmd->flags & DISQUE_CMD_WRITE ||
          c->cmd->proc == pingCommand))
    {
        addReplySds(c,
            sdscatprintf(sdsempty(),
            "-MISCONF Errors writing to the AOF file: %s\r\n",
            strerror(server.aof_last_write_errno)));
        return DISQUE_OK;
    }

    /* Loading DB? Return an error if the command has not the
     * DISQUE_CMD_LOADING flag. */
    if (server.loading && !(c->cmd->flags & DISQUE_CMD_LOADING)) {
        addReply(c, shared.loadingerr);
        return DISQUE_OK;
    }

    call(c,DISQUE_CALL_FULL);
    handleClientsBlockedOnQueues();
    return DISQUE_OK;
}

/*================================== Shutdown =============================== */

/* Close listening sockets. Also unlink the unix domain socket if
 * unlink_unix_socket is non-zero. */
void closeListeningSockets(int unlink_unix_socket) {
    int j;

    for (j = 0; j < server.ipfd_count; j++) close(server.ipfd[j]);
    if (server.sofd != -1) close(server.sofd);
    for (j = 0; j < server.cfd_count; j++) close(server.cfd[j]);
    if (unlink_unix_socket && server.unixsocket) {
        serverLog(DISQUE_NOTICE,"Removing the unix socket file.");
        unlink(server.unixsocket); /* don't care if this fails */
    }
}

/* Performs the operations needed to shutdown correctly the server, including
 * additional operations as specified by 'flags'. */
int prepareForShutdown(int flags) {
    int rewrite = flags & DISQUE_SHUTDOWN_REWRITE_AOF;

    serverLog(DISQUE_WARNING,"User requested shutdown...");
    /* Kill the saving child if there is a background saving in progress.
       We want to avoid race conditions, for instance our saving child may
       overwrite the synchronous saving did by SHUTDOWN. */
    if (server.aof_state != DISQUE_AOF_OFF) {
        /* Kill the AOF saving child as the AOF we already have may be longer
         * but contains the full dataset anyway. */
        if (server.aof_child_pid != -1) {
            /* If we have AOF enabled but haven't written the AOF yet, don't
             * shutdown or else the dataset will be lost. */
            if (server.aof_state == DISQUE_AOF_WAIT_REWRITE) {
                serverLog(DISQUE_WARNING, "Writing initial AOF, can't exit.");
                return DISQUE_ERR;
            }
            serverLog(DISQUE_WARNING,
                "There is a child rewriting the AOF. Killing it!");
            kill(server.aof_child_pid,SIGUSR1);
        }
        /* Append only file: fsync() the AOF and exit */
        serverLog(DISQUE_NOTICE,"Calling fsync() on the AOF file.");
        aof_fsync(server.aof_fd);
    }

    /* Perform a synchronous AOF rewrite if requested. */
    if (rewrite) {
        if (rewriteAppendOnlyFile(server.aof_filename,0) == DISQUE_ERR)
            return DISQUE_ERR;
    }

    if (server.daemonize) {
        serverLog(DISQUE_NOTICE,"Removing the pid file.");
        unlink(server.pidfile);
    }

    /* Close the listening sockets. Apparently this allows faster restarts. */
    closeListeningSockets(1);
    serverLog(DISQUE_WARNING,"Disque is now ready to exit, bye bye...");
    return DISQUE_OK;
}

/*================================== Commands =============================== */

/* Return zero if strings are the same, non-zero if they are not.
 * The comparison is performed in a way that prevents an attacker to obtain
 * information about the nature of the strings just monitoring the execution
 * time of the function.
 *
 * Note that limiting the comparison length to strings up to 512 bytes we
 * can avoid leaking any information about the password length and any
 * possible branch misprediction related leak.
 */
int time_independent_strcmp(char *a, char *b) {
    char bufa[DISQUE_AUTHPASS_MAX_LEN], bufb[DISQUE_AUTHPASS_MAX_LEN];
    /* The above two strlen perform len(a) + len(b) operations where either
     * a or b are fixed (our password) length, and the difference is only
     * relative to the length of the user provided string, so no information
     * leak is possible in the following two lines of code. */
    unsigned int alen = strlen(a);
    unsigned int blen = strlen(b);
    unsigned int j;
    int diff = 0;

    /* We can't compare strings longer than our static buffers.
     * Note that this will never pass the first test in practical circumstances
     * so there is no info leak. */
    if (alen > sizeof(bufa) || blen > sizeof(bufb)) return 1;

    memset(bufa,0,sizeof(bufa));        /* Constant time. */
    memset(bufb,0,sizeof(bufb));        /* Constant time. */
    /* Again the time of the following two copies is proportional to
     * len(a) + len(b) so no info is leaked. */
    memcpy(bufa,a,alen);
    memcpy(bufb,b,blen);

    /* Always compare all the chars in the two buffers without
     * conditional expressions. */
    for (j = 0; j < sizeof(bufa); j++) {
        diff |= (bufa[j] ^ bufb[j]);
    }
    /* Length must be equal as well. */
    diff |= alen ^ blen;
    return diff; /* If zero strings are the same. */
}

void authCommand(client *c) {
    if (!server.requirepass) {
        addReplyError(c,"Client sent AUTH, but no password is set");
    } else if (!time_independent_strcmp(c->argv[1]->ptr, server.requirepass)) {
      c->authenticated = 1;
      addReply(c,shared.ok);
    } else {
      c->authenticated = 0;
      addReplyError(c,"invalid password");
    }
}

/* The PING command. It works in a different way if the client is in
 * in Pub/Sub mode. */
void pingCommand(client *c) {
    /* The command takes zero or one arguments. */
    if (c->argc > 2) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return;
    }

    if (c->argc == 1)
        addReply(c,shared.pong);
    else
        addReplyBulk(c,c->argv[1]);
}

void echoCommand(client *c) {
    addReplyBulk(c,c->argv[1]);
}

void timeCommand(client *c) {
    struct timeval tv;

    /* gettimeofday() can only fail if &tv is a bad address so we
     * don't check for errors. */
    gettimeofday(&tv,NULL);
    addReplyMultiBulkLen(c,2);
    addReplyBulkLongLong(c,tv.tv_sec);
    addReplyBulkLongLong(c,tv.tv_usec);
}

void shutdownCommand(client *c) {
    int flags = 0;

    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    } else if (c->argc == 2) {
        if (!strcasecmp(c->argv[1]->ptr,"rewrite-aof")) {
            flags |= DISQUE_SHUTDOWN_REWRITE_AOF;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }
    /* When SHUTDOWN is called while the server is loading a dataset in
     * memory we need to make sure no attempt is performed to rewrite
     * the dataset on shutdown (otherwise it could overwrite the current DB
     * with half-read data). */
    if (server.loading)
        flags = (flags & ~DISQUE_SHUTDOWN_REWRITE_AOF);
    if (prepareForShutdown(flags) == DISQUE_OK) exit(0);
    addReplyError(c,"Errors trying to SHUTDOWN. Check logs.");
}

/* Helper function for addReplyCommand() to output flags. */
int addReplyCommandFlag(client *c, struct serverCommand *cmd, int f, char *reply) {
    if (cmd->flags & f) {
        addReplyStatus(c, reply);
        return 1;
    }
    return 0;
}

/* Output the representation of a Disque command. Used by the COMMAND command.*/
void addReplyCommand(client *c, struct serverCommand *cmd) {
    if (!cmd) {
        addReply(c, shared.nullbulk);
    } else {
        /* We are adding: command name, arg count, flags, first, last, offset */
        addReplyMultiBulkLen(c, 6);
        addReplyBulkCString(c, cmd->name);
        addReplyLongLong(c, cmd->arity);

        int flagcount = 0;
        void *flaglen = addDeferredMultiBulkLength(c);
        flagcount += addReplyCommandFlag(c,cmd,DISQUE_CMD_WRITE, "write");
        flagcount += addReplyCommandFlag(c,cmd,DISQUE_CMD_READONLY, "readonly");
        flagcount += addReplyCommandFlag(c,cmd,DISQUE_CMD_DENYOOM, "denyoom");
        flagcount += addReplyCommandFlag(c,cmd,DISQUE_CMD_ADMIN, "admin");
        flagcount += addReplyCommandFlag(c,cmd,DISQUE_CMD_RANDOM, "random");
        flagcount += addReplyCommandFlag(c,cmd,DISQUE_CMD_LOADING, "loading");
        flagcount += addReplyCommandFlag(c,cmd,DISQUE_CMD_SKIP_MONITOR, "skip_monitor");
        flagcount += addReplyCommandFlag(c,cmd,DISQUE_CMD_FAST, "fast");
        setDeferredMultiBulkLength(c, flaglen, flagcount);

        addReplyLongLong(c, cmd->firstkey);
        addReplyLongLong(c, cmd->lastkey);
        addReplyLongLong(c, cmd->keystep);
    }
}

/* COMMAND <subcommand> <args> */
void commandCommand(client *c) {
    dictIterator *di;
    dictEntry *de;

    if (c->argc == 1) {
        addReplyMultiBulkLen(c, dictSize(server.commands));
        di = dictGetIterator(server.commands);
        while ((de = dictNext(di)) != NULL) {
            addReplyCommand(c, dictGetVal(de));
        }
        dictReleaseIterator(di);
    } else if (!strcasecmp(c->argv[1]->ptr, "info")) {
        int i;
        addReplyMultiBulkLen(c, c->argc-2);
        for (i = 2; i < c->argc; i++) {
            addReplyCommand(c, dictFetchValue(server.commands, c->argv[i]->ptr));
        }
    } else if (!strcasecmp(c->argv[1]->ptr, "count") && c->argc == 2) {
        addReplyLongLong(c, dictSize(server.commands));
    } else {
        addReplyError(c, "Unknown subcommand or wrong number of arguments.");
        return;
    }
}

/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, unsigned long long n) {
    double d;

    if (n < 1024) {
        /* Bytes */
        sprintf(s,"%lluB",n);
        return;
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        sprintf(s,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        sprintf(s,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        sprintf(s,"%.2fG",d);
    } else if (n < (1024LL*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024);
        sprintf(s,"%.2fT",d);
    } else if (n < (1024LL*1024*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024*1024);
        sprintf(s,"%.2fP",d);
    } else {
        /* Let's hope we never need this */
        sprintf(s,"%lluB",n);
    }
}

/* Create the string returned by the INFO command. This is decoupled
 * by the INFO command itself as we need to report the same information
 * on memory corruption problems. */
sds genDisqueInfoString(char *section) {
    sds info = sdsempty();
    time_t uptime = server.unixtime-server.stat_starttime;
    int j, numcommands;
    struct rusage self_ru, c_ru;
    unsigned long lol, bib;
    int allsections = 0, defsections = 0;
    int sections = 0;

    if (section == NULL) section = "default";
    allsections = strcasecmp(section,"all") == 0;
    defsections = strcasecmp(section,"default") == 0;

    getrusage(RUSAGE_SELF, &self_ru);
    getrusage(RUSAGE_CHILDREN, &c_ru);
    getClientsMaxBuffers(&lol,&bib);

    /* Server */
    if (allsections || defsections || !strcasecmp(section,"server")) {
        static int call_uname = 1;
        static struct utsname name;

        if (sections++) info = sdscat(info,"\r\n");

        if (call_uname) {
            /* Uname can be slow and is always the same output. Cache it. */
            uname(&name);
            call_uname = 0;
        }

        info = sdscatprintf(info,
            "# Server\r\n"
            "disque_version:%s\r\n"
            "disque_git_sha1:%s\r\n"
            "disque_git_dirty:%d\r\n"
            "disque_build_id:%llx\r\n"
            "os:%s %s %s\r\n"
            "arch_bits:%d\r\n"
            "multiplexing_api:%s\r\n"
            "gcc_version:%d.%d.%d\r\n"
            "process_id:%ld\r\n"
            "run_id:%s\r\n"
            "tcp_port:%d\r\n"
            "uptime_in_seconds:%jd\r\n"
            "uptime_in_days:%jd\r\n"
            "hz:%d\r\n"
            "config_file:%s\r\n",
            DISQUE_VERSION,
            disqueGitSHA1(),
            strtol(disqueGitDirty(),NULL,10) > 0,
            (unsigned long long) disqueBuildId(),
            name.sysname, name.release, name.machine,
            server.arch_bits,
            aeGetApiName(),
#ifdef __GNUC__
            __GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
            0,0,0,
#endif
            (long) getpid(),
            server.runid,
            server.port,
            (intmax_t)uptime,
            (intmax_t)(uptime/(3600*24)),
            server.hz,
            server.configfile ? server.configfile : "");
    }

    /* Clients */
    if (allsections || defsections || !strcasecmp(section,"clients")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Clients\r\n"
            "connected_clients:%lu\r\n"
            "client_longest_output_list:%lu\r\n"
            "client_biggest_input_buf:%lu\r\n"
            "blocked_clients:%d\r\n",
            listLength(server.clients),
            lol, bib,
            server.bpop_blocked_clients);
    }

    /* Memory */
    if (allsections || defsections || !strcasecmp(section,"memory")) {
        char hmem[64];
        char peak_hmem[64];
        size_t zmalloc_used = zmalloc_used_memory();

        /* Peak memory is updated from time to time by serverCron() so it
         * may happen that the instantaneous value is slightly bigger than
         * the peak value. This may confuse users, so we update the peak
         * if found smaller than the current memory usage. */
        if (zmalloc_used > server.stat_peak_memory)
            server.stat_peak_memory = zmalloc_used;

        bytesToHuman(hmem,zmalloc_used);
        bytesToHuman(peak_hmem,server.stat_peak_memory);
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Memory\r\n"
            "used_memory:%zu\r\n"
            "used_memory_human:%s\r\n"
            "used_memory_rss:%zu\r\n"
            "used_memory_peak:%zu\r\n"
            "used_memory_peak_human:%s\r\n"
            "mem_fragmentation_ratio:%.2f\r\n"
            "mem_allocator:%s\r\n",
            zmalloc_used,
            hmem,
            server.resident_set_size,
            server.stat_peak_memory,
            peak_hmem,
            zmalloc_get_fragmentation_ratio(server.resident_set_size),
            ZMALLOC_LIB
            );
    }

    /* Jobs */
    if (allsections || defsections || !strcasecmp(section,"jobs")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Jobs\r\n"
            "registered_jobs:%llu\r\n",
            (unsigned long long) dictSize(server.jobs));
    }

    /* Queues */
    if (allsections || defsections || !strcasecmp(section,"queues")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Queues\r\n"
            "registered_queues:%llu\r\n",
            (unsigned long long) dictSize(server.queues));
    }

    /* Persistence */
    if (allsections || defsections || !strcasecmp(section,"persistence")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Persistence\r\n"
            "loading:%d\r\n"
            "aof_enabled:%d\r\n"
            "aof_rewrite_in_progress:%d\r\n"
            "aof_rewrite_scheduled:%d\r\n"
            "aof_last_rewrite_time_sec:%jd\r\n"
            "aof_current_rewrite_time_sec:%jd\r\n"
            "aof_last_bgrewrite_status:%s\r\n"
            "aof_last_write_status:%s\r\n",
            server.loading,
            server.aof_state != DISQUE_AOF_OFF,
            server.aof_child_pid != -1,
            server.aof_rewrite_scheduled,
            (intmax_t)server.aof_rewrite_time_last,
            (intmax_t)((server.aof_child_pid == -1) ?
                -1 : time(NULL)-server.aof_rewrite_time_start),
            (server.aof_lastbgrewrite_status == DISQUE_OK) ? "ok" : "err",
            (server.aof_last_write_status == DISQUE_OK) ? "ok" : "err");

        if (server.aof_state != DISQUE_AOF_OFF) {
            info = sdscatprintf(info,
                "aof_current_size:%lld\r\n"
                "aof_base_size:%lld\r\n"
                "aof_pending_rewrite:%d\r\n"
                "aof_buffer_length:%zu\r\n"
                "aof_rewrite_buffer_length:%lu\r\n"
                "aof_pending_bio_fsync:%llu\r\n"
                "aof_delayed_fsync:%lu\r\n",
                (long long) server.aof_current_size,
                (long long) server.aof_rewrite_base_size,
                server.aof_rewrite_scheduled,
                sdslen(server.aof_buf),
                aofRewriteBufferSize(),
                bioPendingJobsOfType(DISQUE_BIO_AOF_FSYNC),
                server.aof_delayed_fsync);
        }

        if (server.loading) {
            double perc;
            time_t eta, elapsed;
            off_t remaining_bytes = server.loading_total_bytes-
                                    server.loading_loaded_bytes;

            perc = ((double)server.loading_loaded_bytes /
                   server.loading_total_bytes) * 100;

            elapsed = server.unixtime-server.loading_start_time;
            if (elapsed == 0) {
                eta = 1; /* A fake 1 second figure if we don't have
                            enough info */
            } else {
                eta = (elapsed*remaining_bytes)/server.loading_loaded_bytes;
            }

            info = sdscatprintf(info,
                "loading_start_time:%jd\r\n"
                "loading_total_bytes:%llu\r\n"
                "loading_loaded_bytes:%llu\r\n"
                "loading_loaded_perc:%.2f\r\n"
                "loading_eta_seconds:%jd\r\n",
                (intmax_t) server.loading_start_time,
                (unsigned long long) server.loading_total_bytes,
                (unsigned long long) server.loading_loaded_bytes,
                perc,
                (intmax_t)eta
            );
        }
    }

    /* Stats */
    if (allsections || defsections || !strcasecmp(section,"stats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Stats\r\n"
            "total_connections_received:%lld\r\n"
            "total_commands_processed:%lld\r\n"
            "instantaneous_ops_per_sec:%lld\r\n"
            "total_net_input_bytes:%lld\r\n"
            "total_net_output_bytes:%lld\r\n"
            "instantaneous_input_kbps:%.2f\r\n"
            "instantaneous_output_kbps:%.2f\r\n"
            "rejected_connections:%lld\r\n"
            "latest_fork_usec:%lld\r\n",
            server.stat_numconnections,
            server.stat_numcommands,
            getInstantaneousMetric(DISQUE_METRIC_COMMAND),
            server.stat_net_input_bytes,
            server.stat_net_output_bytes,
            (float)getInstantaneousMetric(DISQUE_METRIC_NET_INPUT)/1024,
            (float)getInstantaneousMetric(DISQUE_METRIC_NET_OUTPUT)/1024,
            server.stat_rejected_conn,
            server.stat_fork_time);
    }

    /* CPU */
    if (allsections || defsections || !strcasecmp(section,"cpu")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
        "# CPU\r\n"
        "used_cpu_sys:%.2f\r\n"
        "used_cpu_user:%.2f\r\n"
        "used_cpu_sys_children:%.2f\r\n"
        "used_cpu_user_children:%.2f\r\n",
        (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000,
        (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000,
        (float)c_ru.ru_stime.tv_sec+(float)c_ru.ru_stime.tv_usec/1000000,
        (float)c_ru.ru_utime.tv_sec+(float)c_ru.ru_utime.tv_usec/1000000);
    }

    /* cmdtime */
    if (allsections || !strcasecmp(section,"commandstats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info, "# Commandstats\r\n");
        numcommands = sizeof(serverCommandTable)/sizeof(struct serverCommand);
        for (j = 0; j < numcommands; j++) {
            struct serverCommand *c = serverCommandTable+j;

            if (!c->calls) continue;
            info = sdscatprintf(info,
                "cmdstat_%s:calls=%lld,usec=%lld,usec_per_call=%.2f\r\n",
                c->name, c->calls, c->microseconds,
                (c->calls == 0) ? 0 : ((float)c->microseconds/c->calls));
        }
    }

    return info;
}

void infoCommand(client *c) {
    char *section = c->argc == 2 ? c->argv[1]->ptr : "default";

    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    }
    addReplyBulkSds(c, genDisqueInfoString(section));
}

void monitorCommand(client *c) {
    /* ignore MONITOR if already slave or in monitor mode */
    if (c->flags & DISQUE_MONITOR) return;

    c->flags |= DISQUE_MONITOR;
    listAddNodeTail(server.monitors,c);
    addReply(c,shared.ok);
}

/* ============================ Maxmemory directive  ======================== */

/* freeMemoryIfNeeded() gets called when 'maxmemory' is set on the config
 * file to limit the max memory used by the server, before processing a
 * command.
 *
 * The goal of the function is to free enough memory to keep Disque under the
 * configured memory limit.
 *
 * The function starts calculating how many bytes should be freed to keep
 * Disque under the limit, and enters a loop selecting the best keys to
 * evict accordingly to the configured policy.
 *
 * If all the bytes needed to return back under the limit were freed the
 * function returns DISQUE_OK, otherwise DISQUE_ERR is returned, and the caller
 * should block the execution of commands that will result in more memory
 * used by the server. */

#define ACK_EVICTION_SAMPLE_SIZE 16

int freeMemoryIfNeeded(void) {
    size_t mem_used, mem_tofree, mem_freed, mem_target;
    mstime_t latency;

    /* We start to reclaim memory only at memory warning 2 or greater, that is
     * when 95% of maxmemory is reached. */
    if (getMemoryWarningLevel() < 2) return DISQUE_OK;

    if (server.maxmemory_policy == DISQUE_MAXMEMORY_NO_EVICTION)
        return DISQUE_ERR; /* We need to free memory, but policy forbids. */

    /* Compute how much memory we need to free. */
    mem_used = zmalloc_used_memory();
    mem_target = server.maxmemory / 100 * 95;

    /* The following check is not actaully needed since we already checked
     * that getMemoryWarningLevel() returned 2 or greater, but it is safer
     * to have given that we are workign with unsigned integers to compute
     * mem_tofree. */
    if (mem_used <= mem_target) return DISQUE_OK;

    /* The eviction loop: for up to 2 milliseconds we try to reclaim memory
     * as long we are able to make progresses, otherwise we just stop ASAP. */
    mem_tofree = mem_used - mem_target;
    mem_freed = 0;
    latencyStartMonitor(latency);
    while (mem_freed < mem_tofree) {
        int objects_freed = 0;
        int count, j;
        long long delta;
        dictEntry *des[ACK_EVICTION_SAMPLE_SIZE];

        count = dictGetSomeKeys(server.jobs, des, ACK_EVICTION_SAMPLE_SIZE);
        delta = (long long) zmalloc_used_memory();
        for (j = 0; j < count; j++) {
            job *job = dictGetKey(des[j]);
            if (job->state == JOB_STATE_ACKED) {
                unregisterJob(job);
                freeJob(job);
                objects_freed++;
            }
        }
        delta -= (long long) zmalloc_used_memory();
        mem_freed += delta;

        /* If no object was freed in the latest loop or we are here for
         * more than 1 or 2 milliseconds, return to the caller with a failure
         * return value. */
        if (!objects_freed || (mstime() - latency) > 1) {
            latencyEndMonitor(latency);
            latencyAddSampleIfNeeded("eviction-cycle",latency);
            return DISQUE_ERR; /* nothing to free... */
        }
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle",latency);
    return DISQUE_OK;
}

/* Get the memory warning level:
 *
 * 0 - No memory warning.
 * 1 - Over 75% of maxmemory setting.
 * 2 - Over 95% of maxmemory setting.
 * 3 - Over 95% of maxmemory setting and RSS over 100% of maxmemory setting.
 *
 * The server may change behavior depending on the memory warning level.
 * For example at warning >= 1, new jobs are only replicated to
 * external nodes and a localy copy is not retained. */
int getMemoryWarningLevel(void) {
    size_t mem_used = zmalloc_used_memory();

    if (mem_used > server.maxmemory / 100 * 95 &&
        mem_used > server.resident_set_size) return 3;
    if (mem_used > server.maxmemory / 100 * 95) return 2;
    if (mem_used > server.maxmemory / 100 * 75) return 1;
    return 0;
}

/* =================================== Main! ================================ */

#ifdef __linux__
int linuxOvercommitMemoryValue(void) {
    FILE *fp = fopen("/proc/sys/vm/overcommit_memory","r");
    char buf[64];

    if (!fp) return -1;
    if (fgets(buf,64,fp) == NULL) {
        fclose(fp);
        return -1;
    }
    fclose(fp);

    return atoi(buf);
}

void linuxOvercommitMemoryWarning(void) {
    if (linuxOvercommitMemoryValue() == 0) {
        serverLog(DISQUE_WARNING,"WARNING overcommit_memory is set to 0! Background save may fail under low memory condition. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.");
    }
}
#endif /* __linux__ */

void createPidFile(void) {
    /* Try to write the pid file in a best-effort way. */
    FILE *fp = fopen(server.pidfile,"w");
    if (fp) {
        fprintf(fp,"%d\n",(int)getpid());
        fclose(fp);
    }
}

void daemonize(void) {
    int fd;

    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If Disque is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}

void version(void) {
    printf("Disque server v=%s sha=%s:%d malloc=%s bits=%d build=%llx\n",
        DISQUE_VERSION,
        disqueGitSHA1(),
        atoi(disqueGitDirty()) > 0,
        ZMALLOC_LIB,
        sizeof(long) == 4 ? 32 : 64,
        (unsigned long long) disqueBuildId());
    exit(0);
}

void usage(void) {
    fprintf(stderr,"Usage: ./disque-server [/path/to/disque.conf] [options]\n");
    fprintf(stderr,"       ./disque-server - (read config from stdin)\n");
    fprintf(stderr,"       ./disque-server -v or --version\n");
    fprintf(stderr,"       ./disque-server -h or --help\n");
    fprintf(stderr,"       ./disque-server --test-memory <megabytes>\n\n");
    fprintf(stderr,"Examples:\n");
    fprintf(stderr,"       ./disque-server (run the server with default conf)\n");
    fprintf(stderr,"       ./disque-server /etc/disque/7711.conf\n");
    fprintf(stderr,"       ./disque-server --port 7777\n");
    fprintf(stderr,"       ./disque-server --port 7777 --slaveof 127.0.0.1 8888\n");
    fprintf(stderr,"       ./disque-server /etc/mydisque.conf --loglevel verbose\n\n");
    exit(1);
}

void disqueAsciiArt(void) {
#include "asciilogo.h"
    char *buf = zmalloc(1024*16);

    if (server.syslog_enabled) {
        serverLog(DISQUE_NOTICE,
            "Disque %s (%s/%d) %s bit, port %d, pid %ld ready to start.",
            DISQUE_VERSION,
            disqueGitSHA1(),
            strtol(disqueGitDirty(),NULL,10) > 0,
            (sizeof(long) == 8) ? "64" : "32",
            server.port,
            (long) getpid()
        );
    } else {
        snprintf(buf,1024*16,ascii_logo,
            DISQUE_VERSION,
            disqueGitSHA1(),
            strtol(disqueGitDirty(),NULL,10) > 0,
            (sizeof(long) == 8) ? "64" : "32",
            server.port,
            (long) getpid()
        );
        serverLogRaw(DISQUE_NOTICE|DISQUE_LOG_RAW,buf);
    }
    zfree(buf);
}

static void sigShutdownHandler(int sig) {
    char *msg;

    switch (sig) {
    case SIGINT:
        msg = "Received SIGINT scheduling shutdown...";
        break;
    case SIGTERM:
        msg = "Received SIGTERM scheduling shutdown...";
        break;
    default:
        msg = "Received shutdown signal, scheduling shutdown...";
    };

    /* SIGINT is often delivered via Ctrl+C in an interactive session.
     * If we receive the signal the second time, we interpret this as
     * the user really wanting to quit ASAP without waiting to persist
     * on disk. */
    if (server.shutdown_asap && sig == SIGINT) {
        serverLogFromHandler(DISQUE_WARNING, "You insist... exiting now.");
        exit(1); /* Exit with an error since this was not a clean shutdown. */
    } else if (server.loading) {
        exit(0);
    }

    serverLogFromHandler(DISQUE_WARNING, msg);
    server.shutdown_asap = 1;
}

void setupSignalHandlers(void) {
    struct sigaction act;

    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction is used.
     * Otherwise, sa_handler is used. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sigShutdownHandler;
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);

#ifdef HAVE_BACKTRACE
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
#endif
    return;
}

void memtest(size_t megabytes, int passes);

/* Function called at startup to load RDB or AOF file in memory. */
void loadDataFromDisk(void) {
    long long start = ustime();
    if (server.aof_state == DISQUE_AOF_ON || server.aof_enqueue_jobs_once) {
        if (loadAppendOnlyFile(server.aof_filename) == DISQUE_OK)
            serverLog(DISQUE_NOTICE,"DB loaded from append only file: %.3f seconds",(float)(ustime()-start)/1000000);

        /* Rewrite aof-enqueue-jobs-once setting it to no if enabled: this
         * option has the special property of auto-turning itself off. */
        if (server.aof_enqueue_jobs_once && server.configfile) {
            server.aof_enqueue_jobs_once = 0;
            if (rewriteConfig(server.configfile) == -1) {
                serverLog(DISQUE_WARNING,
                    "CONFIG REWRITE failed "
                    "(executed to turn off aof-enqueue-jobs-once): %s",
                    strerror(errno));
            }
        }
    }
}

void serverOutOfMemoryHandler(size_t allocation_size) {
    serverLog(DISQUE_WARNING,"Out Of Memory allocating %zu bytes!",
        allocation_size);
    serverPanic("Disque aborting for OUT OF MEMORY");
}

void serverSetProcTitle(char *title) {
#ifdef USE_SETPROCTITLE
    setproctitle("%s %s:%d",
        title,
        server.bindaddr_count ? server.bindaddr[0] : "*",
        server.port);
#else
    DISQUE_NOTUSED(title);
#endif
}

int main(int argc, char **argv) {
    struct timeval tv;

    /* We need to initialize our libraries, and the server configuration. */
#ifdef INIT_SETPROCTITLE_REPLACEMENT
    spt_init(argc, argv);
#endif
    setlocale(LC_COLLATE,"");
    zmalloc_enable_thread_safeness();
    zmalloc_set_oom_handler(serverOutOfMemoryHandler);
    srand(time(NULL)^getpid());
    gettimeofday(&tv,NULL);
    dictSetHashFunctionSeed(tv.tv_sec^tv.tv_usec^getpid());
    initServerConfig();

    if (argc >= 2) {
        int j = 1; /* First option to parse in argv[] */
        sds options = sdsempty();
        char *configfile = NULL;

        /* Handle special options --help and --version */
        if (strcmp(argv[1], "-v") == 0 ||
            strcmp(argv[1], "--version") == 0) version();
        if (strcmp(argv[1], "--help") == 0 ||
            strcmp(argv[1], "-h") == 0) usage();
        if (strcmp(argv[1], "--test-memory") == 0) {
            if (argc == 3) {
                memtest(atoi(argv[2]),50);
                exit(0);
            } else {
                fprintf(stderr,"Please specify the amount of memory to test in megabytes.\n");
                fprintf(stderr,"Example: ./disque-server --test-memory 4096\n\n");
                exit(1);
            }
        }

        /* First argument is the config file name? */
        if (argv[j][0] != '-' || argv[j][1] != '-')
            configfile = argv[j++];
        /* All the other options are parsed and conceptually appended to the
         * configuration file. For instance --port 6380 will generate the
         * string "port 6380\n" to be parsed after the actual file name
         * is parsed, if any. */
        while(j != argc) {
            if (argv[j][0] == '-' && argv[j][1] == '-') {
                /* Option name */
                if (sdslen(options)) options = sdscat(options,"\n");
                options = sdscat(options,argv[j]+2);
                options = sdscat(options," ");
            } else {
                /* Option argument */
                options = sdscatrepr(options,argv[j],strlen(argv[j]));
                options = sdscat(options," ");
            }
            j++;
        }
        if (configfile) server.configfile = getAbsolutePath(configfile);
        loadServerConfig(configfile,options);
        sdsfree(options);
    } else {
        serverLog(DISQUE_WARNING, "Warning: no config file specified, using the default config. In order to specify a config file use %s /path/to/disque.conf", argv[0]);
    }
    if (server.daemonize) daemonize();
    initServer();
    if (server.daemonize) createPidFile();
    serverSetProcTitle(argv[0]);
    disqueAsciiArt();

    /* Things not needed when running in Sentinel mode. */
    serverLog(DISQUE_WARNING,"Server started, Disque version " DISQUE_VERSION);
#ifdef __linux__
    linuxOvercommitMemoryWarning();
#endif
    checkTcpBacklogSettings();
    loadDataFromDisk();
    if (server.ipfd_count > 0)
        serverLog(DISQUE_NOTICE,"The server is now ready to accept connections on port %d", server.port);
    if (server.sofd > 0)
        serverLog(DISQUE_NOTICE,"The server is now ready to accept connections at %s", server.unixsocket);

    /* Warning the user about suspicious maxmemory setting. */
    if (server.maxmemory > 0 && server.maxmemory < 1024*1024) {
        serverLog(DISQUE_WARNING,"WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?", server.maxmemory);
    }

    aeSetBeforeSleepProc(server.el,beforeSleep);
    /* Make sure to do a cron run before 1st client is served, so that we
     * can do periodic tasks there that we can assume to always be executed
     * at least once before the server starts serving queries. */
    serverCron(NULL,0,NULL);
    aeMain(server.el);
    aeDeleteEventLoop(server.el);
    return 0;
}

/* The End */
