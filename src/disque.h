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

#ifndef __DISQUE_H
#define __DISQUE_H

#include "fmacros.h"
#include "config.h"

#if defined(__sun)
#include "solarisfixes.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <signal.h>

typedef long long mstime_t; /* millisecond time type. */

#include "ae.h"      /* Event driven programming library */
#include "sds.h"     /* Dynamic safe strings */
#include "dict.h"    /* Hash tables */
#include "adlist.h"  /* Linked lists */
#include "skiplist.h"/* Skip lists. */
#include "zmalloc.h" /* total memory usage aware version of malloc/free */
#include "anet.h"    /* Networking the easy way */
#include "version.h" /* Version macro */
#include "util.h"    /* Misc functions useful in many places */
#include "latency.h" /* Latency monitor API */
#include "sparkline.h" /* ASII graphs API */
#include "rio.h"     /* Abstract I/O. */

/* Error codes */
#define DISQUE_OK                0
#define DISQUE_ERR               -1

/* Static server configuration */
#define DISQUE_DEFAULT_HZ        10      /* Time interrupt calls/sec. */
#define DISQUE_MIN_HZ            1
#define DISQUE_MAX_HZ            500
#define DISQUE_TIME_ERR          500     /* Desynchronization (in ms) */
#define DISQUE_SERVERPORT        7711    /* TCP port */
#define DISQUE_TCP_BACKLOG       511     /* TCP listen backlog */
#define DISQUE_MAXIDLETIME       0       /* default client timeout: infinite */
#define DISQUE_DEFAULT_DBNUM     16
#define DISQUE_CONFIGLINE_MAX    1024
#define DISQUE_DBCRON_DBS_PER_CALL 16
#define DISQUE_MAX_WRITE_PER_EVENT (1024*64)
#define DISQUE_SHARED_INTEGERS 10000
#define DISQUE_SHARED_BULKHDR_LEN 32
#define DISQUE_MAX_LOGMSG_LEN    1024 /* Default maximum length of syslog messages */
#define DISQUE_AOF_REWRITE_PERC  100
#define DISQUE_AOF_REWRITE_MIN_SIZE (64*1024*1024)
#define DISQUE_AOF_REWRITE_ITEMS_PER_CMD 64
#define DISQUE_SLOWLOG_LOG_SLOWER_THAN 10000
#define DISQUE_SLOWLOG_MAX_LEN 128
#define DISQUE_MAX_CLIENTS 10000
#define DISQUE_AUTHPASS_MAX_LEN 512
#define DISQUE_DEFAULT_SLAVE_PRIORITY 100
#define DISQUE_REPL_TIMEOUT 60
#define DISQUE_REPL_PING_SLAVE_PERIOD 10
#define DISQUE_RUN_ID_SIZE 40
#define DISQUE_EOF_MARK_SIZE 40
#define DISQUE_DEFAULT_REPL_BACKLOG_SIZE (1024*1024)    /* 1mb */
#define DISQUE_DEFAULT_REPL_BACKLOG_TIME_LIMIT (60*60)  /* 1 hour */
#define DISQUE_REPL_BACKLOG_MIN_SIZE (1024*16)          /* 16k */
#define DISQUE_BGSAVE_RETRY_DELAY 5 /* Wait a few secs before trying again. */
#define DISQUE_DEFAULT_PID_FILE "/var/run/disque.pid"
#define DISQUE_DEFAULT_SYSLOG_IDENT "disque"
#define DISQUE_DEFAULT_CLUSTER_CONFIG_FILE "nodes.conf"
#define DISQUE_DEFAULT_DAEMONIZE 0
#define DISQUE_DEFAULT_UNIX_SOCKET_PERM 0
#define DISQUE_DEFAULT_TCP_KEEPALIVE 0
#define DISQUE_DEFAULT_LOGFILE ""
#define DISQUE_DEFAULT_SYSLOG_ENABLED 0
#define DISQUE_DEFAULT_STOP_WRITES_ON_BGSAVE_ERROR 1
#define DISQUE_DEFAULT_RDB_COMPRESSION 1
#define DISQUE_DEFAULT_RDB_CHECKSUM 1
#define DISQUE_DEFAULT_RDB_FILENAME "dump.rdb"
#define DISQUE_DEFAULT_REPL_DISKLESS_SYNC 0
#define DISQUE_DEFAULT_REPL_DISKLESS_SYNC_DELAY 5
#define DISQUE_DEFAULT_SLAVE_SERVE_STALE_DATA 1
#define DISQUE_DEFAULT_SLAVE_READ_ONLY 1
#define DISQUE_DEFAULT_REPL_DISABLE_TCP_NODELAY 0
#define DISQUE_DEFAULT_MAXMEMORY (1024*1024*1024) /* 1gb */
#define DISQUE_DEFAULT_MAXMEMORY_SAMPLES 5
#define DISQUE_DEFAULT_AOF_FILENAME "disque.aof"
#define DISQUE_DEFAULT_AOF_NO_FSYNC_ON_REWRITE 0
#define DISQUE_DEFAULT_AOF_LOAD_TRUNCATED 1
#define DISQUE_DEFAULT_AOF_ENQUEUE_JOBS_ONCE 0
#define DISQUE_DEFAULT_ACTIVE_REHASHING 1
#define DISQUE_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC 1
#define DISQUE_DEFAULT_MIN_SLAVES_TO_WRITE 0
#define DISQUE_DEFAULT_MIN_SLAVES_MAX_LAG 10
#define DISQUE_IP_STR_LEN 46 /* INET6_ADDRSTRLEN is 46 but we need to be sure */
#define DISQUE_PEER_ID_LEN (DISQUE_IP_STR_LEN+32) /* Must be enough for ip:port */
#define DISQUE_BINDADDR_MAX 16
#define DISQUE_MIN_RESERVED_FDS 32
#define DISQUE_DEFAULT_LATENCY_MONITOR_THRESHOLD 0

#define ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP 20 /* Loopkups per loop. */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* Microseconds */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* CPU max % for keys collection */
#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

/* Instantaneous metrics tracking. */
#define DISQUE_METRIC_SAMPLES 16     /* Number of samples per metric. */
#define DISQUE_METRIC_COMMAND 0      /* Number of commands executed. */
#define DISQUE_METRIC_NET_INPUT 1    /* Bytes read to network .*/
#define DISQUE_METRIC_NET_OUTPUT 2   /* Bytes written to network. */
#define DISQUE_METRIC_COUNT 3

/* Protocol and I/O related defines */
#define DISQUE_MAX_QUERYBUF_LEN  (1024*1024*1024) /* 1GB max query buffer. */
#define DISQUE_IOBUF_LEN         (1024*16)  /* Generic I/O buffer size */
#define DISQUE_REPLY_CHUNK_BYTES (16*1024) /* 16k output buffer */
#define DISQUE_INLINE_MAX_SIZE   (1024*64) /* Max size of inline reads */
#define DISQUE_MBULK_BIG_ARG     (1024*32)
#define DISQUE_LONGSTR_SIZE      21          /* Bytes needed for long -> str */
#define DISQUE_AOF_AUTOSYNC_BYTES (1024*1024*32) /* fdatasync every 32MB */
/* When configuring the Disque eventloop, we setup it so that the total number
 * of file descriptors we can handle are server.maxclients + RESERVED_FDS + FDSET_INCR
 * that is our safety margin. */
#define DISQUE_EVENTLOOP_FDSET_INCR (DISQUE_MIN_RESERVED_FDS+96)

/* Hash table parameters */
#define DISQUE_HT_MINFILL        10      /* Minimal hash table fill 10% */

/* Command flags. Please check the command table defined in the disque.c file
 * for more information about the meaning of every flag. */
#define DISQUE_CMD_WRITE        (1<<0)  /* "w" flag */
#define DISQUE_CMD_READONLY     (1<<1)  /* "r" flag */
#define DISQUE_CMD_DENYOOM      (1<<2)  /* "m" flag */
#define DISQUE_CMD_ADMIN        (1<<3)  /* "a" flag */
#define DISQUE_CMD_RANDOM       (1<<4)  /* "R" flag */
#define DISQUE_CMD_LOADING      (1<<5)  /* "l" flag */
#define DISQUE_CMD_SKIP_MONITOR (1<<6)  /* "M" flag */
#define DISQUE_CMD_FAST         (1<<7)  /* "F" flag */

/* Object types */
#define DISQUE_STRING 0
#define DISQUE_LIST 1
#define DISQUE_SET 2
#define DISQUE_ZSET 3
#define DISQUE_HASH 4

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
#define DISQUE_ENCODING_RAW 0     /* Raw representation */
#define DISQUE_ENCODING_INT 1     /* Encoded as integer */
#define DISQUE_ENCODING_EMBSTR 2  /* Embedded sds string encoding */

/* AOF states */
#define DISQUE_AOF_OFF 0             /* AOF is off */
#define DISQUE_AOF_ON 1              /* AOF is on */
#define DISQUE_AOF_WAIT_REWRITE 2    /* AOF waits rewrite to start appending */

/* Client flags */
#define DISQUE_MONITOR (1<<0) /* This client is a slave monitor, see MONITOR */
#define DISQUE_BLOCKED (1<<1) /* The client is waiting in a blocking op. */
#define DISQUE_CLOSE_AFTER_REPLY (1<<2) /* Close after writing entire reply. */
#define DISQUE_UNBLOCKED (1<<3)   /* This client was unblocked and is stored in
                                     server.unblocked_clients */
#define DISQUE_CLOSE_ASAP (1<<4)  /* Close this client ASAP */
#define DISQUE_UNIX_SOCKET (1<<5) /* Client connected via Unix domain socket */
#define DISQUE_READONLY (1<<6)    /* Cluster client is in read-only state. */
#define DISQUE_AOF_CLIENT (1<<7)  /* AOF loading client. */

/* Client block type (btype field in client structure)
 * if DISQUE_BLOCKED flag is set. */
#define DISQUE_BLOCKED_NONE 0    /* Not blocked, no DISQUE_BLOCKED flag set. */
#define DISQUE_BLOCKED_JOB_REPL 1 /* Wait job synchronous replication. */
#define DISQUE_BLOCKED_QUEUES 2   /* Wait for new jobs in a set of queues. */

/* Client request types */
#define DISQUE_REQ_INLINE 1
#define DISQUE_REQ_MULTIBULK 2

/* Client classes for client limits, currently used only for
 * the max-client-output-buffer limit implementation. */
#define DISQUE_CLIENT_TYPE_NORMAL 0 /* Normal req-reply clients + MONITORs */
#define DISQUE_CLIENT_TYPE_COUNT 1

/* Synchronous read timeout - slave side */
#define DISQUE_REPL_SYNCIO_TIMEOUT 5

/* List related stuff */
#define DISQUE_HEAD 0
#define DISQUE_TAIL 1

/* Log levels */
#define DISQUE_DEBUG 0
#define DISQUE_VERBOSE 1
#define DISQUE_NOTICE 2
#define DISQUE_WARNING 3
#define DISQUE_LOG_RAW (1<<10) /* Modifier to log without timestamp */
#define DISQUE_DEFAULT_VERBOSITY DISQUE_NOTICE

/* Anti-warning macro... */
#define DISQUE_NOTUSED(V) ((void) V)

/* Append only defines */
#define AOF_FSYNC_NO 0
#define AOF_FSYNC_ALWAYS 1
#define AOF_FSYNC_EVERYSEC 2
#define DISQUE_DEFAULT_AOF_FSYNC AOF_FSYNC_EVERYSEC

/* Disque maxmemory strategies */
#define DISQUE_MAXMEMORY_ACKS 0
#define DISQUE_MAXMEMORY_NO_EVICTION 1
#define DISQUE_DEFAULT_MAXMEMORY_POLICY DISQUE_MAXMEMORY_ACKS

/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

/* SHUTDOWN flags */
#define DISQUE_SHUTDOWN_NOFLAGS 0 /* No flags. */
#define DISQUE_SHUTDOWN_REWRITE_AOF 1 /* Sync AOF rewrite before exiting. */

/* Command call flags, see call() function */
#define DISQUE_CALL_NONE 0
#define DISQUE_CALL_SLOWLOG 1
#define DISQUE_CALL_STATS 2
#define DISQUE_CALL_PROPAGATE 4
#define DISQUE_CALL_FULL (DISQUE_CALL_SLOWLOG | DISQUE_CALL_STATS | DISQUE_CALL_PROPAGATE)

/* Command propagation flags, see propagate() function */
#define DISQUE_PROPAGATE_NONE 0
#define DISQUE_PROPAGATE_AOF 1

/* Get the first bind addr or NULL */
#define DISQUE_BIND_ADDR (server.bindaddr_count ? server.bindaddr[0] : NULL)

/* Using the following macro you can run code inside serverCron() with the
 * specified period, specified in milliseconds.
 * The actual resolution depends on server.hz. */
#define run_with_period(_ms_) if ((_ms_ <= 1000/server.hz) || !(server.cronloops%((_ms_)/(1000/server.hz))))

/* We can print the stacktrace, so our assert is defined this way: */
#define serverAssertWithInfo(_c,_o,_e) ((_e)?(void)0 : (_serverAssertWithInfo(_c,_o,#_e,__FILE__,__LINE__),_exit(1)))
#define serverAssert(_e) ((_e)?(void)0 : (_serverAssert(#_e,__FILE__,__LINE__),_exit(1)))
#define serverPanic(_e) _serverPanic(#_e,__FILE__,__LINE__),_exit(1)

/*-----------------------------------------------------------------------------
 * Data types
 *----------------------------------------------------------------------------*/

/* A disque object, that is a type able to hold a string object with
 * reference counting. The implementation is generic, with a type and pointer
 * field so that it can represent other types if needed. */

/* The actual Disque Object */
typedef struct disqueObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned notused:24;
    int refcount;
    void *ptr;
} robj;

/* Macro used to initialize a Disque object allocated on the stack.
 * Note that this macro is taken near the structure definition to make sure
 * we'll update it when the structure is changed, to avoid bugs like
 * bug #85 introduced exactly in this way. */
#define initStaticStringObject(_var,_ptr) do { \
    _var.refcount = 1; \
    _var.type = DISQUE_STRING; \
    _var.encoding = DISQUE_ENCODING_RAW; \
    _var.ptr = _ptr; \
} while(0);

struct job;

/* This structure holds the blocking operation state for a client.
 * The fields used depend on client->btype. */
typedef struct blockingState {
    /* Generic fields. */
    mstime_t timeout;       /* Blocking operation timeout. If UNIX current time
                             * is > timeout then the operation timed out. */

    /* DISQUE_BLOCKED_JOB_REPL */
    struct job *job;        /* Job we are trying to replicate. */
    mstime_t added_node_time; /* Last time we added a new node. */

    /* DISQUE_BLOCKED_QUEUES */
    dict *queues;           /* Queues we are waiting for. */
} blockingState;

/* With multiplexing we need to take per-client state.
 * Clients are taken in a linked list. */
typedef struct client {
    uint64_t id;            /* Client incremental unique ID. */
    int fd;
    robj *name;             /* As set by CLIENT SETNAME */
    sds querybuf;
    size_t querybuf_peak;   /* Recent (100ms or more) peak of querybuf size */
    int argc;
    robj **argv;
    struct serverCommand *cmd, *lastcmd;
    int reqtype;
    int multibulklen;       /* number of multi bulk arguments left to read */
    long bulklen;           /* length of bulk argument in multi bulk request */
    list *reply;
    unsigned long reply_bytes; /* Tot bytes of objects in reply list */
    int sentlen;            /* Amount of bytes already sent in the current
                               buffer or object being sent. */
    time_t ctime;           /* Client creation time */
    time_t lastinteraction; /* time of the last interaction, used for timeout */
    time_t obuf_soft_limit_reached_time;
    int flags;              /* DISQUE_SLAVE | DISQUE_MONITOR */
    int authenticated;      /* when requirepass is non-NULL */
    int btype;              /* Type of blocking op if DISQUE_BLOCKED. */
    blockingState bpop;     /* blocking state */
    sds peerid;             /* Cached peer ID. */

    /* Response buffer */
    int bufpos;
    char buf[DISQUE_REPLY_CHUNK_BYTES];
} client;

struct sharedObjectsStruct {
    robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *cnegone, *pong, *space,
    *colon, *nullbulk, *nullmultibulk, *queued,
    *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
    *outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
    *masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noreplicaserr,
    *busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
    *unsubscribebulk, *psubscribebulk, *punsubscribebulk, *loadjob, *deljob,
    *minstring, *maxstring,
    *integers[DISQUE_SHARED_INTEGERS],
    *mbulkhdr[DISQUE_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
    *bulkhdr[DISQUE_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */
};

/* ZSETs use a specialized version of Skiplists */
typedef struct zskiplistNode {
    robj *obj;
    double score;
    struct zskiplistNode *backward;
    struct zskiplistLevel {
        struct zskiplistNode *forward;
        unsigned int span;
    } level[];
} zskiplistNode;

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset {
    dict *dict;
    zskiplist *zsl;
} zset;

typedef struct clientBufferLimitsConfig {
    unsigned long long hard_limit_bytes;
    unsigned long long soft_limit_bytes;
    time_t soft_limit_seconds;
} clientBufferLimitsConfig;

extern clientBufferLimitsConfig clientBufferLimitsDefaults[DISQUE_CLIENT_TYPE_COUNT];

/*-----------------------------------------------------------------------------
 * Global server state
 *----------------------------------------------------------------------------*/

struct clusterState;

/* AIX defines hz to __hz, we don't use this define and in order to allow
 * Disque build on AIX we need to undef it. */
#ifdef _AIX
#undef hz
#endif

struct disqueServer {
    /* General */
    pid_t pid;                  /* Main process pid. */
    char *configfile;           /* Absolute config file path, or NULL */
    int hz;                     /* serverCron() calls frequency in hertz */
    dict *commands;             /* Command table */
    dict *orig_commands;        /* Command table before command renaming. */
    aeEventLoop *el;
    int shutdown_asap;          /* SHUTDOWN needed ASAP */
    int activerehashing;        /* Incremental rehash in serverCron() */
    char *requirepass;          /* Pass for AUTH command, or NULL */
    char *pidfile;              /* PID file path */
    int arch_bits;              /* 32 or 64 depending on sizeof(long) */
    int cronloops;              /* Number of times the cron function run */
    char runid[DISQUE_RUN_ID_SIZE+1];  /* ID always different at every exec. */
    char jobid_seed[DISQUE_RUN_ID_SIZE]; /* Job ID generation seed. */
    /* Networking */
    int port;                   /* TCP listening port */
    int tcp_backlog;            /* TCP listen() backlog */
    char *bindaddr[DISQUE_BINDADDR_MAX]; /* Addresses we should bind to */
    int bindaddr_count;         /* Number of addresses in server.bindaddr[] */
    char *unixsocket;           /* UNIX socket path */
    mode_t unixsocketperm;      /* UNIX socket permission */
    int ipfd[DISQUE_BINDADDR_MAX]; /* TCP socket file descriptors */
    int ipfd_count;             /* Used slots in ipfd[] */
    int sofd;                   /* Unix socket file descriptor */
    int cfd[DISQUE_BINDADDR_MAX];/* Cluster bus listening socket */
    int cfd_count;              /* Used slots in cfd[] */
    list *clients;              /* List of active clients */
    list *clients_to_close;     /* Clients to close asynchronously */
    list *monitors;             /* List of MONITORs */
    client *current_client; /* Current client, only used on crash report */
    int clients_paused;         /* True if clients are currently paused */
    mstime_t clients_pause_end_time; /* Time when we undo clients_paused */
    char neterr[ANET_ERR_LEN];   /* Error buffer for anet.c */
    uint64_t next_client_id;    /* Next client unique ID. Incremental. */
    /* Jobs & Queues */
    dict *jobs;                 /* Main jobs hash table, by job ID. */
    dict *queues;               /* Main queues hash table, by queue name. */
    dict *ready_queues;         /* Queues ready to serve blocked clients. */
    skiplist *awakeme;          /* Jobs background processing queue. */
    /* AOF loading information */
    int loading;                /* We are loading data from disk if true */
    off_t loading_total_bytes;
    off_t loading_loaded_bytes;
    time_t loading_start_time;
    off_t loading_process_events_interval_bytes;
    /* Fast pointers to often looked up command */
    struct serverCommand *delCommand, *multiCommand, *lpushCommand,
                         *lpopCommand, *rpopCommand;
    /* Fields used only for stats */
    time_t stat_starttime;          /* Server start time */
    long long stat_numcommands;     /* Number of processed commands */
    long long stat_numconnections;  /* Number of connections received */
    size_t stat_peak_memory;        /* Max used memory record */
    long long stat_fork_time;       /* Time needed to perform latest fork() */
    double stat_fork_rate;          /* Fork rate in GB/sec. */
    long long stat_rejected_conn;   /* Clients rejected because of maxclients */
    list *slowlog;                  /* SLOWLOG list of commands */
    long long slowlog_entry_id;     /* SLOWLOG current entry ID */
    long long slowlog_log_slower_than; /* SLOWLOG time limit (to get logged) */
    unsigned long slowlog_max_len;     /* SLOWLOG max number of items logged */
    size_t resident_set_size;       /* RSS sampled in serverCron(). */
    long long stat_net_input_bytes; /* Bytes read from network. */
    long long stat_net_output_bytes; /* Bytes written to network. */
    /* The following two are used to track instantaneous metrics, like
     * number of operations per second, network traffic. */
    struct {
        long long last_sample_time; /* Timestamp of last sample in ms */
        long long last_sample_count;/* Count in last sample */
        long long samples[DISQUE_METRIC_SAMPLES];
        int idx;
    } inst_metric[DISQUE_METRIC_COUNT];
    /* Configuration */
    int verbosity;                  /* Loglevel in disque.conf */
    int maxidletime;                /* Client timeout in seconds */
    int tcpkeepalive;               /* Set SO_KEEPALIVE if non-zero. */
    int active_expire_enabled;      /* Can be disabled for testing purposes. */
    size_t client_max_querybuf_len; /* Limit for client query buffer length */
    int dbnum;                      /* Total number of configured DBs */
    int daemonize;                  /* True if running as a daemon */
    clientBufferLimitsConfig client_obuf_limits[DISQUE_CLIENT_TYPE_COUNT];
    /* AOF persistence */
    int aof_state;                  /* DISQUE_AOF_(ON|OFF|WAIT_REWRITE) */
    int aof_fsync;                  /* Kind of fsync() policy */
    char *aof_filename;             /* Name of the AOF file */
    int aof_no_fsync_on_rewrite;    /* Don't fsync if a rewrite is in prog. */
    int aof_rewrite_perc;           /* Rewrite AOF if % growth is > M and... */
    off_t aof_rewrite_min_size;     /* the AOF file is at least N bytes. */
    off_t aof_rewrite_base_size;    /* AOF size on latest startup or rewrite. */
    off_t aof_current_size;         /* AOF current size. */
    int aof_rewrite_scheduled;      /* Rewrite once BGSAVE terminates. */
    pid_t aof_child_pid;            /* PID if rewriting process */
    list *aof_rewrite_buf_blocks;   /* Hold changes during an AOF rewrite. */
    sds aof_buf;      /* AOF buffer, written before entering the event loop */
    int aof_fd;       /* File descriptor of currently selected AOF file */
    int aof_selected_db; /* Currently selected DB in AOF */
    time_t aof_flush_postponed_start; /* UNIX time of postponed AOF flush */
    time_t aof_last_fsync;            /* UNIX time of last fsync() */
    time_t aof_rewrite_time_last;   /* Time used by last AOF rewrite run. */
    time_t aof_rewrite_time_start;  /* Current AOF rewrite start time. */
    int aof_lastbgrewrite_status;   /* DISQUE_OK or DISQUE_ERR */
    unsigned long aof_delayed_fsync;  /* delayed AOF fsync() counter */
    int aof_rewrite_incremental_fsync;/* fsync incrementally while rewriting? */
    int aof_last_write_status;      /* DISQUE_OK or DISQUE_ERR */
    int aof_last_write_errno;       /* Valid if aof_last_write_status is ERR */
    int aof_load_truncated;         /* Don't stop on unexpected AOF EOF. */
    int aof_enqueue_jobs_once;      /* Enqueue jobs loading AOF, once. */
    /* AOF pipes used to communicate between parent and child during rewrite. */
    int aof_pipe_write_data_to_child;
    int aof_pipe_read_data_from_parent;
    int aof_pipe_write_ack_to_parent;
    int aof_pipe_read_ack_from_child;
    int aof_pipe_write_ack_to_child;
    int aof_pipe_read_ack_from_parent;
    int aof_stop_sending_diff;     /* If true stop sending accumulated diffs
                                      to child process. */
    sds aof_child_diff;             /* AOF diff accumulator child side. */
    /* Logging */
    char *logfile;                  /* Path of log file */
    int syslog_enabled;             /* Is syslog enabled? */
    char *syslog_ident;             /* Syslog ident */
    int syslog_facility;            /* Syslog facility */
    /* Limits */
    unsigned int maxclients;            /* Max number of simultaneous clients */
    unsigned long long maxmemory;   /* Max number of memory bytes to use */
    int maxmemory_policy;           /* Policy for key eviction */
    int maxmemory_samples;          /* Pricision of random sampling */
    /* Blocked clients */
    unsigned int bpop_blocked_clients; /* Number of clients blocked by lists */
    list *unblocked_clients; /* list of clients to unblock before next loop */
    list *ready_keys;        /* List of readyList structures for BLPOP & co */
    /* Cached time. */
    time_t unixtime;        /* Unix time sampled every cron cycle. */
    long long mstime;       /* Like 'unixtime' but with milliseconds res. */
    /* Cluster */
    mstime_t cluster_node_timeout; /* Cluster node timeout. */
    char *cluster_configfile; /* Cluster auto-generated config file name. */
    struct clusterState *cluster;  /* State of the cluster */
    /* Latency monitor */
    long long latency_monitor_threshold;
    dict *latency_events;
    /* Assert & bug reporting */
    char *assert_failed;
    char *assert_file;
    int assert_line;
    int bug_report_start; /* True if bug report header was already logged. */
    int watchdog_period;  /* Software watchdog period in ms. 0 = off */
};

typedef void serverCommandProc(client *c);

typedef int *redisGetKeysProc(struct serverCommand *cmd, robj **argv, int argc, int *numkeys);

struct serverCommand {
    char *name;
    serverCommandProc *proc;
    int arity;
    char *sflags; /* Flags as string representation, one char per flag. */
    int flags;    /* The actual flags, obtained from the 'sflags' field. */
    /* Use a function to determine keys arguments in a command line.
     * Used for Disque Cluster redirect. */
    redisGetKeysProc *getkeys_proc;
    /* What keys should be loaded in background when calling this command? */
    int firstkey; /* The first argument that's a key (0 = no keys) */
    int lastkey;  /* The last argument that's a key */
    int keystep;  /* The step between first and last key */
    long long microseconds, calls;
};

struct redisFunctionSym {
    char *name;
    unsigned long pointer;
};

/* Structure to hold list iteration abstraction. */
typedef struct {
    robj *subject;
    unsigned char encoding;
    unsigned char direction; /* Iteration direction */
    unsigned char *zi;
    listNode *ln;
} listTypeIterator;

/* Structure for an entry while iterating over a list. */
typedef struct {
    listTypeIterator *li;
    unsigned char *zi;  /* Entry in ziplist */
    listNode *ln;       /* Entry in linked list */
} listTypeEntry;

/* Structure to hold set iteration abstraction. */
typedef struct {
    robj *subject;
    int encoding;
    int ii; /* intset iterator */
    dictIterator *di;
} setTypeIterator;

/* Structure to hold hash iteration abstraction. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
typedef struct {
    robj *subject;
    int encoding;

    unsigned char *fptr, *vptr;

    dictIterator *di;
    dictEntry *de;
} hashTypeIterator;

#define DISQUE_HASH_KEY 1
#define DISQUE_HASH_VALUE 2

/*-----------------------------------------------------------------------------
 * Extern declarations
 *----------------------------------------------------------------------------*/

extern struct disqueServer server;
extern struct sharedObjectsStruct shared;
extern dictType setDictType;
extern dictType zsetDictType;
extern dictType clusterNodesDictType;
extern dictType clusterNodesBlackListDictType;
extern dictType dbDictType;
extern dictType shaScriptObjectDictType;
extern double R_Zero, R_PosInf, R_NegInf, R_Nan;
extern dictType hashDictType;
extern dictType replScriptCacheDictType;

/*-----------------------------------------------------------------------------
 * Functions prototypes
 *----------------------------------------------------------------------------*/

/* Utils */
long long ustime(void);
long long mstime(void);
mstime_t randomTimeError(mstime_t milliseconds);
void getRandomHexChars(char *p, unsigned int len);
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);
void exitFromChild(int retcode);
size_t redisPopcount(void *s, long count);
void serverSetProcTitle(char *title);

/* networking.c -- Networking and Client related operations */
client *createClient(int fd);
void closeTimedoutClients(void);
void freeClient(client *c);
void freeClientAsync(client *c);
void resetClient(client *c);
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask);
void *addDeferredMultiBulkLength(client *c);
void setDeferredMultiBulkLength(client *c, void *node, long length);
void processInputBuffer(client *c);
void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
void addReplyBulk(client *c, robj *obj);
void addReplyBulkCString(client *c, char *s);
void addReplyBulkCBuffer(client *c, void *p, size_t len);
void addReplyBulkLongLong(client *c, long long ll);
void addReply(client *c, robj *obj);
void addReplySds(client *c, sds s);
void addReplyBulkSds(client *c, sds s);
void addReplyError(client *c, char *err);
void addReplyStatus(client *c, char *status);
void addReplyStatusLength(client *c, char *s, size_t len);
void addReplyDouble(client *c, double d);
void addReplyLongLong(client *c, long long ll);
void addReplyMultiBulkLen(client *c, long length);
void copyClientOutputBuffer(client *dst, client *src);
void *dupClientReplyValue(void *o);
void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer);
void formatPeerId(char *peerid, size_t peerid_len, char *ip, int port);
char *getClientPeerId(client *client);
sds catClientInfoString(sds s, client *client);
sds getAllClientsInfoString(void);
void rewriteClientCommandVector(client *c, int argc, ...);
void rewriteClientCommandArgument(client *c, int i, robj *newval);
unsigned long getClientOutputBufferMemoryUsage(client *c);
void freeClientsInAsyncFreeQueue(void);
void asyncCloseClientOnOutputBufferLimitReached(client *c);
int getClientType(client *c);
int getClientTypeByName(char *name);
char *getClientTypeName(int class);
void flushSlavesOutputBuffers(void);
void disconnectSlaves(void);
int listenToPort(int port, int *fds, int *count);
void pauseClients(mstime_t duration);
int clientsArePaused(void);
int processEventsWhileBlocked(void);

#ifdef __GNUC__
void addReplyErrorFormat(client *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
void addReplyStatusFormat(client *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void addReplyErrorFormat(client *c, const char *fmt, ...);
void addReplyStatusFormat(client *c, const char *fmt, ...);
#endif

/* Jobs & Queues */
void unblockClientWaitingJobRepl(client *c);
void handleClientsBlockedOnQueues(void);

/* Disque object implementation */
void decrRefCount(robj *o);
void decrRefCountVoid(void *o);
void incrRefCount(robj *o);
robj *resetRefCount(robj *obj);
void freeStringObject(robj *o);
robj *createObject(int type, void *ptr);
robj *createStringObject(char *ptr, size_t len);
robj *createRawStringObject(char *ptr, size_t len);
robj *createEmbeddedStringObject(char *ptr, size_t len);
robj *dupStringObject(robj *o);
int isObjectRepresentableAsLongLong(robj *o, long long *llongval);
robj *tryObjectEncoding(robj *o);
robj *getDecodedObject(robj *o);
size_t stringObjectLen(robj *o);
robj *createStringObjectFromLongLong(long long value);
robj *createStringObjectFromLongDouble(long double value);
int getLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg);
int checkType(client *c, robj *o, int type);
int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg);
int getDoubleFromObjectOrReply(client *c, robj *o, double *target, const char *msg);
int getLongLongFromObject(robj *o, long long *target);
int getLongDoubleFromObject(robj *o, long double *target);
int getLongDoubleFromObjectOrReply(client *c, robj *o, long double *target, const char *msg);
char *strEncoding(int encoding);
int compareStringObjects(robj *a, robj *b);
int collateStringObjects(robj *a, robj *b);
int equalStringObjects(robj *a, robj *b);
int parseScanCursorOrReply(client *c, robj *o, unsigned long *cursor);
#define sdsEncodedObject(objptr) (objptr->encoding == DISQUE_ENCODING_RAW || objptr->encoding == DISQUE_ENCODING_EMBSTR)

/* Synchronous I/O with timeout */
ssize_t syncWrite(int fd, char *ptr, ssize_t size, long long timeout);
ssize_t syncRead(int fd, char *ptr, ssize_t size, long long timeout);
ssize_t syncReadLine(int fd, char *ptr, ssize_t size, long long timeout);

/* Generic persistence functions */
void startLoading(FILE *fp);
void loadingProgress(off_t pos);
void stopLoading(void);

/* AOF persistence */
void flushAppendOnlyFile(int force);
void feedAppendOnlyFile(robj **argv, int argc);
void aofRemoveTempFile(pid_t childpid);
int rewriteAppendOnlyFileBackground(void);
int loadAppendOnlyFile(char *filename);
void stopAppendOnly(void);
int startAppendOnly(void);
void backgroundRewriteDoneHandler(int exitcode, int bysignal);
void aofRewriteBufferReset(void);
unsigned long aofRewriteBufferSize(void);
int rewriteAppendOnlyFile(char *filename, int background);

/* Core functions */
void flushServerData(void);
int getMemoryWarningLevel(void);
int freeMemoryIfNeeded(void);
int processCommand(client *c);
void setupSignalHandlers(void);
struct serverCommand *lookupCommand(sds name);
struct serverCommand *lookupCommandByCString(char *s);
struct serverCommand *lookupCommandOrOriginal(sds name);
void call(client *c, int flags);
void propagate(struct serverCommand *cmd, robj **argv, int argc, int flags);
int prepareForShutdown(int flags);
#ifdef __GNUC__
void serverLog(int level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void serverLog(int level, const char *fmt, ...);
#endif
void serverLogRaw(int level, const char *msg);
void serverLogFromHandler(int level, const char *msg);
void usage(void);
void updateDictResizePolicy(void);
int htNeedsResize(dict *dict);
void oom(const char *msg);
void populateCommandTable(void);
void resetCommandTableStats(void);
void adjustOpenFilesLimit(void);
void closeListeningSockets(int unlink_unix_socket);
void updateCachedTime(void);
void resetServerStats(void);
unsigned int getLRUClock(void);
void queueCron(void);

/* Configuration */
void loadServerConfig(char *filename, char *options);
void appendServerSaveParams(time_t seconds, int changes);
void resetServerSaveParams(void);
struct rewriteConfigState; /* Forward declaration to export API. */
void rewriteConfigRewriteLine(struct rewriteConfigState *state, char *option, sds line, int force);
int rewriteConfig(char *path);

/* Cluster */
void clusterInit(void);
unsigned short crc16(const char *buf, int len);
void clusterCron(void);
void clusterBeforeSleep(void);

/* Blocked clients */
void processUnblockedClients(void);
void blockClient(client *c, int btype);
void unblockClient(client *c);
void replyToBlockedClientTimedOut(client *c);
int getTimeoutFromObjectOrReply(client *c, robj *object, mstime_t *timeout, int unit);

/* Git SHA1 */
char *disqueGitSHA1(void);
char *disqueGitDirty(void);
uint64_t disqueBuildId(void);

/* Commands prototypes */
void authCommand(client *c);
void pingCommand(client *c);
void infoCommand(client *c);
void shutdownCommand(client *c);
void monitorCommand(client *c);
void debugCommand(client *c);
void configCommand(client *c);
void clusterCommand(client *c);
void clientCommand(client *c);
void slowlogCommand(client *c);
void timeCommand(client *c);
void commandCommand(client *c);
void latencyCommand(client *c);
void addjobCommand(client *c);
void qlenCommand(client *c);
void getjobCommand(client *c);
void showCommand(client *c);
void ackjobCommand(client *c);
void fastackCommand(client *c);
void enqueueCommand(client *c);
void dequeueCommand(client *c);
void loadjobCommand(client *c);
void deljobCommand(client *c);
void bgrewriteaofCommand(client *c);
void helloCommand(client *c);
void qpeekCommand(client *c);
void qscanCommand(client *c);

#if defined(__GNUC__)
void *calloc(size_t count, size_t size) __attribute__ ((deprecated));
void free(void *ptr) __attribute__ ((deprecated));
void *malloc(size_t size) __attribute__ ((deprecated));
void *realloc(void *ptr, size_t size) __attribute__ ((deprecated));
#endif

/* Debugging stuff */
void _serverAssertWithInfo(client *c, robj *o, char *estr, char *file, int line);
void _serverAssert(char *estr, char *file, int line);
void _serverPanic(char *msg, char *file, int line);
void bugReportStart(void);
void serverLogObjectDebugInfo(robj *o);
void sigsegvHandler(int sig, siginfo_t *info, void *secret);
sds genDisqueInfoString(char *section);
void enableWatchdog(int period);
void disableWatchdog(void);
void watchdogScheduleSignal(int period);
void serverLogHexDump(int level, char *descr, void *value, size_t len);

#define disqueDebug(fmt, ...) \
    printf("DEBUG %s:%d > " fmt "\n", __FILE__, __LINE__, __VA_ARGS__)
#define disqueDebugMark() \
    printf("-- MARK %s:%d --\n", __FILE__, __LINE__)

#endif
