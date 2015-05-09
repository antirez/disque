#ifndef __DISQUE_CLUSTER_H
#define __DISQUE_CLUSTER_H

#include "job.h"
#include "queue.h"

/*-----------------------------------------------------------------------------
 * Disque cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/

#define DISQUE_CLUSTER_OK 0          /* Everything looks ok */
#define DISQUE_CLUSTER_FAIL 1        /* The cluster can't work */
#define DISQUE_CLUSTER_NAMELEN 40    /* sha1 hex length */
#define DISQUE_CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* The following defines are amount of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). */
#define DISQUE_CLUSTER_DEFAULT_NODE_TIMEOUT 15000
#define DISQUE_CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* Fail report validity. */
#define DISQUE_CLUSTER_FAIL_UNDO_TIME_MULT 2 /* Undo fail if master is back. */

struct clusterNode;

/* clusterLink encapsulates everything needed to talk with a remote node. */
typedef struct clusterLink {
    mstime_t ctime;             /* Link creation time */
    int fd;                     /* TCP socket file descriptor */
    sds sndbuf;                 /* Packet send buffer */
    sds rcvbuf;                 /* Packet reception buffer */
    struct clusterNode *node;   /* Node related to this link if any, or NULL */
} clusterLink;

/* Cluster node flags and macros. */
#define DISQUE_NODE_PFAIL     (1<<0) /* Failure? Need acknowledge */
#define DISQUE_NODE_FAIL      (1<<1) /* The node is believed to be malfunctioning */
#define DISQUE_NODE_MYSELF    (1<<2) /* This node is myself */
#define DISQUE_NODE_HANDSHAKE (1<<3) /* Node in handshake state. */
#define DISQUE_NODE_NOADDR    (1<<4) /* Node address unknown */
#define DISQUE_NODE_MEET      (1<<5) /* Send a MEET message to this node */
#define DISQUE_NODE_DELETED   (1<<6) /* Node no longer part of the cluster */
#define DISQUE_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

#define nodeInHandshake(n) ((n)->flags & DISQUE_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & DISQUE_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & DISQUE_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->flags & DISQUE_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & DISQUE_NODE_FAIL)

/* This structure represent elements of node->fail_reports. */
typedef struct clusterNodeFailReport {
    struct clusterNode *node;  /* Node reporting the failure condition. */
    mstime_t time;             /* Time of the last report from this node. */
} clusterNodeFailReport;

typedef struct clusterNode {
    char name[DISQUE_CLUSTER_NAMELEN]; /* Node name, hex string, sha1-size */
    mstime_t ctime; /* Node object creation time. */
    int flags;      /* DISQUE_NODE_... */
    mstime_t ping_sent;      /* Unix time we sent latest ping */
    mstime_t pong_received;  /* Unix time we received the pong */
    mstime_t fail_time;      /* Unix time when FAIL flag was set */
    char ip[DISQUE_IP_STR_LEN];  /* Latest known IP address of this node */
    int port;                   /* Latest known port of this node */
    clusterLink *link;          /* TCP/IP link with this node */
    list *fail_reports;         /* List of nodes signaling this as failing */
} clusterNode;

typedef struct clusterState {
    clusterNode *myself;  /* This node */
    int state;            /* DISQUE_CLUSTER_OK, DISQUE_CLUSTER_FAIL, ... */
    int size;             /* Num of known cluster nodes */
    dict *nodes;          /* Hash table of name -> clusterNode structures */
    dict *deleted_nodes;    /* Nodes removed from the cluster. */
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */
    int todo_before_sleep; /* Things to do in clusterBeforeSleep(). */
    /* Reachable nodes array. This array only lists reachable nodes
     * excluding our own node, and is used in order to quickly select
     * random receivers of messages to populate. */
    clusterNode **reachable_nodes;
    int reachable_nodes_count;
    /* Statistics. */
    long long stats_bus_messages_sent;  /* Num of msg sent via cluster bus. */
    long long stats_bus_messages_received; /* Num of msg rcvd via cluster bus.*/
} clusterState;

/* clusterState todo_before_sleep flags. */
#define CLUSTER_TODO_UPDATE_STATE (1<<0)
#define CLUSTER_TODO_SAVE_CONFIG (1<<1)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<2)

/* Disque cluster messages header */

/* Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
#define CLUSTERMSG_TYPE_PING 0          /* Ping. */
#define CLUSTERMSG_TYPE_PONG 1          /* Reply to Ping. */
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message. */
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing. */
#define CLUSTERMSG_TYPE_ADDJOB 4        /* Add a job to receiver. */
#define CLUSTERMSG_TYPE_GOTJOB 5        /* Job received acknowledge. */
#define CLUSTERMSG_TYPE_ENQUEUE 6      /* Enaueue the specified job. */
#define CLUSTERMSG_TYPE_QUEUED 7        /* Update your job qtime. */
#define CLUSTERMSG_TYPE_SETACK 8        /* Move job state as ACKed. */
#define CLUSTERMSG_TYPE_WILLQUEUE 9     /* I'll queue this job, ok? */
#define CLUSTERMSG_TYPE_GOTACK 10       /* Acknowledge SETACK. */
#define CLUSTERMSG_TYPE_DELJOB 11       /* Delete the specified job. */
#define CLUSTERMSG_TYPE_NEEDJOBS 12     /* I need jobs for some queue. */
#define CLUSTERMSG_TYPE_YOURJOBS 13     /* NEEDJOBS reply with jobs. */
#define CLUSTERMSG_TYPE_WORKING 14      /* Postpone re-queueing & dequeue */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages. */
typedef struct {
    char nodename[DISQUE_CLUSTER_NAMELEN];
    uint32_t ping_sent;
    uint32_t pong_received;
    char ip[DISQUE_IP_STR_LEN]; /* IP address last time it was seen */
    uint16_t port;              /* port last time it was seen */
    uint16_t flags;             /* node->flags copy */
    uint16_t notused1;          /* Some room for future improvements. */
    uint32_t notused2;
} clusterMsgDataGossip;

typedef struct {
    char nodename[DISQUE_CLUSTER_NAMELEN];
} clusterMsgDataFail;

/* This data section is used in different message types where we need to
 * transmit one or multiple full jobs.
 *
 * Used by: ADDJOB, YOURJOBS. */
typedef struct {
    uint32_t numjobs;   /* Number of jobs stored here. */
    uint32_t datasize;  /* Number of bytes following to describe jobs. */
    /* The variable data section here is composed of 4 bytes little endian
     * prefixed length + serialized job data for each job:
     * [4 bytes len] + [serialized job] + [4 bytes len] + [serialized job] ...
     * For a total of exactly 'datasize' bytes. */
     unsigned char jobs_data[8]; /* Defined as 8 bytes just for alignment. */
} clusterMsgDataJob;

/* This data section is used when we need to send just a job ID.
 *
 * Used by: GOTJOB, SETACK, and many more. */
typedef struct {
    char id[JOB_ID_LEN];
    uint32_t aux; /* Optional field:
                     For SETACK: Number of nodes that may have this message.
                     For QUEUEJOB: Delay starting from msg reception. */
} clusterMsgDataJobID;

/* This data section is used by NEEDJOBS to specify in what queue we need
 * a job, and how many jobs we request. */
typedef struct {
    uint32_t count;     /* How many jobs we request. */
    uint32_t qnamelen;  /* Queue name total length. */
    char qname[8];      /* Defined as 8 bytes just for alignment. */
} clusterMsgDataNeedJobs;

union clusterMsgData {
    /* PING, MEET and PONG. */
    struct {
        /* Array of N clusterMsgDataGossip structures */
        clusterMsgDataGossip gossip[1];
    } ping;

    /* FAIL. */
    struct {
        clusterMsgDataFail about;
    } fail;

    /* Messages with one or more full jobs. */
    struct {
        clusterMsgDataJob serialized;
    } jobs;

    /* Messages with a single Job ID. */
    struct {
        clusterMsgDataJobID job;
    } jobid;

    /* Messages requesting jobs (NEEDJOBS). */
    struct {
        clusterMsgDataNeedJobs about;
    } jobsreq;
};

#define CLUSTER_PROTO_VER 0 /* Cluster bus protocol version. */

typedef struct {
    char sig[4];        /* Siganture "DbuZ" (Disque Cluster message bus). */
    uint32_t totlen;    /* Total length of this message */
    uint16_t ver;       /* Protocol version, currently set to 0. */
    uint16_t notused0;  /* 2 bytes not used. */
    uint16_t type;      /* Message type */
    uint16_t count;     /* Only used for some kind of messages. */
    char sender[DISQUE_CLUSTER_NAMELEN]; /* Name of the sender node */
    char notused1[32];  /* 32 bytes reserved for future usage. */
    uint16_t port;      /* Sender TCP base port */
    uint16_t flags;     /* Sender node flags */
    unsigned char state; /* Cluster state from the POV of the sender */
    unsigned char mflags[3]; /* Message flags: CLUSTERMSG_FLAG[012]_... */
    union clusterMsgData data;
} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/* Message flags better specify the packet content or are used to
 * provide some information about the node state. */
#define CLUSTERMSG_FLAG0_NOREPLY (1<<0) /* Don't reply to this message. */

/*-----------------------------------------------------------------------------
 * Exported API.
 *----------------------------------------------------------------------------*/

extern clusterNode *myself;

clusterNode *clusterLookupNode(char *name);
void clusterUpdateReachableNodes(void);
int clusterReplicateJob(job *j, int repl, int noreply);
void clusterSendEnqueue(clusterNode *node, job *j, uint32_t delay);
void clusterBroadcastQueued(job *j);
void clusterBroadcastWorking(job *j);
void clusterBroadcastDelJob(job *j);
void clusterSendWillQueue(job *j);
void clusterSendSetAck(clusterNode *node, job *j);
void clusterSendNeedJobs(robj *qname, int numjobs, dict *nodes);
void clusterSendYourJobs(clusterNode *node, job **jobs, uint32_t count);
void clusterBroadcastJobIDMessage(dict *nodes, char *id, int type, uint32_t aux);

#endif /* __DISQUE_CLUSTER_H */
