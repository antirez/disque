/* Disque Cluster implementation.
 *
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

#include "server.h"
#include "cluster.h"
#include "endianconv.h"
#include "ack.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <math.h>

/* A global reference to myself is handy to make code more clear.
 * Myself always points to server.cluster->myself, that is, the clusterNode
 * that represents this node. */
clusterNode *myself = NULL;

clusterNode *createClusterNode(char *nodename, int flags);
int clusterAddNode(clusterNode *node);
void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void clusterReadHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void clusterSendPing(clusterLink *link, int type);
void clusterSendFail(char *nodename);
void clusterSendFailoverAuthIfNeeded(clusterNode *node, clusterMsg *request);
void clusterUpdateState(void);
int clusterNodeGetSlotBit(clusterNode *n, int slot);
sds clusterGenNodesDescription(int filter);
int clusterNodeAddSlave(clusterNode *master, clusterNode *slave);
void clusterSetMaster(clusterNode *n);
void clusterDoBeforeSleep(int flags);
void clusterSendUpdate(clusterLink *link, clusterNode *node);
void clusterSetNodeAsMaster(clusterNode *n);
void clusterDelNode(clusterNode *delnode);
void clusterShuffleReachableNodes(void);
void clusterSendGotJob(clusterNode *node, job *j);
void clusterSendGotAck(clusterNode *node, char *jobid, int known);
void clusterBroadcastQueued(job *j, unsigned char flags);
void clusterBroadcastDelJob(job *j);
sds representClusterNodeFlags(sds ci, uint16_t flags);

/* -----------------------------------------------------------------------------
 * Initialization
 * -------------------------------------------------------------------------- */

/* Load the cluster config from 'filename'.
 *
 * If the file does not exist or is zero-length (this may happen because
 * when we lock the nodes.conf file, we create a zero-length one for the
 * sake of locking if it does not already exist), C_ERR is returned.
 * If the configuration was loaded from the file, C_OK is returned. */
int clusterLoadConfig(char *filename) {
    FILE *fp = fopen(filename,"r");
    struct stat sb;
    char *line;
    int maxline, j;

    if (fp == NULL) {
        if (errno == ENOENT) {
            return C_ERR;
        } else {
            serverLog(LL_WARNING,
                "Loading the cluster node config from %s: %s",
                filename, strerror(errno));
            exit(1);
        }
    }

    /* Check if the file is zero-length: if so return C_ERR to signal
     * we have to write the config. */
    if (fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        fclose(fp);
        return C_ERR;
    }

    /* Parse the file. */
    maxline = 1024;
    line = zmalloc(maxline);
    while(fgets(line,maxline,fp) != NULL) {
        int argc;
        sds *argv;
        clusterNode *n;
        char *p, *s;

        /* Skip blank lines, they can be created either by users manually
         * editing nodes.conf or by the config writing process if stopped
         * before the truncate() call. */
        if (line[0] == '\n') continue;

        /* Split the line into arguments for processing. */
        argv = sdssplitargs(line,&argc);
        if (argv == NULL) goto fmterr;

        /* Handle the special "vars" line. Don't pretend it is the last
         * line even if it actually is when generated by Disque. */
        if (strcasecmp(argv[0],"vars") == 0) {
            for (j = 1; j < argc; j += 2) {
                if (strcasecmp(argv[j],"someVarNameHere") == 0) {
                    /* TODO: currently not used. */
                } else {
                    serverLog(LL_WARNING,
                        "Skipping unknown cluster config variable '%s'",
                        argv[j]);
                }
            }
            sdsfreesplitres(argv,argc);
            continue;
        }

        /* Regular config lines have at least eight fields */
        if (argc != 6) goto fmterr;

        /* Create this node if it does not exist */
        if (strlen(argv[0]) != CLUSTER_NAMELEN) goto fmterr;
        n = clusterLookupNode(argv[0]);
        if (!n) {
            n = createClusterNode(argv[0],0);
            clusterAddNode(n);
        }
        /* Address and port */
        if ((p = strrchr(argv[1],':')) == NULL) goto fmterr;
        *p = '\0';
        memcpy(n->ip,argv[1],strlen(argv[1])+1);
        n->port = atoi(p+1);

        /* Parse flags */
        p = s = argv[2];
        while(p) {
            p = strchr(s,',');
            if (p) *p = '\0';
            if (!strcasecmp(s,"myself")) {
                serverAssert(server.cluster->myself == NULL);
                myself = server.cluster->myself = n;
                n->flags |= CLUSTER_NODE_MYSELF;
            } else if (!strcasecmp(s,"fail?")) {
                n->flags |= CLUSTER_NODE_PFAIL;
            } else if (!strcasecmp(s,"fail")) {
                n->flags |= CLUSTER_NODE_FAIL;
                n->fail_time = mstime();
            } else if (!strcasecmp(s,"handshake")) {
                n->flags |= CLUSTER_NODE_HANDSHAKE;
            } else if (!strcasecmp(s,"noaddr")) {
                n->flags |= CLUSTER_NODE_NOADDR;
            } else if (!strcasecmp(s,"leaving")) {
                n->flags |= CLUSTER_NODE_LEAVING;
            } else if (!strcasecmp(s,"noflags")) {
                /* nothing to do */
            } else {
                serverPanic("Unknown flag in disque cluster config file");
            }
            if (p) s = p+1;
        }

        /* Set ping sent / pong received timestamps */
        if (atoi(argv[3])) n->ping_sent = mstime();
        if (atoi(argv[4])) n->pong_received = mstime();

        sdsfreesplitres(argv,argc);
    }
    /* Config sanity check */
    if (server.cluster->myself == NULL) goto fmterr;

    zfree(line);
    fclose(fp);

    serverLog(LL_NOTICE,"Node configuration loaded, I'm %.40s", myself->name);
    return C_OK;

fmterr:
    serverLog(LL_WARNING,
        "Unrecoverable error: corrupted cluster config file.");
    zfree(line);
    if (fp) fclose(fp);
    exit(1);
}

/* Cluster node configuration is exactly the same as CLUSTER NODES output.
 *
 * This function writes the node config and returns 0, on error -1
 * is returned.
 *
 * Note: we need to write the file in an atomic way from the point of view
 * of the POSIX filesystem semantics, so that if the server is stopped
 * or crashes during the write, we'll end with either the old file or the
 * new one. Since we have the full payload to write available we can use
 * a single write to write the whole file. If the pre-existing file was
 * bigger we pad our payload with newlines that are anyway ignored and truncate
 * the file afterward. */
int clusterSaveConfig(int do_fsync) {
    sds ci;
    size_t content_size;
    struct stat sb;
    int fd;

    server.cluster->todo_before_sleep &= ~CLUSTER_TODO_SAVE_CONFIG;

    /* Get the nodes description and concatenate our "vars" directive to
     * save other persistent state. */
    ci = clusterGenNodesDescription(CLUSTER_NODE_HANDSHAKE);
#if 0 /* TODO: check if we are going to use persistent vars. */
    ci = sdscatprintf(ci,"vars currentEpoch %llu lastVoteEpoch %llu\n",
        (unsigned long long) server.cluster->currentEpoch,
        (unsigned long long) server.cluster->lastVoteEpoch);
#endif
    content_size = sdslen(ci);

    if ((fd = open(server.cluster_configfile,O_WRONLY|O_CREAT,0644))
        == -1) goto err;

    /* Pad the new payload if the existing file length is greater. */
    if (fstat(fd,&sb) != -1) {
        if (sb.st_size > (off_t)content_size) {
            ci = sdsgrowzero(ci,sb.st_size);
            memset(ci+content_size,'\n',sb.st_size-content_size);
        }
    }
    if (write(fd,ci,sdslen(ci)) != (ssize_t)sdslen(ci)) goto err;
    if (do_fsync) {
        server.cluster->todo_before_sleep &= ~CLUSTER_TODO_FSYNC_CONFIG;
        fsync(fd);
    }

    /* Truncate the file if needed to remove the final \n padding that
     * is just garbage. */
    if (content_size != sdslen(ci) && ftruncate(fd,content_size) == -1) {
        /* ftruncate() failing is not a critical error. */
    }
    close(fd);
    sdsfree(ci);
    return 0;

err:
    if (fd != -1) close(fd);
    sdsfree(ci);
    return -1;
}

void clusterSaveConfigOrDie(int do_fsync) {
    if (clusterSaveConfig(do_fsync) == -1) {
        serverLog(LL_WARNING,"Fatal: can't update cluster config file.");
        exit(1);
    }
}

/* Lock the cluster config using flock(), and leaks the file descritor used to
 * acquire the lock so that the file will be locked forever.
 *
 * This works because we always update nodes.conf with a new version
 * in-place, reopening the file, and writing to it in place (later adjusting
 * the length with ftruncate()).
 *
 * On success C_OK is returned, otherwise an error is logged and
 * the function returns C_ERR to signal a lock was not acquired. */
int clusterLockConfig(char *filename) {
/* flock() does not exist on Solaris
 * and a fcntl-based solution won't help, as we constantly re-open that file,
 * which will release _all_ locks anyway
 */
#if !defined(__sun)
    /* To lock it, we need to open the file in a way it is created if
     * it does not exist, otherwise there is a race condition with other
     * processes. */
    int fd = open(filename,O_WRONLY|O_CREAT,0644);
    if (fd == -1) {
        serverLog(LL_WARNING,
            "Can't open %s in order to acquire a lock: %s",
            filename, strerror(errno));
        return C_ERR;
    }

    if (flock(fd,LOCK_EX|LOCK_NB) == -1) {
        if (errno == EWOULDBLOCK) {
            serverLog(LL_WARNING,
                 "Sorry, the cluster configuration file %s is already used "
                 "by a different Disque node. Please make sure that "
                 "different nodes use different cluster configuration "
                 "files.", filename);
        } else {
            serverLog(LL_WARNING,
                "Impossible to lock %s: %s", filename, strerror(errno));
        }
        close(fd);
        return C_ERR;
    }
    /* Lock acquired: leak the 'fd' by not closing it, so that we'll retain the
     * lock to the file as long as the process exists. */
#endif /* __sun */

    return C_OK;
}

void clusterInit(void) {
    int saveconf = 0;

    server.cluster = zmalloc(sizeof(clusterState));
    server.cluster->myself = NULL;
    server.cluster->state = CLUSTER_OK;
    server.cluster->size = 1;
    server.cluster->todo_before_sleep = 0;
    server.cluster->nodes = dictCreate(&clusterNodesDictType,NULL);
    server.cluster->deleted_nodes = dictCreate(&clusterNodesDictType,NULL);
    server.cluster->nodes_black_list =
        dictCreate(&clusterNodesBlackListDictType,NULL);
    server.cluster->stats_bus_messages_sent = 0;
    server.cluster->stats_bus_messages_received = 0;
    server.cluster->reachable_nodes = NULL;
    server.cluster->reachable_nodes_count = 0;

    /* Lock the cluster config file to make sure every node uses
     * its own nodes.conf. */
    if (clusterLockConfig(server.cluster_configfile) == C_ERR)
        exit(1);

    /* Load or create a new nodes configuration. */
    if (clusterLoadConfig(server.cluster_configfile) == C_ERR) {
        /* No configuration found. We will just use the random name provided
         * by the createClusterNode() function. */
        myself = server.cluster->myself =
            createClusterNode(NULL,CLUSTER_NODE_MYSELF);
        serverLog(LL_NOTICE,"No cluster configuration found, I'm %.40s",
            myself->name);
        clusterAddNode(myself);
        saveconf = 1;
    }
    if (saveconf) clusterSaveConfigOrDie(1);

    /* We need a listening TCP port for our cluster messaging needs. */
    server.cfd_count = 0;

    /* Port sanity check II
     * The other handshake port check is triggered too late to stop
     * us from trying to use a too-high cluster port number. */
    if (server.port > (65535-CLUSTER_PORT_INCR)) {
        serverLog(LL_WARNING, "Disque port number too high. "
                   "Cluster communication port is 10,000 port "
                   "numbers higher than your Disque node port. "
                   "Your Disque node port number must be "
                   "lower than 55535.");
        exit(1);
    }

    if (listenToPort(server.port+CLUSTER_PORT_INCR,
        server.cfd,&server.cfd_count) == C_ERR)
    {
        exit(1);
    } else {
        int j;

        for (j = 0; j < server.cfd_count; j++) {
            if (aeCreateFileEvent(server.el, server.cfd[j], AE_READABLE,
                clusterAcceptHandler, NULL) == AE_ERR)
                    serverPanic("Unrecoverable error creating Disque Cluster "
                                "Bus file event.");
        }
    }

    /* Set myself->port to my listening port, we'll just need to discover
     * the IP address via MEET messages. */
    myself->port = server.port;

    /* Update state and list of reachable nodes. */
    clusterUpdateState();
    clusterUpdateReachableNodes();
}

/* Reset a node performing a soft or hard reset:
 *
 * 1) All other nodes are forget.
 * 2) Only for hard reset: a new Node ID is generated.
 * 3) The new configuration is saved and the cluster state updated.
 * 4) Flush away all the jobs. */
void clusterReset(int hard) {
    dictIterator *di;
    dictEntry *de;

    /* Forget all the nodes, but myself. */
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node == myself) continue;
        clusterDelNode(node);
    }
    dictReleaseIterator(di);

    /* Information about reachable nodes is no longer valid. Update it. */
    clusterUpdateReachableNodes();

    /* Hard reset only: change node ID. */
    if (hard) {
        sds oldname;

        /* To change the Node ID we need to remove the old name from the
         * nodes table, change the ID, and re-add back with new name. */
        oldname = sdsnewlen(myself->name, CLUSTER_NAMELEN);
        dictDelete(server.cluster->nodes,oldname);
        sdsfree(oldname);
        getRandomHexChars(myself->name, CLUSTER_NAMELEN);
        clusterAddNode(myself);
    }

    flushServerData();

    /* TODO: flush the deleted nodes hash table and the deleted nodes
     * entries. There are no longer jobs or queues that may reference
     * them. */

    /* Make sure to persist the new config and update the state. */
    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                         CLUSTER_TODO_UPDATE_STATE|
                         CLUSTER_TODO_FSYNC_CONFIG);
}

/* -----------------------------------------------------------------------------
 * CLUSTER communication link
 * -------------------------------------------------------------------------- */

clusterLink *createClusterLink(clusterNode *node) {
    clusterLink *link = zmalloc(sizeof(*link));
    link->ctime = mstime();
    link->sndbuf = sdsempty();
    link->rcvbuf = sdsempty();
    link->node = node;
    link->fd = -1;
    return link;
}

/* Free a cluster link, but does not free the associated node of course.
 * This function will just make sure that the original node associated
 * with this link will have the 'link' field set to NULL. */
void freeClusterLink(clusterLink *link) {
    if (link->fd != -1) {
        aeDeleteFileEvent(server.el, link->fd, AE_WRITABLE);
        aeDeleteFileEvent(server.el, link->fd, AE_READABLE);
    }
    sdsfree(link->sndbuf);
    sdsfree(link->rcvbuf);
    if (link->node)
        link->node->link = NULL;
    close(link->fd);
    zfree(link);
}

#define MAX_CLUSTER_ACCEPTS_PER_CALL 1000
void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    int max = MAX_CLUSTER_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    clusterLink *link;
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_VERBOSE,
                    "Accepting cluster node: %s", server.neterr);
            return;
        }
        anetNonBlock(NULL,cfd);
        anetEnableTcpNoDelay(NULL,cfd);

        /* Use non-blocking I/O for cluster messages. */
        serverLog(LL_VERBOSE,"Accepted cluster node %s:%d", cip, cport);
        /* Create a link object we use to handle the connection.
         * It gets passed to the readable handler when data is available.
         * Initiallly the link->node pointer is set to NULL as we don't know
         * which node is, but the right node is references once we know the
         * node identity. */
        link = createClusterLink(NULL);
        link->fd = cfd;
        aeCreateFileEvent(server.el,cfd,AE_READABLE,clusterReadHandler,link);
    }
}

/* -----------------------------------------------------------------------------
 * CLUSTER node API
 * -------------------------------------------------------------------------- */

/* Create a new cluster node, with the specified flags.
 * If "nodename" is NULL this is considered a first handshake and a random
 * node name is assigned to this node (it will be fixed later when we'll
 * receive the first pong).
 *
 * The node is created and returned to the user, but it is not automatically
 * added to the nodes hash table. */
clusterNode *createClusterNode(char *nodename, int flags) {
    clusterNode *node = zmalloc(sizeof(*node));

    if (nodename)
        memcpy(node->name, nodename, CLUSTER_NAMELEN);
    else
        getRandomHexChars(node->name, CLUSTER_NAMELEN);
    node->ctime = mstime();
    node->flags = flags;
    node->ping_sent = node->pong_received = 0;
    node->fail_time = 0;
    node->link = NULL;
    memset(node->ip,0,sizeof(node->ip));
    node->port = 0;
    node->fail_reports = listCreate();
    listSetFreeMethod(node->fail_reports,zfree);
    return node;
}

/* This function is called every time we get a failure report from a node.
 * The side effect is to populate the fail_reports list (or to update
 * the timestamp of an existing report).
 *
 * 'failing' is the node that is in failure state according to the
 * 'sender' node.
 *
 * The function returns 0 if it just updates a timestamp of an existing
 * failure report from the same sender. 1 is returned if a new failure
 * report is created. */
int clusterNodeAddFailureReport(clusterNode *failing, clusterNode *sender) {
    list *l = failing->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;

    /* If a failure report from the same sender already exists, just update
     * the timestamp. */
    listRewind(l,&li);
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (fr->node == sender) {
            fr->time = mstime();
            return 0;
        }
    }

    /* Otherwise create a new report. */
    fr = zmalloc(sizeof(*fr));
    fr->node = sender;
    fr->time = mstime();
    listAddNodeTail(l,fr);
    return 1;
}

/* Remove failure reports that are too old, where too old means reasonably
 * older than the global node timeout. Note that anyway for a node to be
 * flagged as FAIL we need to have a local PFAIL state that is at least
 * older than the global node timeout, so we don't just trust the number
 * of failure reports from other nodes. */
void clusterNodeCleanupFailureReports(clusterNode *node) {
    list *l = node->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;
    mstime_t maxtime = server.cluster_node_timeout *
                     CLUSTER_FAIL_REPORT_VALIDITY_MULT;
    mstime_t now = mstime();

    listRewind(l,&li);
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (now - fr->time > maxtime) listDelNode(l,ln);
    }
}

/* Remove the failing report for 'node' if it was previously considered
 * failing by 'sender'. This function is called when a node informs us via
 * gossip that a node is OK from its point of view (no FAIL or PFAIL flags).
 *
 * Note that this function is called relatively often as it gets called even
 * when there are no nodes failing, and is O(N), however when the cluster is
 * fine the failure reports list is empty so the function runs in constant
 * time.
 *
 * The function returns 1 if the failure report was found and removed.
 * Otherwise 0 is returned. */
int clusterNodeDelFailureReport(clusterNode *node, clusterNode *sender) {
    list *l = node->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;

    /* Search for a failure report from this sender. */
    listRewind(l,&li);
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (fr->node == sender) break;
    }
    if (!ln) return 0; /* No failure report from this sender. */

    /* Remove the failure report. */
    listDelNode(l,ln);
    clusterNodeCleanupFailureReports(node);
    return 1;
}

/* Return the number of external nodes that believe 'node' is failing,
 * not including this node, that may have a PFAIL or FAIL state for this
 * node as well. */
int clusterNodeFailureReportsCount(clusterNode *node) {
    clusterNodeCleanupFailureReports(node);
    return listLength(node->fail_reports);
}

/* Free a node, however if the node was part of the cluster (no handshake
 * flag is set), the node is actually just disconnected, moved into
 * a deleted_nodes hash table, and flagged as deleted: we may still have
 * references of it in jobs and other places, and the cleanup is a useless
 * and complex effort.
 *
 * Note that this is the low level part of freeing a node, and should only
 * be called by clusterDelNode() that also handles the higher level logic
 * of removing a node from the cluster. */
void freeClusterNode(clusterNode *n) {
    serverAssert(dictDelete(server.cluster->nodes,n->name) == DICT_OK);
    if (n->link) freeClusterLink(n->link);
    /* We can free nodes in handshake state, but we can't free nodes
     * that were part of the cluster: they may still be referenced
     * by jobs. */
    if (nodeInHandshake(n)) {
        listRelease(n->fail_reports);
        zfree(n);
    } else {
        dictAdd(server.cluster->deleted_nodes,n->name,n);
        n->flags |= CLUSTER_NODE_DELETED;
    }
}

/* Add a node to the nodes hash table */
int clusterAddNode(clusterNode *node) {
    int retval;

    retval = dictAdd(server.cluster->nodes, node->name, node);
    return (retval == DICT_OK) ? C_OK : C_ERR;
}

/* Remove a node from the cluster:
 * 1) Remove all the failure reports sent by this node.
 * 2) Free the node using freeClusterNode() that will remove it from
 *    the hash table and actually free the node resources. */
void clusterDelNode(clusterNode *delnode) {
    dictIterator *di;
    dictEntry *de;

    /* 1) Remove failure reports. */
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node == delnode) continue;
        clusterNodeDelFailureReport(node,delnode);
    }
    dictReleaseIterator(di);

    /* 2) Free the node, unlinking it from the cluster. */
    freeClusterNode(delnode);
}

/* Node lookup by name */
clusterNode *clusterLookupNode(char *name) {
    dictEntry *de = dictFind(server.cluster->nodes,name);
    if (de == NULL) return NULL;
    return dictGetVal(de);
}

/* This is only used after the handshake. When we connect a given IP/PORT
 * as a result of CLUSTER MEET we don't have the node name yet, so we
 * pick a random one, and will fix it when we receive the PONG request using
 * this function. */
void clusterRenameNode(clusterNode *node, char *newname) {
    int retval;
    sds s = sdsnewlen(node->name, CLUSTER_NAMELEN);

    serverLog(LL_DEBUG,"Renaming node %.40s into %.40s",
        node->name, newname);
    retval = dictDelete(server.cluster->nodes, s);
    sdsfree(s);
    serverAssert(retval == DICT_OK);
    memcpy(node->name, newname, CLUSTER_NAMELEN);
    clusterAddNode(node);
}

/* -----------------------------------------------------------------------------
 * CLUSTER nodes blacklist
 *
 * The nodes blacklist is just a way to ensure that a given node with a given
 * Node ID is not readded before some time elapsed (this time is specified
 * in seconds in CLUSTER_BLACKLIST_TTL).
 *
 * This is useful when we want to remove a node from the cluster completely:
 * when CLUSTER FORGET is called, it also puts the node into the blacklist so
 * that even if we receive gossip messages from other nodes that still remember
 * about the node we want to remove, we don't re-add it before some time.
 *
 * Currently the CLUSTER_BLACKLIST_TTL is set to 1 minute, this means
 * that disque-trib has 60 seconds to send CLUSTER FORGET messages to nodes
 * in the cluster without dealing with the problem of other nodes re-adding
 * back the node to nodes we already sent the FORGET command to.
 *
 * The data structure used is a hash table with an sds string representing
 * the node ID as key, and the time when it is ok to re-add the node as
 * value.
 * -------------------------------------------------------------------------- */

#define CLUSTER_BLACKLIST_TTL 60      /* 1 minute. */


/* Before of the addNode() or Exists() operations we always remove expired
 * entries from the black list. This is an O(N) operation but it is not a
 * problem since add / exists operations are called very infrequently and
 * the hash table is supposed to contain very little elements at max.
 * However without the cleanup during long uptimes and with some automated
 * node add/removal procedures, entries could accumulate. */
void clusterBlacklistCleanup(void) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes_black_list);
    while((de = dictNext(di)) != NULL) {
        int64_t expire = dictGetUnsignedIntegerVal(de);

        if (expire < server.unixtime)
            dictDelete(server.cluster->nodes_black_list,dictGetKey(de));
    }
    dictReleaseIterator(di);
}

/* Cleanup the blacklist and add a new node ID to the black list. */
void clusterBlacklistAddNode(clusterNode *node) {
    dictEntry *de;
    sds id = sdsnewlen(node->name,CLUSTER_NAMELEN);

    clusterBlacklistCleanup();
    if (dictAdd(server.cluster->nodes_black_list,id,NULL) == DICT_OK) {
        /* If the key was added, duplicate the sds string representation of
         * the key for the next lookup. We'll free it at the end. */
        id = sdsdup(id);
    }
    de = dictFind(server.cluster->nodes_black_list,id);
    dictSetUnsignedIntegerVal(de,time(NULL)+CLUSTER_BLACKLIST_TTL);
    sdsfree(id);
}

/* Return non-zero if the specified node ID exists in the blacklist.
 * You don't need to pass an sds string here, any pointer to 40 bytes
 * will work. */
int clusterBlacklistExists(char *nodeid) {
    sds id = sdsnewlen(nodeid,CLUSTER_NAMELEN);
    int retval;

    clusterBlacklistCleanup();
    retval = dictFind(server.cluster->nodes_black_list,id) != NULL;
    sdsfree(id);
    return retval;
}

/* -----------------------------------------------------------------------------
 * CLUSTER messages exchange - PING/PONG and gossip
 * -------------------------------------------------------------------------- */

/* This function checks if a given node should be marked as FAIL.
 * It happens if the following conditions are met:
 *
 * 1) We received enough failure reports from other master nodes via gossip.
 *    Enough means that the majority of the masters signaled the node is
 *    down recently.
 * 2) We believe this node is in PFAIL state.
 *
 * If a failure is detected we also inform the whole cluster about this
 * event trying to force every other node to set the FAIL flag for the node.
 *
 * Note that the form of agreement used here is weak, as we collect the majority
 * of masters state during some time, and even if we force agreement by
 * propagating the FAIL message, because of partitions we may not reach every
 * node. However:
 *
 * 1) Either we reach the majority and eventually the FAIL state will propagate
 *    to all the cluster.
 * 2) Or there is no majority so no slave promotion will be authorized and the
 *    FAIL flag will be cleared after some time.
 */
void markNodeAsFailingIfNeeded(clusterNode *node) {
    int failures;
    int needed_quorum = (server.cluster->size / 2) + 1;

    if (!nodeTimedOut(node)) return; /* We can reach it. */
    if (nodeFailed(node)) return; /* Already FAILing. */

    failures = clusterNodeFailureReportsCount(node);
    /* Also count myself as a voter if I'm a master. */
    failures++;
    if (failures < needed_quorum) return; /* No weak agreement from masters. */

    serverLog(LL_VERBOSE,
        "Marking node %.40s as failing (quorum reached).", node->name);

    /* Mark the node as failing. */
    node->flags &= ~CLUSTER_NODE_PFAIL;
    node->flags |= CLUSTER_NODE_FAIL;
    node->fail_time = mstime();

    /* Broadcast the failing node name to everybody, forcing all the other
     * reachable nodes to flag the node as FAIL. */
    clusterSendFail(node->name);
    clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
}

/* This function is called only if a node is marked as FAIL, but we are able
 * to reach it again. It checks if there are the conditions to undo the FAIL
 * state. */
void clearNodeFailureIfNeeded(clusterNode *node) {
    serverAssert(nodeFailed(node));

    /* We always clear the FAIL flag if we can contact the node again. */
    serverLog(LL_VERBOSE,
        "Clear FAIL state for node %.40s: it is reachable again.",
            node->name);
    node->flags &= ~CLUSTER_NODE_FAIL;
    clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
}

/* Return true if we already have a node in HANDSHAKE state matching the
 * specified ip address and port number. This function is used in order to
 * avoid adding a new handshake node for the same address multiple times. */
int clusterHandshakeInProgress(char *ip, int port) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (!nodeInHandshake(node)) continue;
        if (!strcasecmp(node->ip,ip) && node->port == port) break;
    }
    dictReleaseIterator(di);
    return de != NULL;
}

/* Start an handshake with the specified address if there is not one
 * already in progress. Returns non-zero if the handshake was actually
 * started. On error zero is returned and errno is set to one of the
 * following values:
 *
 * EAGAIN - There is already an handshake in progress for this address.
 * EINVAL - IP or port are not valid. */
int clusterStartHandshake(char *ip, int port) {
    clusterNode *n;
    char norm_ip[NET_IP_STR_LEN];
    struct sockaddr_storage sa;

    /* IP sanity check */
    if (inet_pton(AF_INET,ip,
            &(((struct sockaddr_in *)&sa)->sin_addr)))
    {
        sa.ss_family = AF_INET;
    } else if (inet_pton(AF_INET6,ip,
            &(((struct sockaddr_in6 *)&sa)->sin6_addr)))
    {
        sa.ss_family = AF_INET6;
    } else {
        errno = EINVAL;
        return 0;
    }

    /* Port sanity check */
    if (port <= 0 || port > (65535-CLUSTER_PORT_INCR)) {
        errno = EINVAL;
        return 0;
    }

    /* Set norm_ip as the normalized string representation of the node
     * IP address. */
    memset(norm_ip,0,NET_IP_STR_LEN);
    if (sa.ss_family == AF_INET)
        inet_ntop(AF_INET,
            (void*)&(((struct sockaddr_in *)&sa)->sin_addr),
            norm_ip,NET_IP_STR_LEN);
    else
        inet_ntop(AF_INET6,
            (void*)&(((struct sockaddr_in6 *)&sa)->sin6_addr),
            norm_ip,NET_IP_STR_LEN);

    if (clusterHandshakeInProgress(norm_ip,port)) {
        errno = EAGAIN;
        return 0;
    }

    /* Add the node with a random address (NULL as first argument to
     * createClusterNode()). Everything will be fixed during the
     * handshake. */
    n = createClusterNode(NULL,CLUSTER_NODE_HANDSHAKE|CLUSTER_NODE_MEET);
    memcpy(n->ip,norm_ip,sizeof(n->ip));
    n->port = port;
    clusterAddNode(n);
    return 1;
}

/* Process the gossip section of PING or PONG packets.
 * Note that this function assumes that the packet is already sanity-checked
 * by the caller, not in the content of the gossip section, but in the
 * length. */
void clusterProcessGossipSection(clusterMsg *hdr, clusterLink *link) {
    uint16_t count = ntohs(hdr->count);
    clusterMsgDataGossip *g = (clusterMsgDataGossip*) hdr->data.ping.gossip;
    clusterNode *sender = link->node ? link->node : clusterLookupNode(hdr->sender);

    while(count--) {
        uint16_t flags = ntohs(g->flags);
        clusterNode *node;
        sds ci;

        ci = representClusterNodeFlags(sdsempty(), flags);
        serverLog(LL_DEBUG,"GOSSIP %.40s %s:%d %s",
            g->nodename,
            g->ip,
            ntohs(g->port),
            ci);
        sdsfree(ci);

        /* Update our state accordingly to the gossip sections */
        node = clusterLookupNode(g->nodename);
        if (node) {
            /* We already know this node. */
            if (sender && node != myself) {
                if (flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_PFAIL)) {
                    if (clusterNodeAddFailureReport(node,sender)) {
                        serverLog(LL_VERBOSE,
                            "Node %.40s reported node %.40s as not reachable.",
                            sender->name, node->name);
                    }
                    markNodeAsFailingIfNeeded(node);
                } else {
                    if (clusterNodeDelFailureReport(node,sender)) {
                        serverLog(LL_VERBOSE,
                            "Node %.40s reported node %.40s is back online.",
                            sender->name, node->name);
                    }
                }
            }

            /* If we already know this node, but it is not reachable, and
             * we see a different address in the gossip section, start an
             * handshake with the (possibly) new address: this will result
             * into a node address update if the handshake will be
             * successful. */
            if (node->flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_PFAIL) &&
                (strcasecmp(node->ip,g->ip) || node->port != ntohs(g->port)))
            {
                clusterStartHandshake(g->ip,ntohs(g->port));
            }
        } else {
            /* If it's not in NOADDR state and we don't have it, we
             * start a handshake process against this IP/PORT pairs.
             *
             * Note that we require that the sender of this gossip message
             * is a well known node in our cluster, otherwise we risk
             * joining another cluster. */
            if (sender &&
                !(flags & CLUSTER_NODE_NOADDR) &&
                !clusterBlacklistExists(g->nodename))
            {
                clusterStartHandshake(g->ip,ntohs(g->port));
            }
        }

        /* Next node */
        g++;
    }
}

/* IP -> string conversion. 'buf' is supposed to at least be 46 bytes. */
void nodeIp2String(char *buf, clusterLink *link) {
    anetPeerToString(link->fd, buf, NET_IP_STR_LEN, NULL);
}

/* Update the node address to the IP address that can be extracted
 * from link->fd, and at the specified port.
 * Also disconnect the node link so that we'll connect again to the new
 * address.
 *
 * If the ip/port pair are already correct no operation is performed at
 * all.
 *
 * The function returns 0 if the node address is still the same,
 * otherwise 1 is returned. */
int nodeUpdateAddressIfNeeded(clusterNode *node, clusterLink *link, int port) {
    char ip[NET_IP_STR_LEN] = {0};

    /* We don't proceed if the link is the same as the sender link, as this
     * function is designed to see if the node link is consistent with the
     * symmetric link that is used to receive PINGs from the node.
     *
     * As a side effect this function never frees the passed 'link', so
     * it is safe to call during packet processing. */
    if (link == node->link) return 0;

    nodeIp2String(ip,link);
    if (node->port == port && strcmp(ip,node->ip) == 0) return 0;

    /* IP / port is different, update it. */
    memcpy(node->ip,ip,sizeof(ip));
    node->port = port;
    if (node->link) freeClusterLink(node->link);
    node->flags &= ~CLUSTER_NODE_NOADDR;
    serverLog(LL_WARNING,"Address updated for node %.40s, now %s:%d",
        node->name, node->ip, node->port);

    return 1;
}

/* When this function is called, there is a packet to process starting
 * at node->rcvbuf. Releasing the buffer is up to the caller, so this
 * function should just handle the higher level stuff of processing the
 * packet, modifying the cluster state if needed.
 *
 * The function returns 1 if the link is still valid after the packet
 * was processed, otherwise 0 if the link was freed since the packet
 * processing lead to some inconsistency error (for instance a PONG
 * received from the wrong sender ID). */
int clusterProcessPacket(clusterLink *link) {
    clusterMsg *hdr = (clusterMsg*) link->rcvbuf;
    uint32_t totlen = ntohl(hdr->totlen);
    uint16_t type = ntohs(hdr->type);
    clusterNode *sender;

    server.cluster->stats_bus_messages_received++;
    serverLog(LL_DEBUG,"--- Processing packet of type %d, %lu bytes",
        type, (unsigned long) totlen);

    /* Perform sanity checks */
    if (totlen < 16) return 1; /* At least signature, version, totlen, count. */
    if (ntohs(hdr->ver) != CLUSTER_PROTO_VER)
        return 1; /* Can't handle versions other than the current one.*/
    if (totlen > sdslen(link->rcvbuf)) return 1;
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET)
    {
        uint16_t count = ntohs(hdr->count);
        uint32_t explen; /* expected length of this packet */

        explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        explen += (sizeof(clusterMsgDataGossip)*count);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataFail);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_REPLJOB ||
               type == CLUSTERMSG_TYPE_YOURJOBS) {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataJob) -
                  sizeof(hdr->data.jobs.serialized.jobs_data) +
                  ntohl(hdr->data.jobs.serialized.datasize);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_GOTJOB ||
               type == CLUSTERMSG_TYPE_ENQUEUE ||
               type == CLUSTERMSG_TYPE_QUEUED ||
               type == CLUSTERMSG_TYPE_WORKING ||
               type == CLUSTERMSG_TYPE_SETACK ||
               type == CLUSTERMSG_TYPE_GOTACK ||
               type == CLUSTERMSG_TYPE_DELJOB ||
               type == CLUSTERMSG_TYPE_WILLQUEUE)
    {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataJobID);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_NEEDJOBS ||
               type == CLUSTERMSG_TYPE_PAUSE)
    {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        explen += sizeof(clusterMsgDataQueueOp) - 8;
        if (totlen < explen) return 1;
        explen += ntohl(hdr->data.queueop.about.qnamelen);
        if (totlen != explen) return 1;
    }

    /* Check if the sender is a known node. */
    sender = clusterLookupNode(hdr->sender);

    /* Initial processing of PING and MEET requests replying with a PONG. */
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_MEET) {
        serverLog(LL_DEBUG,"Ping packet received: %p", (void*)link->node);

        /* We use incoming MEET messages in order to set the address
         * for 'myself', since only other cluster nodes will send us
         * MEET messages on handshakes, when the cluster joins, or
         * later if we changed address, and those nodes will use our
         * official address to connect to us. So by obtaining this address
         * from the socket is a simple way to discover / update our own
         * address in the cluster without it being hardcoded in the config.
         *
         * However if we don't have an address at all, we update the address
         * even with a normal PING packet. If it's wrong it will be fixed
         * by MEET later. */
        if (type == CLUSTERMSG_TYPE_MEET || myself->ip[0] == '\0') {
            char ip[NET_IP_STR_LEN];

            if (anetSockName(link->fd,ip,sizeof(ip),NULL) != -1 &&
                strcmp(ip,myself->ip))
            {
                memcpy(myself->ip,ip,NET_IP_STR_LEN);
                serverLog(LL_WARNING,"IP address for this node updated to %s",
                    myself->ip);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            }
        }

        /* Add this node if it is new for us and the msg type is MEET.
         * In this stage we don't try to add the node with the right
         * flags, slaveof pointer, and so forth, as this details will be
         * resolved when we'll receive PONGs from the node. */
        if (!sender && type == CLUSTERMSG_TYPE_MEET) {
            clusterNode *node;

            node = createClusterNode(NULL,CLUSTER_NODE_HANDSHAKE);
            nodeIp2String(node->ip,link);
            node->port = ntohs(hdr->port);
            clusterAddNode(node);
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
        }

        /* If this is a MEET packet from an unknown node, we still process
         * the gossip section here since we have to trust the sender because
         * of the message type. */
        if (!sender && type == CLUSTERMSG_TYPE_MEET)
            clusterProcessGossipSection(hdr,link);

        /* Anyway reply with a PONG */
        clusterSendPing(link,CLUSTERMSG_TYPE_PONG);
    }

    /* PING, PONG, MEET: process config information. */
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET)
    {
        serverLog(LL_DEBUG,"%s packet received: %p",
            type == CLUSTERMSG_TYPE_PING ? "ping" : "pong",
            (void*)link->node);
        if (link->node) {
            if (nodeInHandshake(link->node)) {
                /* If we already have this node, try to change the
                 * IP/port of the node with the new one. */
                if (sender) {
                    serverLog(LL_VERBOSE,
                        "Handshake: we already know node %.40s, "
                        "updating the address if needed.", sender->name);
                    if (nodeUpdateAddressIfNeeded(sender,link,ntohs(hdr->port)))
                    {
                        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                             CLUSTER_TODO_UPDATE_STATE);
                    }
                    /* Free this node as we already have it. This will
                     * cause the link to be freed as well. */
                    clusterDelNode(link->node);
                    return 0;
                }

                /* First thing to do is replacing the random name with the
                 * right node name if this was a handshake stage. */
                clusterRenameNode(link->node, hdr->sender);
                serverLog(LL_DEBUG,"Handshake with node %.40s completed.",
                    link->node->name);
                link->node->flags &= ~CLUSTER_NODE_HANDSHAKE;
                clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|
                                     CLUSTER_TODO_SAVE_CONFIG);
            } else if (memcmp(link->node->name,hdr->sender,
                        CLUSTER_NAMELEN) != 0)
            {
                /* If the reply has a non matching node ID we
                 * disconnect this node and set it as not having an associated
                 * address. */
                serverLog(LL_DEBUG,"PONG contains mismatching sender ID");
                link->node->flags |= CLUSTER_NODE_NOADDR;
                link->node->ip[0] = '\0';
                link->node->port = 0;
                freeClusterLink(link);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                return 0;
            }
        }

        /* Update the node address if it changed. */
        if (sender && type == CLUSTERMSG_TYPE_PING &&
            !nodeInHandshake(sender) &&
            nodeUpdateAddressIfNeeded(sender,link,ntohs(hdr->port)))
        {
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                 CLUSTER_TODO_UPDATE_STATE);
        }

        /* Update our info about the node */
        if (link->node && type == CLUSTERMSG_TYPE_PONG) {
            link->node->pong_received = mstime();
            link->node->ping_sent = 0;

            /* The PFAIL condition can be reversed without external
             * help if it is momentary (that is, if it does not
             * turn into a FAIL state).
             *
             * The FAIL condition is also reversible under specific
             * conditions detected by clearNodeFailureIfNeeded(). */
            if (nodeTimedOut(link->node)) {
                link->node->flags &= ~CLUSTER_NODE_PFAIL;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                     CLUSTER_TODO_UPDATE_STATE);
            } else if (nodeFailed(link->node)) {
                clearNodeFailureIfNeeded(link->node);
            }
        }

        /* Copy certain flags from what the node publishes. */
        if (sender) {
            int old_flags = sender->flags;
            int reported_flags = ntohs(hdr->flags);
            int flags_to_copy = CLUSTER_NODE_LEAVING;
            sender->flags &= ~flags_to_copy;
            sender->flags |= (reported_flags & flags_to_copy);

            /* Currently we just save the config without FSYNC nor
             * update of the cluster state, since the only flag we update
             * here is LEAVING which is non critical to persist. */
            if (sender->flags != old_flags)
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
        }

        /* Get info from the gossip section */
        if (sender) clusterProcessGossipSection(hdr,link);
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        clusterNode *failing;

        if (!sender) return 1;
        failing = clusterLookupNode(hdr->data.fail.about.nodename);
        if (failing &&
            !(failing->flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_MYSELF)))
        {
            serverLog(LL_VERBOSE,
                "FAIL message received from %.40s about %.40s",
                hdr->sender, hdr->data.fail.about.nodename);
            failing->flags |= CLUSTER_NODE_FAIL;
            failing->fail_time = mstime();
            failing->flags &= ~CLUSTER_NODE_PFAIL;
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                 CLUSTER_TODO_UPDATE_STATE);
        }
    } else if (type == CLUSTERMSG_TYPE_REPLJOB) {
        uint32_t numjobs = ntohl(hdr->data.jobs.serialized.numjobs);
        uint32_t datasize = ntohl(hdr->data.jobs.serialized.datasize);
        job *j;

        /* Only replicate jobs by known nodes. */
        if (!sender || numjobs != 1) return 1;

        /* Don't replicate jobs if we got already memory issues or if we
         * are leaving the cluster. */
        if (getMemoryWarningLevel() > 0 || myselfLeaving()) return 1;

        j = deserializeJob(hdr->data.jobs.serialized.jobs_data,datasize,NULL,SER_MESSAGE);
        if (j == NULL) {
            serverLog(LL_WARNING,
                "Received corrupted job description from node %.40s",
                hdr->sender);
        } else {
            /* Don't replicate jobs about queues paused in input. */
            queue *q = lookupQueue(j->queue);
            if (q && q->flags & QUEUE_FLAG_PAUSED_IN) {
                freeJob(j);
                return 1;
            }
            j->flags |= JOB_FLAG_BCAST_QUEUED;
            j->state = JOB_STATE_ACTIVE;
            int retval = registerJob(j);
            if (retval == C_ERR) {
                /* The job already exists. Just update the list of nodes
                 * that may have a copy. */
                updateJobNodes(j);
            } else {
                AOFLoadJob(j);
            }
            /* Reply with a GOTJOB message, even if we already did in the past.
             * The node receiving ADDJOB may try multiple times, and our
             * GOTJOB messages may get lost. */
            if (!(hdr->mflags[0] & CLUSTERMSG_FLAG0_NOREPLY))
                clusterSendGotJob(sender,j);
            /* Release the job if we already had it. */
            if (retval == C_ERR) freeJob(j);
        }
    } else if (type == CLUSTERMSG_TYPE_YOURJOBS) {
        uint32_t numjobs = ntohl(hdr->data.jobs.serialized.numjobs);
        uint32_t datasize = ntohl(hdr->data.jobs.serialized.datasize);

        if (!sender || numjobs == 0) return 1;
        receiveYourJobs(sender,numjobs,hdr->data.jobs.serialized.jobs_data,datasize);
    } else if (type == CLUSTERMSG_TYPE_GOTJOB) {
        if (!sender) return 1;

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j && j->state == JOB_STATE_WAIT_REPL) {
            dictAdd(j->nodes_confirmed,sender->name,sender);
            if (dictSize(j->nodes_confirmed) == j->repl)
                jobReplicationAchieved(j);
        }
    } else if (type == CLUSTERMSG_TYPE_SETACK) {
        if (!sender) return 1;
        uint32_t mayhave = ntohl(hdr->data.jobid.job.aux);

        serverLog(LL_VERBOSE,"RECEIVED SETACK(%d) FROM %.40s FOR JOB %.*s",
            (int) mayhave,
            sender->name, JOB_ID_LEN, hdr->data.jobid.job.id);

        job *j = lookupJob(hdr->data.jobid.job.id);

        /* If we have the job, change the state to acknowledged. */
        if (j) {
            if (j->state == JOB_STATE_WAIT_REPL) {
                /* The job was acknowledged before ADDJOB achieved the
                 * replication level requested! Unblock the client and
                 * change the job state to active. */
                if (jobReplicationAchieved(j) == C_ERR) {
                    /* The job was externally replicated and deleted from
                     * this node. Nothing to do... */
                    return 1;
                }
            }
            /* ACK it if not already acked. */
            acknowledgeJob(j);
        }

        /* Reply according to the job exact state. */
        if (j == NULL || dictSize(j->nodes_delivered) <= mayhave) {
            /* If we don't know the job or our set of nodes that may have
             * the job is not larger than the sender, reply with GOTACK. */
            int known = j ? 1 : 0;
            clusterSendGotAck(sender,hdr->data.jobid.job.id,known);
        } else {
            /* We have the job but we know more nodes that may have it
             * than the sender, if we are here. Don't reply with GOTACK unless
             * mayhave is 0 (the sender just received an ACK from client about
             * a job it does not know), in order to let the sender delete it. */
            if (mayhave == 0)
                clusterSendGotAck(sender,hdr->data.jobid.job.id,1);
            /* Anyway, start a GC about this job. Because one of the two is
             * true:
             * 1) mayhave in the sender is 0, so it's up to us to start a GC
             *    because the sender has just a dummy ACK.
             * 2) Or, we prevented the sender from finishing the GC since
             *    we are not replying with GOTACK, and we need to take
             *    responsability to evict the job in the cluster. */
            tryJobGC(j);
        }
    } else if (type == CLUSTERMSG_TYPE_GOTACK) {
        if (!sender) return 1;
        uint32_t known = ntohl(hdr->data.jobid.job.aux);

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j) gotAckReceived(sender,j,known);
    } else if (type == CLUSTERMSG_TYPE_DELJOB) {
        if (!sender) return 1;
        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j) {
            serverLog(LL_VERBOSE,"RECEIVED DELJOB FOR JOB %.*s",JOB_ID_LEN,j->id);
            unregisterJob(j);
            freeJob(j);
        }
    } else if (type == CLUSTERMSG_TYPE_ENQUEUE) {
        if (!sender) return 1;
        uint32_t delay = ntohl(hdr->data.jobid.job.aux);

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j && j->state < JOB_STATE_QUEUED) {
            /* Discard this message if the queue is paused in input. */
            queue *q = lookupQueue(j->queue);
            if (q == NULL || !(q->flags & QUEUE_FLAG_PAUSED_IN)) {
                /* We received an ENQUEUE message: consider this node as the
                 * first to queue the job, so no need to broadcast a QUEUED
                 * message the first time we queue it. */
                j->flags &= ~JOB_FLAG_BCAST_QUEUED;
                serverLog(LL_VERBOSE,"RECEIVED ENQUEUE FOR JOB %.*s",
                          JOB_ID_LEN,j->id);
                if (delay == 0) {
                    enqueueJob(j,0);
                } else {
                    updateJobRequeueTime(j,server.mstime+delay*1000);
                }
            }
        }
    } else if (type == CLUSTERMSG_TYPE_QUEUED ||
               type == CLUSTERMSG_TYPE_WORKING) {
        if (!sender) return 1;

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j && j->state <= JOB_STATE_QUEUED) {
            serverLog(LL_VERBOSE,"UPDATING QTIME FOR JOB %.*s",JOB_ID_LEN,j->id);
            /* Move the time we'll re-queue this job in the future. Moreover
             * if the sender has a Node ID greater than our node ID, and we
             * have the message queued as well, dequeue it, to avoid an
             * useless multiple delivery.
             *
             * The message is always dequeued in case the message is of
             * type WORKING, since this is the explicit semantics of WORKING.
             *
             * If the message is WORKING always dequeue regardless of the
             * sender name, since there is a client claiming to work on the
             * message. */
            if (j->state == JOB_STATE_QUEUED &&
                (type == CLUSTERMSG_TYPE_WORKING ||
                 compareNodeIDsByJob(sender,myself,j) > 0))
            {
                dequeueJob(j);
            }

            /* Update the time at which this node will attempt to enqueue
             * the message again. */
            if (j->retry) {
                j->flags |= JOB_FLAG_BCAST_WILLQUEUE;
                updateJobRequeueTime(j,server.mstime+
                                       j->retry*1000+
                                       randomTimeError(DISQUE_TIME_ERR));
            }

            /* Update multiple deliveries counters. */
            if (type == CLUSTERMSG_TYPE_QUEUED) {
                if (hdr->mflags[0] & CLUSTERMSG_FLAG0_INCR_NACKS)
                    j->num_nacks++;
                if (hdr->mflags[0] & CLUSTERMSG_FLAG0_INCR_DELIV)
                    j->num_deliv++;
            }
        } else if (j && j->state == JOB_STATE_ACKED) {
            /* Some other node queued a message that we have as
             * already acknowledged. Try to force it to drop it. */
            clusterSendSetAck(sender,j);
        }
    } else if (type == CLUSTERMSG_TYPE_WILLQUEUE) {
        if (!sender) return 1;

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j) {
            if (j->state == JOB_STATE_ACTIVE ||
                j->state == JOB_STATE_QUEUED)
            {
                dictAdd(j->nodes_delivered,sender->name,sender);
            }
            if (j->state == JOB_STATE_QUEUED)
                clusterBroadcastQueued(j,CLUSTERMSG_NOFLAGS);
            else if (j->state == JOB_STATE_ACKED) clusterSendSetAck(sender,j);
        }
    } else if (type == CLUSTERMSG_TYPE_NEEDJOBS ||
               type == CLUSTERMSG_TYPE_PAUSE)
    {
        if (!sender) return 1;
        uint32_t qnamelen = ntohl(hdr->data.queueop.about.qnamelen);
        uint32_t count = ntohl(hdr->data.queueop.about.aux);
        robj *qname = createStringObject(hdr->data.queueop.about.qname,
                                         qnamelen);
        serverLog(LL_VERBOSE,"RECEIVED %s FOR QUEUE %s (%d)",
            (type == CLUSTERMSG_TYPE_NEEDJOBS) ? "NEEDJOBS" : "PAUSE",
            (char*)qname->ptr,count);
        if (type == CLUSTERMSG_TYPE_NEEDJOBS)
            receiveNeedJobs(sender,qname,count);
        else
            receivePauseQueue(qname,count);
        decrRefCount(qname);
    } else {
        serverLog(LL_WARNING,"Received unknown packet type: %d", type);
    }
    return 1;
}

/* This function is called when we detect the link with this node is lost.
   We set the node as no longer connected. The Cluster Cron will detect
   this connection and will try to get it connected again.

   Instead if the node is a temporary node used to accept a query, we
   completely free the node on error. */
void handleLinkIOError(clusterLink *link) {
    freeClusterLink(link);
}

/* Send data. This is handled using a trivial send buffer that gets
 * consumed by write(). We don't try to optimize this for speed too much
 * as this is a very low traffic channel. */
void clusterWriteHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    clusterLink *link = (clusterLink*) privdata;
    ssize_t nwritten;
    UNUSED(el);
    UNUSED(mask);

    nwritten = write(fd, link->sndbuf, sdslen(link->sndbuf));
    if (nwritten <= 0) {
        serverLog(LL_DEBUG,"I/O error writing to node link: %s",
            strerror(errno));
        handleLinkIOError(link);
        return;
    }
    sdsrange(link->sndbuf,nwritten,-1);
    if (sdslen(link->sndbuf) == 0)
        aeDeleteFileEvent(server.el, link->fd, AE_WRITABLE);
}

/* Read data. Try to read the first field of the header first to check the
 * full length of the packet. When a whole packet is in memory this function
 * will call the function to process the packet. And so forth. */
void clusterReadHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    char buf[sizeof(clusterMsg)];
    ssize_t nread;
    clusterMsg *hdr;
    clusterLink *link = (clusterLink*) privdata;
    unsigned int readlen, rcvbuflen;
    UNUSED(el);
    UNUSED(mask);

    while(1) { /* Read as long as there is data to read. */
        rcvbuflen = sdslen(link->rcvbuf);
        if (rcvbuflen < 8) {
            /* First, obtain the first 8 bytes to get the full message
             * length. */
            readlen = 8 - rcvbuflen;
        } else {
            /* Finally read the full message. */
            hdr = (clusterMsg*) link->rcvbuf;
            if (rcvbuflen == 8) {
                /* Perform some sanity check on the message signature
                 * and length. */
                if (memcmp(hdr->sig,"DbuZ",4) != 0 ||
                    ntohl(hdr->totlen) < CLUSTERMSG_MIN_LEN)
                {
                    serverLog(LL_WARNING,
                        "Bad message length or signature received "
                        "from Cluster bus.");
                    handleLinkIOError(link);
                    return;
                }
            }
            readlen = ntohl(hdr->totlen) - rcvbuflen;
            if (readlen > sizeof(buf)) readlen = sizeof(buf);
        }

        nread = read(fd,buf,readlen);
        if (nread == -1 && errno == EAGAIN) return; /* No more data ready. */

        if (nread <= 0) {
            /* I/O error... */
            serverLog(LL_DEBUG,"I/O error reading from node link: %s",
                (nread == 0) ? "connection closed" : strerror(errno));
            handleLinkIOError(link);
            return;
        } else {
            /* Read data and recast the pointer to the new buffer. */
            link->rcvbuf = sdscatlen(link->rcvbuf,buf,nread);
            hdr = (clusterMsg*) link->rcvbuf;
            rcvbuflen += nread;
        }

        /* Total length obtained? Process this packet. */
        if (rcvbuflen >= 8 && rcvbuflen == ntohl(hdr->totlen)) {
            if (clusterProcessPacket(link)) {
                sdsfree(link->rcvbuf);
                link->rcvbuf = sdsempty();
            } else {
                return; /* Link no longer valid. */
            }
        }
    }
}

/* Put stuff into the send buffer.
 *
 * It is guaranteed that this function will never have as a side effect
 * the link to be invalidated, so it is safe to call this function
 * from event handlers that will do stuff with the same link later. */
void clusterSendMessage(clusterLink *link, unsigned char *msg, size_t msglen) {
    if (sdslen(link->sndbuf) == 0 && msglen != 0)
        aeCreateFileEvent(server.el,link->fd,AE_WRITABLE,
                    clusterWriteHandler,link);

    link->sndbuf = sdscatlen(link->sndbuf, msg, msglen);
    server.cluster->stats_bus_messages_sent++;
}

/* Send a message to all the nodes in the specified dictionary of nodes, that
 * are part of the cluster and have a connected link.
 *
 * It is guaranteed that this function will never have as a side effect
 * some node->link to be invalidated, so it is safe to call this function
 * from event handlers that will do stuff with node links later.
 *
 * Note that this function expects 'nodes' to be stored as keys, so the
 * value can be anything. This is possible since node->name is both the
 * node key, and the pointer to the start of the structure. */
void clusterBroadcastMessage(dict *nodes, void *buf, size_t len) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetKey(de);

        if (!node->link) continue;
        if (node->flags & (CLUSTER_NODE_MYSELF|CLUSTER_NODE_HANDSHAKE))
            continue;
        clusterSendMessage(node->link,buf,len);
    }
    dictReleaseIterator(di);
}

/* Build the message header */
void clusterBuildMessageHdr(clusterMsg *hdr, int type) {
    int totlen = 0;

    memset(hdr,0,sizeof(*hdr));
    hdr->ver = htons(CLUSTER_PROTO_VER);
    hdr->sig[0] = 'D';
    hdr->sig[1] = 'b';
    hdr->sig[2] = 'u';
    hdr->sig[3] = 'Z';
    hdr->type = htons(type);
    memcpy(hdr->sender,myself->name,CLUSTER_NAMELEN);
    hdr->port = htons(server.port);
    hdr->flags = htons(myself->flags);
    hdr->state = server.cluster->state;

    /* Compute the message length for certain messages. For other messages
     * this is up to the caller. */
    if (type == CLUSTERMSG_TYPE_FAIL) {
        totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        totlen += sizeof(clusterMsgDataFail);
    } else if (type == CLUSTERMSG_TYPE_GOTJOB ||
               type == CLUSTERMSG_TYPE_ENQUEUE ||
               type == CLUSTERMSG_TYPE_QUEUED ||
               type == CLUSTERMSG_TYPE_WORKING ||
               type == CLUSTERMSG_TYPE_SETACK ||
               type == CLUSTERMSG_TYPE_GOTACK ||
               type == CLUSTERMSG_TYPE_DELJOB ||
               type == CLUSTERMSG_TYPE_WILLQUEUE)
    {
        totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        totlen += sizeof(clusterMsgDataJobID);
    }
    hdr->totlen = htonl(totlen);
    /* For PING, PONG, and MEET, fixing the totlen field is up to the caller. */
}

/* Send a PING or PONG packet to the specified node, making sure to add enough
 * gossip informations. */
void clusterSendPing(clusterLink *link, int type) {
    unsigned char *buf;
    clusterMsg *hdr;
    int gossipcount = 0; /* Number of gossip sections added so far. */
    int wanted; /* Number of gossip sections we want to append if possible. */
    int totlen; /* Total packet length. */
    /* freshnodes is the max number of nodes we can hope to append at all:
     * nodes available minus two (ourself and the node we are sending the
     * message to). However practically there may be less valid nodes since
     * nodes in handshake state, disconnected, are not considered. */
    int freshnodes = dictSize(server.cluster->nodes)-2;

    /* How many gossip sections we want to add? 1/10 of the number of nodes
     * and anyway at least 3. Why 1/10?
     *
     * If we have N masters, with N/10 entries, and we consider that in
     * node_timeout we exchange with each other node at least 4 packets
     * (we ping in the worst case in node_timeout/2 time, and we also
     * receive two pings from the host), we have a total of 8 packets
     * in the node_timeout*2 falure reports validity time. So we have
     * that, for a single PFAIL node, we can expect to receive the following
     * number of failure reports (in the specified window of time):
     *
     * PROB * GOSSIP_ENTRIES_PER_PACKET * TOTAL_PACKETS:
     *
     * PROB = probability of being featured in a single gossip entry,
     *        which is 1 / NUM_OF_NODES.
     * ENTRIES = 10.
     * TOTAL_PACKETS = 2 * 4 * NUM_OF_MASTERS.
     *
     * If we assume we have just masters (so num of nodes and num of masters
     * is the same), with 1/10 we always get over the majority, and specifically
     * 80% of the number of nodes, to account for many masters failing at the
     * same time.
     *
     * Since we have non-voting slaves that lower the probability of an entry
     * to feature our node, we set the number of entires per packet as
     * 10% of the total nodes we have. */
    wanted = floor(dictSize(server.cluster->nodes)/10);
    if (wanted < 3) wanted = 3;
    if (wanted > freshnodes) wanted = freshnodes;

    /* Compute the maxium totlen to allocate our buffer. We'll fix the totlen
     * later according to the number of gossip sections we really were able
     * to put inside the packet. */
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += (sizeof(clusterMsgDataGossip)*wanted);
    /* Note: clusterBuildMessageHdr() expects the buffer to be always at least
     * sizeof(clusterMsg) or more. */
    if (totlen < (int)sizeof(clusterMsg)) totlen = sizeof(clusterMsg);
    buf = zcalloc(totlen);
    hdr = (clusterMsg*) buf;

    /* Populate the header. */
    if (link->node && type == CLUSTERMSG_TYPE_PING)
        link->node->ping_sent = mstime();
    clusterBuildMessageHdr(hdr,type);

    /* Populate the gossip fields */
    int maxiterations = wanted*3;
    while(freshnodes > 0 && gossipcount < wanted && maxiterations--) {
        dictEntry *de = dictGetRandomKey(server.cluster->nodes);
        clusterNode *this = dictGetVal(de);
        clusterMsgDataGossip *gossip;
        int j;

        /* Don't include this node: the whole packet header is about us
         * already, so we just gossip about other nodes. */
        if (this == myself) continue;

        /* Give a bias to FAIL/PFAIL nodes. */
        if (maxiterations > wanted*2 &&
            !(this->flags & (CLUSTER_NODE_PFAIL|CLUSTER_NODE_FAIL)))
            continue;

        /* In the gossip section don't include:
         * 1) Nodes in HANDSHAKE state.
         * 3) Nodes with the NOADDR flag set.
         * 4) Disconnected nodes if they don't have configured slots.
         */
        if (this->flags & (CLUSTER_NODE_HANDSHAKE|CLUSTER_NODE_NOADDR) ||
            this->link == NULL)
        {
            freshnodes--; /* Tecnically not correct, but saves CPU. */
            continue;
        }

        /* Check if we already added this node */
        for (j = 0; j < gossipcount; j++) {
            if (memcmp(hdr->data.ping.gossip[j].nodename,this->name,
                    CLUSTER_NAMELEN) == 0) break;
        }
        if (j != gossipcount) continue;

        /* Add it */
        freshnodes--;
        gossip = &(hdr->data.ping.gossip[gossipcount]);
        memcpy(gossip->nodename,this->name,CLUSTER_NAMELEN);
        gossip->ping_sent = htonl(this->ping_sent);
        gossip->pong_received = htonl(this->pong_received);
        memcpy(gossip->ip,this->ip,sizeof(this->ip));
        gossip->port = htons(this->port);
        gossip->flags = htons(this->flags);
        gossip->notused1 = 0;
        gossip->notused2 = 0;
        gossipcount++;
    }

    /* Ready to send... fix the totlen field and queue the message in the
     * output buffer. */
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += (sizeof(clusterMsgDataGossip)*gossipcount);
    hdr->count = htons(gossipcount);
    hdr->totlen = htonl(totlen);
    clusterSendMessage(link,buf,totlen);
    zfree(buf);
}

/* Send a FAIL message to all the nodes we are able to contact.
 * The FAIL message is sent when we detect that a node is failing
 * (CLUSTER_NODE_PFAIL) and we also receive a gossip confirmation of this:
 * we switch the node state to CLUSTER_NODE_FAIL and ask all the other
 * nodes to do the same ASAP. */
void clusterSendFail(char *nodename) {
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_FAIL);
    memcpy(hdr->data.fail.about.nodename,nodename,CLUSTER_NAMELEN);
    clusterBroadcastMessage(server.cluster->nodes,buf,ntohl(hdr->totlen));
}

/* -----------------------------------------------------------------------------
 * CLUSTER job related messages
 * -------------------------------------------------------------------------- */

/* Broadcast a REPLJOB message to 'repl' nodes, and populates the list of jobs
 * that may have the job.
 *
 * If there are already nodes that received the message, additional
 * 'repl' nodes will be added to the list (if possible), and the message
 * broadcasted again to all the nodes that may already have the message,
 * plus the new ones.
 *
 * However for nodes for which we already have the GOTJOB reply, we flag
 * the message with the NOREPLY flag. If the 'noreply' argument of the function
 * is true we also flag the message with NOREPLY regardless of the fact the
 * node we are sending REPLJOB to already replied or not.
 *
 * Nodes are selected from server.cluster->reachable_nodes list.
 * The function returns the number of new (additional) nodes that MAY have
 * received the message. */
int clusterReplicateJob(job *j, int repl, int noreply) {
    int i, added = 0;

    if (repl <= 0) return 0;

    /* Add the specified number of nodes to the list of receivers. */
    clusterShuffleReachableNodes();
    for (i = 0; i < server.cluster->reachable_nodes_count; i++) {
        clusterNode *node = server.cluster->reachable_nodes[i];

        if (node->link == NULL) continue; /* No link, no party... */
        if (dictAdd(j->nodes_delivered,node->name,node) == DICT_OK) {
            /* Only counts non-duplicated nodes. */
            added++;
            if (--repl == 0) break;
        }
    }

    /* Resend the message to all the past and new nodes. */
    unsigned char buf[sizeof(clusterMsg)], *payload;
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;

    sds serialized = serializeJob(sdsempty(),j,SER_MESSAGE);

    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataJob) -
              sizeof(hdr->data.jobs.serialized.jobs_data) +
              sdslen(serialized);

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_REPLJOB);
    hdr->data.jobs.serialized.numjobs = htonl(1);
    hdr->data.jobs.serialized.datasize = htonl(sdslen(serialized));
    hdr->totlen = htonl(totlen);

    if (totlen < sizeof(buf)) {
        payload = buf;
    } else {
        payload = zmalloc(totlen);
        memcpy(payload,buf,sizeof(clusterMsg));
        hdr = (clusterMsg*) payload;
    }
    memcpy(hdr->data.jobs.serialized.jobs_data,serialized,sdslen(serialized));
    sdsfree(serialized);

    /* Actual delivery of the message to the list of nodes. */
    dictIterator *di = dictGetIterator(j->nodes_delivered);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        if (node == myself) continue;

        /* We ask for reply only if 'noreply' is false and the target node
         * did not already replied with GOTJOB. */
        int acked = j->nodes_confirmed && dictFind(j->nodes_confirmed,node);
        if (noreply || acked) {
            hdr->mflags[0] |= CLUSTERMSG_FLAG0_NOREPLY;
        } else {
            hdr->mflags[0] &= ~CLUSTERMSG_FLAG0_NOREPLY;
        }

        /* If the target node acknowledged the message already, send it again
         * only if there are additional nodes. We want the target node to refresh
         * its list of receivers. */
        if (node->link && !(acked && added == 0))
            clusterSendMessage(node->link,payload,totlen);
    }
    dictReleaseIterator(di);

    if (payload != buf) zfree(payload);
    return added;
}

/* Helper function to send all the messages that have just a type,
 * a Job ID, and an optional 'aux' additional value. */
void clusterSendJobIDMessage(int type, clusterNode *node, char *id, int aux) {
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;

    if (node->link == NULL) return; /* This is a best effort message. */
    clusterBuildMessageHdr(hdr,type);
    memcpy(hdr->data.jobid.job.id,id,JOB_ID_LEN);
    hdr->data.jobid.job.aux = htonl(aux);
    clusterSendMessage(node->link,buf,ntohl(hdr->totlen));
}

/* Like clusterSendJobIDMessage(), but sends the message to the specified set
 * of nodes (excluding myself if included in the set of nodes). */
void clusterBroadcastJobIDMessage(dict *nodes, char *id, int type, uint32_t aux, unsigned char flags) {
    dictIterator *di = dictGetIterator(nodes);
    dictEntry *de;
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;

    /* Build the message one time, send the same to everybody. */
    clusterBuildMessageHdr(hdr,type);
    memcpy(hdr->data.jobid.job.id,id,JOB_ID_LEN);
    hdr->data.jobid.job.aux = htonl(aux);
    hdr->mflags[0] = flags;

    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        if (node == myself) continue;
        if (node->link)
            clusterSendMessage(node->link,buf,ntohl(hdr->totlen));
    }
    dictReleaseIterator(di);
}

/* Send a GOTJOB message to the specified node, if connected.
 * GOTJOB messages only contain the ID of the job, and are acknowledges
 * that the job was replicated to a target node. The receiver of the message
 * will be able to reply to the client that the job was accepted by the
 * system when enough nodes have a copy of the job. */
void clusterSendGotJob(clusterNode *node, job *j) {
    clusterSendJobIDMessage(CLUSTERMSG_TYPE_GOTJOB,node,j->id,0);
}

/* Force the receiver to queue a job, if it has that job in an active
 * state. */
void clusterSendEnqueue(clusterNode *node, job *j, uint32_t delay) {
    clusterSendJobIDMessage(CLUSTERMSG_TYPE_ENQUEUE,node,j->id,delay);
}

/* Tell the receiver that the specified job was just re-queued by the
 * sender, so it should update its qtime to the future, and if the job
 * is also queued by the receiving node, to drop it from the queue if the
 * sender has a greater node ID (so that we try in a best-effort way to
 * avoid useless multi deliveries).
 *
 * This message is sent to all the nodes we believe may have a copy
 * of the message and are reachable. */
void clusterBroadcastQueued(job *j, unsigned char flags) {
    serverLog(LL_VERBOSE,"BCAST QUEUED: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(j->nodes_delivered,j->id,
                                 CLUSTERMSG_TYPE_QUEUED,0,flags);
}

/* WORKING is like QUEUED, but will always force the receiver to dequeue. */
void clusterBroadcastWorking(job *j) {
    serverLog(LL_VERBOSE,"BCAST WORKING: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(j->nodes_delivered,j->id,
                                 CLUSTERMSG_TYPE_WORKING,0,CLUSTERMSG_NOFLAGS);
}

/* Send a DELJOB message to all the nodes that may have a copy. */
void clusterBroadcastDelJob(job *j) {
    serverLog(LL_VERBOSE,"BCAST DELJOB: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(j->nodes_delivered,j->id,
                                 CLUSTERMSG_TYPE_DELJOB,0,CLUSTERMSG_NOFLAGS);
}

/* Tell the receiver to reply with a QUEUED message if it has the job
 * already queued, to prevent us from queueing it in the next few
 * milliseconds. */
void clusterSendWillQueue(job *j) {
    serverLog(LL_VERBOSE,"BCAST WILLQUEUE: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(j->nodes_delivered,j->id,
                                 CLUSTERMSG_TYPE_WILLQUEUE,0,CLUSTERMSG_NOFLAGS);
}

/* Force the receiver to acknowledge the job as delivered. */
void clusterSendSetAck(clusterNode *node, job *j) {
    uint32_t maxowners = dictSize(j->nodes_delivered);
    clusterSendJobIDMessage(CLUSTERMSG_TYPE_SETACK,node,j->id,maxowners);
}

/* Acknowledge a SETACK message. */
void clusterSendGotAck(clusterNode *node, char *jobid, int known) {
    clusterSendJobIDMessage(CLUSTERMSG_TYPE_GOTACK,node,jobid,known);
}

/* Send a NEEDJOBS message to the specified set of nodes. */
void clusterSendNeedJobs(robj *qname, int numjobs, dict *nodes) {
    uint32_t totlen, qnamelen = sdslen(qname->ptr);
    uint32_t alloclen;
    clusterMsg *hdr;

    serverLog(LL_VERBOSE,"Sending NEEDJOBS for %s %d, %d nodes",
        (char*)qname->ptr, (int)numjobs, (int)dictSize(nodes));

    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataQueueOp) - 8 + qnamelen;
    alloclen = totlen;
    if (alloclen < (int)sizeof(clusterMsg)) alloclen = sizeof(clusterMsg);
    hdr = zmalloc(alloclen);

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_NEEDJOBS);
    hdr->data.queueop.about.aux = htonl(numjobs);
    hdr->data.queueop.about.qnamelen = htonl(qnamelen);
    memcpy(hdr->data.queueop.about.qname, qname->ptr, qnamelen);
    hdr->totlen = htonl(totlen);
    clusterBroadcastMessage(nodes,hdr,totlen);
    zfree(hdr);
}

/* Send a PAUSE message to the specified set of nodes. */
void clusterSendPause(robj *qname, uint32_t flags, dict *nodes) {
    uint32_t totlen, qnamelen = sdslen(qname->ptr);
    uint32_t alloclen;
    clusterMsg *hdr;

    serverLog(LL_VERBOSE,"Sending PAUSE for %s flags=%d, %d nodes",
        (char*)qname->ptr, (int)flags, (int)dictSize(nodes));

    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataQueueOp) - 8 + qnamelen;
    alloclen = totlen;
    if (alloclen < (int)sizeof(clusterMsg)) alloclen = sizeof(clusterMsg);
    hdr = zmalloc(alloclen);

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_PAUSE);
    hdr->data.queueop.about.aux = htonl(flags);
    hdr->data.queueop.about.qnamelen = htonl(qnamelen);
    memcpy(hdr->data.queueop.about.qname, qname->ptr, qnamelen);
    hdr->totlen = htonl(totlen);
    clusterBroadcastMessage(nodes,hdr,totlen);
    zfree(hdr);
}

/* Same as clusterSendPause() but broadcasts the message to the
 * whole cluster. */
void clusterBroadcastPause(robj *qname, uint32_t flags) {
    clusterSendPause(qname,flags,server.cluster->nodes);
}

/* Send a YOURJOBS message to the specified node, with a serialized copy of
 * the jobs referneced by the 'jobs' array and containing 'count' jobs. */
void clusterSendYourJobs(clusterNode *node, job **jobs, uint32_t count) {
    unsigned char buf[sizeof(clusterMsg)], *payload;
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen, j;

    if (!node->link) return;

    serverLog(LL_VERBOSE,"Sending %d jobs to %.40s", (int)count,node->name);

    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataJob) -
              sizeof(hdr->data.jobs.serialized.jobs_data);

    sds serialized = sdsempty();
    for (j = 0; j < count; j++)
        serialized = serializeJob(serialized,jobs[j],SER_MESSAGE);
    totlen += sdslen(serialized);

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_YOURJOBS);
    hdr->data.jobs.serialized.numjobs = htonl(count);
    hdr->data.jobs.serialized.datasize = htonl(sdslen(serialized));
    hdr->totlen = htonl(totlen);

    if (totlen < sizeof(buf)) {
        payload = buf;
    } else {
        payload = zmalloc(totlen);
        memcpy(payload,buf,sizeof(clusterMsg));
        hdr = (clusterMsg*) payload;
    }
    memcpy(hdr->data.jobs.serialized.jobs_data,serialized,sdslen(serialized));
    sdsfree(serialized);
    clusterSendMessage(node->link,payload,totlen);
    if (payload != buf) zfree(payload);
}

/* -----------------------------------------------------------------------------
 * CLUSTER cron job
 * -------------------------------------------------------------------------- */

/* This is executed 10 times every second */
void clusterCron(void) {
    dictIterator *di;
    dictEntry *de;
    int update_state = 0;
    mstime_t min_pong = 0, now = mstime();
    clusterNode *min_pong_node = NULL;
    static unsigned long long iteration = 0;
    mstime_t handshake_timeout;

    iteration++; /* Number of times this function was called so far. */

    /* The handshake timeout is the time after which a handshake node that was
     * not turned into a normal node is removed from the nodes. Usually it is
     * just the NODE_TIMEOUT value, but when NODE_TIMEOUT is too small we use
     * the value of 1 second. */
    handshake_timeout = server.cluster_node_timeout;
    if (handshake_timeout < 1000) handshake_timeout = 1000;

    /* Check if we have disconnected nodes and re-establish the connection. */
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node->flags & (CLUSTER_NODE_MYSELF|CLUSTER_NODE_NOADDR)) continue;

        /* A Node in HANDSHAKE state has a limited lifespan equal to the
         * configured node timeout. */
        if (nodeInHandshake(node) && now - node->ctime > handshake_timeout) {
            clusterDelNode(node);
            continue;
        }

        if (node->link == NULL) {
            int fd;
            mstime_t old_ping_sent;
            clusterLink *link;

            fd = anetTcpNonBlockBindConnect(server.neterr, node->ip,
                node->port+CLUSTER_PORT_INCR, NET_FIRST_BIND_ADDR);
            if (fd == -1) {
                /* We got a synchronous error from connect before
                 * clusterSendPing() had a chance to be called.
                 * If node->ping_sent is zero, failure detection can't work,
                 * so we claim we actually sent a ping now (that will
                 * be really sent as soon as the link is obtained). */
                if (node->ping_sent == 0) node->ping_sent = mstime();
                serverLog(LL_DEBUG, "Unable to connect to "
                    "Cluster Node [%s]:%d -> %s", node->ip,
                    node->port+CLUSTER_PORT_INCR,
                    server.neterr);
                continue;
            }
            anetEnableTcpNoDelay(NULL,fd);
            link = createClusterLink(node);
            link->fd = fd;
            node->link = link;
            aeCreateFileEvent(server.el,link->fd,AE_READABLE,
                    clusterReadHandler,link);
            /* Queue a PING in the new connection ASAP: this is crucial
             * to avoid false positives in failure detection.
             *
             * If the node is flagged as MEET, we send a MEET message instead
             * of a PING one, to force the receiver to add us in its node
             * table. */
            old_ping_sent = node->ping_sent;
            clusterSendPing(link, node->flags & CLUSTER_NODE_MEET ?
                    CLUSTERMSG_TYPE_MEET : CLUSTERMSG_TYPE_PING);
            if (old_ping_sent) {
                /* If there was an active ping before the link was
                 * disconnected, we want to restore the ping time, otherwise
                 * replaced by the clusterSendPing() call. */
                node->ping_sent = old_ping_sent;
            }
            /* We can clear the flag after the first packet is sent.
             * If we'll never receive a PONG, we'll never send new packets
             * to this node. Instead after the PONG is received and we
             * are no longer in meet/handshake status, we want to send
             * normal PING packets. */
            node->flags &= ~CLUSTER_NODE_MEET;

            serverLog(LL_DEBUG,"Connecting with Node %.40s at %s:%d",
                    node->name, node->ip, node->port+CLUSTER_PORT_INCR);
        }
    }
    dictReleaseIterator(di);

    /* Ping some random node 1 time every 10 iterations, so that we usually ping
     * one random node every second. */
    if (!(iteration % 10)) {
        int j;

        /* Check a few random nodes and ping the one with the oldest
         * pong_received time. */
        for (j = 0; j < 5; j++) {
            de = dictGetRandomKey(server.cluster->nodes);
            clusterNode *this = dictGetVal(de);

            /* Don't ping nodes disconnected or with a ping currently active. */
            if (this->link == NULL || this->ping_sent != 0) continue;
            if (this->flags & (CLUSTER_NODE_MYSELF|CLUSTER_NODE_HANDSHAKE))
                continue;
            if (min_pong_node == NULL || min_pong > this->pong_received) {
                min_pong_node = this;
                min_pong = this->pong_received;
            }
        }
        if (min_pong_node) {
            serverLog(LL_DEBUG,"Pinging node %.40s", min_pong_node->name);
            clusterSendPing(min_pong_node->link, CLUSTERMSG_TYPE_PING);
        }
    }

    /* Iterate nodes to check if we need to flag something as failing.
     * This loop is also responsible to:
     * 1) Check if there are orphaned masters (masters without non failing
     *    slaves).
     * 2) Count the max number of non failing slaves for a single master.
     * 3) Count the number of slaves for our master, if we are a slave. */
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        now = mstime(); /* Use an updated time at every iteration. */
        mstime_t delay;

        if (node->flags &
            (CLUSTER_NODE_MYSELF|CLUSTER_NODE_NOADDR|CLUSTER_NODE_HANDSHAKE))
                continue;

        /* If we are waiting for the PONG more than half the cluster
         * timeout, reconnect the link: maybe there is a connection
         * issue even if the node is alive. */
        if (node->link && /* is connected */
            now - node->link->ctime >
            server.cluster_node_timeout && /* was not already reconnected */
            node->ping_sent && /* we already sent a ping */
            node->pong_received < node->ping_sent && /* still waiting pong */
            /* and we are waiting for the pong more than timeout/2 */
            now - node->ping_sent > server.cluster_node_timeout/2)
        {
            /* Disconnect the link, it will be reconnected automatically. */
            freeClusterLink(node->link);
        }

        /* If we have currently no active ping in this instance, and the
         * received PONG is older than half the cluster timeout, send
         * a new ping now, to ensure all the nodes are pinged without
         * a too big delay. */
        if (node->link &&
            node->ping_sent == 0 &&
            (now - node->pong_received) > server.cluster_node_timeout/2)
        {
            clusterSendPing(node->link, CLUSTERMSG_TYPE_PING);
            continue;
        }

        /* Check only if we have an active ping for this instance. */
        if (node->ping_sent == 0) continue;

        /* Compute the delay of the PONG. Note that if we already received
         * the PONG, then node->ping_sent is zero, so can't reach this
         * code at all. */
        delay = now - node->ping_sent;

        if (delay > server.cluster_node_timeout) {
            /* Timeout reached. Set the node as possibly failing if it is
             * not already in this state. */
            if (!(node->flags & (CLUSTER_NODE_PFAIL|CLUSTER_NODE_FAIL))) {
                serverLog(LL_DEBUG,"*** NODE %.40s possibly failing",
                    node->name);
                node->flags |= CLUSTER_NODE_PFAIL;
                update_state = 1;
            }
        }
    }
    dictReleaseIterator(di);

    if (update_state || server.cluster->state == CLUSTER_FAIL)
        clusterUpdateState();

    clusterUpdateReachableNodes();
}

/* This function is called before the event handler returns to sleep for
 * events. It is useful to perform operations that must be done ASAP in
 * reaction to events fired but that are not safe to perform inside event
 * handlers, or to perform potentially expansive tasks that we need to do
 * a single time before replying to clients. */
void clusterBeforeSleep(void) {
    /* Update the cluster state. */
    if (server.cluster->todo_before_sleep & CLUSTER_TODO_UPDATE_STATE)
        clusterUpdateState();

    /* Save the config, possibly using fsync. */
    if (server.cluster->todo_before_sleep & CLUSTER_TODO_SAVE_CONFIG) {
        int fsync = server.cluster->todo_before_sleep &
                    CLUSTER_TODO_FSYNC_CONFIG;
        clusterSaveConfigOrDie(fsync);
    }

    /* Reset our flags (not strictly needed since every single function
     * called for flags set should be able to clear its flag). */
    server.cluster->todo_before_sleep = 0;
}

void clusterDoBeforeSleep(int flags) {
    server.cluster->todo_before_sleep |= flags;
}

/* -----------------------------------------------------------------------------
 * Cluster state evaluation function
 * -------------------------------------------------------------------------- */

/* The following are defines that are only used in the evaluation function
 * and are based on heuristics. Actaully the main point about the rejoin and
 * writable delay is that they should be a few orders of magnitude larger
 * than the network latency. */
#define CLUSTER_MAX_REJOIN_DELAY 5000
#define CLUSTER_MIN_REJOIN_DELAY 500
#define CLUSTER_WRITABLE_DELAY 2000

void clusterUpdateState(void) {
    server.cluster->todo_before_sleep &= ~CLUSTER_TODO_UPDATE_STATE;

    /* Compute the cluster size, that is the number of nodes
     * actually partecipating to the cluster (so nodes in handshake
     * state are excluded).
     *
     * Disque does not use any quorum, so this is only useful in order to
     * mark node from PFAIL to FAIL when we have failure reports from
     * majority. The notion of failure is only used for optimizations,
     * for example if a node appears to be failing we don't try to send
     * it ACKs or replicate messages to it. */
    {
        dictIterator *di;
        dictEntry *de;

        server.cluster->size = 0;
        di = dictGetSafeIterator(server.cluster->nodes);
        while((de = dictNext(di)) != NULL) {
            clusterNode *node = dictGetVal(de);

            if (!(node->flags & CLUSTER_NODE_HANDSHAKE))
                server.cluster->size++;
        }
        dictReleaseIterator(di);
    }
}

/* -----------------------------------------------------------------------------
 * Nodes to string representation functions.
 * -------------------------------------------------------------------------- */

struct disqueNodeFlags {
    uint16_t flag;
    char *name;
};

static struct disqueNodeFlags disqueNodeFlagsTable[] = {
    {CLUSTER_NODE_MYSELF,    "myself,"},
    {CLUSTER_NODE_PFAIL,     "fail?,"},
    {CLUSTER_NODE_FAIL,      "fail,"},
    {CLUSTER_NODE_HANDSHAKE, "handshake,"},
    {CLUSTER_NODE_NOADDR,    "noaddr,"},
    {CLUSTER_NODE_LEAVING,   "leaving,"}
};

/* Concatenate the comma separated list of node flags to the given SDS
 * string 'ci'. */
sds representClusterNodeFlags(sds ci, uint16_t flags) {
    if (flags == 0) {
        ci = sdscat(ci,"noflags,");
    } else {
        int i, size = sizeof(disqueNodeFlagsTable)/sizeof(struct disqueNodeFlags);
        for (i = 0; i < size; i++) {
            struct disqueNodeFlags *nodeflag = disqueNodeFlagsTable + i;
            if (flags & nodeflag->flag) ci = sdscat(ci, nodeflag->name);
        }
    }
    sdsIncrLen(ci,-1); /* Remove trailing comma. */
    return ci;
}

/* Generate a csv-alike representation of the specified cluster node.
 * See clusterGenNodesDescription() top comment for more information.
 *
 * The function returns the string representation as an SDS string. */
sds clusterGenNodeDescription(clusterNode *node) {
    sds ci;

    /* Node coordinates */
    ci = sdscatprintf(sdsempty(),"%.40s %s:%d ",
        node->name,
        node->ip,
        node->port);

    /* Flags */
    ci = representClusterNodeFlags(ci, node->flags);

    /* Latency from the POV of this node, link status */
    ci = sdscatprintf(ci," %lld %lld %s",
        (long long) node->ping_sent,
        (long long) node->pong_received,
        (node->link || node->flags & CLUSTER_NODE_MYSELF) ?
                    "connected" : "disconnected");

    return ci;
}

/* Generate a csv-alike representation of the nodes we are aware of,
 * including the "myself" node, and return an SDS string containing the
 * representation (it is up to the caller to free it).
 *
 * All the nodes matching at least one of the node flags specified in
 * "filter" are excluded from the output, so using zero as a filter will
 * include all the known nodes in the representation, including nodes in
 * the HANDSHAKE state.
 *
 * The representation obtained using this function is used for the output
 * of the CLUSTER NODES function, and as format for the cluster
 * configuration file (nodes.conf) for a given node. */
sds clusterGenNodesDescription(int filter) {
    sds ci = sdsempty(), ni;
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node->flags & filter) continue;
        ni = clusterGenNodeDescription(node);
        ci = sdscatsds(ci,ni);
        sdsfree(ni);
        ci = sdscatlen(ci,"\n",1);
    }
    dictReleaseIterator(di);
    return ci;
}

/* -----------------------------------------------------------------------------
 * CLUSTER utility functions
 * -------------------------------------------------------------------------- */

/* Update server.reachable_nodes and server.reachable_nodes_count with
 * a list of reachable nodes (not in PFAIL state), excluding this node.
 * Note that nodes in handshake are skipped as not yet part of the cluster,
 * this means that handshake nodes are not referenced by jobs, and are nodes
 * that we can safely free. */
void clusterUpdateReachableNodes(void) {
    dictIterator *di;
    dictEntry *de;
    int maxsize = dictSize(server.cluster->nodes) * sizeof(clusterNode*);

    server.cluster->reachable_nodes =
        zrealloc(server.cluster->reachable_nodes,maxsize);
    server.cluster->reachable_nodes_count = 0;

    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node->flags & (CLUSTER_NODE_MYSELF|
                           CLUSTER_NODE_HANDSHAKE|
                           CLUSTER_NODE_LEAVING|
                           CLUSTER_NODE_PFAIL|
                           CLUSTER_NODE_FAIL)) continue;
        server.cluster->reachable_nodes[server.cluster->reachable_nodes_count++]
            = node;
    }
    dictReleaseIterator(di);
}

/* Shuffle the array of reachable nodes using the Fisher Yates method so the
 * caller can just pick the first N to send messages to N random nodes.
 * */
void clusterShuffleReachableNodes(void) {
    int r, i;
    clusterNode *tmp;
    for(i = server.cluster->reachable_nodes_count - 1; i > 0; i--) {
        r = rand() % (i + 1);
        tmp = server.cluster->reachable_nodes[r];
        server.cluster->reachable_nodes[r] = server.cluster->reachable_nodes[i];
        server.cluster->reachable_nodes[i] = tmp;
    }
}

/* -----------------------------------------------------------------------------
 * CLUSTER command
 * -------------------------------------------------------------------------- */

void clusterCommand(client *c) {
    if (!strcasecmp(c->argv[1]->ptr,"meet") && c->argc == 4) {
        long long port;

        if (getLongLongFromObject(c->argv[3], &port) != C_OK) {
            addReplyErrorFormat(c,"Invalid TCP port specified: %s",
                                (char*)c->argv[3]->ptr);
            return;
        }

        if (clusterStartHandshake(c->argv[2]->ptr,port) == 0 &&
            errno == EINVAL)
        {
            addReplyErrorFormat(c,"Invalid node address specified: %s:%s",
                            (char*)c->argv[2]->ptr, (char*)c->argv[3]->ptr);
        } else {
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"nodes") && c->argc == 2) {
        /* CLUSTER NODES */
        robj *o;
        sds ci = clusterGenNodesDescription(0);

        o = createObject(OBJ_STRING,ci);
        addReplyBulk(c,o);
        decrRefCount(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"myid") && c->argc == 2) {
        /* CLUSTER MYID */
        addReplyBulkCBuffer(c,myself->name, CLUSTER_NAMELEN);
    } else if (!strcasecmp(c->argv[1]->ptr,"info") && c->argc == 2) {
        /* CLUSTER INFO */
        char *statestr[] = {"ok","fail","needhelp"};

        sds info = sdscatprintf(sdsempty(),
            "cluster_state:%s\r\n"
            "cluster_known_nodes:%lu\r\n"
            "cluster_reachable_nodes:%d\r\n"
            "cluster_size:%d\r\n"
            "cluster_stats_messages_sent:%lld\r\n"
            "cluster_stats_messages_received:%lld\r\n"
            , statestr[server.cluster->state],
            dictSize(server.cluster->nodes),
            server.cluster->reachable_nodes_count,
            server.cluster->size,
            server.cluster->stats_bus_messages_sent,
            server.cluster->stats_bus_messages_received
        );
        addReplyBulkSds(c, info);
    } else if (!strcasecmp(c->argv[1]->ptr,"saveconfig") && c->argc == 2) {
        int retval = clusterSaveConfig(1);

        if (retval == 0)
            addReply(c,shared.ok);
        else
            addReplyErrorFormat(c,"error saving the cluster node config: %s",
                strerror(errno));
    } else if (!strcasecmp(c->argv[1]->ptr,"forget") && c->argc == 3) {
        /* CLUSTER FORGET <NODE ID> */
        clusterNode *n;

        if (sdslen(c->argv[2]->ptr) != CLUSTER_NAMELEN) {
            addReplyError(c,"Invalid node identifier");
            return;
        }

        n = clusterLookupNode(c->argv[2]->ptr);

        if (!n) {
            addReplyErrorFormat(c,"Unknown node %s", (char*)c->argv[2]->ptr);
            return;
        } else if (n == myself) {
            addReplyError(c,"I tried hard but I can't forget myself...");
            return;
        }
        clusterBlacklistAddNode(n);
        clusterDelNode(n);
        clusterUpdateReachableNodes();
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|
                             CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"reset") &&
               (c->argc == 2 || c->argc == 3))
    {
        /* CLUSTER RESET [SOFT|HARD] */
        int hard = 0;

        /* Parse soft/hard argument. Default is soft. */
        if (c->argc == 3) {
            if (!strcasecmp(c->argv[2]->ptr,"hard")) {
                hard = 1;
            } else if (!strcasecmp(c->argv[2]->ptr,"soft")) {
                hard = 0;
            } else {
                addReply(c,shared.syntaxerr);
                return;
            }
        }

        clusterReset(hard);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"leaving") &&
               (c->argc == 2 || c->argc == 3))
    {
        /* CLUSTER LEAVING [yes|no] */
        if (c->argc == 3) {
            int oldflags = myself->flags;
            if (!strcasecmp(c->argv[2]->ptr,"yes")) {
                myself->flags |= CLUSTER_NODE_LEAVING;
                addReply(c,shared.ok);
            } else if (!strcasecmp(c->argv[2]->ptr,"no")) {
                myself->flags &= ~CLUSTER_NODE_LEAVING;
                addReply(c,shared.ok);
            } else {
                addReplyError(c,
                    "Wrong argument for CLUSTER LEAVING. Use 'yes' or 'no'");
            }
            if (oldflags != myself->flags)
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
        } else {
            addReplySds(c,
                (myself->flags & CLUSTER_NODE_LEAVING) ?
                sdsnew("+yes\r\n") : sdsnew("+no\r\n")
            );
        }
    } else {
        addReplyError(c,"Wrong CLUSTER subcommand or number of arguments");
    }
}

/* HELLO handshake command. Returns an array with:
 * 1) Reply version. 1 for this version.
 * 2) This node ID.
 * The following elements list all the available nodes with
 * ID, IP, port, priority. A smaller priority means better node
 * in terms of availability / latency. */
void helloCommand(client *c) {
    addReplyMultiBulkLen(c,2+dictSize(server.cluster->nodes));
    addReplyLongLong(c,1); /* Version. */
    addReplyBulkCBuffer(c,myself->name,CLUSTER_NAMELEN); /* My ID. */
    dictForeach(server.cluster->nodes,de)
        clusterNode *node = dictGetVal(de);
        int priority = 1;
        if (node->link == NULL && node != server.cluster->myself) priority = 5;
        if (node->flags & CLUSTER_NODE_PFAIL) priority = 10;
        if (node->flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_LEAVING))
            priority = 100;
        addReplyMultiBulkLen(c,4);
        addReplyBulkCBuffer(c,node->name,CLUSTER_NAMELEN); /* ID. */
        addReplyBulkCString(c,node->ip); /* IP address. */
        addReplyBulkLongLong(c,node->port); /* TCP port. */
        addReplyBulkLongLong(c,priority); /* Priority. */
    dictEndForeach
}
