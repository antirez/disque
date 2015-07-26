/*
 * Copyright (c) 2009-2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2014, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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
 *   * Neither the name of Redis nor the names of its contributors may be used
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

/* This skip list implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful to model certain problems.
 *
 * This implementation originates from the Redis code base but was modified
 * in different ways. */

#include "server.h"
#include "zmalloc.h"
#include "skiplist.h"

#include <math.h>
#include <stdlib.h>
#include <sys/types.h>

#ifdef TEST_MAIN
#define zmalloc malloc
#define zfree free
#include <stdlib.h>
#endif

/* Create a skip list node with the specified number of levels, pointing to
 * the specified object. */
skiplistNode *skiplistCreateNode(int level, void *obj) {
    skiplistNode *zn = zmalloc(sizeof(*zn)+level*sizeof(struct skiplistLevel));
    zn->obj = obj;
    return zn;
}

/* Create a new skip list with the specified function used in order to
 * compare elements. The function return value is the same as strcmp(). */
skiplist *skiplistCreate(int (*compare)(const void *, const void *)) {
    int j;
    skiplist *sl;

    sl = zmalloc(sizeof(*sl));
    sl->level = 1;
    sl->length = 0;
    sl->header = skiplistCreateNode(SKIPLIST_MAXLEVEL,NULL);
    for (j = 0; j < SKIPLIST_MAXLEVEL; j++) {
        sl->header->level[j].forward = NULL;
        sl->header->level[j].span = 0;
    }
    sl->header->backward = NULL;
    sl->tail = NULL;
    sl->compare = compare;
    return sl;
}

/* Free a skiplist node. We don't free the node's pointed object. */
void skiplistFreeNode(skiplistNode *node) {
    zfree(node);
}

/* Free an entire skiplist. */
void skiplistFree(skiplist *sl) {
    skiplistNode *node = sl->header->level[0].forward, *next;

    zfree(sl->header);
    while(node) {
        next = node->level[0].forward;
        skiplistFreeNode(node);
        node = next;
    }
    zfree(sl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and SKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
int skiplistRandomLevel(void) {
    int level = 1;
    while ((random()&0xFFFF) < (SKIPLIST_P * 0xFFFF))
        level += 1;
    return (level<SKIPLIST_MAXLEVEL) ? level : SKIPLIST_MAXLEVEL;
}

/* Insert the specified object, return NULL if the element already
 * exists. */
skiplistNode *skiplistInsert(skiplist *sl, void *obj) {
    skiplistNode *update[SKIPLIST_MAXLEVEL], *x;
    unsigned int rank[SKIPLIST_MAXLEVEL];
    int i, level;

    x = sl->header;
    for (i = sl->level-1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        rank[i] = i == (sl->level-1) ? 0 : rank[i+1];
        while (x->level[i].forward &&
               sl->compare(x->level[i].forward->obj,obj) < 0)
        {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* If the element is already inside, return NULL. */
    if (x->level[0].forward &&
        sl->compare(x->level[0].forward->obj,obj) == 0) return NULL;

    /* Add a new node with a random number of levels. */
    level = skiplistRandomLevel();
    if (level > sl->level) {
        for (i = sl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = sl->header;
            update[i]->level[i].span = sl->length;
        }
        sl->level = level;
    }
    x = skiplistCreateNode(level,obj);
    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    for (i = level; i < sl->level; i++) {
        update[i]->level[i].span++;
    }

    x->backward = (update[0] == sl->header) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        sl->tail = x;
    sl->length++;
    return x;
}

/* Internal function used by skiplistDelete, it needs an array of other
 * skiplist nodes that point to the node to delete in order to update
 * all the references of the node we are going to remove. */
void skiplistDeleteNode(skiplist *sl, skiplistNode *x, skiplistNode **update) {
    int i;
    for (i = 0; i < sl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        sl->tail = x->backward;
    }
    while(sl->level > 1 && sl->header->level[sl->level-1].forward == NULL)
        sl->level--;
    sl->length--;
}

/* Delete an element from the skiplist. If the element was found and deleted
 * 1 is returned, otherwise if the element was not there, 0 is returned. */
int skiplistDelete(skiplist *sl, void *obj) {
    skiplistNode *update[SKIPLIST_MAXLEVEL], *x;
    int i;

    x = sl->header;
    for (i = sl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
               sl->compare(x->level[i].forward->obj,obj) < 0)
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    x = x->level[0].forward;
    if (x && sl->compare(x->obj,obj) == 0) {
        skiplistDeleteNode(sl, x, update);
        skiplistFreeNode(x);
        return 1;
    }
    return 0; /* not found */
}

/* Search for the element in the skip list, if found the
 * node pointer is returned, otherwise NULL is returned. */
void *skiplistFind(skiplist *sl, void *obj) {
    skiplistNode *x;
    int i;

    x = sl->header;
    for (i = sl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
               sl->compare(x->level[i].forward->obj,obj) < 0)
        {
            x = x->level[i].forward;
        }
    }
    x = x->level[0].forward;
    if (x && sl->compare(x->obj,obj) == 0) {
        return x;
    } else {
        return NULL;
    }
}

/* If the skip list is empty, NULL is returned, otherwise the element
 * at head is removed and its pointed object returned. */
void *skiplistPopHead(skiplist *sl) {
    skiplistNode *x = sl->header;

    x = x->level[0].forward;
    if (!x) return NULL;
    void *ptr = x->obj;
    skiplistDelete(sl,ptr);
    return ptr;
}

/* If the skip list is empty, NULL is returned, otherwise the element
 * at tail is removed and its pointed object returned. */
void *skiplistPopTail(skiplist *sl) {
    skiplistNode *x = sl->tail;

    if (!x) return NULL;
    void *ptr = x->obj;
    skiplistDelete(sl,ptr);
    return ptr;
}

unsigned long skiplistLength(skiplist *sl) {
    return sl->length;
}

#ifdef TEST_MAIN
#include <stdio.h>
#include <string.h>
#include <unistd.h>
int compare(const void *a, const void *b) {
    return strcmp(a,b);
}

int main(void) {
    char *words[] = {
        "foo", "bar", "zap", "pomo", "pera", "arancio", "limone", NULL
    };
    int j;

    skiplist *sl = skiplistCreate(compare);
    for (j = 0; words[j] != NULL; j++)
        printf("Insert %s: %p\n",
            words[j],
            skiplistInsert(sl,words[j]));

    /* The following should fail. */
    printf("\nInsert %s again: %p\n\n", words[2], skiplistInsert(sl,words[2]));

    skiplistNode *x;
    x = sl->header;
    x = x->level[0].forward;
    while(x) {
        printf("%s\n", x->obj);
        x = x->level[0].forward;
    }

    printf("Searching for 'hello': %p\n", skiplistFind(sl,"hello"));
    printf("Searching for 'pera': %p\n", skiplistFind(sl,"pera"));

    printf("Pop from head: %s\n", skiplistPopHead(sl));
    printf("Pop from head: %s\n", skiplistPopHead(sl));

    printf("Pop from tail: %s\n", skiplistPopTail(sl));
    printf("Pop from tail: %s\n", skiplistPopTail(sl));
    printf("Pop from tail: %s\n", skiplistPopTail(sl));
    printf("Pop from tail: %s\n", skiplistPopTail(sl));
    printf("Pop from tail: %s\n", skiplistPopTail(sl));
    printf("Pop from tail: %s\n", skiplistPopTail(sl));

    printf("Pop from head: %s\n", skiplistPopTail(sl));
}
#endif
