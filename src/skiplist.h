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


#ifndef __DISQUE_SKIPLIST_H
#define __DISQUE_SKIPLIST_H

#define SKIPLIST_MAXLEVEL 32 /* Should be enough for 2^32 elements */
#define SKIPLIST_P 0.25      /* Skiplist P = 1/4 */

typedef struct skiplistNode {
    void *obj;
    struct skiplistNode *backward;
    struct skiplistLevel {
        struct skiplistNode *forward;
        unsigned int span;
    } level[];
} skiplistNode;

typedef struct skiplist {
    struct skiplistNode *header, *tail;
    int (*compare)(const void *, const void *);
    unsigned long length;
    int level;
} skiplist;

typedef struct skiplistIter {
    skiplistNode *next;
    int direction;
} skiplistIter;

skiplist *skiplistCreate(int (*compare)(const void *, const void *));
void skiplistFree(skiplist *sl);
skiplistNode *skiplistInsert(skiplist *sl, void *obj);
int skiplistDelete(skiplist *sl, void *obj);
void *skiplistFind(skiplist *sl, void *obj);
void *skiplistPopHead(skiplist *sl);
void *skiplistPopTail(skiplist *sl);
unsigned long skiplistLength(skiplist *sl);

/* Directions for iterators */
#define SL_START_HEAD 0
#define SL_START_TAIL 1

/* Create an iterator in the skiplist private iterator structure
 * skiplistIter should be a pointer point to the struct.
 * */
#define skiplistRewind(skiplist, skiplistIter) do { \
    (skiplistIter)->next = (skiplist)->header->level[0].forward; \
    (skiplistIter)->direction = SL_START_HEAD; \
} while(0)

#define skiplistRewindTail(skiplist, skiplistIter) do { \
    (skiplistIter)->next = (skiplist)->tail; \
    (skiplistIter)->direction = SL_START_TAIL; \
} while(0)

inline skiplistNode *skiplistNext(skiplistIter *iter) {
    skiplistNode *current = iter->next;
    if (current != NULL) {
        iter->next = iter->direction == SL_START_HEAD ?
            current->level[0].forward : current->backward;
    }
    return current;
}

#endif
