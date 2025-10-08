#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "types.h"
#include "config.h"

typedef struct slice slice_t;
typedef struct slice_list slice_list_t;
typedef struct slice_allocator slice_allocator_t;

static slice_allocator_t slice_allocator;

struct slice {
    tuple_t *tuples;
    size_t n_tuples;
    slice_t *next;
};

struct slice_list {
    slice_t *head;
};

struct slice_allocator {
    slice_t *slices;
    size_t max_size;
    atomic_size_t next_free;
};

static inline void slice_list_add(slice_list_t *list, slice_t *slice);

static inline void slice_allocator_init(size_t max_size);
static inline void slice_allocator_free(void);
static inline slice_t *slice_alloc(void);
static inline slice_t *slice_alloc_atomic(void);

void slice_list_add(slice_list_t *list, slice_t *slice) {
    slice->next = list->head;
    list->head = slice;
}

void slice_allocator_init(size_t max_size) {
    slice_allocator.slices = calloc(max_size, sizeof(slice_t));
    BUG_ON(!slice_allocator.slices);
    slice_allocator.max_size = max_size;
    atomic_init(&slice_allocator.next_free, 0);
}

void slice_allocator_free(void) {
    free(slice_allocator.slices);
    slice_allocator.slices = NULL;
    slice_allocator.max_size = 0;
    slice_allocator.next_free = 0;
}

slice_t *slice_alloc(void) {
    if (slice_allocator.next_free >= slice_allocator.max_size) {
        return NULL;
    }
    return &slice_allocator.slices[slice_allocator.next_free++];
}

slice_t *slice_alloc_atomic(void) {
    size_t idx = atomic_fetch_add(&slice_allocator.next_free, 1);
    if (idx >= slice_allocator.max_size) {
        return NULL;
    }
    return &slice_allocator.slices[idx];
}
