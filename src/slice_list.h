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
    bool is_compressed;
    uint8_t compressed_radix;
};

struct slice_list {
    slice_t *head;
};

struct slice_allocator {
    slice_t *slices;
    size_t max_size;
    atomic_size_t next_free;
};

static void slice_list_add(slice_list_t *list, slice_t *slice) {
    slice->next = list->head;
    list->head = slice;
}

static void slice_allocator_init(size_t size) {
    size_t max_size = (size_t)(size * OVERALLOC);
    slice_allocator.slices = calloc(max_size, sizeof(slice_t));
    BUG_ON(!slice_allocator.slices);
    slice_allocator.max_size = max_size;
    atomic_init(&slice_allocator.next_free, 0);
}

static void slice_allocator_free(void) {
    free(slice_allocator.slices);
    slice_allocator.slices = NULL;
    slice_allocator.max_size = 0;
    slice_allocator.next_free = 0;
}

static slice_t *slice_alloc(void) {
    if (slice_allocator.next_free >= slice_allocator.max_size) {
        return NULL;
    }
    return &slice_allocator.slices[slice_allocator.next_free++];
}

static slice_t *slice_alloc_atomic(void) {
    size_t idx = atomic_fetch_add(&slice_allocator.next_free, 1);
    if (idx >= slice_allocator.max_size) {
        return NULL;
    }
    return &slice_allocator.slices[idx];
}
