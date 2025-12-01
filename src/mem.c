#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mem.h"
#include "config.h"

struct allocator {
    alignas(CACHELINE_SIZE)
    uintptr_t cur;
    uintptr_t end;
    uint8_t _pad[(CACHELINE_SIZE - (2 * sizeof(uintptr_t)))];
};
typedef struct allocator allocator_t;

struct mem {
    void *base;
    size_t size;
    size_t n_threads;
    uint64_t *thist_r;
    uint64_t *thist_s;
    //uint64_t *ghist_r;
    //uint64_t *ghist_s;
    //uint8_t *node_part_assign;
    allocator_t *alloc; /* one allocator per thread */
};
typedef struct mem mem_t;

static mem_t mem = { 0 };

size_t hist_size(size_t fanout) {
    return round_up(fanout * sizeof(uint64_t), CACHELINE_SIZE);
}

void mem_alloc(size_t r_tuples, size_t s_tuples, size_t n_threads) {

    /* global */
    size_t p1_thist = hist_size(FANOUT_PASS1) * n_threads;
    //size_t p1_offs = hist_size(FANOUT_PASS1) * n_threads;
    //size_t p1_tmp = 0;

    /* per thread */
    // TODO: here we assume uniform relations
    // TODO: here we assume R and S are same size
    // TODO: here we assume cacheline alignment
    (void)s_tuples;
    size_t per_thread = r_tuples * sizeof(tuple_t) / 2UL;
    /* allocator metadata */
    size_t meta = round_up(n_threads * sizeof(allocator_t), CACHELINE_SIZE);

    /* alloc */
    size_t bytes = 0;
    bytes += 2 * p1_thist;
    bytes += meta;
    bytes += (per_thread * n_threads);

#if DEBUG
    printf("Initializing local memory (size = %.3lf MiB): ",
            (double) bytes / 1024.0 / 1024.0);
    fflush(stdout);
#endif

    mem.base = aligned_alloc(CACHELINE_SIZE, bytes);
    BUG_ON(!mem.base);
    memset(mem.base, 0, bytes);
    mem.size = bytes;

    /* init */
    uintptr_t ptr = (uintptr_t)mem.base;
    mem.thist_r = (uint64_t *)ptr;
    ptr += p1_thist;
    mem.thist_s = (uint64_t *)ptr;
    ptr += p1_thist;
    mem.alloc = (allocator_t *)ptr;
    ptr += meta;

    for (size_t i = 0; i < n_threads; i++) {
        uintptr_t start = ptr + i * per_thread;
        mem.alloc[i].cur = start;
        mem.alloc[i].end = start + per_thread;
    }

    mem.n_threads = n_threads;

#if DEBUG
    printf("OK\n");
#endif
}

void mem_free(void) {
    free(mem.base);
}

uint64_t *mem_p1_thread_hist_r(void) {
    return mem.thist_r;
}

uint64_t *mem_p1_thread_hist_s(void) {
    return mem.thist_s;
}

void *mem_for(size_t tid, size_t bytes) {
    BUG_ON(!mem.base);
    BUG_ON(!bytes);
    BUG_ON(tid >= mem.n_threads);

    size_t want = round_up(bytes, CACHELINE_SIZE);

    uintptr_t cur = mem.alloc[tid].cur;
    uintptr_t end = mem.alloc[tid].end;

    BUG_ON(cur + want > end);

    void *res = (void *)cur;
    cur += want;
    mem.alloc[tid].cur = cur;

    return res;
}

