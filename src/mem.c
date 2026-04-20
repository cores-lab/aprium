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
    uint64_t *thread_hist_r;
    uint64_t *thread_hist_s;
    uint8_t  *part_assign;
    uint64_t *local_offs_r;
    uint64_t *local_offs_s;
    tuple_t *local_tmp_r;
    tuple_t *local_tmp_s;
    //uint64_t *global_hist_r;
    //uint64_t *global_hist_s;
    allocator_t *alloc; /* one allocator per thread */
};
typedef struct mem mem_t;

static mem_t mem = { 0 };

size_t hist_size(size_t fanout) {
    return round_up(fanout * sizeof(uint64_t), CACHELINE_SIZE);
}

static size_t tmp_size(size_t n_tuples) {
    return round_up(n_tuples * sizeof(tuple_t) + RELATION_PADDING, CACHELINE_SIZE);
}

void mem_alloc(size_t r_tuples, size_t s_tuples, size_t n_threads) {

    /* global */
    size_t p1_thread_hist = hist_size(FANOUT_PASS1) * n_threads;
    size_t p1_local_offs = hist_size(FANOUT_PASS1 + 1);
    size_t p1_local_tmp_r = tmp_size(r_tuples);
    size_t p1_local_tmp_s = tmp_size(s_tuples);

    size_t p1_part_assign = round_up(FANOUT_PASS1 * sizeof(uint8_t), CACHELINE_SIZE);

    /* per thread */
    // TODO: here we assume uniform relations
    // TODO: here we assume R and S are same size
    // TODO: here we assume cacheline alignment
    size_t per_thread = 4 * (round_up(2 * sizeof(uint64_t) * FANOUT_PASS1, CACHELINE_SIZE)
        + round_up(1 * sizeof(uint64_t) * FANOUT_PASS2, CACHELINE_SIZE)
        + round_up(2 * sizeof(uint64_t) * (FANOUT_PASS2+1), CACHELINE_SIZE)
        + round_up(1 * 144 * FANOUT_PASS1, CACHELINE_SIZE)
        + round_up((r_tuples / n_threads) * sizeof(tuple_t), CACHELINE_SIZE))
        + round_up(10 * L1_CACHE_SIZE, CACHELINE_SIZE);

    /* allocator metadata */
    size_t meta = round_up(n_threads * sizeof(allocator_t), CACHELINE_SIZE);

    /* alloc */
    size_t bytes = 0;
    bytes += 2ULL * p1_thread_hist;
    bytes += 2ULL * p1_local_offs;
    bytes += p1_part_assign;
    bytes += p1_local_tmp_r;
    bytes += p1_local_tmp_s;
    bytes += meta;
    bytes += (per_thread * n_threads);

    BUG_ON(bytes % CACHELINE_SIZE);

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
    mem.thread_hist_r = (uint64_t *)ptr;
    ptr += p1_thread_hist;
    mem.thread_hist_s = (uint64_t *)ptr;
    ptr += p1_thread_hist;
    mem.local_offs_r = (uint64_t *)ptr;
    ptr += p1_local_offs;
    mem.local_offs_s = (uint64_t *)ptr;
    ptr += p1_local_offs;
    mem.part_assign = (uint8_t *)ptr;
    ptr += p1_part_assign;
    mem.local_tmp_r = (tuple_t *)ptr;
    ptr += p1_local_tmp_r;
    mem.local_tmp_s = (tuple_t *)ptr;
    ptr += p1_local_tmp_s;
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
    return mem.thread_hist_r;
}

uint64_t *mem_p1_thread_hist_s(void) {
    return mem.thread_hist_s;
}

uint8_t *mem_p1_part_assign(void) {
    return mem.part_assign;
}

uint64_t *mem_p1_local_offs_r(void) {
    return mem.local_offs_r;
}

uint64_t *mem_p1_local_offs_s(void) {
    return mem.local_offs_s;
}

tuple_t *mem_p1_local_tmp_r(void) {
    return mem.local_tmp_r;
}

tuple_t *mem_p1_local_tmp_s(void) {
    return mem.local_tmp_s;
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

void *mem_reuse_for(size_t tid, size_t bytes) {
    BUG_ON(!mem.base);
    BUG_ON(!bytes);
    BUG_ON(tid >= mem.n_threads);

    size_t want = round_up(bytes, CACHELINE_SIZE);

    uintptr_t cur = mem.alloc[tid].cur;

    // TODO: unsafe backwards reach here. check bounds of allocator mem!

    return (void *)(cur - want);
}
