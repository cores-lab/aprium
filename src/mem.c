#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mem.h"
#include "config.h"

struct mem {
    void *base;
    size_t size;
    size_t n_threads;
    uint64_t *hist_r;
    uint64_t *hist_s;
    void **tcur;
    void **tend;
};
typedef struct mem mem_t;

static mem_t mem = { 0 };

size_t hist_size(size_t fanout) {
    return round_up(fanout * sizeof(uint64_t), CACHELINE_SIZE);
}

void mem_alloc(size_t r_tuples, size_t s_tuples, size_t n_threads) {

    /* global */
    size_t p1_hist = hist_size(FANOUT_PASS1) * n_threads;
    //size_t p1_offs = hist_size(FANOUT_PASS1) * n_threads;
    //size_t p1_tmp = 0;

    /* per thread */
    // TODO: here we assume uniform relations
    // TODO: here we assume R and S are same size
    // TODO: here we assume cacheline alignment
    (void)s_tuples;
    size_t per_thread = r_tuples * sizeof(tuple_t) / 2UL;
    /* allocator metadata */
    size_t meta = round_up(n_threads * sizeof(void *), CACHELINE_SIZE);

    /* alloc */
    size_t bytes = 0;
    bytes += p1_hist;
    bytes += meta;
    bytes *= 2;
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
    mem.hist_r = (uint64_t *)ptr;
    ptr += p1_hist;
    mem.hist_s = (uint64_t *)ptr;
    ptr += p1_hist;
    mem.tcur = (void **)ptr;
    ptr += meta;
    mem.tend = (void **)ptr;
    ptr += meta;
    for (size_t i = 0; i < n_threads; i++) {
        uintptr_t start = ptr + i * (per_thread);
        uintptr_t end = start + (per_thread);
        mem.tcur[i] = (void *)start;
        mem.tend[i] = (void *)end;
    }
    mem.n_threads = n_threads;
#if DEBUG
    printf("OK\n");
#endif
}

void mem_free(void) {
    free(mem.base);
}

uint64_t *mem_p1_hist_r(void) {
    return mem.hist_r;
}

uint64_t *mem_p1_hist_s(void) {
    return mem.hist_s;
}

void *mem_for(size_t tid, size_t bytes) {
    BUG_ON(!mem.base);
    BUG_ON(!bytes);
    BUG_ON(tid >= mem.n_threads);

    size_t want = round_up(bytes, CACHELINE_SIZE);

    uintptr_t cur = (uintptr_t)mem.tcur[tid];
    uintptr_t end = (uintptr_t)mem.tend[tid];

    BUG_ON(cur + want > end);

    void *res = (void *)cur;
    cur += want;
    mem.tcur[tid] = (void *)cur;

    return res;
}

