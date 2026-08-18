#include <pthread.h>
#include <sched.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mem.h"
#include "config.h"

typedef struct {
    alignas(CACHELINE_SIZE)
    atomic_uintptr_t cur;
    uintptr_t end;
    uint8_t *base;
} allocator_t;

typedef struct {
    alignas(CACHELINE_SIZE)
    uintptr_t addr;
} padded_addr_t;

typedef struct {
    void *base;
    size_t n_threads;
    uint8_t  *part_assign;
    uint64_t *local_offs_r;
    uint64_t *local_offs_s;
    tuple_t *local_tmp_r;
    tuple_t *local_tmp_s;
    allocator_t allocators[N_NUMA_NODES];
    padded_addr_t *last_alloc;
} layout_t;

static layout_t layout = { 0 };

static void *touch_worker(void *arg) {
    allocator_t *alloc = (allocator_t *)arg;
    memset(alloc->base, 0, alloc->end - (uintptr_t)alloc->base);
    return NULL;
}

static inline int get_cpu_in_numa_node(size_t numa_node) {
    for (size_t c = 0; c < N_CPUS; c++) {
        if (NUMA_MAPPING[c] == numa_node) {
            return c;
        }
    }
    return 0;
}

void mem_alloc(size_t r_tuples, size_t s_tuples, size_t n_threads, size_t n_nodes) {
    layout.n_threads = n_threads;

    /* shared */
    size_t p1_local_offs = round_up((FANOUT_PASS1 + 1) * sizeof(uint64_t), CACHELINE_SIZE);
    size_t padd = FANOUT_PASS1 * (n_threads + 3) * CACHELINE_SIZE;
    size_t p1_local_tmp_r = round_up((r_tuples / n_nodes) * sizeof(tuple_t) + padd, CACHELINE_SIZE);
    size_t p1_local_tmp_s = round_up((s_tuples / n_nodes) * sizeof(tuple_t) + padd, CACHELINE_SIZE);
    size_t p1_part_assign = round_up(FANOUT_PASS1 * sizeof(uint8_t), CACHELINE_SIZE);
    size_t last_alloc = round_up(n_threads * sizeof(padded_addr_t), CACHELINE_SIZE);

    size_t shared = 0;
    shared += 2ULL * p1_local_offs;
    shared += p1_part_assign;
    shared += p1_local_tmp_r;
    shared += p1_local_tmp_s;
    shared += last_alloc;

    BUG_ON(shared % CACHELINE_SIZE != 0);

    /* per NUMA node */
    size_t worst = (r_tuples + s_tuples) * sizeof(tuple_t) * 1.5;
    size_t numa = round_up(worst, CACHELINE_SIZE);

#if DEBUG
    size_t total = shared + numa;
    printf("Initializing local memory (size = %.3lf MiB): ",
            (double) total / 1024.0 / 1024.0);
    fflush(stdout);
#endif

    layout.base = aligned_alloc(CACHELINE_SIZE, shared);
    BUG_ON(!layout.base);
    memset(layout.base, 0, shared);

    uintptr_t ptr = (uintptr_t)layout.base;
    layout.local_offs_r = (uint64_t *)ptr;
    ptr += p1_local_offs;
    layout.local_offs_s = (uint64_t *)ptr;
    ptr += p1_local_offs;
    layout.part_assign = (uint8_t *)ptr;
    ptr += p1_part_assign;
    layout.local_tmp_r = (tuple_t *)ptr;
    ptr += p1_local_tmp_r;
    layout.local_tmp_s = (tuple_t *)ptr;
    ptr += p1_local_tmp_s;
    layout.last_alloc = (padded_addr_t *)ptr;
    ptr += last_alloc;

    size_t numa_hist[N_NUMA_NODES] = { 0 };
    for (size_t t = 0; t < n_threads; t++) {
        size_t n = NUMA_MAPPING[CPU_MAPPING[t]];
        numa_hist[n]++;
    }

    pthread_t tids[N_NUMA_NODES];
    pthread_attr_t attr;
    int err = pthread_attr_init(&attr);
    BUG_ON(err != 0);
    cpu_set_t cpuset;

    for (size_t i = 0; i < N_NUMA_NODES; i++) {
        if (numa_hist[i] == 0) {
            continue;
        }

        size_t pool = round_up((numa * numa_hist[i]) / n_threads, PAGE_SIZE);
        void *mem = aligned_alloc(PAGE_SIZE, pool);
        BUG_ON(!mem);

        layout.allocators[i].base = (uint8_t *)mem;
        atomic_init(&layout.allocators[i].cur, (uintptr_t)mem);
        layout.allocators[i].end = (uintptr_t)mem + pool;

        CPU_ZERO(&cpuset);
        CPU_SET(get_cpu_in_numa_node(i), &cpuset);
        int err = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        BUG_ON(err != 0);
        err = pthread_create(&tids[i], &attr, touch_worker, &layout.allocators[i]);
        BUG_ON(err != 0);
    }

    err = pthread_attr_destroy(&attr);
    BUG_ON(err != 0);
    for (size_t i = 0; i < N_NUMA_NODES; i++) {
        if (numa_hist[i] > 0) {
            pthread_join(tids[i], NULL);
        }
    }

#if DEBUG
    printf("OK\n");
#endif
}

void mem_free(void) {
    free(layout.base);
    for (size_t i = 0; i < N_NUMA_NODES; i++) {
        free(layout.allocators[i].base);
    }
}

uint8_t *mem_p1_part_assign(void) {
    return layout.part_assign;
}

uint64_t *mem_p1_local_offs_r(void) {
    return layout.local_offs_r;
}

uint64_t *mem_p1_local_offs_s(void) {
    return layout.local_offs_s;
}

tuple_t *mem_p1_local_tmp_r(void) {
    return layout.local_tmp_r;
}

tuple_t *mem_p1_local_tmp_s(void) {
    return layout.local_tmp_s;
}

void *mem_for(size_t tid, size_t bytes) {
    BUG_ON(!bytes);
    BUG_ON(tid >= layout.n_threads);

    size_t want = round_up(bytes, CACHELINE_SIZE);
    size_t numa_node = NUMA_MAPPING[CPU_MAPPING[tid]];
    allocator_t *alloc = &layout.allocators[numa_node];

    uintptr_t addr = atomic_fetch_add_explicit(&alloc->cur, want, memory_order_relaxed);

    BUG_ON(addr + want > alloc->end);

    layout.last_alloc[tid].addr = addr;
    return (void *)addr;
}

void *mem_reuse_for(size_t tid, size_t bytes) {
    BUG_ON(!bytes);
    BUG_ON(tid >= layout.n_threads);

    // This assumes bytes is smaller than the last allocation.
    uintptr_t prev_addr = layout.last_alloc[tid].addr;
    BUG_ON(!prev_addr);

    return (void *)prev_addr;
}
