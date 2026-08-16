#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>
#include <x86intrin.h>

#include "cxl.h"
#include "config.h"

/* n_nodes of cachlines for flags (in each first uint64_t):
 * one       cacheline  (release flag,  coord  -> worker)
 * n_nodes-1 cachelines (arrival flags, worker -> coord)
 */
typedef struct barrier {
    uint64_t flag;
    uint8_t _pad[(CACHELINE_SIZE - sizeof(uint64_t))];
} barrier_t;

typedef struct {
    void *base;
    size_t size;
    size_t my_nid;
    size_t n_nodes;
    barrier_t *barrier; /* one barrier per node */
    tuple_t *gen_r;
    tuple_t *gen_s;
    uint64_t *thread_hist_r;
    uint64_t *thread_hist_s;
    uint64_t *node_hist_r;
    uint64_t *node_hist_s;
    uint64_t *offs_r;
    uint64_t *offs_s;
    tuple_t *remote_tmp_r;
    tuple_t *remote_tmp_s;
} layout_t;

static layout_t layout = { 0 };

typedef struct {
    char const *path;
    size_t size;
} dax_device_t;

/* Init */
void *cxl_map(void) {
    dax_device_t devices[] = {
        {"/dev/dax0.0", 260650827776ULL},
        {"/dev/dax1.0", 289104986112ULL},
        {"/dev/dax2.0", 274877906944ULL}
    };

    size_t n_devices = sizeof(devices) / sizeof(devices[0]);
    size_t total_size = 0;

    for (size_t i = 0; i < n_devices; i++) {
        total_size += devices[i].size;
    }

    void *base = mmap(NULL, total_size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    BUG_ON(base == MAP_FAILED);

    size_t offset = 0;
    for (size_t i = 0; i < n_devices; i++) {
        int fd = open(devices[i].path, O_RDWR);
        BUG_ON(fd < 0);
        void *target = (uint8_t *)base + offset;
        void *addr = mmap(target, devices[i].size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
        BUG_ON(addr == MAP_FAILED);
        close(fd);
        offset += devices[i].size;
    }

    layout.base = base;
    layout.size = total_size;

    return base;

    // dax_device_t devices[] = {
    //     {"/dev/shm/mock_dax0.0", 64 * (1ULL << 30)},
    // };

    // int fd = open(devices[0].path, O_RDWR | O_CREAT, 0666);
    // BUG_ON(fd < 0);
    // BUG_ON(ftruncate(fd, devices[0].size) != 0);

    // void *base = mmap(NULL, devices[0].size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    // BUG_ON(base == MAP_FAILED);

    // close(fd);

    // layout.base = base;
    // layout.size = devices[0].size;

    // return base;
}

void cxl_alloc(size_t r_tuples, size_t s_tuples, size_t n_threads, size_t my_nid, size_t n_nodes) {
    bool const is_coordinator_node = (my_nid == COORDINATION_NODE);

    void *base = cxl_map();

    size_t barrier = n_nodes * sizeof(barrier_t);
    size_t r_size = round_up(r_tuples * sizeof(tuple_t), CACHELINE_SIZE);
    size_t s_size = round_up(s_tuples * sizeof(tuple_t), CACHELINE_SIZE);
    size_t thread_hist = round_up(FANOUT_PASS1 * sizeof(uint64_t), CACHELINE_SIZE) * n_threads * n_nodes;
    size_t node_hist = round_up(FANOUT_PASS1 * sizeof(uint64_t), CACHELINE_SIZE) * n_nodes;
    size_t offs = round_up((FANOUT_PASS1 + 1) * sizeof(uint64_t), CACHELINE_SIZE) * n_nodes;
    size_t padd = FANOUT_PASS1 * (n_nodes * n_threads + 3) * CACHELINE_SIZE;
    size_t tmp_r = round_up((r_tuples / n_nodes) * sizeof(tuple_t) + padd, CACHELINE_SIZE);
    size_t tmp_s = round_up(s_tuples * sizeof(tuple_t) + padd, CACHELINE_SIZE);

    size_t bytes = 0;
    bytes += barrier;
    bytes += r_size;
    bytes += s_size;
    bytes += 2ULL * thread_hist;
    bytes += 2ULL * node_hist;
    bytes += 2ULL * offs;
    bytes += tmp_r;
    bytes += tmp_s;

    BUG_ON(bytes % CACHELINE_SIZE != 0);
    BUG_ON(bytes > 384ULL * (1ULL << 30));

#if DEBUG
    printf("Initializing CXL memory (size = %.3lf MiB, addr = %p): ",
(double) bytes / 1024.0 / 1024.0, base);
    fflush(stdout);
#endif

    /* Coordinator zeros out CXL memory */
    if (is_coordinator_node) {
        memset(base, 0, bytes);
    }
    else {
        sleep(5);
    }

    cache_inv(base, bytes, true);

    uintptr_t ptr = (uintptr_t)base;
    layout.barrier = (barrier_t *)(ptr);
    ptr += barrier;
    layout.gen_r = (tuple_t *)ptr;
    ptr += r_size;
    layout.gen_s = (tuple_t *)ptr;
    ptr += s_size;
    layout.thread_hist_r = (uint64_t *)ptr;
    ptr += thread_hist;
    layout.thread_hist_s = (uint64_t *)ptr;
    ptr += thread_hist;
    layout.node_hist_r = (uint64_t *)ptr;
    ptr += node_hist;
    layout.node_hist_s = (uint64_t *)ptr;
    ptr += node_hist;
    layout.offs_r = (uint64_t *)ptr;
    ptr += offs;
    layout.offs_s = (uint64_t *)ptr;
    ptr += offs;
    layout.remote_tmp_r = (tuple_t *)ptr;
    ptr += tmp_r;
    layout.remote_tmp_s = (tuple_t *)ptr;
    ptr += tmp_s;

    layout.my_nid = my_nid;
    layout.n_nodes = n_nodes;

#if DEBUG
    printf("OK\n");
#endif

}

void cxl_free(void) {
    int ret = munmap(layout.base, layout.size);
    BUG_ON(ret);
}

/* Barrier */
static inline void store_flush(volatile uint64_t *p, uint64_t v) {
    *p = v;
    cache_wb((void *)p, sizeof(p), true);
}

static inline uint64_t load_inval(volatile uint64_t *p) {
    cache_inv((void *)p, sizeof(p), true);
    return *p;
}

static inline void spin(void) {
    __asm__ volatile("pause" ::: "memory");
}

void cxl_barrier(void) {
    static uint64_t gen = 1;
    if (layout.my_nid == COORDINATION_NODE) {
        for (size_t i = 0; i < layout.n_nodes; i++) {
            if (i == COORDINATION_NODE) {
                continue;
            }
            while (load_inval(&layout.barrier[i].flag) != gen) {
                spin();
            }
        }
        store_flush(&layout.barrier[COORDINATION_NODE].flag, gen);
        gen++;
    }
    else {
        store_flush(&layout.barrier[layout.my_nid].flag, gen);
        while (load_inval(&layout.barrier[COORDINATION_NODE].flag) != gen) {
            spin();
        }
        gen++;
    }
}

/* Memory */
tuple_t *cxl_gen_r(void) {
    return layout.gen_r;
}

tuple_t *cxl_gen_s(void) {
    return layout.gen_s;
}

uint64_t *cxl_p1_thread_hist_r(void) {
    return layout.thread_hist_r;
}

uint64_t *cxl_p1_thread_hist_s(void) {
    return layout.thread_hist_s;
}

uint64_t *cxl_p1_node_hist_r(void) {
    return layout.node_hist_r;
}

uint64_t *cxl_p1_node_hist_s(void) {
    return layout.node_hist_s;
}

uint64_t *cxl_p1_remote_offs_r(void) {
    return layout.offs_r;
}

uint64_t *cxl_p1_remote_offs_s(void) {
    return layout.offs_s;
}

tuple_t *cxl_p1_remote_tmp_r(void) {
    return layout.remote_tmp_r;
}

tuple_t *cxl_p1_remote_tmp_s(void) {
    return layout.remote_tmp_s;
}
