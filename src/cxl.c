#include <fcntl.h>
#include <stdatomic.h>
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
struct barrier {
    uint64_t flag;
    uint8_t _pad[(CACHELINE_SIZE - sizeof(uint64_t))];
};
typedef struct barrier barrier_t;

struct mem {
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
};
typedef struct mem mem_t;

static mem_t mem = { 0 };

typedef struct {
    char const *path;
    size_t size;
} dax_device_t;

/* Init */
void *cxl_map(void) {
    // dax_device_t devices[] = {
    //     {"/dev/dax0.0", 260650827776ULL},
    //     {"/dev/dax1.0", 289104986112ULL},
    //     {"/dev/dax2.0", 274877906944ULL}
    // };

    // size_t num_devices = sizeof(devices) / sizeof(devices[0]);
    // size_t total_size = 0;

    // for (size_t i = 0; i < num_devices; i++) {
    //     total_size += devices[i].size;
    // }

    // void *base = mmap(NULL, total_size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    // BUG_ON(base == MAP_FAILED);

    // size_t offset = 0;
    // for (size_t i = 0; i < num_devices; i++) {
    //     int fd = open(devices[i].path, O_RDWR);
    //     BUG_ON(fd < 0);
    //     void *target = (uint8_t *)base + offset;
    //     void *addr = mmap(target, devices[i].size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
    //     BUG_ON(addr == MAP_FAILED);
    //     close(fd);
    //     offset += devices[i].size;
    // }

    // mem.base = base;
    // mem.size = total_size;

    // return base;

    dax_device_t devices[] = {
        {"/dev/shm/mock_dax0.0", 64 * (1ULL << 30)},
    };

    int fd = open(devices[0].path, O_RDWR | O_CREAT, 0666);
    BUG_ON(fd < 0);
    BUG_ON(ftruncate(fd, devices[0].size) != 0);

    void *base = mmap(NULL, devices[0].size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    BUG_ON(base == MAP_FAILED);

    close(fd);

    mem.base = base;
    mem.size = devices[0].size;

    return base;
}

static size_t tmp_size(size_t n_tuples, size_t n_nodes) {
    // TODO: this is a pessimistic view (alloc as if one machine does all the work)
    size_t tmp = round_up(n_tuples * sizeof(tuple_t) + RELATION_PADDING, CACHELINE_SIZE);
    return tmp * n_nodes;
}

void cxl_alloc(size_t my_nid, size_t n_nodes, size_t n_threads, size_t r_tuples, size_t s_tuples) {
    bool const is_coordinator_node = (my_nid == COORDINATION_NODE);

    void *base = cxl_map();
    size_t bytes = 0;

    size_t barrier = n_nodes * sizeof(barrier_t);
    size_t r_size = round_up(r_tuples * sizeof(tuple_t), CACHELINE_SIZE);
    size_t s_size = round_up(s_tuples * sizeof(tuple_t), CACHELINE_SIZE);
    size_t thread_hist = round_up(FANOUT_PASS1 * sizeof(uint64_t), CACHELINE_SIZE) * n_threads * n_nodes;
    size_t node_hist = round_up(FANOUT_PASS1 * sizeof(uint64_t), CACHELINE_SIZE) * n_nodes;
    size_t offs = round_up((FANOUT_PASS1 + 1) * sizeof(uint64_t), CACHELINE_SIZE) * n_nodes;
    size_t tmp_r = tmp_size(r_tuples, n_nodes);
    size_t tmp_s = tmp_size(s_tuples, n_nodes);

    bytes += barrier;
    bytes += r_size;
    bytes += s_size;
    bytes += 2ULL * thread_hist;
    bytes += 2ULL * node_hist;
    bytes += 2ULL * offs;
    bytes += tmp_r;
    bytes += tmp_s;

    BUG_ON(bytes % CACHELINE_SIZE);
    BUG_ON(bytes > 512ULL * 1024 * 1024 * 1024); /* >512 GiB? */

#if DEBUG
    printf("Initializing CXL memory (size = %.3lf MiB, addr = %p): ",
            (double) bytes / 1024.0 / 1024.0, base);
    fflush(stdout);
#endif

    /* Coordinator zeros out CXL memory, all nodes flush caches */
    if (is_coordinator_node) {
        memset(base, 0, bytes);
    }
    else {
        sleep(5);
    }

    cache_wb(base, bytes, true);

    uintptr_t ptr = (uintptr_t)base;
    mem.barrier = (barrier_t *)(ptr);
    ptr += barrier;
    mem.gen_r = (tuple_t *)ptr;
    ptr += r_size;
    mem.gen_s = (tuple_t *)ptr;
    ptr += s_size;
    mem.thread_hist_r = (uint64_t *)ptr;
    ptr += thread_hist;
    mem.thread_hist_s = (uint64_t *)ptr;
    ptr += thread_hist;
    mem.node_hist_r = (uint64_t *)ptr;
    ptr += node_hist;
    mem.node_hist_s = (uint64_t *)ptr;
    ptr += node_hist;
    mem.offs_r = (uint64_t *)ptr;
    ptr += offs;
    mem.offs_s = (uint64_t *)ptr;
    ptr += offs;
    mem.remote_tmp_r = (tuple_t *)ptr;
    ptr += tmp_r;
    mem.remote_tmp_s = (tuple_t *)ptr;
    ptr += tmp_s;

    mem.my_nid = my_nid;
    mem.n_nodes = n_nodes;

#if DEBUG
    printf("OK\n");
#endif

}

void cxl_free(void) {
    int ret = munmap(mem.base, mem.size);
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
    asm volatile("pause" ::: "memory");
}

void cxl_barrier(void) {
    static uint64_t gen = 1;
    if (mem.my_nid == COORDINATION_NODE) {
        for (size_t i = 0; i < mem.n_nodes; i++) {
            if (i == COORDINATION_NODE) {
                continue;
            }
            while (load_inval(&mem.barrier[i].flag) != gen) {
                spin();
            }
        }
        store_flush(&mem.barrier[COORDINATION_NODE].flag, gen);
        gen++;
    }
    else {
        store_flush(&mem.barrier[mem.my_nid].flag, gen);
        while (load_inval(&mem.barrier[COORDINATION_NODE].flag) != gen) {
            spin();
        }
        gen++;
    }
}

/* Memory */
tuple_t *cxl_gen_r(void) {
    return mem.gen_r;
}

tuple_t *cxl_gen_s(void) {
    return mem.gen_s;
}

uint64_t *cxl_p1_thread_hist_r(void) {
    return mem.thread_hist_r;
}

uint64_t *cxl_p1_thread_hist_s(void) {
    return mem.thread_hist_s;
}

uint64_t *cxl_p1_node_hist_r(void) {
    return mem.node_hist_r;
}

uint64_t *cxl_p1_node_hist_s(void) {
    return mem.node_hist_s;
}

uint64_t *cxl_p1_remote_offs_r(void) {
    return mem.offs_r;
}

uint64_t *cxl_p1_remote_offs_s(void) {
    return mem.offs_s;
}

tuple_t *cxl_p1_remote_tmp_r(void) {
    return mem.remote_tmp_r;
}

tuple_t *cxl_p1_remote_tmp_s(void) {
    return mem.remote_tmp_s;
}

