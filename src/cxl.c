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
    uint64_t *nhist_r;
    uint64_t *nhist_s;
    uint64_t *ghist_r;
    uint64_t *ghist_s;
    // old
    uint64_t *roffs_r;
    uint64_t *roffs_s;
    tuple_t *tmp_r;
    tuple_t *tmp_s;
};
typedef struct mem mem_t;

static mem_t mem = { 0 };

/* Init */
void *cxl_map(size_t size1, size_t size2, size_t offset) {
    char const *dev1 = "/dev/dax0.0";
    char const *dev2 = "/dev/dax1.0";

    /*
     * daberg301:
     * - size1:
     * - size2:
     * - offset:
     *
     * daberg303:
     * - size1: 242.75 GiB == 260650827776ul
     * - size2: 141.25 GiB == 151666032640ul
     * - offset: 0 GiB == 0ul
     *           -OR-
     *           256 GiB == 256ul << 30
     *
     * daberg304:
     * - size1: 242.75 GiB == 260650827776ul
     * - size2: 77.25 GiB == 82934620160ul & ~(align - 1) == 82933972992ul
     * - offset: 0 GiB == 0ul
     *           -OR-
     *           192 GiB == 192ul << 30
     */

    uint64_t total = size1 + size2;
    uint64_t align = 2ul * 1024 * 1024; /* 2 MiB */

    int fd1 = open(dev1, O_RDWR);
    BUG_ON(fd1 < 0);
    int fd2 = open(dev2, O_RDWR);
    BUG_ON(fd2 < 0);

    size_t size;
    int prot;
    int flags;

    size = total + align;
    prot = PROT_NONE;
    flags = MAP_PRIVATE | MAP_ANONYMOUS;
    void *reserve = mmap(NULL, size, prot, flags, -1, 0);
    BUG_ON(reserve == MAP_FAILED);

    uintptr_t rbase = (uintptr_t)reserve;
    uintptr_t abase = round_up(rbase, align);
    BUG_ON(abase + total > rbase + size);

    size_t prefix = abase - rbase;
    if (prefix) {
        munmap((void *)rbase, prefix);
    }

    uintptr_t suffix_addr = abase + total;
    size_t suffix = (rbase + size) - suffix_addr;
    if (suffix) {
        munmap((void *)suffix_addr, suffix);
    }

    prot = PROT_READ | PROT_WRITE;
    flags = MAP_SHARED | MAP_FIXED;
    void *mem1 = mmap((void *)abase, size1, prot, flags, fd1, 0);
    BUG_ON(mem1 == MAP_FAILED);
    void *mem2 = mmap((void *)(abase + size1), size2, prot, flags, fd2, 0);
    BUG_ON(mem2 == MAP_FAILED);

    close(fd1);
    close(fd2);

    mem.base = (void *)(abase);
    mem.size = total;

    return (void *)(abase + offset);
}

size_t tmp_size(size_t n_tuples, size_t n_nodes) {
    size_t tmp = rcl((n_tuples / n_nodes) * sizeof(tuple_t) + RELATION_PADDING);
    return tmp * n_nodes;
}

void cxl_alloc(size_t size1, size_t size2, size_t offset, size_t my_nid,
               size_t n_nodes, size_t r_tuples, size_t s_tuples)
{
    bool const is_coordinator_node = (my_nid == COORDINATION_NODE);

    void *base = cxl_map(size1, size2, offset);
    size_t bytes = 0;

    size_t barrier = n_nodes * sizeof(barrier_t);
    size_t r_size = rcl(r_tuples * sizeof(tuple_t));
    size_t s_size = rcl(s_tuples * sizeof(tuple_t));
    size_t ghist = rcl(FANOUT_PASS1 * sizeof(uint64_t));
    size_t nhist = ghist * n_nodes;
    size_t roffs = rcl((FANOUT_PASS1 + 1) * sizeof(uint64_t)) * n_nodes;
    size_t tmp_r = tmp_size(r_tuples, n_nodes);
    size_t tmp_s = tmp_size(s_tuples, n_nodes);

    bytes += barrier;
    bytes += r_size;
    bytes += s_size;
    bytes += 2 * nhist;
    bytes += 2 * ghist;
    bytes += 2 * roffs;
    bytes += tmp_r;
    bytes += tmp_s;

    BUG_ON(bytes > 127ul * 1024 * 1024 * 1024); /* >127 GiB? */

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
        sleep(2);
    }

    uintptr_t ptr = (uintptr_t)base;
    for (uintptr_t p = ptr; p < ptr + bytes; p += CACHELINE_SIZE) {
        _mm_clflushopt((void *)p);
    }
    _mm_sfence();
    //atomic_thread_fence(memory_order_seq_cst);

    mem.barrier = (barrier_t *)(ptr);
    ptr += barrier;
    mem.gen_r = (tuple_t *)ptr;
    ptr += r_size;
    mem.gen_s = (tuple_t *)ptr;
    ptr += s_size;
    mem.nhist_r = (uint64_t *)ptr;
    ptr += nhist;
    mem.nhist_s = (uint64_t *)ptr;
    ptr += nhist;
    mem.ghist_r = (uint64_t *)ptr;
    ptr += ghist;
    mem.ghist_s = (uint64_t *)ptr;
    ptr += ghist;
    mem.roffs_r = (uint64_t *)ptr;
    ptr += roffs;
    mem.roffs_s = (uint64_t *)ptr;
    ptr += roffs;
    mem.tmp_r = (tuple_t *)ptr;
    ptr += tmp_r;
    mem.tmp_s = (tuple_t *)ptr;
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
    _mm_clwb((void *)p);
    _mm_sfence();
}

static inline uint64_t load_inval(volatile uint64_t *p) {
    _mm_clflushopt((void *)p);
    _mm_lfence();
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

uint64_t *cxl_p1_node_hist_r(void) {
    return mem.nhist_r;
}

uint64_t *cxl_p1_node_hist_s(void) {
    return mem.nhist_s;
}

uint64_t *cxl_p1_global_hist_r(void) {
    return mem.ghist_r;
}

uint64_t *cxl_p1_global_hist_s(void) {
    return mem.ghist_s;
}

//old
uint64_t *cxl_p1_roffs_r(void) {
    return mem.roffs_r;
}

uint64_t *cxl_p1_roffs_s(void) {
    return mem.roffs_s;
}

tuple_t *cxl_p1_tmp_r(void) {
    return mem.tmp_r;
}

tuple_t *cxl_p1_tmp_s(void) {
    return mem.tmp_s;
}

