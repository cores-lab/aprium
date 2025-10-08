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

struct cxl_allocator {
    void *base;
    size_t offset;
    size_t total;
};
typedef struct cxl_allocator cxl_allocator_t;

struct cxl_barrier {
    size_t n_nodes;
    /* n_nodes of cachlines for flags (in each first uint64_t):
     * one       cacheline  (release flag,  coord  -> worker)
     * n_nodes-1 cachelines (arrival flags, worker -> coord)
     */
    uint64_t *flags;
};
typedef struct cxl_barrier cxl_barrier_t;

static cxl_allocator_t allocator;
static cxl_barrier_t barrier;
static size_t my_nid;

/* Init */
void cxl_mem_init(char *device, size_t nid, size_t n_nodes) {
    my_nid = nid;
    bool const is_coordinator_node = (my_nid == COORDINATION_NODE);

    (void)device;

    const char *dev1 = "/dev/dax0.0";
    const char *dev2 = "/dev/dax1.0";

    uint64_t size1 = 0;
    uint64_t size2 = 0;
    uint64_t offset_gib = 0;

    if (nid == 0) {
        size1 = 260650827776ULL;  // 242.75 GiB
        size2 = 151666032640ULL;  // 141.25 GiB
        offset_gib = 256ULL;
    }
    else if (nid == 1) {
        size1 = 260650827776ULL;  // 242.75 GiB
        size2 = 82934620160ULL - 647168ULL;   // 77.25 GiB - minus align to 2MiB
        offset_gib = 192ULL;
    }

    const uint64_t total = size1 + size2;
    const uint64_t dev_align = 2097152ULL; /* 2 MiB */
    const uint64_t offset_bytes = offset_gib << 30;

    int fd1 = open(dev1, O_RDWR);
    BUG_ON(fd1 < 0);
    int fd2 = open(dev2, O_RDWR);
    BUG_ON(fd2 < 0);

    size_t reserve_len = total + dev_align;
    void *reserve = mmap(NULL, reserve_len, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    BUG_ON(reserve == MAP_FAILED);

    uintptr_t rbase = (uintptr_t)reserve;
    uintptr_t aligned_base = round_up(rbase, dev_align);

    BUG_ON(aligned_base + total > rbase + reserve_len);

    size_t prefix = aligned_base - rbase;
    if (prefix) munmap((void*)rbase, prefix);

    uintptr_t suffix_addr = aligned_base + total;
    size_t suffix = (rbase + reserve_len) - suffix_addr;
    if (suffix) munmap((void*)suffix_addr, suffix);

    void *base = (void*)aligned_base;

    void *m1 = mmap(base, size1, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd1, 0);
    BUG_ON(m1 == MAP_FAILED);

    /* Map second device right after the first */
    void *m2 = mmap((void*)(aligned_base + size1), size2, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd2, 0);
    BUG_ON(m2 == MAP_FAILED);

    size_t size = 127ul * 1024 * 1024 * 1024;
    void *mem_base = (void*)(aligned_base + offset_bytes);

    ///* Map CXL memory */
    //int fd = open(device, O_RDWR);
    //BUG_ON(fd < 0);

    //size_t size = 128ul * 1024 * 1024 * 1024;
    //int prot = PROT_READ | PROT_WRITE;
    //int flags = MAP_SHARED;
    //void *mem_base = mmap(NULL, size, prot, flags, fd, 0);
    //BUG_ON(mem_base == MAP_FAILED);
    //close(fd);

#if DEBUG
    printf("Initializing CXL memory (size = %.3lf MiB, addr = %p): ",
            size / 1024.0 / 1024.0, mem_base);
    fflush(stdout);
#endif

    /* Coordinator zeros out CXL memory, all nodes flush caches */
    uintptr_t ptr = (uintptr_t)mem_base;
    if (is_coordinator_node) {
        memset((void *)ptr, 0, size);
    }
    else {
        sleep(1);
    }
    for (uintptr_t p = ptr; p < ptr + size; p += CACHELINE_SIZE) {
        _mm_clflushopt((void *)p);
    }
    _mm_sfence();
    //atomic_thread_fence(memory_order_seq_cst);

    /* Init global barrier */
    size_t barrier_bytes = n_nodes * CACHELINE_SIZE;
    barrier.n_nodes = n_nodes;
    barrier.flags = (uint64_t *)(ptr);
    ptr += barrier_bytes;
    size -= barrier_bytes;

    /* Init allocator */
    allocator.base = (void *)ptr;
    allocator.offset = 0;
    allocator.total = size;

#if DEBUG
    printf("OK\n");
#endif

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

static inline void cpu_pause(void) {
    asm volatile("pause" ::: "memory");
}

void cxl_barrier(void) {
    bool is_coordinator_node = (my_nid == COORDINATION_NODE);
    static uint64_t gen = 1;

    if (is_coordinator_node) {
        for (size_t i = 1; i < barrier.n_nodes; i++) {
            uintptr_t arrival = (uintptr_t)barrier.flags + i * CACHELINE_SIZE;
            while (load_inval((uint64_t *)arrival) != gen) { cpu_pause(); }
        }
        uintptr_t release = (uintptr_t)barrier.flags;
        store_flush((uint64_t *)release, gen);
        gen++;
    }
    else {
        uintptr_t arrival = (uintptr_t)barrier.flags + my_nid * CACHELINE_SIZE;
        store_flush((uint64_t *)arrival, gen);
        uintptr_t release = (uintptr_t)barrier.flags;
        while (load_inval((uint64_t *)release) != gen) { cpu_pause(); }
        gen++;
    }
}

/* Allocator */
void cxl_alloc_reset(void) {
    allocator.offset = 0;
}

void *cxl_alloc(size_t size) {
  uintptr_t raw = (uintptr_t)allocator.base + allocator.offset;
  /* Align all returned pointers to cache line size */
  uintptr_t aligned = round_up(raw, CACHELINE_SIZE);
  size_t actual = aligned + size - (uintptr_t)allocator.base;

  if (actual > allocator.total) {
      return NULL;
  }

  allocator.offset = actual;
  return (void *)aligned;
}

void cxl_free(void *ptr) {
    (void)ptr;
}

