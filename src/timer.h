#pragma once

#include <stdint.h>
#include <sys/time.h>

#include "config.h"

#if !defined(__x86_64__)
#warning No supported architecture found -- timers will return junk.
#endif

// TODO: measure time one machine waits for the other after first part pass
struct timing {
    uint64_t start;
    uint64_t part_distr;
    uint64_t part_assign;
    uint64_t part_local;
    uint64_t build_probe;
    uint64_t end;
};
typedef struct timing timing_t;

static void print_timing(size_t r_size, size_t s_size, size_t n_threads, size_t n_tuples, timing_t *timing)
{
    uint64_t total = timing->end - timing->start;
    uint64_t part_distr = timing->part_distr - timing->start;
    uint64_t part_assign = timing->part_assign - timing->part_distr;
    uint64_t part_local = timing->part_local - timing->part_assign;
    uint64_t build_probe = timing->build_probe - timing->part_local;
    double per_tuple = total / n_tuples;

#if DEBUG
    printf("r_tuples,s_tuples,fill,fill_param,n_threads,radix_bits,n_tuples,total,part_distr,part_assign,part_local,build_probe,per_tuple\n");
#endif
    printf("%lu,%lu,%s,%.1lf,%lu,%u,%lu,%lu,%lu,%lu,%lu,%lu,%.4lf\n", r_size, s_size, (GEN_MODE == FILL_ZIPF) ? "zipf" : "uniform", (GEN_MODE == FILL_ZIPF)? ZIPF : 0.0, n_threads, N_RADIX_BITS, n_tuples, total, part_distr, part_assign, part_local, build_probe, per_tuple);
}

static inline uint64_t cpu_cycle_count() {
    uint64_t tick;
#if defined(__x86_64__)
    uint32_t high;
    uint32_t low;
    asm volatile ("rdtsc" : "=a" (low), "=d" (high));
    tick = (uint64_t) high << 32 | low;
#endif
    return tick;
}

static inline uint64_t timestamp() {
    return cpu_cycle_count();
}

static inline void start_timer(uint64_t *t) {
    *t = cpu_cycle_count();
}

static inline void stop_timer(uint64_t *t) {
    *t = cpu_cycle_count() - *t;
}

