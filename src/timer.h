#pragma once

#include <stdint.h>
#include <sys/time.h>

#include "config.h"
#include "cli.h"

#if !defined(__x86_64__)
#warning No supported architecture found -- timers will return junk.
#endif

struct timing {
    uint64_t start;
    uint64_t part_distr;
    uint64_t part_assign;
    uint64_t part_local;
    uint64_t build_probe;
    uint64_t end;
};
typedef struct timing timing_t;

static void print_timing(param_t *params, size_t n_tuples, timing_t *timing)
{
    uint64_t total = timing->end - timing->start;
    uint64_t part_distr = timing->part_distr - timing->start;
    uint64_t part_assign = timing->part_assign - timing->part_distr;
    uint64_t part_local = timing->part_local - timing->part_assign;
    uint64_t build_probe = timing->build_probe - timing->part_local;
    double per_tuple = (double)total / n_tuples;

#if DEBUG
    printf("r_tuples,s_tuples,fill,fill_param,n_threads,radix_bits_pass1,radix_bits_pass2,n_tuples,total,part_distr,part_assign,part_local,build_probe,per_tuple\n");
#endif

    printf("%lu,%lu,%s,%.1lf,%lu,%u,%u,%lu,%lu,%lu,%lu,%lu,%lu,%.4lf\n",
           params->r_size,
           params->s_size,
           (params->fill_mode == ZIPF) ? "zipf" : "uniform",
           (params->fill_mode == ZIPF) ? params->zipf_alpha : 0.0,
           params->n_threads,
           N_RADIX_BITS_PASS1,
           N_RADIX_BITS_PASS2,
           n_tuples,
           total,
           part_distr,
           part_assign,
           part_local,
           build_probe,
           per_tuple);
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

