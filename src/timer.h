#define _GNU_SOURCE

#pragma once

#include <stdint.h>
#include <sys/time.h>

#if !defined(__x86_64__)
#warning No supported architecture found -- timers will return junk.
#endif

struct timing {
    uint64_t start;
    uint64_t end;
    uint64_t part;
    uint64_t build_probe;
};
typedef struct timing timing_t;

static void print_timing(size_t n_tuples, timing_t *perf)
{
    uint64_t total = perf->end - perf->start;
    uint64_t part = perf->part;
    uint64_t build_probe = perf->build_probe;
    double per_tuple = total / n_tuples;

    printf("n_tuples,total,part,build_probe,per_tuple\n");
    printf("%lu,%lu,%lu,%lu,%.4lf\n", n_tuples, total, part, build_probe,
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

