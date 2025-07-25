#define _GNU_SOURCE

#pragma once

#include <stdint.h>
#include <sys/time.h>

#if !defined(__x86_64__)
#warning No supported architecture found -- timers will return junk.
#endif

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
