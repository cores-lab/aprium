#pragma once

#include "types.h"

/* Program */
#define VERSION "0.1.1"
#define DEBUG 1
#define PERF 1

/* Hardware */
#define N_CPUS 128
static_assert(N_CPUS >= 2);
static uint8_t const CPU_MAPPING[] = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
    50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
    60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
    70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
    80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
    90, 91, 92, 93, 94, 95, 96, 97, 98, 99,
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
    110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    120, 121, 122, 123, 124, 125, 126, 127
};
static_assert(sizeof(CPU_MAPPING) == sizeof(CPU_MAPPING[0]) * N_CPUS);
#define CACHELINE_SIZE 64
#define L1_CACHE_SIZE 49152
#define L1_ASSOCIATIVITY 12

/* Distributed */
#define COORDINATION_NODE 0
#define COORDINATION_THREAD 0

/* Join */
#define N_RADIX_BITS 16
#define N_PASSES 2
static_assert(N_PASSES <= 2);
#define N_RADIX_BITS_PASS1 (N_RADIX_BITS / N_PASSES)
#define N_RADIX_BITS_PASS2 (N_RADIX_BITS - N_RADIX_BITS_PASS1)
#define FANOUT_PASS1 (1 << N_RADIX_BITS_PASS1)
static_assert(N_RADIX_BITS_PASS1 == 8);
#define COMPRESSED_TUPLE_SIZE 15
#define FANOUT_PASS2 (1 << N_RADIX_BITS_PASS2)
/* Padding mem layout:
 * | Pass1_part0                    | P | ... | Pass1_partN | P |
 *  ============ equals ============
 * | Pass2_part0 | P | P2_part1 | P | P | ... |
 */
#define P_BYTES (3 * CACHELINE_SIZE)
#define P_TUPLES (P_BYTES / sizeof(tuple_t))
#define PADDING_TUPLES (P_TUPLES * (FANOUT_PASS2 + 1))
#define RELATION_PADDING (PADDING_TUPLES * FANOUT_PASS1 * sizeof(tuple_t))
#define SKEW 0.0
static_assert(SKEW >= 0 && SKEW <= 1);

/* Helper */
#define BUG_ON(cond)                                         \
    do {                                                     \
        if (cond) {                                          \
            fprintf(stderr, "BUG_ON: %s:%d in %s(): `%s`\n", \
                    __FILE__, __LINE__, __func__, #cond);    \
        abort();                                             \
        }                                                    \
    } while (0)

static inline size_t round_up(size_t s, size_t a) {
    return (s + (a - 1)) & ~(a - 1);
}

static inline size_t rcl(size_t s) {
    return round_up(s, CACHELINE_SIZE);
}
