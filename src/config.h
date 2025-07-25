#pragma once

#include "types.h"

/* Program: */
#define VERSION "0.1.0"
#define DEBUG 1
#define PERF 1

/* Hardware: */
#define N_CPUS 8
static uint8_t const CPU_MAPPING[] = {0, 1, 2, 3, 4, 5, 6, 7};
static_assert(sizeof(CPU_MAPPING) == sizeof(CPU_MAPPING[0]) * N_CPUS);
#define CACHELINE_SIZE 64
#define L1_CACHE_SIZE 49152
#define L1_ASSOCIATIVITY 12

/* Join Parameter: */
#define N_RADIX_BITS 15
#define N_PASSES 2
static_assert(N_PASSES <= 2);
#define N_RADIX_BITS_PASS1 (N_RADIX_BITS / N_PASSES)
#define N_RADIX_BITS_PASS2 (N_RADIX_BITS - N_RADIX_BITS_PASS1)
#define FANOUT_PASS1 (1 << N_RADIX_BITS_PASS1)
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

