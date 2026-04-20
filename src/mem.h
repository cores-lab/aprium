#pragma once

#include <stddef.h>
#include <stdint.h>

#include "config.h"

void mem_alloc(size_t r_tuples, size_t s_tuples, size_t n_threads);
void mem_free(void);

uint64_t *mem_p1_thread_hist_r(void);
uint64_t *mem_p1_thread_hist_s(void);
uint8_t  *mem_p1_part_assign(void);
uint64_t *mem_p1_local_offs_r(void);
uint64_t *mem_p1_local_offs_s(void);
tuple_t *mem_p1_local_tmp_r(void);
tuple_t *mem_p1_local_tmp_s(void);
void *mem_for(size_t tid, size_t bytes);
void *mem_reuse_for(size_t tid, size_t bytes);

