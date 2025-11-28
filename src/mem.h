#pragma once

#include <stddef.h>
#include <stdint.h>

void mem_alloc(size_t r_tuples, size_t s_tuples, size_t n_threads);
void mem_free(void);

uint64_t *mem_p1_hist_r(void);
uint64_t *mem_p1_hist_s(void);
void *mem_for(size_t tid, size_t bytes);

