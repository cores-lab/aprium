#pragma once

#include <stdint.h>

#include "config.h"

void cxl_alloc(size_t dev1, size_t dev2, size_t offset, size_t my_nid,
               size_t n_nodes, size_t r_tuples, size_t s_tuples);
void cxl_free(void);

void cxl_barrier(void);

tuple_t *cxl_gen_r(void);
tuple_t *cxl_gen_s(void);
uint64_t *cxl_p1_node_hist_r(void);
uint64_t *cxl_p1_node_hist_s(void);
uint64_t *cxl_p1_remote_offs_r(void);
uint64_t *cxl_p1_remote_offs_s(void);
tuple_t *cxl_p1_remote_tmp_r(void);
tuple_t *cxl_p1_remote_tmp_s(void);
