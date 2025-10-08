#pragma once

void cxl_mem_init(char *device, size_t my_nid, size_t n_nodes);

void cxl_barrier(void);

void *cxl_alloc(size_t size);
void cxl_free(void *ptr);
void cxl_alloc_reset(void);

