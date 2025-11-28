#pragma once

/* CLI parameters */
struct param {
    size_t r_size;
    size_t s_size;
    size_t n_threads;
    size_t n_nodes;
    size_t my_nid;
    size_t cxl_dev1_size;
    size_t cxl_dev2_size;
    size_t cxl_offset;
};
typedef struct param param_t;

void parse_args(int argc, char **argv, param_t *params);
