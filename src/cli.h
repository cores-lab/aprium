#pragma once

/* CLI parameters */
typedef struct {
    size_t r_size;
    size_t s_size;
    int fill_mode;
    double zipf_alpha;
    size_t n_threads;
    size_t n_nodes;
    size_t my_nid;
} param_t;

void parse_args(int argc, char **argv, param_t *params);
