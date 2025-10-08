#pragma once

/* CLI parameters */
struct param {
    size_t r_size;
    size_t s_size;
    size_t n_threads;
    size_t n_nodes;
    size_t my_nid;
    char *cxl_dev;
};
typedef struct param param_t;

void print_help(char **argv);
void print_version(void);
void parse_args(int argc, char **argv, param_t *params);
void init_default_params(param_t *params);
