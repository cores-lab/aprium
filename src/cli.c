#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#include "cli.h"
#include "config.h"

void print_help(char **argv) {
    printf("Usage: %s [options]\n", argv[0]);

    printf(
"Options:\n"
" -r, --r-size          Number of tuples in inner relation R [67108864]\n"
" -s, --s-size          Number of tuples in outer relation S [67108864]\n"
" -t, --threads         Number of threads (2 <= t <= nproc) [2]\n"
" -n, --nodes           Number of nodes (at least 2) [2]\n"
" -i, --node-id         ID of this node (must be unique; 0 <= i < n) [0]\n"
" -1, --cxl-dev1-size   Size in bytes of the first CXL dax device [0]\n"
" -2, --cxl-dev2-size   Size in bytes of the second CXL dax device [0]\n"
" -o, --cxl-offset      Offset in bytes into the CXL memory region [0]\n"
" -h, --help            Show this message\n"
" -V, --version         Show version\n"
    );
}

void print_version() {
    printf("%s\n", VERSION);
}

void init_default_params(param_t *params) {
    params->r_size        = 67108864;
    params->s_size        = 67108864;
    params->n_threads     = 2;
    params->n_nodes       = 2;
    params->my_nid        = 0;
    params->cxl_dev1_size = 0;
    params->cxl_dev2_size = 0;
    params->cxl_offset    = 0;
}

void parse_args(int argc, char **argv, param_t *params) {

    init_default_params(params);

    static struct option long_options[] = {
        {"r-size",        required_argument, NULL, 'r'},
        {"s-size",        required_argument, NULL, 's'},
        {"threads",       required_argument, NULL, 't'},
        {"nodes",         required_argument, NULL, 'n'},
        {"node-id",       required_argument, NULL, 'i'},
        {"cxl-dev1-size", required_argument, NULL, '1'},
        {"cxl-dev2-size", required_argument, NULL, '2'},
        {"cxl-offset",    required_argument, NULL, 'o'},
        {"help",          no_argument,       NULL, 'h'},
        {"version",       no_argument,       NULL, 'v'},
        {0,               0,                 0,     0}
    };

    int opt;
    int option_index = 0;
    const char *optstring = "r:s:t:n:i:1:2:o:hV";

    while (1) {
        opt = getopt_long (argc, argv, optstring, long_options, &option_index);

        if (opt == -1) {
            break;
        }

        switch (opt) {
            case 'r':
                params->r_size = atoll(optarg);
                break;
            case 's':
                params->s_size = atoll(optarg);
                break;
            case 't':
                params->n_threads = atoll(optarg);
                BUG_ON(!(params->n_threads >= 2));
                BUG_ON(!(params->n_threads <= N_CPUS));
                break;
            case 'n':
                params->n_nodes = atoll(optarg);
                BUG_ON(!(params->n_nodes >= 2));
                break;
            case 'i':
                params->my_nid = atoll(optarg);
                BUG_ON(!(params->my_nid < params->n_nodes));
                break;
            case '1':
                params->cxl_dev1_size = atoll(optarg);
                break;
            case '2':
                params->cxl_dev2_size = atoll(optarg);
                break;
            case 'o':
                params->cxl_offset = atoll(optarg);
                size_t cxl_size = params->cxl_dev1_size + params->cxl_dev2_size;
                BUG_ON(params->cxl_offset >= cxl_size);
                break;
            case '?':
            case 'h':
                print_help(argv);
                exit(EXIT_SUCCESS);
                break;
            case 'V':
                print_version();
                exit(EXIT_SUCCESS);
                break;
            default:
                fprintf(stderr, "Unknown option: %c\n", opt);
                break;
        }
    }
}

