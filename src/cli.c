#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#include "cli.h"
#include "config.h"

void print_help(char **argv) {
    printf("Usage: %s [options]\n", argv[0]);

    printf(
"Options:\n"
" -r, --r-size        Number of tuples in build relation R [67108864]\n"
" -s, --s-size        Number of tuples in probe relation S [67108864]\n"
" -t, --threads       Number of threads (2 <= t <= nproc) [2]\n"
" -n, --nodes         Number of nodes (at least 2) [2]\n"
" -i, --node-id       ID of this node (must be unique; 0 <= i < n) [0]\n"
" -d, --cxl-dev       Path to the CXL dax device [/dev/dax0.0]\n"
" -h, --help          Show this message\n"
" -V, --version       Show version\n"
    );
}

void print_version() {
    printf("%s\n", VERSION);
}

void parse_args(int argc, char **argv, param_t *params) {
    static struct option long_options[] = {
        {"r-size",  required_argument, NULL, 'r'},
        {"s-size",  required_argument, NULL, 's'},
        {"threads", required_argument, NULL, 't'},
        {"nodes",   required_argument, NULL, 'n'},
        {"node-id", required_argument, NULL, 'i'},
        {"cxl-dev", required_argument, NULL, 'd'},
        {"help",    no_argument,       NULL, 'h'},
        {"version", no_argument,       NULL, 'v'},
        {0,         0,                 0,     0}
    };

    int opt;
    int option_index = 0;
    const char *optstring = "r:s:t:n:i:d:hV";

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
            case 'd':
                params->cxl_dev = optarg;
                BUG_ON(!(params->cxl_dev));
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

void init_default_params(param_t *params) {
    params->r_size    = 67108864;
    params->s_size    = 67108864;
    params->n_threads = 2;
    params->n_nodes   = 2;
    params->my_nid    = 0;
    params->cxl_dev   = "/dev/dax0.0";
}
