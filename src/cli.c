#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#include "cli.h"
#include "config.h"

void print_help(char **argv) {
    printf("Usage: %s [options]\n", argv[0]);
    printf(
    "Options:\n"
    " -r, --r-size      Number of tuples in inner relation R [67108864]\n"
    " -s, --s-size      Number of tuples in outer relation S [67108864]\n"
    " -u, --uniform     Use uniform key distribution\n"
    " -z, --zipf        Use Zipfian key distribution with alpha factor (0.0 <= z <= 1.5) [0.0]\n"
    " -t, --threads     Number of threads (2 <= t <= nproc) [2]\n"
    " -n, --nodes       Number of nodes (at least 2) [2]\n"
    " -i, --node-id     ID of this node (must be unique; 0 <= i < n) [0]\n"
    " -h, --help        Show this message\n"
    " -V, --version     Show version\n"
    );
}

void print_version() {
    printf("%s\n", VERSION);
}

void init_default_params(param_t *params) {
    params->r_size        = 67108864;
    params->s_size        = 67108864;
    params->fill_mode     = UNIFORM;
    params->zipf_alpha    = 0.0;
    params->n_threads     = 2;
    params->n_nodes       = 2;
    params->my_nid        = 0;
}

void parse_args(int argc, char **argv, param_t *params) {

    init_default_params(params);

    static struct option long_options[] = {
        {"r-size",        required_argument, NULL, 'r'},
        {"s-size",        required_argument, NULL, 's'},
        {"uniform",       no_argument,       NULL, 'u'},
        {"zipf",          required_argument, NULL, 'z'},
        {"threads",       required_argument, NULL, 't'},
        {"nodes",         required_argument, NULL, 'n'},
        {"node-id",       required_argument, NULL, 'i'},
        {"help",          no_argument,       NULL, 'h'},
        {"version",       no_argument,       NULL, 'V'},
        {0,               0,                 0,     0}
    };

    int opt;
    int option_index = 0;
    char const *optstring = "r:s:uz:t:n:i:hV";

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
            case 'u':
                params->fill_mode = UNIFORM;
                break;
            case 'z':
                params->fill_mode = ZIPF;
                params->zipf_alpha = atof(optarg);
                BUG_ON(params->zipf_alpha < 0.0 || params->zipf_alpha > 1.5);
                break;
            case 't':
                params->n_threads = atoll(optarg);
                //BUG_ON(!(params->n_threads >= 2));
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

    BUG_ON(params->r_size % params->n_nodes != 0);
    BUG_ON(params->s_size % params->n_nodes != 0);
}
