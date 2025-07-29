#define _GNU_SOURCE

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <sched.h>

#include "types.h"
#include "config.h"
#include "generate.h"
#include "join.h"

/* CLI parameters */
struct param {
    size_t n_threads;
    size_t r_size;
    size_t s_size;
};
typedef struct param param_t;

void print_help(char **argv);
void print_version(void);
void parse_args(int argc, char **argv, param_t *params);

int main(int argc, char **argv) {
    relation_t rel_r;
    relation_t rel_s;
    [[maybe_unused]] uint64_t res;

    /* Start initially on CPU-0 */
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    int err = sched_setaffinity(0, sizeof(set), &set);
    BUG_ON(err);

    /* CLI parameters */
    param_t params;

    /* Default values if not specified on command line */
    params.n_threads = 2;
    params.r_size = 67108864;
    params.s_size = 67108864;

    parse_args(argc, argv, &params);

    /* Create relation R */
#if DEBUG
    printf("Creating relation R with size = %.3lf MiB, #tuples = %lu : ",
            (double) sizeof(tuple_t) * params.r_size / 1024.0 / 1024.0,
            params.r_size);
    fflush(stdout);
#endif
    create_relation(&rel_r, params.r_size);
#if DEBUG
    printf("OK\n");
#endif
    /* Create relation S */
#if DEBUG
    printf("Creating relation S with size = %.3lf MiB, #tuples = %lu : ",
            (double) sizeof(tuple_t) * params.s_size / 1024.0 / 1024.0,
            params.s_size);
    fflush(stdout);
#endif
    create_relation(&rel_s, params.s_size);
#if DEBUG
    printf("OK\n");
#endif

    /* Run join algorithm */
#if DEBUG
    printf("Running join algorithm...\n");
#endif
    res = join_relations(&rel_r, &rel_s, params.n_threads);
#if DEBUG
    printf("Finished join algorithm\n");
    printf("Results = %lu\n", res);
#endif

    /* Clean-up */
    delete_relation(&rel_r);
    delete_relation(&rel_s);

    return 0;
}

void print_help(char **argv) {
    printf("Usage: %s [options]\n", argv[0]);

    printf(
        "Options:\n"
        " -t, --threads       Number of threads [2]\n"
        " -r, --r-size        Number of tuples in build relation R [67108864]\n"
        " -s, --s-size        Number of tuples in probe relation S [67108864]\n"
        " -h, --help          Show this message\n"
        " -V, --version       Show version\n"
    );
}

void print_version() {
    printf("%s\n", VERSION);
}

void parse_args(int argc, char **argv, param_t *params) {
    static struct option long_options[] = {
        {"threads",   required_argument, NULL, 't'},
        {"r-size",    required_argument, NULL, 'r'},
        {"s-size",    required_argument, NULL, 's'},
        {"help",      no_argument,       NULL, 'h'},
        {"version",   no_argument,       NULL, 'V'},
        {0,           0,                 0,     0}
    };

    int opt;
    int option_index = 0;
    const char *optstring = "t:r:s:hV";

    while (1) {
        opt = getopt_long (argc, argv, optstring, long_options, &option_index);

        if (opt == -1) {
            break;
        }

        switch (opt) {
            case 't':
                params->n_threads = atoll(optarg);
                break;
            case 'r':
                params->r_size = atoll(optarg);
                break;
            case 's':
                params->s_size = atoll(optarg);
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

