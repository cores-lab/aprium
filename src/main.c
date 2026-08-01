#include <stdio.h>
#include <stdlib.h>
#include <sched.h>

#include "types.h"
#include "config.h"
#include "cli.h"
#include "mem.h"
#include "cxl.h"
#include "generate.h"
#include "join.h"

int main(int argc, char **argv) {
    /* Start initially on CPU 0 */
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    int err = sched_setaffinity(0, sizeof(set), &set);
    BUG_ON(err);

    /* Parse CLI arguments */
    param_t params;
    parse_args(argc, argv, &params);

    /* Init */
    cxl_alloc(params.my_nid, params.n_nodes, params.n_threads, params.r_size, params.s_size);
    mem_alloc(params.r_size, params.s_size, params.n_threads);
    relation_t slice_r;
    relation_t slice_s;
    uint64_t res;
    get_slices(&slice_r, &slice_s, &params);

#if DEBUG
    printf("Got relation R slice (size = %.3lf MiB, #tuples = %lu)\n",
            (double) sizeof(tuple_t) * slice_r.n_tuples / 1024.0 / 1024.0,
            slice_r.n_tuples);
    printf("Got relation S slice (size = %.3lf MiB, #tuples = %lu)\n",
            (double) sizeof(tuple_t) * slice_s.n_tuples / 1024.0 / 1024.0,
            slice_s.n_tuples);
#endif

    /* Join */
    res = join_relations(&slice_r, &slice_s, &params);

#if DEBUG
    printf("Finished join algorithm\n");
    printf("Results = %lu\n", res);
#else
    (void)res;
#endif

    /* Clean-up */
    release_slices(&slice_r, &slice_s);
    mem_free();
    cxl_free();

    return 0;
}
