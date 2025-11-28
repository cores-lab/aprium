#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "generate.h"
#include "config.h"
#include "cxl.h"

static int init_done = 0;

void fill_relation(relation_t *rel) {
    for (size_t i = 0; i < rel->n_tuples; i++) {
        rel->tuples[i].key = i;
    }

    for (size_t i = rel->n_tuples - 1; i > 0; i--) {
        size_t j = ((double) rand() / ((double) RAND_MAX + 1)) * i;
        uint64_t tmp = rel->tuples[i].key;
        rel->tuples[i].key = rel->tuples[j].key;
        rel->tuples[j].key = tmp;
    }

    for (size_t i = 0; i < rel->n_tuples; i++) {
        rel->tuples[i].rid = i;
    }
}

void create_relation(relation_t *rel, size_t n_tuples) {
    if (!init_done) {
        srand(time(NULL));
        init_done = 1;
    }
    size_t bytes = n_tuples * sizeof(tuple_t) + RELATION_PADDING;
    //void *ptr = aligned_alloc(CACHELINE_SIZE, bytes);
    void *ptr = cxl_alloc(bytes);
    BUG_ON(!ptr);
    rel->tuples = ptr;
    rel->n_tuples = n_tuples;

    fill_relation(rel);
}

void delete_relation(relation_t *rel) {
    free(rel->tuples);
}

void get_slices(relation_t *slice_r, relation_t *slice_s, param_t *params) {
    relation_t r;
    relation_t s;
    bool const is_coordinator_node = (params->my_nid == COORDINATION_NODE);

    /* Init relations R & S */
    r.tuples = cxl_alloc(params->r_size * sizeof(tuple_t));
    BUG_ON(!r.tuples);
    r.n_tuples = params->r_size;
    s.tuples = cxl_alloc(params->s_size * sizeof(tuple_t));
    BUG_ON(!s.tuples);
    s.n_tuples = params->s_size;

    if (is_coordinator_node) {
#if DEBUG
        printf("Creating relation R (size = %.3lf MiB, #tuples = %lu) : ",
                (double) sizeof(tuple_t) * params->r_size / 1024.0 / 1024.0,
                params->r_size);
        fflush(stdout);
#endif

        fill_relation(&r);

#if DEBUG
        printf("OK\n");
#endif

#if DEBUG
        printf("Creating relation S (size = %.3lf MiB, #tuples = %lu) : ",
                (double) sizeof(tuple_t) * params->s_size / 1024.0 / 1024.0,
                params->s_size);
        fflush(stdout);
#endif

        fill_relation(&s);

#if DEBUG
        printf("OK\n");
#endif
    }

    cxl_barrier();

    size_t r_slice = params->r_size / params->n_nodes;
    size_t s_slice = params->s_size / params->n_nodes;

    size_t my_r_size;
    size_t my_s_size;
    size_t r_offset = params->my_nid * r_slice;
    size_t s_offset;

    if (params->my_nid == params->n_nodes - 1) {
        my_r_size = params->r_size - r_slice * (params->n_nodes - 1);
    } else {
        my_r_size = r_slice;
    }

    if (params->my_nid == 0) {
        my_s_size = params->s_size - s_slice * (params->n_nodes - 1);
        s_offset   = s_slice * (params->n_nodes - 1);
    } else {
        my_s_size = s_slice;
        s_offset   = (params->my_nid - 1) * s_slice;
    }

    tuple_t *my_r_tuples;
    my_r_tuples = aligned_alloc(CACHELINE_SIZE, my_r_size * sizeof(tuple_t));
    BUG_ON(!my_r_tuples);
    tuple_t *my_s_tuples;
    my_s_tuples = aligned_alloc(CACHELINE_SIZE, my_s_size * sizeof(tuple_t));
    BUG_ON(!my_s_tuples);

    memcpy(my_r_tuples, r.tuples + r_offset, my_r_size * sizeof(tuple_t));
    memcpy(my_s_tuples, s.tuples + s_offset, my_s_size * sizeof(tuple_t));

    slice_r->tuples   = my_r_tuples;
    slice_r->n_tuples = my_r_size;
    slice_s->tuples   = my_s_tuples;
    slice_s->n_tuples = my_s_size;

    cxl_alloc_reset();
}

void release_slices(relation_t *slice_r, relation_t *slice_s) {
    free(slice_r->tuples);
    free(slice_s->tuples);
}
