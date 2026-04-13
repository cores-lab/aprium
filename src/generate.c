#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "generate.h"
#include "config.h"
#include "cxl.h"

typedef struct {
    tuple_t *tuples;
    size_t lo;
    size_t hi;
    uint64_t n;
    uint64_t a;
    uint64_t b;
} fill_arg_t;

static uint64_t gcd64(uint64_t x, uint64_t y) {
    while (y != 0) {
        uint64_t t = x % y;
        x = y;
        y = t;
    }
    return x;
}

static uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static void *fill_worker(void *p) {
    fill_arg_t *a = (fill_arg_t *)p;

    for (size_t i = a->lo; i < a->hi; ++i) {
        a->tuples[i].key = (a->a * i + a->b) % a->n;
        a->tuples[i].rid = 0;
    }
    return NULL;
}

void fill_relation(relation_t *rel, size_t n_threads) {
    if (n_threads > rel->n_tuples) n_threads = rel->n_tuples;

    uint64_t n = rel->n_tuples;
    uint64_t seed = (uint64_t)time(NULL) ^ (uint64_t)(uintptr_t)rel;

    uint64_t a = mix64(seed) | 1ULL;
    while (gcd64(a, n) != 1) {
        a += 2;
    }
    uint64_t b = mix64(seed + 1) % n;

    pthread_t tids[n_threads];
    fill_arg_t args[n_threads];

    size_t base = rel->n_tuples / n_threads;
    size_t rem  = rel->n_tuples % n_threads;
    size_t pos  = 0;

    for (size_t t = 0; t < n_threads; ++t) {
        size_t len = base + (t < rem);

        args[t].tuples = rel->tuples;
        args[t].lo = pos;
        args[t].hi = pos + len;
        args[t].n = n;
        args[t].a = a;
        args[t].b = b;

        pthread_create(&tids[t], NULL, fill_worker, &args[t]);
        pos += len;
    }

    for (size_t t = 0; t < n_threads; ++t) {
        pthread_join(tids[t], NULL);
    }
}

//void fill_relation(relation_t *rel) {
//    for (size_t i = 0; i < rel->n_tuples; i++) {
//        rel->tuples[i].key = i;
//    }
//
//    for (size_t i = rel->n_tuples - 1; i > 0; i--) {
//        size_t j = ((double) rand() / ((double) RAND_MAX + 1)) * i;
//        uint64_t tmp = rel->tuples[i].key;
//        rel->tuples[i].key = rel->tuples[j].key;
//        rel->tuples[j].key = tmp;
//    }
//
//    for (size_t i = 0; i < rel->n_tuples; i++) {
//        rel->tuples[i].rid = i;
//    }
//}

//void fill_relation(relation_t *rel, char const *filename) {
//    FILE *fp = fopen(filename, "rb");
//    BUG_ON(!fp);
//
//    if (fseeko(fp, 0, SEEK_END) != 0) {
//        fclose(fp);
//        BUG_ON(1);
//    }
//
//    off_t file_size = ftello(fp);
//    BUG_ON(file_size < 0);
//
//    BUG_ON((size_t)file_size < rel->n_tuples * sizeof(tuple_t));
//
//    if (fseeko(fp, 0, SEEK_SET) != 0) {
//        fclose(fp);
//        BUG_ON(1);
//    }
//
//    size_t got = fread(rel->tuples, 1, rel->n_tuples * sizeof(tuple_t), fp);
//    fclose(fp);
//
//    BUG_ON(got != rel->n_tuples * sizeof(tuple_t));
//}


void get_slices(relation_t *slice_r, relation_t *slice_s, param_t *params) {
    relation_t r;
    relation_t s;
    bool const is_coordinator_node = (params->my_nid == COORDINATION_NODE);

    /* Init relations R & S */
    r.tuples = cxl_gen_r();
    r.n_tuples = params->r_size;
    s.tuples = cxl_gen_s();
    s.n_tuples = params->s_size;

    if (is_coordinator_node) {
#if DEBUG
        printf("Creating relation R (size = %.3lf MiB, #tuples = %lu) : ",
                (double) sizeof(tuple_t) * params->r_size / 1024.0 / 1024.0,
                params->r_size);
        fflush(stdout);
#endif

        //fill_relation(&r);
        fill_relation(&r, params->n_threads);
        //fill_relation(&r, "/mnt/nvme5/moritz/r-64GiB.bin");

#if DEBUG
        printf("OK\n");
#endif

#if DEBUG
        printf("Creating relation S (size = %.3lf MiB, #tuples = %lu) : ",
                (double) sizeof(tuple_t) * params->s_size / 1024.0 / 1024.0,
                params->s_size);
        fflush(stdout);
#endif

        //fill_relation(&s);
        fill_relation(&s, params->n_threads);
        //fill_relation(&s, "/mnt/nvme5/moritz/s-64GiB.bin");

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

    //tuple_t *my_r_tuples;
    //my_r_tuples = aligned_alloc(CACHELINE_SIZE, my_r_size * sizeof(tuple_t));
    //BUG_ON(!my_r_tuples);
    //tuple_t *my_s_tuples;
    //my_s_tuples = aligned_alloc(CACHELINE_SIZE, my_s_size * sizeof(tuple_t));
    //BUG_ON(!my_s_tuples);

    //memcpy(my_r_tuples, r.tuples + r_offset, my_r_size * sizeof(tuple_t));
    //memcpy(my_s_tuples, s.tuples + s_offset, my_s_size * sizeof(tuple_t));

    //slice_r->tuples   = my_r_tuples;
    //slice_r->n_tuples = my_r_size;
    //slice_s->tuples   = my_s_tuples;
    //slice_s->n_tuples = my_s_size;

    slice_r->tuples   = r.tuples + r_offset;
    slice_r->n_tuples = my_r_size;
    slice_s->tuples   = s.tuples + s_offset;
    slice_s->n_tuples = my_s_size;
}

void release_slices(relation_t *slice_r, relation_t *slice_s) {
    (void)slice_r;
    (void)slice_s;
    //free(slice_r->tuples);
    //free(slice_s->tuples);
}
