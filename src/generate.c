#include <immintrin.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <sys/types.h>

#include "generate.h"
#include "config.h"
#include "cxl.h"

/* PRNG SplitMix64 */

#define PRNG_WEYL_CONST 0x9e3779b97f4a7c15ULL // Golden ratio fractional part
#define PRNG_MIX1       0xbf58476d1ce4e5b9ULL // Avalanche multiplier 1
#define PRNG_MIX2       0x94d049bb133111ebULL // Avalanche multiplier 2

static uint64_t mix64(uint64_t x) {
    x = (x ^ (x >> 30)) * PRNG_MIX1;
    x = (x ^ (x >> 27)) * PRNG_MIX2;
    return x ^ (x >> 31);
}

static uint64_t next_uint64(uint64_t *state) {
    *state += PRNG_WEYL_CONST;
    return mix64(*state);
}

static double uint64_to_unit(uint64_t x) {
    // Keep 53 bits for standard IEEE 754 double precision mantissa
    return (x >> 11) * (1.0 / (double)(1ULL << 53));
}

/* Uniform Distribution */

typedef struct {
    tuple_t *tuples;
    size_t lo;
    size_t hi;
    uint64_t n;
    uint64_t a;
    uint64_t b;
} fill_uniform_arg_t;

static uint64_t gcd64(uint64_t x, uint64_t y) {
    while (y != 0) {
        uint64_t t = x % y;
        x = y;
        y = t;
    }
    return x;
}

static void *fill_worker_uniform(void *p) {
    fill_uniform_arg_t *a = (fill_uniform_arg_t *)p;

    for (size_t i = a->lo; i < a->hi; i++) {
        a->tuples[i].key = (a->a * i + a->b) % a->n;
        a->tuples[i].rid = 0;
    }

    cache_wb(&a->tuples[a->lo], sizeof(a->tuples[0]) * (a->hi - a->lo), true);
    return NULL;
}

void fill_relation_uniform(relation_t *rel, size_t n_threads) {
    size_t n = rel->n_tuples;
    if (n_threads > n) {
        n_threads = n;
    }

    uint64_t seed = (uint64_t)time(NULL) ^ (uint64_t)(uintptr_t)rel;

    // Calculate a and b for the LCG bijection to ensure unique key permutations
    uint64_t a = mix64(seed) | 1ULL;
    while (gcd64(a, n) != 1) {
        a += 2;
    }
    uint64_t b = mix64(seed + 1) % n;

    pthread_t tids[n_threads];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    cpu_set_t set;
    fill_uniform_arg_t args[n_threads];

    size_t base = n / n_threads;
    size_t rem = n % n_threads;
    size_t pos = 0;

    for (size_t t = 0; t < n_threads; t++) {
        size_t len = base + (t < rem ? 1 : 0);

        args[t].tuples = rel->tuples;
        args[t].lo = pos;
        args[t].hi = pos + len;
        args[t].n = n;
        args[t].a = a;
        args[t].b = b;

        int cpu = CPU_MAPPING[t];
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        BUG_ON(pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set));
        BUG_ON(pthread_create(&tids[t], &attr, fill_worker_uniform, &args[t]));
        pos += len;
    }

    for (size_t t = 0; t < n_threads; t++) {
        pthread_join(tids[t], NULL);
    }
}

typedef struct {
    tuple_t *tuples;
    size_t lo;
    size_t hi;
    uint64_t r_size;
    uint64_t seed;
} fill_fk_uniform_arg_t;

static void *fill_worker_fk_uniform(void *p) {
    fill_fk_uniform_arg_t *a = (fill_fk_uniform_arg_t *)p;

    for (size_t i = a->lo; i < a->hi; i++) {
        a->tuples[i].key = next_uint64(&a->seed) % a->r_size;
        a->tuples[i].rid = 0;
    }

    cache_wb(&a->tuples[a->lo], sizeof(a->tuples[0]) * (a->hi - a->lo), true);
    return NULL;
}

void fill_relation_fk_uniform(relation_t *rel, size_t n_threads, size_t r_size) {
    size_t n = rel->n_tuples;
    if (n_threads > n) {
        n_threads = n;
    }

    uint64_t base_seed = (uint64_t)time(NULL) ^ (uint64_t)(uintptr_t)rel;

    pthread_t tids[n_threads];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    cpu_set_t set;
    fill_fk_uniform_arg_t args[n_threads];

    size_t base = n / n_threads;
    size_t rem = n % n_threads;
    size_t pos = 0;

    for (size_t t = 0; t < n_threads; t++) {
        size_t len = base + (t < rem ? 1 : 0);

        args[t].tuples = rel->tuples;
        args[t].lo = pos;
        args[t].hi = pos + len;
        args[t].r_size = r_size;
        args[t].seed = mix64(base_seed + t);

        int cpu = CPU_MAPPING[t];
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        BUG_ON(pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set));
        BUG_ON(pthread_create(&tids[t], &attr, fill_worker_fk_uniform, &args[t]));
        pos += len;
    }

    for (size_t t = 0; t < n_threads; t++) {
        pthread_join(tids[t], NULL);
    }
}

/* Zipfian distribution */

typedef struct {
    tuple_t *tuples;
    size_t lo;
    size_t hi;
    uint64_t const *keys; // random permutation of 0..domain_size-1
    double const *cdf;    // Zipf CDF over ranks 1..domain_size
    size_t domain_size;
    uint64_t seed;
} fill_zipf_arg_t;

static double *make_zipf_cdf(size_t n, double tau) {
    double *cdf = malloc(n * sizeof(double));
    BUG_ON(!cdf);

    double sum = 0.0;
    for (size_t k = 1; k <= n; k++) {
        sum += 1.0 / pow((double)k, tau);
    }

    double acc = 0.0;
    for (size_t k = 1; k <= n; k++) {
        acc += (1.0 / pow((double)k, tau)) / sum;
        cdf[k - 1] = acc;
    }
    cdf[n - 1] = 1.0;
    return cdf;
}

static uint64_t *make_keys(size_t n, uint64_t *seed) {
    uint64_t *keys = malloc(n * sizeof(uint64_t));
    BUG_ON(!keys);

    for (size_t i = 0; i < n; i++) {
        keys[i] = i;
    }

    for (size_t i = n - 1; i > 0; i--) {
        size_t j = (size_t)(next_uint64(seed) % (i + 1));
        uint64_t tmp = keys[i];
        keys[i] = keys[j];
        keys[j] = tmp;
    }
    return keys;
}

static uint64_t sample_zipf(uint64_t const *keys, double const *cdf, size_t n, uint64_t *seed) {
    double r = uint64_to_unit(next_uint64(seed));
    size_t low = 0;
    size_t high = n - 1;

    while (low < high) {
        size_t mid = low + (high - low) / 2;
        if (r > cdf[mid]) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return keys[low];
}

static void *fill_worker_zipf(void *p) {
    fill_zipf_arg_t *a = (fill_zipf_arg_t *)p;

    for (size_t i = a->lo; i < a->hi; ++i) {
        a->tuples[i].key = sample_zipf(a->keys, a->cdf, a->domain_size, &a->seed);
        a->tuples[i].rid = 0;
    }

    cache_wb(&a->tuples[a->lo], sizeof(a->tuples[0]) * (a->hi - a->lo), true);
    return NULL;
}

void fill_relation_fk_zipf(relation_t *rel, size_t n_threads, size_t domain_size, double tau) {
    size_t n = rel->n_tuples;
    if (n_threads > n) {
        n_threads = n;
    }

    uint64_t base_seed = (uint64_t)time(NULL) ^ (uint64_t)(uintptr_t)rel;

    double *cdf = make_zipf_cdf(domain_size, tau);
    uint64_t *keys = make_keys(domain_size, &base_seed);

    pthread_t tids[n_threads];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    cpu_set_t set;
    fill_zipf_arg_t args[n_threads];

    size_t base = n / n_threads;
    size_t rem = n % n_threads;
    size_t pos = 0;

    for (size_t t = 0; t < n_threads; t++) {
        size_t len = base + (t < rem ? 1 : 0);

        args[t].tuples = rel->tuples;
        args[t].lo = pos;
        args[t].hi = pos + len;
        args[t].keys = keys;
        args[t].cdf = cdf;
        args[t].domain_size = domain_size;
        args[t].seed = mix64(base_seed + t);

        int cpu = CPU_MAPPING[t];
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        BUG_ON(pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set));
        BUG_ON(pthread_create(&tids[t], &attr, fill_worker_zipf, &args[t]));
        pos += len;
    }

    for (size_t t = 0; t < n_threads; t++) {
        pthread_join(tids[t], NULL);
    }

    free(keys);
    free(cdf);
}

void fill_relation_zipf(relation_t *rel, size_t n_threads, double tau) {
    fill_relation_fk_zipf(rel, n_threads, rel->n_tuples, tau);
}

/* Loading */

void load_relation(relation_t *rel, char const *filename) {
    FILE *fp = fopen(filename, "rb");
    BUG_ON(!fp);

    BUG_ON(fseeko(fp, 0, SEEK_END) != 0);
    off_t file_size = ftello(fp);
    BUG_ON(file_size < 0 || (size_t)file_size < rel->n_tuples * sizeof(tuple_t));

    BUG_ON(fseeko(fp, 0, SEEK_SET) != 0);
    size_t got = fread(rel->tuples, sizeof(tuple_t), rel->n_tuples, fp);
    fclose(fp);

    BUG_ON(got != rel->n_tuples * sizeof(tuple_t));
}

static void assign_slice(relation_t *slice, relation_t const *rel,
                         size_t my_nid, size_t n_nodes, size_t shift) {
    size_t const unit = CACHELINE_SIZE / sizeof(tuple_t);
    size_t idx = (my_nid + shift) % n_nodes;

    size_t total = rel->n_tuples / unit;
    size_t tail = rel->n_tuples % unit;

    size_t base = total / n_nodes;
    size_t rem = total % n_nodes;

    size_t lines = base + (idx < rem ? 1 : 0);
    size_t my_tuples = lines * unit;

    size_t offs = (idx < rem) ? idx : rem;
    size_t my_offs = ((idx * base) + offs) * unit;

    if (idx == n_nodes - 1) {
        my_tuples += tail;
    }

    slice->tuples   = rel->tuples + my_offs;
    slice->n_tuples = my_tuples;
}

/* Slicing */
void accquire_slices(relation_t *slice_r, relation_t *slice_s, param_t *params) {
    relation_t r;
    relation_t s;
    bool const is_coordinator = (params->my_nid == COORDINATION_NODE);

    r.tuples = cxl_gen_r();
    r.n_tuples = params->r_size;
    s.tuples = cxl_gen_s();
    s.n_tuples = params->s_size;

    if (is_coordinator) {
#if DEBUG
        printf("Creating build relation R (size = %.3lf MiB, #tuples = %lu) : ",
            (double)sizeof(tuple_t) * params->r_size / (1024.0 * 1024.0),
            params->r_size);
        fflush(stdout);
#endif

        fill_relation_uniform(&r, params->n_threads);
        //load_relation(&r, "/path/to/rel");

#if DEBUG
        printf("OK\n");
#endif

#if DEBUG
        printf("Creating probe relation S (%s = %.1lf, size = %.3lf MiB, #tuples = %lu) : ",
            params->fill_mode == UNIFORM ? "uniform" : "zipf",
            params->fill_mode == UNIFORM ? 0.0 : params->zipf_alpha,
            (double)sizeof(tuple_t) * params->s_size / (1024.0 * 1024.0),
            params->s_size);
        fflush(stdout);
#endif

        if (params->fill_mode == UNIFORM) {
            fill_relation_fk_uniform(&s, params->n_threads, params->r_size);
        } else if (params->fill_mode == ZIPF) {
            fill_relation_fk_zipf(&s, params->n_threads, params->r_size, params->zipf_alpha);
        } else {
            //load_relation(&s, "/path/to/rel");
            exit(EXIT_FAILURE);
        }

#if DEBUG
        printf("OK\n");
#endif
    }

    cxl_barrier();

    assign_slice(slice_r, &r, params->my_nid, params->n_nodes, 0);
    assign_slice(slice_s, &s, params->my_nid, params->n_nodes, params->n_nodes - 1);
}

void release_slices(relation_t *slice_r, relation_t *slice_s) {
    (void)slice_r;
    (void)slice_s;
}