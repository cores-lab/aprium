#define _GNU_SOURCE

#include <pthread.h>
#include <sched.h>
#include <stdio.h>

#include "join.h"
#include "config.h"
#include "task_queue.h"

#if PERF
#include "timer.h"
#endif

/* Worker thread arguments */
struct arg {
    alignas(CACHELINE_SIZE)
    relation_t r;
    uint32_t **hist_r;
    tuple_t  *tmp_r;
    size_t total_tuples_r;
    relation_t s;
    uint32_t **hist_s;
    tuple_t  *tmp_s;
    size_t total_tuples_s;
    task_queue_t *join_queue;
    task_queue_t *part_queue;
    pthread_barrier_t *barrier;
    uint64_t matches;
    size_t my_tid;
    size_t n_threads;
#if PERF
    timing_t perf;
#endif
};
typedef struct arg arg_t;

/* "Multi-threaded partition phase 1st pass per relation" arguments */
struct part {
    alignas(CACHELINE_SIZE)
    relation_t rel;
    uint32_t **hist;
    uint64_t  *offset;
    tuple_t   *tmp;
    size_t total_tuples;
    //uint64_t ignore_bits; // only need these if skewed
    //uint64_t radix_bits;
    size_t my_tid;
    size_t n_threads;
    pthread_barrier_t *barrier;
};
typedef struct part part_t;

/* join */
uint64_t single_threaded(relation_t *r, relation_t *s);
uint64_t multi_threaded(relation_t *r, relation_t *s, size_t n_threads);
void *prj_thread(void *args);
void parallel_radix_partition(part_t * const part);
void serial_radix_partition(task_t * const task, task_queue_t *queue,
                            uint64_t const shift_bits,
                            uint64_t const radix_bits);
void radix_partition(relation_t * restrict in, relation_t * restrict out,
                     uint32_t * restrict hist, uint64_t const shift_bits,
                     uint64_t const radix_bits, uint64_t const padding_tuples);
uint64_t bucket_chaining_join(relation_t const * const r,
                              relation_t const * const s);


/* helper */
static inline uint64_t next_pow2(uint64_t v) {
    if (v == 0) return 1;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    return v + 1;
}

static inline uint64_t
hash_bit_modulo(uint64_t k, uint64_t mask, uint64_t nbits) {
    return (k & mask) >> nbits;
}

static inline void barrier_arrive(pthread_barrier_t *barrier) {
    int err = pthread_barrier_wait(barrier);
    BUG_ON(err != 0 && err != PTHREAD_BARRIER_SERIAL_THREAD);
}

/* impl */
uint64_t join_relations(relation_t *r, relation_t *s, size_t n_threads) {
    uint64_t result;
#if 0
    (void)n_threads;
    result = single_threaded(r, s);
#else
    result = multi_threaded(r, s, n_threads);
#endif
    return result;
}

/**
 * @brief Performs a parallel radix‐partitioned join between two relations.
 *
 * This function partitions relations R and S in parallel using multiple
 * threads, then performs a build–probe join on the resulting partitions.
 *
 * @param[in]  r          Relation R.
 * @param[in]  s          Relation S.
 * @param[in]  n_threads  Number of worker threads to spawn.
 *
 * @return Total number of matching tuple pairs found by the join.
 */
uint64_t multi_threaded(relation_t *r, relation_t *s, size_t n_threads) {
    uint64_t matches = 0;

    /* Threads */
    pthread_t threads[n_threads];
    pthread_attr_t attr;
    pthread_barrier_t barrier;
    cpu_set_t set;
    arg_t args[n_threads];

    /* Thread-local histograms */
    uint32_t **hist_r;
    uint32_t **hist_s;

    /* Temporary space for partitioning */
    tuple_t *tmp_r;
    tuple_t *tmp_s;

    /* Queues for partition and join tasks */
    task_queue_t *part_queue;
    task_queue_t *join_queue;

    /* Init */
    part_queue = task_queue_init(FANOUT_PASS1);
    BUG_ON(!part_queue);
    join_queue = task_queue_init(1 << N_RADIX_BITS);
    BUG_ON(!join_queue);

    size_t tmp_size;
    tmp_size = r->n_tuples * sizeof(tuple_t) + RELATION_PADDING;
    tmp_r = (tuple_t *) aligned_alloc(CACHELINE_SIZE, tmp_size);
    BUG_ON(!tmp_r);
    tmp_size = s->n_tuples * sizeof(tuple_t) + RELATION_PADDING;
    tmp_s = (tuple_t *) aligned_alloc(CACHELINE_SIZE, tmp_size);
    BUG_ON(!tmp_s);

    size_t hist_size = n_threads * sizeof(uint32_t *);
    hist_r = (uint32_t **) aligned_alloc(CACHELINE_SIZE, hist_size);
    BUG_ON(!hist_r);
    hist_s = (uint32_t **) aligned_alloc(CACHELINE_SIZE, hist_size);
    BUG_ON(!hist_s);

    int err = pthread_barrier_init(&barrier, NULL, n_threads);
    BUG_ON(err);

    pthread_attr_init(&attr);

    /* Assign slices of R & S for each thread */
    size_t slice_r = r->n_tuples / n_threads;
    size_t slice_s = s->n_tuples / n_threads;
    for (size_t i = 0; i < n_threads; i++) {
        int is_last_thread = (i == (n_threads - 1));

        args[i].r.tuples = r->tuples + i * slice_r;
        args[i].r.n_tuples = is_last_thread ? (r->n_tuples - i * slice_r)
                                            : slice_r;
        args[i].hist_r = hist_r;
        args[i].tmp_r = tmp_r;
        args[i].total_tuples_r = r->n_tuples;

        args[i].s.tuples = s->tuples + i * slice_s;
        args[i].s.n_tuples = is_last_thread ? (s->n_tuples - i * slice_s)
                                            : slice_s;
        args[i].hist_s = hist_s;
        args[i].tmp_s = tmp_s;
        args[i].total_tuples_s = s->n_tuples;

        args[i].part_queue = part_queue;
        args[i].join_queue = join_queue;

        args[i].my_tid = i;
        args[i].n_threads = n_threads;
        args[i].barrier = &barrier;

        int cpu = CPU_MAPPING[i % N_CPUS];
#if DEBUG
        printf("Assigning thread %ld to CPU %d\n", i, cpu);
#endif
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);

        int err;
        err = pthread_create(&threads[i], &attr, prj_thread, (void*)&args[i]);
        BUG_ON(err);
    }

    /* Wait for threads to finish */
    for (size_t i = 0; i < n_threads; i++) {
        pthread_join(threads[i], NULL);
        matches += args[i].matches;
    }

#if PERF
    print_timing(matches, &args[0].perf);
#endif

    /* Clean-up */
    for (size_t i = 0; i < n_threads; i++) {
        free(hist_r[i]);
        free(hist_s[i]);
    }
    free(hist_r);
    free(hist_s);
    task_queue_free(part_queue);
    task_queue_free(join_queue);
    free(tmp_r);
    free(tmp_s);

    return matches;
}

/**
 * @brief Worker function executed by each join thread.
 *
 * Each thread initializes its own histogram buffers and offset arrays,
 * performs the first pass of radix partitioning on its slice of R and S,
 * then either enqueues sub‐tasks for further partitioning or performs
 * the build–probe join directly.
 *
 * This function is intended to be passed to pthread_create().
 *
 * @param[in,out] arg  Thread arguments.
 *
 * @return Always returns NULL.
 */
void *prj_thread(void *arg) {
    arg_t *args = (arg_t *) arg;
    size_t const my_tid = args->my_tid;
    int const is_coordinator = (my_tid == 0);

    uint64_t *offset_r;
    uint64_t *offset_s;
    offset_r = (uint64_t *) calloc((FANOUT_PASS1 + 1), sizeof(uint64_t));
    BUG_ON(!offset_r);
    offset_s = (uint64_t *) calloc((FANOUT_PASS1 + 1), sizeof(uint64_t));
    BUG_ON(!offset_s);

    args->hist_r[my_tid] = (uint32_t *) calloc(FANOUT_PASS1, sizeof(uint32_t));
    BUG_ON(!(args->hist_r[my_tid]));
    args->hist_s[my_tid] = (uint32_t *) calloc(FANOUT_PASS1, sizeof(uint32_t));
    BUG_ON(!(args->hist_s[my_tid]));

    task_queue_t *part_queue = args->part_queue;
    task_queue_t *join_queue = args->join_queue;

    /* Wait until each thread is initialized */
    barrier_arrive(args->barrier);

    /* Partition phase */
#if PERF
    args->perf.start = timestamp();
    start_timer(&args->perf.part);
#endif

    /* 1st pass */
    part_t part;
    //part.ignore_bits  = 0;
    //part.radix_bits   = N_RADIX_BITS_PASS1;
    part.my_tid       = my_tid;
    part.n_threads    = args->n_threads;
    part.barrier      = args->barrier;

    /* Partition R */
    part.rel          = args->r;
    part.tmp          = args->tmp_r;
    part.hist         = args->hist_r;
    part.offset       = offset_r;
    part.total_tuples = args->total_tuples_r;

    parallel_radix_partition(&part);

    /* Partition S */
    part.rel          = args->s;
    part.tmp          = args->tmp_s;
    part.hist         = args->hist_s;
    part.offset       = offset_s;
    part.total_tuples = args->total_tuples_s;

    parallel_radix_partition(&part);

    /* Wait until each thread finishes 1st pass */
    barrier_arrive(args->barrier);

    /* Coordinator creates tasks for next step (2nd pass or buildprobe) */
    if (is_coordinator) {
        for (size_t i = 0; i < FANOUT_PASS1; i++) {
            uint32_t count_r = offset_r[i + 1] - offset_r[i] - PADDING_TUPLES;
            uint32_t count_s = offset_s[i + 1] - offset_s[i] - PADDING_TUPLES;

            if (count_r > 0 && count_s > 0) {
                task_t *t = task_queue_get_slot(part_queue);

                t->r.tuples = args->tmp_r + offset_r[i];
                t->r.n_tuples = count_r;
                /* Here we assume initial R has enough room for padding */
                t->tmp_r.tuples = args->r.tuples + offset_r[i];
                t->tmp_r.n_tuples = count_r;

                t->s.tuples = args->tmp_s + offset_s[i];
                t->s.n_tuples = count_s;
                /* Here we assume initial S has enough room for padding */
                t->tmp_s.tuples = args->s.tuples + offset_s[i];
                t->tmp_s.n_tuples = count_s;

                task_queue_add(part_queue, t);
            }
        }
    }

    /* Wait until coordinator adds all tasks */
    barrier_arrive(args->barrier);

#if N_PASSES==1
    task_queue_t *swap = join_queue;
    join_queue = part_queue;
    part_queue = swap;
#elif N_PASSES==2
    /* 2nd pass */
#if DEBUG
    if (is_coordinator) {
        printf("2nd pass: #tasks = %ld\n", part_queue->size);
    }
    barrier_arrive(args->barrier);
#endif
    task_t *part_task;
    while ((part_task = task_queue_get_atomic(part_queue))) {
        uint64_t shift_bits = N_RADIX_BITS_PASS1;
        uint64_t radix_bits = N_RADIX_BITS_PASS2;
        serial_radix_partition(part_task, join_queue, shift_bits, radix_bits);
    }

    /* Wait until parallel threads add all join tasks */
    barrier_arrive(args->barrier);
#endif

    /* Buildprobe phase */
#if PERF
    stop_timer(&args->perf.part);
    start_timer(&args->perf.build_probe);
#endif

#if DEBUG
    if (is_coordinator) {
        printf("Buildprobe: #tasks = %ld\n", join_queue->size);
    }
    barrier_arrive(args->barrier);
#endif

    uint64_t matches = 0;
    task_t *join_task;
    while ((join_task = task_queue_get_atomic(join_queue))) {
        matches += bucket_chaining_join(&join_task->r, &join_task->s);
    }
    args->matches = matches;

#if PERF
    barrier_arrive(args->barrier);
    stop_timer(&args->perf.build_probe);
    args->perf.end = timestamp();
#endif

    /* Clean-up */
    free(offset_r);
    free(offset_s);

    return NULL;
}

/**
 * @brief Performs one pass of parallel radix partitioning on a relation slice.
 *
 * This function:
 *   1. Computes a local histogram of partition sizes based on low-order bits.
 *   2. Converts the histogram into a prefix sum to determine local offsets.
 *   3. Synchronizes with other threads at a barrier.
 *   4. Computes global partition start offsets by accumulating other threads’
 *      histograms.
 *   5. Copies tuples from the input slice into `tmp` clustered by partition.
 *
 * @param[in,out] part  Partitioning pass arguments.
 */
void parallel_radix_partition(part_t *const part) {
    tuple_t const * restrict rel = part->rel.tuples;
    uint32_t      **hist         = part->hist;
    tuple_t       * restrict tmp = part->tmp;

    size_t const n_tuples  = part->rel.n_tuples;
    size_t const my_tid    = part->my_tid;
    size_t const n_threads = part->n_threads;

    //uint64_t const ignore_bits = part->ignore_bits;
    //uint64_t const radix_bits  = part->radix_bits;
    uint64_t const ignore_bits = 0;
    uint64_t const radix_bits  = N_RADIX_BITS_PASS1;
    size_t   const fanout      = 1 << radix_bits;
    uint64_t const mask        = (fanout - 1) << ignore_bits;

    /* offset: cluster i starts at offset[i] and ends at offset[i+1]-1 */
    /* dst: current write-out position within cluster i stored in dst[i] */
    uint64_t * restrict offset = part->offset;
    uint64_t dst[fanout];

    /* Compute local histogram for the assigned chunk of rel */
    uint32_t *my_hist = hist[my_tid];
    for (size_t i = 0; i < n_tuples; i++) {
        size_t idx = hash_bit_modulo(rel[i].key, mask, ignore_bits);
        my_hist[idx]++;
    }

    /* Compute local prefix sum on hist */
    size_t sum = 0;
    for (size_t i = 0; i < fanout; i++) {
        sum += my_hist[i];
        my_hist[i] = sum;
    }

    /* Wait until other parallel threads compute histogram + prefix sum */
    barrier_arrive(part->barrier);

    /* Determine the start and end of each cluster */
    for (size_t i = 0; i < my_tid; i++) {
        for (size_t j = 0; j < fanout; j++) {
            offset[j] += hist[i][j];
        }
    }
    for (size_t i = my_tid; i < n_threads; i++) {
        for (size_t j = 1; j < fanout; j++) {
            offset[j] += hist[i][j - 1];
        }
    }
    for (size_t i = 0; i < fanout; i++) {
        offset[i] += i * PADDING_TUPLES;
        dst[i] = offset[i];
    }
    offset[fanout] = part->total_tuples + fanout * PADDING_TUPLES;

    /* Copy tuples to their corresponding clusters */
    for(size_t i = 0; i < n_tuples; i++) {
        size_t idx = hash_bit_modulo(rel[i].key, mask, ignore_bits);
        tmp[dst[idx]] = rel[i];
        ++dst[idx];
    }
}

/**
 * This function implements the radix partitioning of two input relations
 * defined in task. After partitioning, each partition pair is added to queue
 * for further partitioning or buildprobe.
 *
 * @param[in,out] task          Description of the relations to be partitioned.
 * @param[in,out] queue         Task queue to add join tasks to after
 *                              partitioning pass.
 * @param[in]     shift_bits    Number of lower bits to skip before extracting
 *                              radix bits.
 * @param[in]     radix_bits    Number of bits to extract to form the partition
 *                              index.
 */
void
serial_radix_partition(task_t * const task,
                       task_queue_t *queue,
                       uint64_t const shift_bits,
                       uint64_t const radix_bits)
{
    size_t offset_r = 0;
    size_t offset_s = 0;
    size_t const fanout = 1 << radix_bits;

    uint32_t *hist_r = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    BUG_ON(!hist_r);
    uint32_t *hist_s = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    BUG_ON(!hist_s);

    radix_partition(&task->r, &task->tmp_r, hist_r, shift_bits, radix_bits,
                    P_TUPLES);
    radix_partition(&task->s, &task->tmp_s, hist_s, shift_bits, radix_bits,
                    P_TUPLES);

    for(size_t i = 0; i < fanout; i++) {
        if (hist_r[i] > 0 && hist_s[i] > 0) {
            task_t *t = task_queue_get_slot_atomic(queue);

            t->r.n_tuples = hist_r[i];
            t->r.tuples = task->tmp_r.tuples + offset_r + i * P_TUPLES;
            t->tmp_r.tuples = task->r.tuples + offset_r + i * P_TUPLES;
            offset_r += hist_r[i];

            t->s.n_tuples = hist_s[i];
            t->s.tuples = task->tmp_s.tuples + offset_s + i * P_TUPLES;
            t->tmp_s.tuples = task->s.tuples + offset_s + i * P_TUPLES;
            offset_s += hist_s[i];

            task_queue_add_atomic(queue, t);
        }
        else {
            offset_r += hist_r[i];
            offset_s += hist_s[i];
        }
    }
    free(hist_r);
    free(hist_s);
}

/**
 * @brief Radix partitioning algorithm (originally described by Manegold et al.)
 *
 * The algorithm mimics the 2-pass radix clustering algorithm from Kim et al.
 * The difference is that it does not compute prefix-sum, instead the sum
 * (offset in the code) is computed iteratively.
 *
 * @param[in]  in           Input relation.
 * @param[out] out          Output relation (result of the partitioning).
 * @param[out] hist         Number of tuples in each partition.
 * @param[in]  shift_bits   Number of lower bits to skip before extracting radix
 *                          bits of the tuple key.
 * @param[in]  radix_bits   Number of bits to extract to form the partition
 *                          index of the key.
 * @param[in]  radix_bits   Number of empty tuples to insert as padding between
 *                          relations.
 */
void
radix_partition(relation_t * restrict in,
                relation_t * restrict out,
                uint32_t * restrict hist,
                uint64_t const shift_bits,
                uint64_t const radix_bits,
                uint64_t const padding_tuples)
{
    size_t const fanout = 1 << radix_bits;
    uint64_t const mask = (fanout - 1) << shift_bits;
    size_t offset = 0;

    uint32_t dst[fanout];

    /* Count tuples per partition */
    for (size_t i = 0; i < in->n_tuples; i++) {
        size_t idx = hash_bit_modulo(in->tuples[i].key, mask, shift_bits);
        hist[idx]++;
    }
    /* Determine the start and end of each partition depending on the counts */
    for (size_t i = 0; i < fanout; i++) {
        dst[i] = offset + i * padding_tuples;
        offset += hist[i];
    }

    /* Copy tuples to their corresponding partitions at appropriate offsets */
    for(size_t i = 0; i < in->n_tuples; i++) {
        size_t idx = hash_bit_modulo(in->tuples[i].key, mask, shift_bits);
        out->tuples[ dst[idx] ] = in->tuples[i];
        ++dst[idx];
    }
}

/**
 * @brief Join relations R and S using a bucket-chaining algorithm.
 *
 * This join builds the hashtable using the bucket-chaining algorithm proposed
 * by Manegold et al. Relations R and S typically fit into L2 cache or at least
 * R and (|R| * sizeof(uint32_t)) fits into L2 cache.
 *
 * @param[in] r     Relation R.
 * @param[in] s     Relation S.
 *
 * @return Number of result tuples.
 */
uint64_t
bucket_chaining_join(relation_t const * const r,
                     relation_t const * const s)
{
    uint64_t matches = 0;
    uint64_t n_buckets = next_pow2(r->n_tuples);
    uint64_t const mask = (n_buckets - 1) << (N_RADIX_BITS);

    uint32_t *next = (uint32_t *) malloc(r->n_tuples * sizeof(uint32_t));
    BUG_ON(!next);
    uint32_t *bucket = (uint32_t *) calloc(n_buckets, sizeof(uint32_t));
    BUG_ON(!bucket);

    /* Build loop */
    for (size_t i = 0; i < r->n_tuples; ) {
        size_t idx = hash_bit_modulo(r->tuples[i].key, mask, N_RADIX_BITS);
        next[i] = bucket[idx];
        /* Start positions from 1, 0 marks empty bucket */
        bucket[idx] = ++i;
    }

    /* Probe loop */
    for (size_t i = 0; i < s->n_tuples; i++) {
        size_t idx = hash_bit_modulo(s->tuples[i].key, mask, N_RADIX_BITS);
        for(int hit = bucket[idx]; hit > 0; hit = next[hit - 1]){
            if(s->tuples[i].key == r->tuples[hit - 1].key){
                matches ++;
            }
        }
    }

    /* Clean-up */
    free(bucket);
    free(next);

    return matches;
}

/**
 * @brief Performs a single-threaded radix hash join between two relations.
 *
 * Partitions the input relations using radix partitioning and performs a
 * bucket-chaining join on matching partitions to find tuple matches.
 *
 * @param[in] r     Relation R.
 * @param[in] s     Relation S.
 *
 * @return Number of matching tuples.
 */
uint64_t single_threaded(relation_t *r, relation_t *s)
{
    uint64_t matches = 0;
    size_t fanout;

#if PERF
    timing_t perf;
#endif

    relation_t *out_r = (relation_t *) malloc(sizeof(relation_t));
    BUG_ON(!out_r);
    relation_t *out_s = (relation_t *) malloc(sizeof(relation_t));
    BUG_ON(!out_s);

    /* Allocate temporary space for partitioning */
    out_r->tuples = (tuple_t *) malloc(r->n_tuples * sizeof(tuple_t));
    BUG_ON(!(out_r->tuples));
    out_r->n_tuples = r->n_tuples;

    out_s->tuples = (tuple_t *) malloc(s->n_tuples * sizeof(tuple_t));
    BUG_ON(!(out_s->tuples));
    out_s->n_tuples = s->n_tuples;

    /* Allocate histogram space for counts */
    fanout = 1 << N_RADIX_BITS_PASS1;
    uint32_t *hist_r = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    BUG_ON(!hist_r);
    uint32_t *hist_s = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    BUG_ON(!hist_s);

    /* Partition phase */
#if PERF
    perf.start = timestamp();
    start_timer(&perf.part);
#endif

    /* 1st pass */
    radix_partition(r, out_r, hist_r, 0, N_RADIX_BITS_PASS1, 0);
    radix_partition(s, out_s, hist_s, 0, N_RADIX_BITS_PASS1, 0);

#if N_PASSES==1
    r = out_r;
    s = out_s;
#elif N_PASSES==2
    free(hist_r);
    free(hist_s);

    fanout = 1 << N_RADIX_BITS_PASS2;
    hist_r = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    BUG_ON(!hist_r);
    hist_s = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    BUG_ON(!hist_s);

    /* 2nd pass */
    radix_partition(out_r, r, hist_r, N_RADIX_BITS_PASS1, N_RADIX_BITS_PASS2,
                    0);
    radix_partition(out_s, s, hist_s, N_RADIX_BITS_PASS1, N_RADIX_BITS_PASS2,
                    0);

    free(hist_r);
    free(hist_s);

    fanout = 1 << N_RADIX_BITS;
    hist_r = (uint32_t *) calloc(fanout, sizeof(uint32_t));
    BUG_ON(!hist_r);
    hist_s = (uint32_t *) calloc(fanout, sizeof(uint32_t));
    BUG_ON(!hist_s);

    /* Count number of tuples per partition */
    for(size_t i = 0; i < r->n_tuples; i++) {
        size_t idx = (r->tuples[i].key) & (fanout -1);
        hist_r[idx]++;
    }
    for(size_t i = 0; i < s->n_tuples; i++) {
        size_t idx = (s->tuples[i].key) & (fanout -1);
        hist_s[idx]++;
    }
#endif

    /* Buildprobe phase */
#if PERF
    stop_timer(&perf.part);
    start_timer(&perf.build_probe);
#endif

    relation_t tmp_r;
    relation_t tmp_s;
    size_t offset_r = 0;
    size_t offset_s = 0;
    for(size_t i = 0; i < fanout; i++) {
        if (hist_r[i] > 0 && hist_s[i] > 0) {
            tmp_r.n_tuples = hist_r[i];
            tmp_r.tuples = r->tuples + offset_r;
            offset_r += hist_r[i];

            tmp_s.n_tuples = hist_s[i];
            tmp_s.tuples = s->tuples + offset_s;
            offset_s += hist_s[i];

            matches += bucket_chaining_join(&tmp_r, &tmp_s);
        }
        else {
            offset_r += hist_r[i];
            offset_s += hist_s[i];
        }
    }

#if PERF
    stop_timer(&perf.build_probe);
    perf.end = timestamp();
    print_timing(matches, &perf);
#endif

    /* Clean-up */
    free(hist_r);
    free(hist_s);
    free(out_r->tuples);
    free(out_s->tuples);
    free(out_r);
    free(out_s);

    return matches;
}

