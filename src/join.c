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
    int32_t **hist_r;
    tuple_t  *rel_r;
    tuple_t  *part_buf_r;
    int32_t **hist_s;
    tuple_t  *rel_s;
    tuple_t  *part_buf_s;
    size_t n_tuples_r;
    size_t n_tuples_s;
    size_t total_tuples_r;
    size_t total_tuples_s;
    //task_queue_t **join_queue;
    //task_queue_t **part_queue;
    pthread_barrier_t *barrier;
    uint64_t matches;
    size_t my_tid;
    size_t n_threads;
};
typedef struct arg arg_t;

/* Partitioning phase arguments */
struct part {
    alignas(CACHELINE_SIZE)
    tuple_t  *rel;
    tuple_t  *tmp;
    int32_t **hist;
    int64_t  *out;
    arg_t    *targs;
    size_t n_tuples;
    size_t total_tuples;
    uint64_t ignore_bits;
    uint64_t radix_bits;
};
typedef struct part part_t;

#if PERF
struct timing {
    uint64_t start;
    uint64_t end;
    uint64_t part;
    uint64_t build_probe;
};
typedef struct timing timing_t;

static void print_timing(size_t n_tuples, timing_t *perf)
{
    uint64_t total = perf->end - perf->start;
    uint64_t part = perf->part;
    uint64_t build_probe = perf->build_probe;
    double per_tuple = total / n_tuples;

    printf("n_tuples,total,part,build_probe,per_tuple\n");
    printf("%lu,%lu,%lu,%lu,%.4lf\n", n_tuples, total, part, build_probe,
            per_tuple);
}

#endif

/* join */
uint64_t rj(relation_t *r, relation_t *s);

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
    if (err != 0 && err != PTHREAD_BARRIER_SERIAL_THREAD) {
        printf("pthread_barrier_wait error: %d\n", err);
        exit(EXIT_FAILURE);
    }
}

/* impl */
uint64_t join_relations(relation_t *r, relation_t *s,
                        size_t n_threads) {
    (void)n_threads;
    return rj(r, s);
//    uint64_t matches = 0;
//    pthread_t threads[n_threads];
//    pthread_attr_t attr;
//    pthread_barrier_t barrier;
//    cpu_set_t set;
//    arg_t args[n_threads];
//
//    int32_t **hist_r;
//    int32_t **hist_s;
//    tuple_t *part_buf_r;
//    tuple_t *part_buf_s;
//
//    ///* task_queue_t * part_queue, * join_queue; */
//    //int numnuma = 1;
//    //task_queue_t * part_queue[numnuma];
//    //task_queue_t * join_queue[numnuma];
//
//    //for(i = 0; i < numnuma; i++){
//    //    part_queue[i] = task_queue_init(FANOUT_PASS1);
//    //    join_queue[i] = task_queue_init((1<<NUM_RADIX_BITS));
//    //}
//
//    /* Allocate temporary space for partitioning */
//    size_t buf_size_r = rel_r->n_tuples * sizeof(tuple_t) + RELATION_PADDING;
//    part_buf_r = (tuple_t *) aligned_alloc(CACHELINE_SIZE, buf_size_r);
//    if (part_buf_r == NULL) {
//        perror("aligned_alloc");
//        exit(EXIT_FAILURE);
//    }
//    size_t buf_size_s = rel_s->n_tuples * sizeof(tuple_t) + RELATION_PADDING;
//    part_buf_s = (tuple_t *) aligned_alloc(CACHELINE_SIZE, buf_size_s);
//    if (part_buf_s == NULL) {
//        perror("aligned_alloc");
//        exit(EXIT_FAILURE);
//    }
//
//    /* Allocate histograms arrays, actual allocation is local to threads */
//    size_t hist_size = n_threads * sizeof(int32_t *);
//    hist_r = (int32_t **) aligned_alloc(CACHELINE_SIZE, hist_size);
//    if (hist_r == NULL) {
//        perror("aligned_alloc");
//        exit(EXIT_FAILURE);
//    }
//    hist_s = (int32_t **) aligned_alloc(CACHELINE_SIZE, hist_size);
//    if (hist_s == NULL) {
//        perror("aligned_alloc");
//        exit(EXIT_FAILURE);
//    }
//
//    int err = pthread_barrier_init(&barrier, NULL, n_threads);
//    if (err) {
//        printf("pthread_barrier_init error: %d\n", err);
//        exit(EXIT_FAILURE);
//    }
//
//    pthread_attr_init(&attr);
//
//    /* Assign chunks of R & S for each thread */
//    int32_t tuples_per_thread_r = rel_r->n_tuples / n_threads;
//    int32_t tuples_per_thread_s = rel_s->n_tuples / n_threads;
//    for (size_t i = 0; i < n_threads; i++) {
//        int cpu = CPU_MAPPING[i % N_CPUS];
//
//#if DEBUG
//        printf("Assigning thread %ld to CPU %d\n", i, cpu);
//#endif
//
//        CPU_ZERO(&set);
//        CPU_SET(cpu, &set);
//        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);
//
//        args[i].rel_r = rel_r->tuples + i * tuples_per_thread_r;
//        args[i].hist_r = hist_r;
//        args[i].part_buf_r = part_buf_r;
//
//        args[i].rel_s = rel_s->tuples + i * tuples_per_thread_s;
//        args[i].hist_s = hist_s;
//        args[i].part_buf_s = part_buf_s;
//
//        int is_last_thread = (i == (n_threads - 1));
//        size_t n_tuples_r;
//        size_t n_tuples_s;
//        if (is_last_thread) {
//            n_tuples_r = rel_r->n_tuples - i * tuples_per_thread_r;
//            n_tuples_s = rel_s->n_tuples - i * tuples_per_thread_s;
//        }
//        else {
//            n_tuples_r = tuples_per_thread_r;
//            n_tuples_s = tuples_per_thread_s;
//        }
//        args[i].n_tuples_r = n_tuples_r;
//        args[i].n_tuples_s = n_tuples_s;
//        args[i].total_tuples_r = rel_r->n_tuples;
//        args[i].total_tuples_s = rel_s->n_tuples;
//
//        args[i].my_tid = i;
//        //args[i].part_queue = part_queue;
//        //args[i].join_queue = join_queue;
//        args[i].barrier = &barrier;
//        args[i].n_threads = n_threads;
//
//        int err;
//        err = pthread_create(&threads[i], &attr, prj_thread, (void*)&args[i]);
//        if (err) {
//            printf("pthread_create error %d\n", err);
//            exit(EXIT_FAILURE);
//        }
//    }
//
//    /* Wait for threads to finish */
//    for (size_t i = 0; i < n_threads; i++) {
//        pthread_join(threads[i], NULL);
//        matches += args[i].matches;
//    }
//
//    /* Clean-up */
//    for (size_t i = 0; i < n_threads; i++) {
//        free(hist_r[i]);
//        free(hist_s[i]);
//    }
//    free(hist_r);
//    free(hist_s);
//
//    //for(size_t i = 0; i < numnuma; i++){
//    //    task_queue_free(part_queue[i]);
//    //    task_queue_free(join_queue[i]);
//    //}
//
//    free(part_buf_r);
//    free(part_buf_s);
//
//    return matches;
//}
//
//void *prj_thread(void *arg) {
//    arg_t *args = (arg_t *) arg;
//    size_t const my_tid = args->my_tid;
//    int const is_coordinator = (my_tid == 0);
//
//    part_t part;
//    //task_t * task;
//    //task_queue_t * part_queue;
//    //task_queue_t * join_queue;
//
//    int64_t *outputR = (int64_t *) calloc((FANOUT_PASS1 + 1), sizeof(int64_t));
//    if (outputR == NULL) {
//        perror("calloc");
//        exit(EXIT_FAILURE);
//    }
//    int64_t *outputS = (int64_t *) calloc((FANOUT_PASS1 + 1), sizeof(int64_t));
//    if (outputS == NULL) {
//        perror("calloc");
//        exit(EXIT_FAILURE);
//    }
//
//    //int numaid = get_numa_id(my_tid);
//    //part_queue = args->part_queue[numaid];
//    //join_queue = args->join_queue[numaid];
//
//    args->hist_r[my_tid] = (int32_t *) calloc(FANOUT_PASS1, sizeof(int32_t));
//    if (args->hist_r[my_tid] == NULL) {
//        perror("calloc");
//        exit(EXIT_FAILURE);
//    }
//    args->hist_s[my_tid] = (int32_t *) calloc(FANOUT_PASS1, sizeof(int32_t));
//    if (args->hist_s[my_tid] == NULL) {
//        perror("calloc");
//        exit(EXIT_FAILURE);
//    }
//
//    /* Wait until each thread is initialized */
//    barrier_arrive(args->barrier);
//
//#if PERF
//    /* Start profiling partition phase */
//    if (is_coordinator) {
//    }
//#endif
//
//    /* 1st pass of multi-pass partitioning */
//    part.ignore_bits    = 0;
//    part.radix_bits     = N_RADIX_BITS_PASS1;
//    part.targs          = args;
//
//    /* Partition R */
//    part.rel          = args->rel_r;
//    part.tmp          = args->part_buf_r;
//    part.hist         = args->hist_r;
//    part.out          = outputR;
//    part.n_tuples     = args->n_tuples_r;
//    part.total_tuples = args->total_tuples_r;
//
//    parallel_radix_partition(&part);
//
//    /* Partition S */
//    part.rel          = args->rel_s;
//    part.tmp          = args->part_buf_s;
//    part.hist         = args->hist_s;
//    part.out          = outputS;
//    part.n_tuples     = args->n_tuples_s;
//    part.total_tuples = args->total_tuples_s;
//
//    parallel_radix_partition(&part);
//
//    /* Wait until each thread finishes 1st partitioning pass */
//    barrier_arrive(args->barrier);
//
//    /* End of 1st partitioning pass */
//
//    /* Coordinator creates partitioning tasks for 2nd pass */
//    if (is_coordinator) {
//        //for (size_t i = 0; i < FANOUT_PASS1; i++) {
//        //    int32_t ntupR = outputR[i+1] - outputR[i] - PADDING_TUPLES;
//        //    int32_t ntupS = outputS[i+1] - outputS[i] - PADDING_TUPLES;
//
//        //    if (ntupR > 0 && ntupS > 0) {
//        //        /* Determine the NUMA node of each partition: */
//        //        void * ptr = (void*)&((args->tmpR + outputR[i])[0]);
//        //        int pq_idx = get_numa_node_of_address(ptr);
//
//        //        task_queue_t * numalocal_part_queue = args->part_queue[pq_idx];
//
//        //        task_t * t = task_queue_get_slot(numalocal_part_queue);
//
//        //        t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
//        //        t->relR.tuples = args->tmpR + outputR[i];
//        //        t->tmpR.tuples = args->relR + outputR[i];
//
//        //        t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
//        //        t->relS.tuples = args->tmpS + outputS[i];
//        //        t->tmpS.tuples = args->relS + outputS[i];
//
//        //        task_queue_add(numalocal_part_queue, t);
//        //    }
//        //}
//
//        ///* debug partitioning task queue */
//        //DEBUGMSG(1, "Pass-2: # partitioning tasks = %d\n", part_queue->count);
//    }
//
//    /* Wait until coordinator adds all partitioning tasks */
//    barrier_arrive(args->barrier);
//
////    /************ 2nd pass of multi-pass partitioning ********************/
////    /* 4. now each thread further partitions and add to join task queue **/
////
////#if N_PASSES==1
////    /* If the partitioning is single pass we directly add tasks from pass-1 */
////    task_queue_t * swap = join_queue;
////    join_queue = part_queue;
////    /* part_queue is used as a temporary queue for handling skewed parts */
////    part_queue = swap;
////
////#elif N_PASSES==2
////    while ((task = task_queue_get_atomic(part_queue))) {
////        serial_radix_partition(task, join_queue, R, D);
////    }
////#endif
////
////    free(outputR);
////    free(outputS);
////
////    /* wait at a barrier until all threads add all join tasks */
////    barrier_arrive(args->barrier);
////
////    DEBUGMSG((my_tid == 0), "Number of join tasks = %d\n", join_queue->count);
////
////#if PERF
////    /* Stop profiling partition phase */
////    if (is_coordinator) {
////        //stopTimer(&args->timer3);
////    }
////    /* Start profiling buildprobe phase */
////#endif
////
////    while ((task = task_queue_get_atomic(join_queue))) {
////        /* do the actual join. join method differs for different algorithms,
////           i.e. bucket chaining, histogram-based, histogram-based with simd &
////           prefetching  */
////        results += args->join_function(&task->relR, &task->relS, &task->tmpR, chainedbuf);
////
////        args->parts_processed ++;
////    }
////
////    args->result = results;
//
//#if PERF
//    /* Stop profiling buildprobe phase */
//    barrier_arrive(args->barrier);
//    if (is_coordinator) {
//    }
//#endif
//
//    return NULL;
}

void parallel_radix_partition(part_t *const part) {
    tuple_t const * restrict rel = part->rel;
    int32_t       **hist         = part->hist;
    tuple_t       * restrict tmp = part->tmp;

    size_t const my_tid    = part->targs->my_tid;
    size_t const n_threads = part->targs->n_threads;
    size_t const n_tuples  = part->n_tuples;

    uint64_t const ignore_bits = part->ignore_bits;
    uint64_t const radix_bits  = part->radix_bits;
    size_t   const fanout      = 1 << radix_bits;
    uint64_t const mask        = (fanout - 1) << ignore_bits;

    /* out: cluster i starts at out[i] and ends one before out[i+1] */
    /* dst: current position within cluster i stored in dst[i] */
    int64_t * restrict out = part->out;
    uint64_t dst[fanout];

    /* Compute local histogram for the assigned chunk of rel */
    int32_t *my_hist = hist[my_tid];
    for (size_t i = 0; i < n_tuples; i++) {
        size_t idx = hash_bit_modulo(rel[i].key, mask, ignore_bits);
        my_hist[idx] ++;
    }

    /* Compute local prefix sum on hist */
    size_t sum = 0;
    for (size_t i = 0; i < fanout; i++) {
        sum += my_hist[i];
        my_hist[i] = sum;
    }

    /* Wait until other parallel threads compute histogram + prefix sum */
    barrier_arrive(part->targs->barrier);

    /* Determine the start and end of each cluster */
    for (size_t i = 0; i < my_tid; i++) {
        for (size_t j = 0; j < fanout; j++) {
            out[j] += hist[i][j];
        }
    }
    for (size_t i = my_tid; i < n_threads; i++) {
        for (size_t j = 1; j < fanout; j++) {
            out[j] += hist[i][j - 1];
        }
    }
    for (size_t i = 0; i < fanout; i++) {
        out[i] += i * PADDING_TUPLES;
        dst[i] = out[i];
    }
    out[fanout] = part->total_tuples + fanout * PADDING_TUPLES;

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
 * @param task [in, out] description of the relations to be partitioned
 * @param queue [in, out] task_queue to add join tasks to after partitioning
 * @param shift_bits [in] Number of lower bits to skip before extracting radix
 *                        bits.
 * @param radix_bits [in] Number of bits to extract to form the partition index.
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
    if (hist_r == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }
    uint32_t *hist_s = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    if (hist_s == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    radix_partition(&task->rel_r, &task->tmp_r, hist_r, shift_bits, radix_bits,
                    P_TUPLES);
    radix_partition(&task->rel_s, &task->tmp_s, hist_s, shift_bits, radix_bits,
                    P_TUPLES);

    for(size_t i = 0; i < fanout; i++) {
        if (hist_r[i] > 0 && hist_s[i] > 0) {
            task_t *t = task_queue_get_slot_atomic(queue);

            t->rel_r.n_tuples = hist_r[i];
            t->rel_r.tuples = task->tmp_r.tuples + offset_r + i * P_TUPLES;
            t->tmp_r.tuples = task->rel_r.tuples + offset_r + i * P_TUPLES;
            offset_r += hist_r[i];

            t->rel_s.n_tuples = hist_s[i];
            t->rel_s.tuples = task->tmp_s.tuples + offset_s + i * P_TUPLES;
            t->tmp_s.tuples = task->rel_s.tuples + offset_s + i * P_TUPLES;
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
 * Radix partitioning algorithm (originally described by Manegold et al.).
 * The algorithm mimics the 2-pass radix clustering algorithm from Kim et al.
 * The difference is that it does not compute prefix-sum, instead the sum
 * (offset in the code) is computed iteratively.
 *
 * @param in [in] Input relation
 * @param out [out] Output relation (result of the partitioning)
 * @param hist [out] Number of tuples in each partition
 * @param shift_bits [in] Number of lower bits to skip before extracting radix
 *                        bits of the tuple key.
 * @param radix_bits [in] Number of bits to extract to form the partition index
 *                        of the key.
 * @param radix_bits [in] Number of empty tuples to insert as padding between
 *                        relations.
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
 * This join builds the hashtable using the bucket chaining algorithm proposed
 * by Manegold et al. Relations R and S typically fit into L2 cache or at least
 * R and (|R| * sizeof(uint32_t)) fits into L2 cache.
 *
 * @param r [in] Relation R
 * @param s [in] Relation S
 *
 * @return Number of result tuples
 */
uint64_t
bucket_chaining_join(relation_t const * const r,
                     relation_t const * const s)
{
    uint64_t matches = 0;
    uint64_t n_buckets = next_pow2(r->n_tuples);
    uint64_t const mask = (n_buckets - 1) << (N_RADIX_BITS);

    uint32_t *next = (uint32_t *) malloc(r->n_tuples * sizeof(uint32_t));
    if (next == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    };
    uint32_t *bucket = (uint32_t *) calloc(n_buckets, sizeof(uint32_t));
    if (bucket == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    };

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
 * radix join, single threaded
 *
 * @param r [in] input relation R
 * @param s [in] input relation S
 *
 * @return number of result tuples
 */
uint64_t rj(relation_t *r, relation_t *s)
{
    uint64_t matches = 0;
    size_t fanout;

#if PERF
    timing_t perf;
#endif

    relation_t *out_r = (relation_t *) malloc(sizeof(relation_t));
    if (out_r == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    relation_t *out_s = (relation_t *) malloc(sizeof(relation_t));
    if (out_r == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    /* Allocate temporary space for partitioning */
    out_r->n_tuples = r->n_tuples;
    out_r->tuples = (tuple_t *) malloc(r->n_tuples * sizeof(tuple_t));
    if (out_r->tuples == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    out_s->n_tuples = s->n_tuples;
    out_s->tuples = (tuple_t *) malloc(s->n_tuples * sizeof(tuple_t));
    if (out_s->tuples == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    /* Allocate histogram space for counts */
    fanout = 1 << N_RADIX_BITS_PASS1;
    uint32_t *hist_r = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    if (hist_r == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }
    uint32_t *hist_s = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    if (hist_s == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

#if PERF
    perf.start = timestamp();
    start_timer(&(perf.part));
#endif

    /* Partition phase */
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
    if (hist_r == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }
    hist_s = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    if (hist_s == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    /* 2nd pass */
    radix_partition(out_r, r, hist_r, N_RADIX_BITS_PASS1, N_RADIX_BITS_PASS2,
                    0);
    radix_partition(out_s, s, hist_s, N_RADIX_BITS_PASS1, N_RADIX_BITS_PASS2,
                    0);

    free(hist_r);
    free(hist_s);

    fanout = 1 << N_RADIX_BITS;
    hist_r = (uint32_t *) calloc(fanout, sizeof(uint32_t));
    if (hist_r == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }
    hist_s = (uint32_t *) calloc(fanout, sizeof(uint32_t));
    if (hist_s == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

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

#if PERF
    stop_timer(&(perf.part));
    start_timer(&(perf.build_probe));
#endif

    /* Join phase */
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
    stop_timer(&(perf.build_probe));
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

