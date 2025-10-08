#define _GNU_SOURCE

#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <x86intrin.h>

#include "join.h"
#include "config.h"
#include "task_queue.h"
#include "cxl.h"

#if PERF
#include "timer.h"
#endif

/* Worker thread arguments */
struct arg {
    alignas(CACHELINE_SIZE)
    relation_t r;
    uint64_t *hist_r;
    uint64_t *offset_r;
    size_t offset_stride;
    tuple_t *tmp_r;
    size_t tmp_stride;
    size_t r_total_tuples;
    relation_t s;
    uint64_t *hist_s;
    uint64_t *offset_s;
    tuple_t *tmp_s;
    size_t s_total_tuples;
    task_queue_t *join_queue;
    task_queue_t *part_queue;
    pthread_barrier_t *barrier;
    uint64_t matches;
    size_t my_tid;
    size_t n_threads;
    size_t my_nid;
    size_t n_nodes;
#if PERF
    timing_t timing;
#endif
};
typedef struct arg arg_t;

/* "Multi-threaded partition phase 1st pass per relation" arguments */
struct part {
    alignas(CACHELINE_SIZE)
    relation_t rel;
    uint64_t *hist;
    uint64_t *offset;
    size_t offset_stride;
    tuple_t *tmp;
    size_t tmp_stride;
    size_t total_tuples;
    size_t my_tid;
    size_t n_threads;
    size_t my_nid;
    pthread_barrier_t *barrier;
};
typedef struct part part_t;

/* join */
uint64_t single_threaded(relation_t *r, relation_t *s);
uint64_t multi_threaded(relation_t *r, relation_t *s, size_t n_threads);
uint64_t distributed(relation_t *r, relation_t *s, param_t *params);
void *drj_thread(void *args);
void parallel_radix_partition(part_t * const part);
void serial_radix_partition(task_t * const task, task_queue_t *queue,
                            uint64_t const shift_bits,
                            uint64_t const radix_bits);
void radix_partition(slice_list_t * restrict in, tuple_t * restrict out,
                     uint32_t * restrict hist, uint64_t const shift_bits,
                     uint64_t const radix_bits, uint64_t const padding_tuples);
uint64_t bucket_chaining_join(task_t *join_task);


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
hash(uint64_t k, uint64_t mask, uint64_t nbits) {
    return (k & mask) >> nbits;
}

static inline void local_barrier(pthread_barrier_t *barrier) {
    int err = pthread_barrier_wait(barrier);
    BUG_ON(err != 0 && err != PTHREAD_BARRIER_SERIAL_THREAD);
}

static inline void global_barrier(size_t my_tid, pthread_barrier_t *barrier) {
    if (my_tid == COORDINATION_THREAD) {
        cxl_barrier();
    }
    local_barrier(barrier);
}

/* impl */
uint64_t join_relations(relation_t *r, relation_t *s, param_t *params) {
    uint64_t result;
    result = distributed(r, s, params);
    return result;
}

uint64_t distributed(relation_t *r, relation_t *s, param_t *params) {
#if DEBUG
    printf("Distributed mode\n");
#endif
    uint64_t matches = 0;

    /* Threads */
    pthread_t threads[params->n_threads];
    pthread_attr_t attr;
    pthread_barrier_t barrier;
    cpu_set_t set;
    arg_t args[params->n_threads];

    /* Histograms */
    uint64_t *hist_r;
    uint64_t *hist_s;
    uint64_t *offset_r;
    uint64_t *offset_s;
    size_t offset_stride;

    /* Temporary space for partitioning */
    tuple_t *tmp_r;
    tuple_t *tmp_s;
    // TODO: here we assume R and S are same size
    size_t tmp_stride;

    /* Queues for partition and join tasks */
    task_queue_t *part_queue;
    task_queue_t *join_queue;

    /* Init */
    part_queue = task_queue_init(FANOUT_PASS1);
    BUG_ON(!part_queue);
    join_queue = task_queue_init(1 << N_RADIX_BITS);
    BUG_ON(!join_queue);
    // TODO: use whatever is bigger from above
    slice_allocator_init(FANOUT_PASS1 * params->n_nodes + (1 << N_RADIX_BITS));

    size_t bytes;
    bytes = FANOUT_PASS1 * sizeof(uint64_t) * params->n_threads;
    hist_r = aligned_alloc(CACHELINE_SIZE, bytes);
    BUG_ON(!hist_r);
    memset(hist_r, 0, bytes);
    hist_s = aligned_alloc(CACHELINE_SIZE, bytes);
    BUG_ON(!hist_s);
    memset(hist_s, 0, bytes);

    // TODO: here we assume perfectly symmetrical machines & workloads
    bytes = round_up((FANOUT_PASS1 + 1) * sizeof(uint64_t), CACHELINE_SIZE);
    offset_stride = bytes / sizeof(uint64_t);
    bytes *= params->n_nodes;
    offset_r = cxl_alloc(bytes);
    BUG_ON(!offset_r);
    offset_s = cxl_alloc(bytes);
    BUG_ON(!offset_s);

    // TODO: here we assume relation splits equally among nodes
    size_t slice;
    slice = params->r_size / params->n_nodes;
    bytes = round_up(slice * sizeof(tuple_t) + RELATION_PADDING, CACHELINE_SIZE);
    tmp_stride = bytes / sizeof(tuple_t);
    bytes *= params->n_nodes;
    tmp_r = cxl_alloc(bytes);
    BUG_ON(!tmp_r);
    slice = params->s_size / params->n_nodes;
    bytes = (slice * sizeof(tuple_t) + RELATION_PADDING) * params->n_nodes;
    tmp_s = cxl_alloc(bytes);
    BUG_ON(!tmp_s);

    int err = pthread_barrier_init(&barrier, NULL, params->n_threads);
    BUG_ON(err);
    pthread_attr_init(&attr);

    /* Assign slices of R & S for each thread */
    size_t r_slice = r->n_tuples / params->n_threads;
    size_t s_slice = s->n_tuples / params->n_threads;

    for (size_t i = 0; i < params->n_threads; i++) {
        bool last = (i == params->n_threads);

        args[i].r.tuples = r->tuples + i * r_slice;
        args[i].r.n_tuples = last ? (r->n_tuples - i * r_slice) : r_slice;
        args[i].hist_r = hist_r;
        args[i].offset_r = offset_r;
        args[i].tmp_r = tmp_r;
        args[i].r_total_tuples = r->n_tuples;

        args[i].s.tuples = s->tuples + i * s_slice;
        args[i].s.n_tuples = last ? (s->n_tuples - i * s_slice) : s_slice;
        args[i].hist_s = hist_s;
        args[i].offset_s = offset_s;
        args[i].tmp_s = tmp_s;
        args[i].s_total_tuples = s->n_tuples;

        args[i].part_queue = part_queue;
        args[i].join_queue = join_queue;

        args[i].offset_stride = offset_stride;
        args[i].tmp_stride = tmp_stride;
        args[i].barrier = &barrier;
        args[i].my_tid = i;
        args[i].n_threads = params->n_threads;
        args[i].my_nid = params->my_nid;
        args[i].n_nodes = params->n_nodes;

        int cpu = CPU_MAPPING[i];
#if DEBUG
        printf("Assigning thread %ld to CPU %d\n", i, cpu);
#endif
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);

        int err;
        err = pthread_create(&threads[i], &attr, drj_thread, (void *)&args[i]);
        BUG_ON(err);
    }

    /* Wait for threads to finish */
    for (size_t i = 0; i < params->n_threads; i++) {
        pthread_join(threads[i], NULL);
        matches += args[i].matches;
    }

#if PERF
    print_timing(matches, &args[0].timing);
#endif

    /* Clean-up */
    task_queue_free(part_queue);
    task_queue_free(join_queue);
    slice_allocator_free();
    free(hist_r);
    free(hist_s);
    cxl_free(offset_r);
    cxl_free(offset_s);
    cxl_free(tmp_r);
    cxl_free(tmp_s);

    return matches;
}

void *drj_thread(void *arg) {
    arg_t *args = (arg_t *) arg;
    bool const is_coordinator_thread = (args->my_tid == COORDINATION_THREAD);

    task_queue_t *part_queue = args->part_queue;
    task_queue_t *join_queue = args->join_queue;

    global_barrier(args->my_tid, args->barrier);

#if PERF
    args->timing.start = timestamp();
#endif

    part_t part;

    part.offset_stride = args->offset_stride;
    part.tmp_stride    = args->tmp_stride;
    part.my_tid     = args->my_tid;
    part.n_threads  = args->n_threads;
    part.my_nid     = args->my_nid;
    part.barrier    = args->barrier;

    /* Partition R */
    part.rel          = args->r;
    part.hist         = args->hist_r;
    part.offset       = args->offset_r;
    part.tmp          = args->tmp_r;
    part.total_tuples = args->r_total_tuples;

    parallel_radix_partition(&part);

    /* Partition S */
    part.rel          = args->s;
    part.hist         = args->hist_s;
    part.offset       = args->offset_s;
    part.tmp          = args->tmp_s;
    part.total_tuples = args->s_total_tuples;

    parallel_radix_partition(&part);

    global_barrier(args->my_tid, args->barrier);
#if PERF
    args->timing.part_distr = timestamp();
#endif

    // partition assignment
    if (is_coordinator_thread) {
        bool last_node = ((args->my_nid + 1) == args->n_nodes);
        size_t slice = FANOUT_PASS1 / args->n_nodes;
        size_t start = args->my_nid * slice;
        size_t end = last_node ? FANOUT_PASS1 : (args->my_nid + 1) * slice;

        for (size_t i = start; i < end; i++) {
            size_t r_count = 0;
            size_t s_count = 0;

            for (size_t j = 0; j < args->n_nodes; j++) {
                uint64_t *offset;
                offset = &args->offset_r[j * args->offset_stride];
                r_count += offset[i + 1] - offset[i] - PADDING_TUPLES;
                offset = &args->offset_s[j * args->offset_stride];
                s_count += offset[i + 1] - offset[i] - PADDING_TUPLES;
            }


            if (r_count == 0 || s_count == 0) {
                continue;
            }

            task_t *task = task_queue_get_slot(part_queue);
            //BUG_ON(!task);

            for (size_t j = 0; j < args->n_nodes; j++) {
                slice_t *slice;
                uint64_t *offset;
                tuple_t *tmp;

                slice = slice_alloc();
                //BUG_ON(!slice);
                offset = &args->offset_r[j * args->offset_stride];
                tmp = &args->tmp_r[j * args->tmp_stride];
                slice->tuples = &tmp[offset[i]];
                slice->n_tuples = offset[i + 1] - offset[i] - PADDING_TUPLES;
                slice_list_add(&task->slices_r, slice);

                slice = slice_alloc();
                //BUG_ON(!slice);
                offset = &args->offset_s[j * args->offset_stride];
                tmp = &args->tmp_s[j * args->tmp_stride];
                slice->tuples = &tmp[offset[i]];
                slice->n_tuples = offset[i + 1] - offset[i] - PADDING_TUPLES;
                slice_list_add(&task->slices_s, slice);
            }

            task->r_total_tuples = r_count;
            task->s_total_tuples = s_count;
            task_queue_add(part_queue, task);
        }
    }

    local_barrier(args->barrier);
#if PERF
    args->timing.part_assign = timestamp();
#endif

#if N_PASSES==1
    task_queue_t *swap = join_queue;
    join_queue = part_queue;
    part_queue = swap;
#elif N_PASSES==2
    /* 2nd pass */
#if DEBUG
    if (is_coordinator_thread) {
        printf("2nd pass: #tasks = %ld\n", part_queue->size);
    }
    local_barrier(args->barrier);
#endif
    task_t *part_task;
    while ((part_task = task_queue_get_atomic(part_queue))) {
        uint64_t shift_bits = N_RADIX_BITS_PASS1;
        uint64_t radix_bits = N_RADIX_BITS_PASS2;
        serial_radix_partition(part_task, join_queue, shift_bits, radix_bits);
    }

    /* Wait until parallel threads add all join tasks */
    local_barrier(args->barrier);
#endif

#if PERF
    args->timing.part_local = timestamp();
#endif

    /* Buildprobe phase */
#if DEBUG
    if (is_coordinator_thread) {
        printf("Buildprobe: #tasks = %ld\n", join_queue->size);
    }
    local_barrier(args->barrier);
#endif

    uint64_t matches = 0;
    task_t *join_task;
    while ((join_task = task_queue_get_atomic(join_queue))) {
        matches += bucket_chaining_join(join_task);
    }
    args->matches = matches;

#if PERF
    local_barrier(args->barrier);
    args->timing.build_probe = timestamp();
    global_barrier(args->my_tid, args->barrier);
    args->timing.end = timestamp();
#endif

    /* Clean-up */
    return NULL;
}

void parallel_radix_partition(part_t * const part) {
    tuple_t const * restrict rel = part->rel.tuples;
    uint64_t      * hist         = part->hist;
    tuple_t       * restrict tmp = part->tmp;

    size_t const n_tuples     = part->rel.n_tuples;
    size_t const total_tuples = part->total_tuples;
    size_t const my_tid       = part->my_tid;
    size_t const n_threads    = part->n_threads;
    size_t const my_nid       = part->my_nid;
    size_t const offset_stride = part->offset_stride;
    size_t const tmp_stride = part->tmp_stride;

    bool const is_coordinator_thread = (my_tid == COORDINATION_THREAD);

    uint64_t const ignore_bits = 0;
    uint64_t const radix_bits  = N_RADIX_BITS_PASS1;
    size_t   const fanout      = 1 << radix_bits;
    uint64_t const mask        = (fanout - 1) << ignore_bits;

    /* offset: cluster i starts at offset[i] and ends at offset[i+1]-1 */
    /* dst: current write-out position within cluster i stored in dst[i] */
    uint64_t * restrict offset = part->offset;
    //uint64_t dst[fanout];
    uint64_t * restrict dst = calloc(fanout, sizeof(uint64_t));
    BUG_ON(!dst);

    /* Compute local histogram for the assigned slice of rel */
    uint64_t *my_hist = &hist[my_tid * fanout];
    for (size_t i = 0; i < n_tuples; i++) {
        size_t idx = hash(rel[i].key, mask, ignore_bits);
        my_hist[idx]++;
    }

    /* Compute local prefix sum on hist */
    size_t sum = 0;
    for (size_t i = 0; i < fanout; i++) {
        sum += my_hist[i];
        my_hist[i] = sum;
    }

    /* Wait until other parallel threads compute histogram + prefix sum */
    local_barrier(part->barrier);

    /* Determine the start and end of each cluster */
    for (size_t i = 0; i < my_tid; i++) {
        for (size_t j = 0; j < fanout; j++) {
            dst[j] += hist[(i * fanout) + j];
        }
    }
    for (size_t i = my_tid; i < n_threads; i++) {
        for (size_t j = 1; j < fanout; j++) {
            dst[j] += hist[(i * fanout) + (j - 1)];
        }
    }
    for (size_t i = 0; i < fanout; i++) {
        dst[i] += i * PADDING_TUPLES;
    }

    if (is_coordinator_thread) {
        // TODO: try memcpy here?
        uint64_t *my_offset = &offset[my_nid * offset_stride];
        for (size_t i = 0; i < fanout; i++) {
            my_offset[i] = dst[i];
        }
        my_offset[fanout] = total_tuples + fanout * PADDING_TUPLES;

        /* Flush */
        uintptr_t ptr = (uintptr_t) my_offset;
        size_t size = offset_stride * sizeof(uint64_t);
        for (uintptr_t p = ptr; p < ptr + size; p += CACHELINE_SIZE) {
            _mm_clwb((void *)p);
        }
        //_mm_sfence();

    }

    /* Copy tuples to their corresponding clusters */
    // TODO: we have the segment approach
    //       therefore, tuples for my machine do not need to be written to CXL memory
    tuple_t *my_tmp = &tmp[my_nid * tmp_stride];
    for(size_t i = 0; i < n_tuples; i++) {
        size_t idx = hash(rel[i].key, mask, ignore_bits);
        my_tmp[dst[idx]] = rel[i];
        dst[idx]++;
    }

    local_barrier(part->barrier);

    /* Flush */
    // TODO: clwb can be parallelized
    //       each thread writes back it's slice of this node's tmp buffer
    //       (via n_tuples/total_tuples and offset into my_tmp)
    if (is_coordinator_thread) {
        uintptr_t ptr = (uintptr_t) my_tmp;
        size_t size = tmp_stride * sizeof(tuple_t);
        for (uintptr_t p = ptr; p < ptr + size; p += CACHELINE_SIZE) {
            _mm_clwb((void *)p);
        }
        _mm_sfence();
    }

    /* Clean-up */
    free(dst);
}

void
serial_radix_partition(task_t * const task,
                       task_queue_t *queue,
                       uint64_t const shift_bits,
                       uint64_t const radix_bits)
{
    size_t offset_r = 0;
    size_t offset_s = 0;
    size_t const fanout = 1 << radix_bits;

    // TODO: remove memory allocation from critical path
    tuple_t *tmp_r = (tuple_t *) malloc(task->r_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
    //BUG_ON(!tmp_r);
    tuple_t *tmp_s = (tuple_t *) malloc(task->s_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
    //BUG_ON(!tmp_s);
    uint32_t *hist_r = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    //BUG_ON(!hist_r);
    uint32_t *hist_s = (uint32_t *) calloc(fanout + 1, sizeof(uint32_t));
    //BUG_ON(!hist_s);

    radix_partition(&task->slices_r, tmp_r, hist_r, shift_bits, radix_bits,
                    P_TUPLES);
    radix_partition(&task->slices_s, tmp_s, hist_s, shift_bits, radix_bits,
                    P_TUPLES);

    for(size_t i = 0; i < fanout; i++) {
        if (hist_r[i] > 0 && hist_s[i] > 0) {
            task_t *t = task_queue_get_slot_atomic(queue);
            slice_t *s = slice_alloc_atomic();

            t->slices_r.head = s;
            t->slices_r.head->n_tuples = hist_r[i];
            t->slices_r.head->tuples = tmp_r + offset_r + i * P_TUPLES;
            t->r_total_tuples = hist_r[i];
            offset_r += hist_r[i];

            s = slice_alloc_atomic();

            t->slices_s.head = s;
            t->slices_s.head->n_tuples = hist_s[i];
            t->slices_s.head->tuples = tmp_s + offset_s + i * P_TUPLES;
            t->s_total_tuples = hist_s[i];
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
radix_partition(slice_list_t * restrict in,
                tuple_t * restrict out,
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
    slice_t *slice = in->head;
    while (slice) {
        for (size_t i = 0; i < slice->n_tuples; i++) {
            size_t idx = hash(slice->tuples[i].key, mask, shift_bits);
            hist[idx]++;
        }
        slice = slice->next;
    }
    /* Determine the start and end of each partition depending on the counts */
    for (size_t i = 0; i < fanout; i++) {
        dst[i] = offset + i * padding_tuples;
        offset += hist[i];
    }

    /* Copy tuples to their corresponding partitions at appropriate offsets */
    slice = in->head;
    while (slice) {
        for(size_t i = 0; i < slice->n_tuples; i++) {
            size_t idx = hash(slice->tuples[i].key, mask, shift_bits);
            out[ dst[idx] ] = slice->tuples[i];
            ++dst[idx];
        }
        slice = slice->next;
    }
}

uint64_t
bucket_chaining_join(task_t *task)
{
    // TODO: we can optimize here if slice list only has one slice!
    uint64_t matches = 0;

    size_t n_tuples = task->r_total_tuples;
    uint64_t n_buckets = next_pow2(n_tuples);
    uint64_t const mask = (n_buckets - 1) << (N_RADIX_BITS);

    uint64_t *key = (uint64_t *)malloc(n_tuples * sizeof(uint64_t));
    //BUG_ON(!key);
    uint32_t *next = (uint32_t *)malloc(n_tuples * sizeof(uint32_t));
    //BUG_ON(!next);
    uint32_t *bucket = (uint32_t *)calloc(n_buckets, sizeof(uint32_t));
    //BUG_ON(!bucket);

    /* Build loop */
    slice_t *slice = task->slices_r.head;
    size_t eidx = 0;
    while (slice) {
        for (size_t i = 0; i < slice->n_tuples; i++) {
            size_t idx = hash(slice->tuples[i].key, mask, N_RADIX_BITS);
            key[eidx] = slice->tuples[i].key;
            next[eidx] = bucket[idx];
            /* Start positions from 1, 0 marks empty bucket */
            bucket[idx] = ++eidx;
        }
        slice = slice->next;
    }

    /* Probe loop */
    slice = task->slices_s.head;
    while (slice) {
        for (size_t i = 0; i < slice->n_tuples; i++) {
            size_t idx = hash(slice->tuples[i].key, mask, N_RADIX_BITS);
            for (int hit = bucket[idx]; hit > 0; hit = next[hit - 1]) {
                if (slice->tuples[i].key == key[hit - 1]) {
                    matches++;
                }
            }
        }
        slice = slice->next;
    }

    /* Clean-up */
    free(key);
    free(next);
    free(bucket);

    return matches;
}

