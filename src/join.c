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
#include "mem.h"
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
    uint64_t *thread_hist;
    uint64_t *node_hist;
    uint64_t *global_hist;
    uint64_t *local_offs;
    uint64_t *remote_offs;
    size_t offset_stride;
    tuple_t *local_tmp;
    tuple_t *remote_tmp;
    size_t tmp_stride;
    size_t total_tuples;
    size_t my_tid;
    size_t n_threads;
    size_t my_nid;
    size_t n_nodes;
    pthread_barrier_t *barrier;
};
typedef struct part part_t;

/* Software write-combining buffer per partition (L1 cache optimized) */
struct wc_buffer {
    uint8_t buf[128];        // Safely fits up to 78 bytes + 16-byte SIMD shift margin
    size_t bytes_used;
    size_t cxl_byte_offset;  // Absolute byte offset in the CXL remote memory
};

typedef struct wc_buffer wc_buffer_t;

/* Join */
uint64_t distributed(relation_t *r, relation_t *s, param_t *params);
void *drj_thread(void *args);
void parallel_radix_partition(part_t * const part);
void serial_radix_partition(task_t * const task, task_queue_t *queue,
                            uint64_t const shift_bits,
                            uint64_t const radix_bits, size_t tid);
void radix_partition(slice_list_t * restrict in, tuple_t * restrict out,
                     uint64_t * restrict hist, uint64_t const shift_bits,
                     uint64_t const radix_bits, size_t tid);
uint64_t bucket_chaining_join(task_t *join_task, size_t tid);

/* Helper */
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

static inline uint64_t hash(uint64_t k, uint64_t mask, uint64_t nbits) {
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

// TODO: this assumes uniform relations
static inline size_t node_part_assign(size_t p) {
    return p % 2;
}

/* Impl */
uint64_t join_relations(relation_t *r, relation_t *s, param_t *params) {
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
    hist_r = mem_p1_thread_hist_r();
    hist_s = mem_p1_thread_hist_s();

    // TODO: here we assume perfectly symmetrical machines & workloads
    bytes = round_up((FANOUT_PASS1 + 1) * sizeof(uint64_t), CACHELINE_SIZE);
    offset_stride = bytes / sizeof(uint64_t);
    offset_r = cxl_p1_remote_offs_r();
    offset_s = cxl_p1_remote_offs_s();

    // TODO: here we assume uniform relations
    size_t slice;
    slice = params->r_size / params->n_nodes;
    bytes = round_up(slice * sizeof(tuple_t) + RELATION_PADDING, CACHELINE_SIZE);
    tmp_stride = bytes / sizeof(tuple_t);
    tmp_r = cxl_p1_remote_tmp_r();
    tmp_s = cxl_p1_remote_tmp_s();

    int err = pthread_barrier_init(&barrier, NULL, params->n_threads);
    BUG_ON(err);
    pthread_attr_init(&attr);

    /* Assign slices of R & S for each thread */
    size_t r_slice = r->n_tuples / params->n_threads;
    size_t s_slice = s->n_tuples / params->n_threads;

    for (size_t i = 0; i < params->n_threads; i++) {
        bool last = (i == params->n_threads - 1);

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
    part.n_nodes    = args->n_nodes;
    part.barrier    = args->barrier;

    /* Partition R */
    part.rel          = args->r;
    part.thread_hist         = mem_p1_thread_hist_r();
    //part.node_hist         = cxl_p1_node_hist_r();
    //part.global_hist         = cxl_p1_global_hist_r();
    part.local_offs       = mem_p1_local_offs_r();
    part.remote_offs       = cxl_p1_remote_offs_r();
    part.local_tmp          = mem_p1_local_tmp_r();
    part.remote_tmp          = cxl_p1_remote_tmp_r();
    part.total_tuples = args->r_total_tuples;

    parallel_radix_partition(&part);

    /* Partition S */
    part.rel          = args->s;
    part.thread_hist         = mem_p1_thread_hist_s();
    //part.node_hist         = cxl_p1_node_hist_s();
    //part.global_hist         = cxl_p1_global_hist_s();
    part.local_offs       = mem_p1_local_offs_s();
    part.remote_offs       = cxl_p1_remote_offs_s();
    part.local_tmp          = mem_p1_local_tmp_s();
    part.remote_tmp          = cxl_p1_remote_tmp_s();
    part.total_tuples = args->s_total_tuples;

    parallel_radix_partition(&part);

    global_barrier(args->my_tid, args->barrier);
#if PERF
    args->timing.part_distr = timestamp();
#endif

    // partition assignment
    if (is_coordinator_thread) {
        size_t idx = 0;
        for (size_t p = 0; p < FANOUT_PASS1; p++) {
            if (node_part_assign(p) != args->my_nid) {
                continue;
            }

            uint64_t *local_offset_r = mem_p1_local_offs_r();
            uint64_t *remote_offset_r = &cxl_p1_remote_offs_r()[((args->my_nid + 1) % 2) * args->offset_stride];
            uint64_t *local_offset_s = mem_p1_local_offs_s();
            uint64_t *remote_offset_s = &cxl_p1_remote_offs_s()[((args->my_nid + 1) % 2) * args->offset_stride];

            size_t r_local, r_remote, s_local, s_remote;

            r_local = local_offset_r[idx + 1] - local_offset_r[idx] - PADDING_TUPLES;
            s_local = local_offset_s[idx + 1] - local_offset_s[idx] - PADDING_TUPLES;

            // Remote partitions: Offsets are tightly packed BYTES, no padding!
            r_remote = (remote_offset_r[idx + 1] - remote_offset_r[idx]) / COMPRESSED_TUPLE_SIZE;
            s_remote = (remote_offset_s[idx + 1] - remote_offset_s[idx]) / COMPRESSED_TUPLE_SIZE;

            size_t r_count = r_local + r_remote;
            size_t s_count = s_local + s_remote;

            //printf("p-%03ld: r = %03ld + %03ld,\ts = %03ld + %03ld\n", p, r_local, r_remote, s_local, s_remote);

            if (r_count == 0 || s_count == 0) {
                idx++;
                continue;
            }

            task_t *task = task_queue_get_slot(part_queue);
            BUG_ON(!task);

            slice_t *slice;
            uint64_t *offset;
            tuple_t *tmp;
            uint8_t *remote_bytes;

            // TODO: if one of the slices is empty we still add it anyway.
            //       this can be improved
            slice = slice_alloc();
            BUG_ON(!slice);
            offset = local_offset_r;
            tmp = mem_p1_local_tmp_r();
            slice->tuples = &tmp[offset[idx]];
            slice->n_tuples = offset[idx + 1] - offset[idx] - PADDING_TUPLES;
            slice_list_add(&task->slices_r, slice);

            slice = slice_alloc();
            BUG_ON(!slice);
            offset = local_offset_s;
            tmp = mem_p1_local_tmp_s();
            slice->tuples = &tmp[offset[idx]];
            slice->n_tuples = offset[idx + 1] - offset[idx] - PADDING_TUPLES;
            slice_list_add(&task->slices_s, slice);

            slice = slice_alloc();
            BUG_ON(!slice);
            offset = remote_offset_r;
            tmp = &cxl_p1_remote_tmp_r()[((args->my_nid + 1) % 2) * args->tmp_stride];
            // We MUST cast the base pointer to uint8_t* before applying the offset!
            // Otherwise C scales it by 16 bytes per index.
            remote_bytes = (uint8_t *)tmp;
            // Cast it back to tuple_t* so it fits in the struct (it will be decompressed later)
            slice->tuples = (tuple_t *)&remote_bytes[offset[idx]];
            slice->n_tuples = r_remote;
            slice_list_add(&task->slices_r, slice);

            slice = slice_alloc();
            BUG_ON(!slice);
            offset = remote_offset_s;
            tmp = &cxl_p1_remote_tmp_s()[((args->my_nid + 1) % 2) * args->tmp_stride];
            remote_bytes = (uint8_t *)tmp;
            slice->tuples = (tuple_t *)&remote_bytes[offset[idx]];
            slice->n_tuples = s_remote;
            slice_list_add(&task->slices_s, slice);

            task->r_total_tuples = r_count;
            task->s_total_tuples = s_count;
            task_queue_add(part_queue, task);

            idx++;
        }

        //bool last_node = ((args->my_nid + 1) == args->n_nodes);
        //size_t slice = FANOUT_PASS1 / args->n_nodes;
        //size_t start = args->my_nid * slice;
        //size_t end = last_node ? FANOUT_PASS1 : (args->my_nid + 1) * slice;

        //for (size_t i = start; i < end; i++) {
        //    size_t r_count = 0;
        //    size_t s_count = 0;

        //    for (size_t j = 0; j < args->n_nodes; j++) {
        //        uint64_t *offset;
        //        offset = &args->offset_r[j * args->offset_stride];
        //        r_count += offset[i + 1] - offset[i] - PADDING_TUPLES;
        //        offset = &args->offset_s[j * args->offset_stride];
        //        s_count += offset[i + 1] - offset[i] - PADDING_TUPLES;
        //    }


        //    if (r_count == 0 || s_count == 0) {
        //        continue;
        //    }

        //    task_t *task = task_queue_get_slot(part_queue);
        //    //BUG_ON(!task);

        //    for (size_t j = 0; j < args->n_nodes; j++) {
        //        slice_t *slice;
        //        uint64_t *offset;
        //        tuple_t *tmp;

        //        slice = slice_alloc();
        //        //BUG_ON(!slice);
        //        offset = &args->offset_r[j * args->offset_stride];
        //        tmp = &args->tmp_r[j * args->tmp_stride];
        //        slice->tuples = &tmp[offset[i]];
        //        slice->n_tuples = offset[i + 1] - offset[i] - PADDING_TUPLES;
        //        slice_list_add(&task->slices_r, slice);

        //        slice = slice_alloc();
        //        //BUG_ON(!slice);
        //        offset = &args->offset_s[j * args->offset_stride];
        //        tmp = &args->tmp_s[j * args->tmp_stride];
        //        slice->tuples = &tmp[offset[i]];
        //        slice->n_tuples = offset[i + 1] - offset[i] - PADDING_TUPLES;
        //        slice_list_add(&task->slices_s, slice);
        //    }

        //    task->r_total_tuples = r_count;
        //    task->s_total_tuples = s_count;
        //    task_queue_add(part_queue, task);
        //}
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
        uint64_t shift = N_RADIX_BITS_PASS1;
        uint64_t radix = N_RADIX_BITS_PASS2;
        size_t tid = args->my_tid;
        serial_radix_partition(part_task, join_queue, shift, radix, tid);
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
    void *prealloc = mem_for(args->my_tid, 10 * L1_CACHE_SIZE);
    (void)prealloc;
    while ((join_task = task_queue_get_atomic(join_queue))) {
        matches += bucket_chaining_join(join_task, args->my_tid);
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
    uint64_t * thread_hist = part->thread_hist;
    //uint64_t      * node_hist = part->node_hist;
    //uint64_t      * restrict global_hist = part->global_hist;
    tuple_t * local_tmp = part->local_tmp;
    tuple_t * remote_tmp = part->remote_tmp;

    size_t const n_tuples      = part->rel.n_tuples;
    //size_t const total_tuples  = part->total_tuples;
    size_t const my_tid        = part->my_tid;
    size_t const n_threads     = part->n_threads;
    size_t const my_nid        = part->my_nid;
    //size_t const n_nodes       = part->n_nodes;
    size_t const offset_stride = part->offset_stride;
    size_t const tmp_stride    = part->tmp_stride;

    bool const is_coordinator_thread = (my_tid == COORDINATION_THREAD);
    //bool const is_coordinator_node   = (my_tid == COORDINATION_NODE);

    uint64_t const ignore_bits = 0;
    uint64_t const radix_bits  = N_RADIX_BITS_PASS1;
    size_t   const fanout      = 1 << radix_bits;
    uint64_t const mask        = (fanout - 1) << ignore_bits;

    /* offset: cluster i starts at offset[i] and ends at offset[i+1]-1 */
    /* dst: current write-out position within cluster i stored in dst[i] */
    /* local stays on this node, remote goes to CXL memory */
    uint64_t * local_offs = part->local_offs;
    uint64_t * remote_offs = part->remote_offs;
    //uint64_t * remote_offs = part->remote_offs;
    //uint64_t * local_dst = mem_for(my_tid, fanout * sizeof(uint64_t));
    //uint64_t * remote_dst = mem_for(my_tid, fanout * sizeof(uint64_t));

    /* Compute thread histogram */
    size_t stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));
    uint64_t *my_thread_hist = &thread_hist[my_tid * stride];
    for (size_t t = 0; t < n_tuples; t++) {
        size_t p = hash(rel[t].key, mask, ignore_bits);
        my_thread_hist[p]++;
    }

    local_barrier(part->barrier);

    //if (is_coordinator_thread) {
    //    for (size_t t = 0; t < n_threads; t++) {
    //        uint64_t *t_hist = &thread_hist[t * stride];
    //        uint64_t *my_node_hist = &node_hist[my_nid * stride];
    //        for (size_t p = 0; p < fanout; p++) {
    //            my_node_hist[p] += t_hist[p];
    //        }
    //    }

    //    uint64_t local_sum;
    //    uint64_t remote_sum;
    //    size_t local_idx;
    //    size_t remote_idx;
    //    for (size_t p = 0; p < fanout; p++) {
    //        if (node_part_assign[p] == my_nid) {
    //            dst[p] =
    //        }
    //    }
    //}

    /* Compute node historgram */
    // TODO: optimize with vector instr?
    uint64_t *node_hist = mem_for(my_tid, fanout * sizeof(uint64_t));
    for (size_t t = 0; t < n_threads; t++) {
        uint64_t *hist = &thread_hist[t * stride];
        for (size_t p = 0; p < fanout; p++) {
            node_hist[p] += hist[p];
        }
    }

    // TODO: node part assignment must happen here!

    uint64_t *dst = mem_for(my_tid, fanout * sizeof(uint64_t));
    uint64_t local_sum = 0;
    uint64_t remote_sum_bytes = 0; // Track remote in bytes!
    size_t local_idx = 0;
    for (size_t p = 0; p < fanout; p++) {
        if (node_part_assign(p) == my_nid) {
            dst[p] = local_sum + (local_idx * PADDING_TUPLES);
            local_sum += node_hist[p];
            local_idx++;
        }
        else {
            // Remote partitions are tightly packed byte-arrays (No padding logic here!)
            dst[p] = remote_sum_bytes;
            remote_sum_bytes += node_hist[p] * COMPRESSED_TUPLE_SIZE;
        }
    }

    for (size_t t = 0; t < my_tid; t++) {
        uint64_t *hist = &thread_hist[t * stride];
        for (size_t p = 0; p < fanout; p++) {
            if (node_part_assign(p) == my_nid) {
                dst[p] += hist[p]; // Local increments by tuples
            } else {
                dst[p] += hist[p] * COMPRESSED_TUPLE_SIZE; // Remote increments by bytes
            }
        }
    }

    //global_barrier(my_tid, part->barrier);

    ///* Compute global histogram */
    //// TODO: optimize with vector instr?
    //if (is_coordinator_node && is_coordinator_thread) {
    //    for (size_t i = 0; i < n_nodes; i++) {
    //        for (size_t j = 0; j < fanout; j++) {
    //            global_hist[j] += node_hist[(i * stride) + j];
    //        }
    //    }
    //}

    //global_barrier(my_tid, part->barrier);

    ///* Compute local prefix sum on thread hist */
    //size_t sum = 0;
    //for (size_t i = 0; i < fanout; i++) {
    //    sum += my_thread_hist[i];
    //    my_thread_hist[i] = sum;
    //}

    ///* Wait until other parallel threads compute thread hist + prefix sum */
    //local_barrier(part->barrier);

    ///* Determine the start and end of each cluster */
    //for (size_t t = 0; t < my_tid; t++) {
    //    for (size_t p = 0; p < fanout; p++) {
    //        //bool my_p = (node_part_assign(p) == my_nid);
    //        //uint64_t *dst = my_p ? local_dst : remote_dst;
    //        dst[p] += thread_hist[(t * stride) + p];
    //    }
    //}

    //for (size_t i = my_tid; i < n_threads; i++) {
    //    for (size_t j = 1; j < fanout; j++) {
    //        dst[j] += thread_hist[(i * stride) + (j - 1)];
    //    }
    //}

    //for (size_t i = 0; i < fanout; i++) {
    //    dst[i] += i * PADDING_TUPLES;
    //}

    // TODO: use non-temporal stores here?
    if (is_coordinator_thread) {
        /* Copy to global hist */

        // 64 bit version:
        uint64_t *my_remote_offs = &remote_offs[my_nid * offset_stride];
        size_t local_idx = 0;
        size_t remote_idx = 0;
        for (size_t p = 0; p < fanout; p++) {
            if (node_part_assign(p) == my_nid) {
                local_offs[local_idx] = dst[p];
                local_idx++;
            }
            else {
                my_remote_offs[remote_idx] = dst[p];
                remote_idx++;
            }
        }

        // 128 bit version:
        //uint64_t *my_remote_offs = &remote_offs[my_nid * offset_stride];
        //size_t local_idx = 0;
        //size_t remote_idx = 0;
        //bool have_pending = false;
        //size_t pending_val = 0;
        //for (size_t p = 0; p < fanout; p++) {
        //    size_t o = dst[p];
        //    if (node_part_assign(p) == my_nid) {
        //        //local_offs[local_idx] = dst[p];
        //        //local_idx++;
        //        local_offs[local_idx++] = o;
        //    }
        //    else {
        //        //my_remote_offs[remote_idx] = dst[p];
        //        //remote_idx++;
        //        if (!have_pending) {
        //            pending_val = o;
        //            have_pending = true;
        //        }
        //        else {
        //            __m128i vec = _mm_set_epi64x((long long)o, (long long)pending_val);
        //            _mm_stream_si128((__m128i*)&my_remote_offs[remote_idx], vec);
        //            remote_idx += 2;
        //            have_pending = false;
        //        }
        //    }
        //}

        //if (have_pending) {
        //    _mm_stream_si64((long long*)&my_remote_offs[remote_idx], (long long)pending_val);
        //    remote_idx++;
        //}

        // 512 bit version:
        //uint64_t *my_remote_offs = &remote_offs[my_nid * offset_stride];
        //size_t local_idx = 0;
        //size_t remote_idx = 0;
        //int32_t pending = 0;
        //uint64_t buf512[8];
        //for (size_t p = 0; p < fanout; p++) {
        //    size_t o = dst[p];
        //    if (node_part_assign(p) == my_nid) {
        //        //local_offs[local_idx] = dst[p];
        //        //local_idx++;
        //        local_offs[local_idx++] = o;
        //    }
        //    else {
        //        buf512[pending++] = o;
        //        if (pending == 8) {
        //            __m512i vec = _mm512_loadu_si512((const void*)buf512);
        //            _mm512_stream_si512((__m512i*)&my_remote_offs[remote_idx], vec);
        //            remote_idx += 8;
        //            pending = 0;
        //        }
        //    }
        //}

        //int rem = pending;
        //int off = 0;
        //while (rem >= 2) {
        //    __m128i v128 = _mm_set_epi64x((long long)buf512[off + 1],
        //                                  (long long)buf512[off + 0]);
        //    _mm_stream_si128((__m128i*)&my_remote_offs[remote_idx], v128);
        //    remote_idx += 2;
        //    off += 2;
        //    rem -= 2;
        //}
        //if (rem == 1) {
        //    _mm_stream_si64((long long*)&my_remote_offs[remote_idx], (long long)buf512[off]);
        //    remote_idx++;
        //}

        //=====

        //memcpy(my_offs, dst, fanout * sizeof(dst[0]));
        local_offs[local_idx] = local_sum + local_idx * PADDING_TUPLES;
        my_remote_offs[remote_idx] = remote_sum_bytes;

        /* Flush */
        uintptr_t ptr = (uintptr_t) my_remote_offs;
        size_t size = round_up(offset_stride * sizeof(uint64_t), CACHELINE_SIZE);
        for (uintptr_t p = ptr; p < ptr + size; p += CACHELINE_SIZE) {
            _mm_clwb((void *)p);
        }
        //_mm_sfence();

    }

    wc_buffer_t *wc_bufs = mem_for(my_tid, fanout * sizeof(wc_buffer_t));

    // Note: dst[p] contains TUPLE indices for local partitions, but BYTE offsets for remote!
    for (size_t p = 0; p < fanout; p++) {
        if (node_part_assign(p) != my_nid) {
            wc_bufs[p].cxl_byte_offset = dst[p];
        }
    }

    // TODO: use non-temporal stores here?
    // TODO: how to do correct bandwidth combining here? probably need to find correct local/remote tuple ratio for writeout!
    /* Write out tuples */
    tuple_t *my_remote_tmp = &remote_tmp[my_nid * tmp_stride];
    uint8_t *my_remote_tmp_bytes = (uint8_t *)my_remote_tmp;
    for(size_t i = 0; i < n_tuples; i++) {
        //size_t p = hash(rel[i].key, mask, ignore_bits);
        //if (node_part_assign(p) == my_nid) {
        //    local_tmp[dst[p]] = rel[i];
        //} else {
        //    my_remote_tmp[dst[p]] = rel[i];
        //}
        //dst[p]++;

        size_t p = hash(rel[i].key, mask, ignore_bits);

        if (node_part_assign(p) == my_nid) {
            size_t idx = dst[p]++;
            __m128i t = _mm_loadu_si128((const __m128i*)&rel[i]);
            // local write: store
            _mm_storeu_si128((__m128i*)&local_tmp[idx], t);
        } else {
            // remote write: compression, write-combining, non-temp store
            wc_buffer_t *wc = &wc_bufs[p];

            uint64_t key = ((uint64_t*)&rel[i])[0];
            uint64_t payload = ((uint64_t*)&rel[i])[1];

            // Cut out the 8 radix bits
            uint64_t lower_mask = (1ULL << ignore_bits) - 1;
            uint64_t compressed_key = (key & lower_mask) | ((key >> (ignore_bits + 8)) << ignore_bits);

            // SUPER FAST PACKING: Overlapping scalar stores (no memcpy, no complex SIMD)
            // byte 0-7 gets key. Then byte 7-14 gets payload, perfectly overwriting the unused 8th key byte!
            uint8_t *dest = wc->buf + wc->bytes_used;
            *(uint64_t*)dest = compressed_key;
            *(uint64_t*)(dest + 7) = payload;

            wc->bytes_used += COMPRESSED_TUPLE_SIZE;

            // If we've crossed the 64-byte threshold, flush exactly one cache line
            if (wc->bytes_used >= 64) {
                uint8_t *cxl_dest = my_remote_tmp_bytes + wc->cxl_byte_offset;

                // Stream exactly 64 bytes using a single AVX-512 instruction
                // cxl_dest is perfectly aligned by our initialization logic.
#if defined(__AVX512F__)
                __m512i vec = _mm512_loadu_si512((const __m512i *)&wc->buf[0]);
                _mm512_storeu_si512((__m512i *)&cxl_dest[0], vec);
#elif defined(__AVX2__)
                __m256i v0 = _mm256_loadu_si256((const __m256i *)&wc->buf[0]);
                __m256i v1 = _mm256_loadu_si256((const __m256i *)&wc->buf[32]);
                _mm256_storeu_si256((__m256i *)&cxl_dest[0], v0);
                _mm256_storeu_si256((__m256i *)&cxl_dest[32], v1);
#else
                __m128i v0 = _mm_loadu_si128((const __m128i *)&wc->buf[0]);
                __m128i v1 = _mm_loadu_si128((const __m128i *)&wc->buf[16]);
                __m128i v2 = _mm_loadu_si128((const __m128i *)&wc->buf[32]);
                __m128i v3 = _mm_loadu_si128((const __m128i *)&wc->buf[48]);
                _mm_storeu_si128((__m128i *)&cxl_dest[0], v0);
                _mm_storeu_si128((__m128i *)&cxl_dest[16], v1);
                _mm_storeu_si128((__m128i *)&cxl_dest[32], v2);
                _mm_storeu_si128((__m128i *)&cxl_dest[48], v3);
#endif

                // TODO: check if write-back helps here
                //// Force the unaligned write out to CXL memory.
                //// Since it's unaligned, it may cross two cache lines.
                //_mm_clwb(&cxl_dest[0]);
                //_mm_clwb(&cxl_dest[63]);

                wc->cxl_byte_offset += 64;
                wc->bytes_used -= 64;

                // Shift the remainder (max 14 bytes) to the front of the buffer
                // A single 16-byte SIMD read/write safely covers the remaining bytes
                _mm_storeu_si128((__m128i*)&wc->buf[0], _mm_loadu_si128((__m128i*)&wc->buf[64]));
            }
        }
    }

    for (size_t p = 0; p < fanout; p++) {
        if (node_part_assign(p) != my_nid && wc_bufs[p].bytes_used > 0) {
            uint8_t *cxl_dest = my_remote_tmp_bytes + wc_bufs[p].cxl_byte_offset;
            size_t bytes_to_write = wc_bufs[p].bytes_used;

            // Write the unaligned remainder safely
            memcpy(cxl_dest, wc_bufs[p].buf, bytes_to_write);

            // TODO: check if write-back helps here
            //_mm_clwb(cxl_dest);
            //_mm_clwb(cxl_dest + 63);
        }
    }

    local_barrier(part->barrier);

    /* Flush */
    // TODO: clwb can be parallelized
    //       each thread writes back it's slice of this node's tmp buffer
    //       (via n_tuples/total_tuples and offset into my_tmp)
    // TODO: do we need to flush entire buffer?
    if (is_coordinator_thread) {
        uintptr_t ptr = (uintptr_t) my_remote_tmp;
        size_t size = tmp_stride * sizeof(tuple_t);
        for (uintptr_t p = ptr; p < ptr + size; p += CACHELINE_SIZE) {
            // TODO: use flush or wb here?
            _mm_clwb((void *)p);
        }
        _mm_sfence();
    }
}

void
serial_radix_partition(task_t * const task,
                       task_queue_t *queue,
                       uint64_t const shift_bits,
                       uint64_t const radix_bits,
                       size_t tid)
{
    size_t offset_r = 0;
    size_t offset_s = 0;
    size_t const fanout = 1 << radix_bits;

    //printf("serial_radix: tuple\n");
    //printf("total_tuples: %ld, fanout: %ld, pad: %ld\n", task->r_total_tuples, FANOUT_PASS2, P_BYTES);
    tuple_t *tmp_r = mem_for(tid, task->r_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
    //printf("total_tuples: %ld, fanout: %ld, pad: %ld\n", task->s_total_tuples, FANOUT_PASS2, P_BYTES);
    tuple_t *tmp_s = mem_for(tid, task->s_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
    //printf("serial_radix: hist\n");
    uint64_t *hist_r = mem_for(tid, (fanout + 1) * sizeof(uint64_t));
    uint64_t *hist_s = mem_for(tid, (fanout + 1) * sizeof(uint64_t));

    radix_partition(&task->slices_r, tmp_r, hist_r, shift_bits, radix_bits, tid);
    radix_partition(&task->slices_s, tmp_s, hist_s, shift_bits, radix_bits, tid);

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
                uint64_t * restrict hist,
                uint64_t const shift_bits,
                uint64_t const radix_bits,
                size_t tid)
{
    //size_t const fanout = 1 << radix_bits;
    //uint64_t const mask = (fanout - 1) << shift_bits;
    //size_t offset = 0;
    //uint64_t const padding_tuples = P_TUPLES;

    ////printf("radix: dst\n");
    //uint64_t *dst = mem_for(tid, fanout * sizeof(uint64_t));

    ///* Count tuples per partition */
    //slice_t *slice = in->head;
    //while (slice) {
    //    for (size_t i = 0; i < slice->n_tuples; i++) {
    //        size_t idx = hash(slice->tuples[i].key, mask, shift_bits);
    //        hist[idx]++;
    //    }
    //    slice = slice->next;
    //}
    ///* Determine the start and end of each partition depending on the counts */
    //for (size_t i = 0; i < fanout; i++) {
    //    dst[i] = offset + i * padding_tuples;
    //    offset += hist[i];
    //}

    ///* Copy tuples to their corresponding partitions at appropriate offsets */
    //slice = in->head;
    //while (slice) {
    //    for(size_t i = 0; i < slice->n_tuples; i++) {
    //        size_t idx = hash(slice->tuples[i].key, mask, shift_bits);
    //        out[ dst[idx] ] = slice->tuples[i];
    //        ++dst[idx];
    //    }
    //    slice = slice->next;
    //}

    size_t const fanout = 1 << radix_bits;
    uint64_t const mask = (fanout - 1) << shift_bits;
    size_t offset = 0;
    uint64_t const padding_tuples = P_TUPLES;

    //printf("radix: dst\n");
    uint64_t *dst = mem_for(tid, fanout * sizeof(uint64_t));

    // The list has exactly two slices: local (uncompressed) and remote (compressed)
    slice_t *remote_slice = in->head;
    slice_t *local_slice = in->head->next;

    // Extract pass-1 radix bits from the first tuple of the local slice
    uint64_t p1_radix = local_slice->tuples[0].key & ((1ULL << shift_bits) - 1);

    /* Count tuples per partition */
    // 1. Local slice (uncompressed)
    for (size_t i = 0; i < local_slice->n_tuples; i++) {
        size_t idx = hash(local_slice->tuples[i].key, mask, shift_bits);
        hist[idx]++;
    }

    // 2. Remote slice (compressed, 15 bytes)
    // The bits we need were shifted down to the very bottom during compression!
    uint8_t *remote_bytes = (uint8_t *)remote_slice->tuples;
    uint32_t pass2_mask = fanout - 1;
    for (size_t i = 0; i < remote_slice->n_tuples; i++) {
        uint8_t *src = remote_bytes + (i * COMPRESSED_TUPLE_SIZE);
        // Ultra-fast histogram: load lowest bytes directly and mask. Zero bit-shifts.
        size_t idx = (*(uint32_t*)src) & pass2_mask;
        hist[idx]++;
    }

    /* Determine the start and end of each partition depending on the counts */
    for (size_t i = 0; i < fanout; i++) {
        dst[i] = offset + i * padding_tuples;
        offset += hist[i];
    }

    /* Copy tuples to their corresponding partitions at appropriate offsets */
    // 1. Local slice (uncompressed)
    for(size_t i = 0; i < local_slice->n_tuples; i++) {
        size_t idx = hash(local_slice->tuples[i].key, mask, shift_bits);
        out[ dst[idx] ] = local_slice->tuples[i];
        ++dst[idx];
    }

    // 2. Remote slice (compressed, 15 bytes)
    // Pre-create a 128-bit vector holding just the p1_radix in the lowest byte
    __m128i radix_vec = _mm_set_epi64x(0, p1_radix);

    for(size_t i = 0; i < remote_slice->n_tuples; i++) {
        uint8_t *src = remote_bytes + (i * COMPRESSED_TUPLE_SIZE);

        // Ultra-fast index calculation (same as histogram loop)
        size_t idx = (*(uint32_t*)src) & pass2_mask;

        // 1. Load 16 bytes (15 bytes of tuple + 1 garbage byte from next tuple)
        __m128i t = _mm_loadu_si128((const __m128i*)src);

        // 2. Shift entire 128-bit vector LEFT by 1 byte (8 bits)
        // This drops the garbage byte off the end, aligns the 15-byte tuple
        // to the top 15 bytes, and sets byte 0 to 0x00!
        t = _mm_slli_si128(t, 1);

        // 3. Drop the p1_radix into the 0x00 hole at byte 0
        t = _mm_or_si128(t, radix_vec);

        // 4. Store the perfectly reconstructed 16-byte tuple
        _mm_storeu_si128((__m128i*)&out[dst[idx]], t);
        ++dst[idx];
    }
}

uint64_t
bucket_chaining_join(task_t *task, size_t tid)
{
    uint64_t matches = 0;

    size_t n_tuples = task->r_total_tuples;
    if (n_tuples == 0) return 0;

    uint64_t n_buckets = next_pow2(n_tuples);
    uint64_t const mask = (n_buckets - 1) << (N_RADIX_BITS);

    size_t next_size = round_up(n_tuples * sizeof(uint32_t), CACHELINE_SIZE);
    size_t bucket_size = round_up(n_buckets * sizeof(uint32_t), CACHELINE_SIZE);

    uintptr_t base = (uintptr_t)mem_reuse_for(tid, next_size + bucket_size);
    memset((void *)(base + next_size), 0, bucket_size);

    uint32_t *next = (uint32_t *)(base);
    uint32_t *bucket = (uint32_t *)(base + next_size);

    // We assume exactly one slice per relation in this phase
    slice_t *slice_r = task->slices_r.head;
    tuple_t *build_tuples = slice_r->tuples;

    /* Build loop */
    for (size_t i = 0; i < slice_r->n_tuples; i++) {
        size_t idx = hash(build_tuples[i].key, mask, N_RADIX_BITS);
        next[i] = bucket[idx];
        /* Start positions from 1, 0 marks empty bucket */
        bucket[idx] = i + 1;
    }

    /* Probe loop */
    slice_t *slice_s = task->slices_s.head;
    if (slice_s && slice_s->n_tuples > 0) {
        tuple_t *probe_tuples = slice_s->tuples;
        for (size_t i = 0; i < slice_s->n_tuples; i++) {
            size_t idx = hash(probe_tuples[i].key, mask, N_RADIX_BITS);
            for (uint32_t hit = bucket[idx]; hit > 0; hit = next[hit - 1]) {
                // Fetch the key directly from the build tuples slice
                if (probe_tuples[i].key == build_tuples[hit - 1].key) {
                    matches++;
                }
            }
        }
    }

    return matches;
}

