#include <pthread.h>
#include <sched.h>
#include <stdalign.h>
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

typedef struct {
    alignas(CACHELINE_SIZE)
    relation_t r;
    size_t r_total_tuples;
    relation_t s;
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
} worker_arg_t;

typedef struct {
    alignas(CACHELINE_SIZE)
    relation_t rel;
    size_t total_tuples;
    size_t my_tid;
    size_t n_threads;
    size_t my_nid;
    size_t n_nodes;
    pthread_barrier_t *barrier;
    uint64_t *thread_hist;
    uint64_t *node_hist;
    uint64_t *local_offs;
    tuple_t *local_tmp;
    tuple_t *remote_tmp;
} parallel_part_arg_t;

typedef struct {
    alignas(CACHELINE_SIZE)
    size_t partition;
    relation_t *slices;
    tuple_t *out;
    uint64_t *hist;
    uint64_t *offs;
    uint64_t *p1_thread_hist;
    size_t n_threads;
    size_t my_nid;
} serial_part_arg_t;

typedef struct {
    alignas(CACHELINE_SIZE)
    uint8_t buf[2 * CACHELINE_SIZE - 2 * sizeof(size_t)]; // must be >= 80
    size_t used;
    size_t write_offs;
} wc_buffer_t;

/* Write-combining buffer must fit exactly in 2 cache lines */
static_assert(sizeof(wc_buffer_t) == 2 * CACHELINE_SIZE);

/* Helper */
static inline uint64_t next_pow2(uint64_t v) {
    if (v == 0) {
        return 1;
    }
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    return v + 1;
}

static inline uint64_t hash(uint64_t k, uint64_t mask, uint64_t n_bits) {
    return (k & mask) >> n_bits;
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

/* Join Implementation */
static uint64_t bucket_chaining_join(task_t *task, size_t tid) {
    uint64_t matches = 0;

    size_t n_tuples = task->r_total_tuples;
    if (n_tuples == 0) {
        return 0;
    }

    uint64_t n_buckets = next_pow2(n_tuples);
    uint64_t const mask = (n_buckets - 1) << N_RADIX_BITS;

    size_t next_size = round_up(n_tuples * sizeof(uint32_t), CACHELINE_SIZE);
    size_t bucket_size = round_up(n_buckets * sizeof(uint32_t), CACHELINE_SIZE);

    uintptr_t base = (uintptr_t)mem_reuse_for(tid, next_size + bucket_size);
    memset((void *)(base + next_size), 0, bucket_size);

    uint32_t *next = (uint32_t *)(base);
    uint32_t *bucket = (uint32_t *)(base + next_size);

    /* We have only one local slice per relation in this phase */
    relation_t *r = &task->slices_r[0];
    tuple_t *build_tuples = r->tuples;

    /* Build loop */
    for (size_t i = 0; i < r->n_tuples; i++) {
        size_t idx = hash(build_tuples[i].key, mask, N_RADIX_BITS);
        next[i] = bucket[idx];
        bucket[idx] = i + 1;
    }

    relation_t *s = &task->slices_s[0];
    if (s->n_tuples == 0) {
        return 0;
    }
    tuple_t *probe_tuples = s->tuples;

    /* Probe loop */
    for (size_t i = 0; i < s->n_tuples; i++) {
        size_t idx = hash(probe_tuples[i].key, mask, N_RADIX_BITS);
        for (uint32_t hit = bucket[idx]; hit > 0; hit = next[hit - 1]) {
            if (probe_tuples[i].key == build_tuples[hit - 1].key) {
                matches++;
            }
        }
    }

    return matches;
}

static void serial_radix_partition(serial_part_arg_t *args) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS2;
    uint64_t const mask = (fanout - 1) << N_RADIX_BITS_PASS1;
    uint64_t const compr_shift = N_RADIX_BITS_PASS1 - 8;
    uint64_t const compr_mask = (fanout - 1) << compr_shift;

    size_t const p1_hist_stride = round_up(FANOUT_PASS1, CACHELINE_SIZE / sizeof(uint64_t));
    size_t const remote_nid = (args->my_nid + 1) % 2;
    uint64_t *p1_thread_hist = &args->p1_thread_hist[remote_nid * args->n_threads * p1_hist_stride];

    relation_t *local_slice = &args->slices[0];
    relation_t *remote_slice = &args->slices[1];
    uint64_t *hist = args->hist;
    uint64_t *offs = args->offs;
    tuple_t *out = args->out;

    /* Zero out the reused histogram buffer */
    memset(hist, 0, fanout * sizeof(uint64_t));

    /* Count tuples per partition */
    if (local_slice->n_tuples > 0) {
        for (size_t i = 0; i < local_slice->n_tuples; i++) {
            size_t idx = hash(local_slice->tuples[i].key, mask, N_RADIX_BITS_PASS1);
            hist[idx]++;
        }
    }

    if (remote_slice->n_tuples > 0) {
        uint8_t *base = (uint8_t *)remote_slice->tuples;
        for (size_t t = 0; t < args->n_threads; t++) {
            size_t idx = t * p1_hist_stride + args->partition;
            uint64_t count = p1_thread_hist[idx];
            if (count == 0) {
                continue;
            }

            for (size_t i = 0; i < count; i++) {
                uint8_t *src = base + (i * COMPRESSED_TUPLE_SIZE);
                size_t idx = hash(*(uint32_t *)src, compr_mask, compr_shift);
                hist[idx]++;
            }
            base += round_up(count * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);
        }
    }

    /* Determine offsets */
    size_t sum = 0;
    for (size_t p = 0; p < fanout; p++) {
        offs[p] = sum + p * P_TUPLES;
        sum += hist[p];
    }

    /* Stream tuples to partition buffer */
    if (local_slice->n_tuples > 0) {
        for(size_t i = 0; i < local_slice->n_tuples; i++) {
            size_t idx = hash(local_slice->tuples[i].key, mask, N_RADIX_BITS_PASS1);
            out[offs[idx]] = local_slice->tuples[i];
            offs[idx]++;
        }
    }

    __m128i radix = _mm_set_epi64x(0, (uint8_t)(args->partition));
    if (remote_slice->n_tuples > 0) {
        uint8_t *base = (uint8_t *)remote_slice->tuples;
        for (size_t t = 0; t < args->n_threads; t++) {
            size_t idx = t * p1_hist_stride + args->partition;
            uint64_t count = p1_thread_hist[idx];
            if (count == 0) {
                continue;
            }

            for(size_t i = 0; i < count; i++) {
                uint8_t *src = base + (i * COMPRESSED_TUPLE_SIZE);
                size_t idx = hash(*(uint32_t*)src, compr_mask, compr_shift);
                /* Decompress tuple */
                __m128i tuple = _mm_loadu_si128((__m128i const *)src);
                tuple = _mm_slli_si128(tuple, 1);
                tuple = _mm_or_si128(tuple, radix);
                _mm_store_si128((__m128i*)&out[offs[idx]], tuple);
                offs[idx]++;
            }
            base += round_up(count * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);
        }
    }
}

static void radix_partition(task_t * const task, worker_arg_t *args, uint64_t *hist_r, uint64_t *hist_s, uint64_t *offs) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS2;

    tuple_t *tmp_r = mem_for(args->my_tid, task->r_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
    BUG_ON(!tmp_r);
    tuple_t *tmp_s = mem_for(args->my_tid, task->s_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
    BUG_ON(!tmp_s);

    serial_part_arg_t p2_args;

    p2_args.partition = task->partition;
    p2_args.n_threads = args->n_threads;
    p2_args.my_nid = args->my_nid;
    p2_args.offs = offs;

    p2_args.slices = task->slices_r;
    p2_args.out = tmp_r;
    p2_args.hist = hist_r;
    p2_args.p1_thread_hist = cxl_p1_thread_hist_r();

    serial_radix_partition(&p2_args);

    p2_args.slices = task->slices_s;
    p2_args.out = tmp_s;
    p2_args.hist = hist_s;
    p2_args.p1_thread_hist = cxl_p1_thread_hist_s();

    serial_radix_partition(&p2_args);

    size_t offset_r = 0;
    size_t offset_s = 0;

    for(size_t p = 0; p < fanout; p++) {
        if (hist_r[p] > 0 && hist_s[p] > 0) {
            task_t *task = mem_for(args->my_tid, sizeof(task_t));
            BUG_ON(!task);

            /* Local R */
            task->slices_r[0].n_tuples = hist_r[p];
            task->slices_r[0].tuples = tmp_r + offset_r + p * P_TUPLES;
            /* Remote R - None in pass 2 */
            task->slices_r[1].n_tuples = 0;
            task->slices_r[1].tuples = NULL;
            task->r_total_tuples = hist_r[p];
            offset_r += hist_r[p];

            /* Local S */
            task->slices_s[0].n_tuples = hist_s[p];
            task->slices_s[0].tuples = tmp_s + offset_s + p * P_TUPLES;
            /* Remote S - None in pass 2 */
            task->slices_s[1].n_tuples = 0;
            task->slices_s[1].tuples = NULL;
            task->s_total_tuples = hist_s[p];
            offset_s += hist_s[p];

            task->partition = p;

            task_queue_add_atomic(args->join_queue, task);
        }
        else {
            offset_r += hist_r[p];
            offset_s += hist_s[p];
        }
    }
}

static void compute_local_hist(worker_arg_t *args) {
    uint64_t const ignore_bits = 0;
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    uint64_t const mask = (fanout - 1) << ignore_bits;

    size_t stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));

    uint64_t *my_hist_r = &cxl_p1_thread_hist_r()[(args->my_nid * args->n_threads + args->my_tid) * stride];
    uint64_t *my_hist_s = &cxl_p1_thread_hist_s()[(args->my_nid * args->n_threads + args->my_tid) * stride];

    for (size_t t = 0; t < args->r.n_tuples; t++) {
        size_t p = hash(args->r.tuples[t].key, mask, ignore_bits);
        my_hist_r[p]++;
    }

    for (size_t t = 0; t < args->s.n_tuples; t++) {
        size_t p = hash(args->s.tuples[t].key, mask, ignore_bits);
        my_hist_s[p]++;
    }

    cache_wb(my_hist_r, fanout * sizeof(uint64_t), false);
    cache_wb(my_hist_s, fanout * sizeof(uint64_t), false);
}

static void publish_node_hist(worker_arg_t *args) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;

    size_t stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));

    uint64_t *my_node_hist_r = &cxl_p1_node_hist_r()[args->my_nid * stride];
    uint64_t *my_node_hist_s = &cxl_p1_node_hist_s()[args->my_nid * stride];

    for (size_t t = 0; t < args->n_threads; t++) {
        uint64_t *t_hist_r = &cxl_p1_thread_hist_r()[(args->my_nid * args->n_threads + t) * stride];
        uint64_t *t_hist_s = &cxl_p1_thread_hist_s()[(args->my_nid * args->n_threads + t) * stride];

        for (size_t p = 0; p < fanout; p++) {
            my_node_hist_r[p] += t_hist_r[p];
            my_node_hist_s[p] += t_hist_s[p];
        }
    }

    cache_wb(my_node_hist_r, fanout * sizeof(uint64_t), false);
    cache_wb(my_node_hist_s, fanout * sizeof(uint64_t), false);
}

static void compute_part_assign(worker_arg_t *args) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    size_t stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));

    // TODO: this assumes 2 nodes
    size_t remote_nid = (args->my_nid + 1) % 2;

    uint64_t *local_node_hist_r = &cxl_p1_node_hist_r()[args->my_nid * stride];
    uint64_t *local_node_hist_s = &cxl_p1_node_hist_s()[args->my_nid * stride];
    uint64_t *remote_node_hist_r = &cxl_p1_node_hist_r()[remote_nid * stride];
    uint64_t *remote_node_hist_s = &cxl_p1_node_hist_s()[remote_nid * stride];

    cache_inv(remote_node_hist_r, fanout * sizeof(uint64_t), false);
    cache_inv(remote_node_hist_s, fanout * sizeof(uint64_t), true);

    uint8_t *part_assign = mem_p1_part_assign();

    // TODO: don't calculate here, pass in as parameter?
    uint64_t total_tuples = 0;
    for (size_t p = 0; p < fanout; p++) {
        total_tuples += local_node_hist_r[p] + remote_node_hist_r[p];
        total_tuples += local_node_hist_s[p] + remote_node_hist_s[p];
    }

    uint64_t max_capacity = (total_tuples >> 1);
    //uint64_t max_capacity = (total_tuples >> 1) + (total_tuples >> 5); // 3.125% tolerance
    uint64_t load0 = 0;
    uint64_t load1 = 0;

    for (size_t p = 0; p < fanout; p++) {
        uint64_t count0_r = (args->my_nid == 0) ? local_node_hist_r[p] : remote_node_hist_r[p];
        uint64_t count0_s = (args->my_nid == 0) ? local_node_hist_s[p] : remote_node_hist_s[p];
        uint64_t count0 = count0_r + count0_s;

        uint64_t count1_r = (args->my_nid == 1) ? local_node_hist_r[p] : remote_node_hist_r[p];
        uint64_t count1_s = (args->my_nid == 1) ? local_node_hist_s[p] : remote_node_hist_s[p];
        uint64_t count1 = count1_r + count1_s;

        uint64_t part_total = count0 + count1;
        uint8_t preferred_node = (count0 >= count1) ? 0 : 1;

        if (preferred_node == 0) {
            if (load0 + part_total <= max_capacity) {
                part_assign[p] = 0;
                load0 += part_total;
            } else {
                part_assign[p] = 1;
                load1 += part_total;
            }
        } else {
            if (load1 + part_total <= max_capacity) {
                part_assign[p] = 1;
                load1 += part_total;
            } else {
                part_assign[p] = 0;
                load0 += part_total;
            }
        }
    }

#if DEBUG
    double p0 = ((double)load0 / (double)total_tuples) * 100.0;
    double p1 = ((double)load1 / (double)total_tuples) * 100.0;
    printf("Got partition assignment (imbalance = +%.2f%%, node0: #tuples = %ld [%.2f%%], node1: #tuples = %ld [%.2f%%])\n",
            (load0 > load1) ? (p0 - 50.0) : (p1 - 50.0), load0, p0, load1, p1);
#endif
}

static uint64_t *compute_part_offs(parallel_part_arg_t *args) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    size_t const hist_stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));

    uint64_t *node_hist = &args->node_hist[args->my_nid * hist_stride];
    uint64_t *offs = mem_for(args->my_tid, fanout * sizeof(uint64_t));
    BUG_ON(!offs);
    uint8_t *part_assign = mem_p1_part_assign();

    uint64_t local_sum = 0;  // Offset in local memory (tuples)
    uint64_t remote_sum = 0; // Offset in CXL memory (bytes)
    size_t local_idx = 0;

    for (size_t p = 0; p < fanout; p++) {
        uint8_t owner = part_assign[p];

        if (owner == args->my_nid) {
            /* Compute base local offset */
            offs[p] = local_sum + (local_idx * PADDING_TUPLES);

            if (args->my_tid == COORDINATION_THREAD) {
                args->local_offs[local_idx] = offs[p];
            }

            local_sum += node_hist[p];

            /* Advance past preceding threads on this node */
            for (size_t t = 0; t < args->my_tid; t++) {
                size_t idx = (args->my_nid * args->n_threads + t) * hist_stride + p;
                offs[p] += args->thread_hist[idx];
            }
            local_idx++;
        }

        /* Always advance offset in CXL space */
        for (size_t n = 0; n < args->n_nodes; n++) {
            /* Owner writes locally, not to CXL */
            if (n == owner) {
                continue;
            }

            for (size_t t = 0; t < args->n_threads; t++) {
                size_t idx = (n * args->n_threads + t) * hist_stride + p;
                uint64_t count = args->thread_hist[idx];
                // TODO: FIXME: remote partitions should also have PADDING_TUPLES to avoid cache aliasing!
                size_t bytes = round_up(count * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);

                /* If this is my remote contribution, assign my offset */
                if (owner != args->my_nid && n == args->my_nid && t == args->my_tid) {
                    offs[p] = remote_sum;
                }
                remote_sum += bytes;
            }
        }
    }

    if (args->my_tid == COORDINATION_THREAD) {
        args->local_offs[local_idx] = local_sum + (local_idx * PADDING_TUPLES);
    }

    return offs;
}

static void stream_tuples_to_parts(parallel_part_arg_t *args, uint64_t *offs) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    uint64_t const ignore_bits = 0;
    uint64_t const mask = (fanout - 1) << ignore_bits;

    uint8_t *part_assign = mem_p1_part_assign();

    wc_buffer_t *wc_buffers = mem_for(args->my_tid, fanout * sizeof(wc_buffer_t));
    BUG_ON(!wc_buffers);
    for (size_t p = 0; p < fanout; p++) {
        if (part_assign[p] != args->my_nid) {
            wc_buffers[p].write_offs = offs[p];
            wc_buffers[p].used = 0;
        }
    }

    tuple_t const *rel = args->rel.tuples;
    tuple_t *local_tmp = args->local_tmp;
    uint8_t *remote_tmp = (uint8_t *)(args->remote_tmp);

    for(size_t i = 0; i < args->rel.n_tuples; i++) {
        size_t p = hash(rel[i].key, mask, ignore_bits);

        if (part_assign[p] == args->my_nid) {
            /* Local write: regular store */
            size_t idx = offs[p]++;
            __m128i t = _mm_load_si128((__m128i const *)&rel[i]);
            _mm_store_si128((__m128i *)&local_tmp[idx], t);
        } else {
            /* Remote write: compression + write-combining store */
            wc_buffer_t *wc = &wc_buffers[p];

            /* Compress using overlapping scalar stores */
            uint8_t *dest = wc->buf + wc->used;
            uint64_t compr_key = rel[i].key >> 8;
            memcpy(dest, &compr_key, sizeof(uint64_t));
            memcpy(dest + 7, &rel[i].rid, sizeof(uint64_t));
            wc->used += COMPRESSED_TUPLE_SIZE;

            /* If we've crossed 64-byte, flush exactly one cache line */
            if (wc->used >= CACHELINE_SIZE) {
                uint8_t *dest = remote_tmp + wc->write_offs;

                __m512i line = _mm512_load_si512((__m512i const *)wc->buf);
                _mm512_store_si512((__m512i *)dest, line);

                wc->write_offs += CACHELINE_SIZE;
                wc->used -= CACHELINE_SIZE;

                /* Shift the spill (max 14 bytes) to the front of the buffer */
                __m128i spill = _mm_load_si128((__m128i const *)&wc->buf[CACHELINE_SIZE]);
                _mm_store_si128((__m128i *)&wc->buf[0], spill);
            }
        }
    }

    /* Flush any remaining partial tuples in the buffers */
    for (size_t p = 0; p < fanout; p++) {
        if (part_assign[p] != args->my_nid && wc_buffers[p].used > 0) {
            uint8_t *dest = remote_tmp + wc_buffers[p].write_offs;
            memcpy(dest, wc_buffers[p].buf, wc_buffers[p].used);
        }
    }

    local_barrier(args->barrier);

    /* Flush caches */
    if (args->my_tid == COORDINATION_THREAD) {
        cache_wb(remote_tmp, args->total_tuples * sizeof(tuple_t), true);
    }
}

static void parallel_radix_partition(parallel_part_arg_t * const args) {
    uint64_t *offs = compute_part_offs(args);
    stream_tuples_to_parts(args, offs);
}

static void prepare_part_tasks(worker_arg_t *args) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    size_t const hist_stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));

    uint8_t *part_assign = mem_p1_part_assign();
    uint64_t *local_offs_r = mem_p1_local_offs_r();
    uint64_t *local_offs_s = mem_p1_local_offs_s();
    uint64_t *thread_hist_r = cxl_p1_thread_hist_r();
    uint64_t *thread_hist_s = cxl_p1_thread_hist_s();
    tuple_t *local_tmp_r = mem_p1_local_tmp_r();
    tuple_t *local_tmp_s = mem_p1_local_tmp_s();
    uint8_t *remote_tmp_r = (uint8_t *)cxl_p1_remote_tmp_r();
    uint8_t *remote_tmp_s = (uint8_t *)cxl_p1_remote_tmp_s();

    size_t remote_sum_r = 0;
    size_t remote_sum_s = 0;
    size_t local_idx = 0;

    for (size_t p = 0; p < fanout; p++) {
        uint8_t owner = part_assign[p];

        /* Calculate the total remote bytes/tuples for this partition */
        size_t remote_bytes_r = 0;
        size_t remote_tuples_r = 0;
        size_t remote_bytes_s = 0;
        size_t remote_tuples_s = 0;

        for (size_t n = 0; n < args->n_nodes; n++) {
            if (n == owner) continue;
            for (size_t t = 0; t < args->n_threads; t++) {
                uint64_t count_r = thread_hist_r[(n * args->n_threads + t) * hist_stride + p];
                remote_bytes_r += round_up(count_r * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);
                remote_tuples_r += count_r;

                uint64_t count_s = thread_hist_s[(n * args->n_threads + t) * hist_stride + p];
                remote_bytes_s += round_up(count_s * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);
                remote_tuples_s += count_s;
            }
        }

        /* Only create tasks for partitions assigned to this node */
        if (owner == args->my_nid) {
            size_t local_r = local_offs_r[local_idx + 1] - local_offs_r[local_idx] - PADDING_TUPLES;
            size_t local_s = local_offs_s[local_idx + 1] - local_offs_s[local_idx] - PADDING_TUPLES;

            if ((local_r + remote_tuples_r > 0) && (local_s + remote_tuples_s > 0)) {
                task_t *task = mem_for(args->my_tid, sizeof(task_t));
                BUG_ON(!task);

                task->partition = p;

                /* Setup R slices */
                task->slices_r[0].n_tuples = local_r;
                task->slices_r[0].tuples = (local_r > 0) ? &local_tmp_r[local_offs_r[local_idx]] : NULL;
                task->slices_r[1].n_tuples = remote_tuples_r;
                task->slices_r[1].tuples = (remote_tuples_r > 0) ? (tuple_t *)&remote_tmp_r[remote_sum_r] : NULL;
                task->r_total_tuples = local_r + remote_tuples_r;

                /* Setup S slices */
                task->slices_s[0].n_tuples = local_s;
                task->slices_s[0].tuples = (local_s > 0) ? &local_tmp_s[local_offs_s[local_idx]] : NULL;
                task->slices_s[1].n_tuples = remote_tuples_s;
                task->slices_s[1].tuples = (remote_tuples_s > 0) ? (tuple_t *)&remote_tmp_s[remote_sum_s] : NULL;
                task->s_total_tuples = local_s + remote_tuples_s;

                task_queue_add(args->part_queue, task);
            }
            local_idx++;
        }

        remote_sum_r += remote_bytes_r;
        remote_sum_s += remote_bytes_s;
    }
}

void *join_worker(void *arg) {
    worker_arg_t *args = (worker_arg_t *) arg;
    bool const is_coordinator_thread = (args->my_tid == COORDINATION_THREAD);

    local_barrier(args->barrier);

    /* 1st pass */
#if DEBUG
    if (is_coordinator_thread) {
        printf("Starting 1st pass\n");
    }
    local_barrier(args->barrier);
#endif

#if PERF
    args->timing.start = timestamp();
#endif

    compute_local_hist(args);
    local_barrier(args->barrier);

    if (is_coordinator_thread) {
        publish_node_hist(args);
    }

    global_barrier(args->my_tid, args->barrier);

    if (is_coordinator_thread) {
        compute_part_assign(args);
    }

    local_barrier(args->barrier);

    task_queue_t *part_queue = args->part_queue;
    task_queue_t *join_queue = args->join_queue;

    parallel_part_arg_t p1_args;

    p1_args.my_tid     = args->my_tid;
    p1_args.n_threads  = args->n_threads;
    p1_args.my_nid     = args->my_nid;
    p1_args.n_nodes    = args->n_nodes;
    p1_args.barrier    = args->barrier;

    /* Partition R */
    p1_args.rel          = args->r;
    p1_args.total_tuples = args->r_total_tuples;
    p1_args.thread_hist  = cxl_p1_thread_hist_r();
    p1_args.node_hist    = cxl_p1_node_hist_r();
    p1_args.local_offs   = mem_p1_local_offs_r();
    p1_args.local_tmp    = mem_p1_local_tmp_r();
    p1_args.remote_tmp   = cxl_p1_remote_tmp_r();

    parallel_radix_partition(&p1_args);

    /* Partition S */
    p1_args.rel          = args->s;
    p1_args.total_tuples = args->s_total_tuples;
    p1_args.thread_hist  = cxl_p1_thread_hist_s();
    p1_args.node_hist    = cxl_p1_node_hist_s();
    p1_args.local_offs   = mem_p1_local_offs_s();
    p1_args.local_tmp    = mem_p1_local_tmp_s();
    p1_args.remote_tmp   = cxl_p1_remote_tmp_s();

    parallel_radix_partition(&p1_args);

    global_barrier(args->my_tid, args->barrier);
#if PERF
    args->timing.part_distr = timestamp();
#endif

    if (is_coordinator_thread) {
        prepare_part_tasks(args);
    }

    local_barrier(args->barrier);
#if PERF
    args->timing.part_assign = timestamp();
#endif

    /* 2nd pass */
#if DEBUG
    if (is_coordinator_thread) {
        printf("Starting 2nd pass (#tasks = %ld)\n", part_queue->size);
    }
    local_barrier(args->barrier);
#endif

    size_t const p2_fanout = 1 << N_RADIX_BITS_PASS2;
    uint64_t *hist_r = mem_for(args->my_tid, (p2_fanout + 1) * sizeof(uint64_t));
    BUG_ON(!hist_r);
    uint64_t *hist_s = mem_for(args->my_tid, (p2_fanout + 1) * sizeof(uint64_t));
    BUG_ON(!hist_s);
    uint64_t *offs = mem_for(args->my_tid, p2_fanout * sizeof(uint64_t));
    BUG_ON(!offs);

    task_t *part_task;
    while ((part_task = task_queue_get_atomic(part_queue))) {
        radix_partition(part_task, args, hist_r, hist_s, offs);
    }

    /* Wait until parallel threads add all join tasks */
    local_barrier(args->barrier);

#if PERF
    args->timing.part_local = timestamp();
#endif

    /* Buildprobe phase */
#if DEBUG
    if (is_coordinator_thread) {
        printf("Starting buildprobe (#tasks = %ld)\n", join_queue->size);
    }
    local_barrier(args->barrier);
#endif

    uint64_t matches = 0;
    task_t *join_task;
    void *prealloc = mem_for(args->my_tid, 10 * L1_CACHE_SIZE);
    BUG_ON(!prealloc);
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

    return NULL;
}

uint64_t join_relations(relation_t *r, relation_t *s, param_t *params) {
    uint64_t matches = 0;

    /* Threads */
    pthread_t threads[params->n_threads];
    worker_arg_t args[params->n_threads];
    cpu_set_t set;
    pthread_barrier_t barrier;
    int err = pthread_barrier_init(&barrier, NULL, params->n_threads);
    BUG_ON(err != 0);
    pthread_attr_t attr;
    err = pthread_attr_init(&attr);
    BUG_ON(err != 0);

    /* Queues for partition and join tasks */
    task_queue_t *part_queue = task_queue_init(FANOUT_PASS1);
    task_queue_t *join_queue = task_queue_init(1 << N_RADIX_BITS);

    /* Assign slices of R & S for each thread */
    size_t r_slice = r->n_tuples / params->n_threads;
    size_t s_slice = s->n_tuples / params->n_threads;

    for (size_t i = 0; i < params->n_threads; i++) {
        bool last = (i == params->n_threads - 1);

        args[i].r.tuples = r->tuples + i * r_slice;
        args[i].r.n_tuples = last ? (r->n_tuples - i * r_slice) : r_slice;
        args[i].r_total_tuples = r->n_tuples;

        args[i].s.tuples = s->tuples + i * s_slice;
        args[i].s.n_tuples = last ? (s->n_tuples - i * s_slice) : s_slice;
        args[i].s_total_tuples = s->n_tuples;

        args[i].part_queue = part_queue;
        args[i].join_queue = join_queue;
        args[i].barrier = &barrier;
        args[i].my_tid = i;
        args[i].n_threads = params->n_threads;
        args[i].my_nid = params->my_nid;
        args[i].n_nodes = params->n_nodes;

        int cpu = CPU_MAPPING[i];
#if DEBUG
        printf("Assigning worker %ld to CPU %d\n", i, cpu);
#endif
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        int err = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);
        BUG_ON(err != 0);
        err = pthread_create(&threads[i], &attr, join_worker, (void *)&args[i]);
        BUG_ON(err != 0);
    }

    err = pthread_attr_destroy(&attr);
    BUG_ON(err != 0);

    /* Wait for threads to finish */
    for (size_t i = 0; i < params->n_threads; i++) {
        pthread_join(threads[i], NULL);
        matches += args[i].matches;
    }

#if PERF
    print_timing(&args[0].timing, params, matches);
#endif

    /* Clean-up */
    task_queue_free(part_queue);
    task_queue_free(join_queue);

    return matches;
}
