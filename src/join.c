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

/* Worker thread arguments */
typedef struct {
    alignas(CACHELINE_SIZE)
    relation_t r;
    uint64_t *hist_r;
    tuple_t *tmp_r;
    size_t r_total_tuples;
    relation_t s;
    uint64_t *hist_s;
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
} arg_t;

/* Parallel radix partition pass arguments */
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
    size_t offset_stride;
    tuple_t *local_tmp;
    tuple_t *remote_tmp;
} part_t;

/* Software write-combining buffer per partition */
typedef struct {
    alignas(CACHELINE_SIZE) uint8_t buf[2 * CACHELINE_SIZE];        // Safely fits up to 78 bytes + 16-byte SIMD shift margin
    size_t bytes_used;
    size_t cxl_byte_offset;  // Absolute byte offset in the CXL remote memory
} wc_buffer_t;

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

/* Join Implementation */
static uint64_t bucket_chaining_join(task_t *task, size_t tid) {
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

    /* We assume exactly one slice per relation in this phase */
    slice_t *slice_r = task->slices_r.head;
    tuple_t *build_tuples = slice_r->tuples;

    /* Build loop */
    for (size_t i = 0; i < slice_r->n_tuples; i++) {
        size_t idx = hash(build_tuples[i].key, mask, N_RADIX_BITS);
        next[i] = bucket[idx];
        /* Start positions from 1, 0 marks empty bucket */
        bucket[idx] = i + 1;
    }

    slice_t *slice_s = task->slices_s.head;
    if (slice_s->n_tuples == 0) {
        return 0;
    }
    tuple_t *probe_tuples = slice_s->tuples;

    /* Probe loop */
    for (size_t i = 0; i < slice_s->n_tuples; i++) {
        size_t idx = hash(probe_tuples[i].key, mask, N_RADIX_BITS);
        for (uint32_t hit = bucket[idx]; hit > 0; hit = next[hit - 1]) {
            // Fetch the key directly from the build tuples slice
            if (probe_tuples[i].key == build_tuples[hit - 1].key) {
                matches++;
            }
        }
    }

    return matches;
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
static void
radix_partition(slice_list_t * restrict in,
                tuple_t * restrict out,
                uint64_t * restrict hist,
                uint64_t const shift_bits,
                uint64_t const radix_bits,
                size_t tid,
                size_t n_threads,
                uint64_t * restrict remote_thread_hist)
{
    size_t const fanout = 1 << radix_bits;
    uint64_t const mask = (fanout - 1) << shift_bits;
    uint64_t compressed_shift = shift_bits - 8;
    uint64_t compressed_mask = (fanout - 1) << compressed_shift;

    size_t const pass1_fanout = 1 << N_RADIX_BITS_PASS1;
    size_t const hist_stride = round_up(pass1_fanout, CACHELINE_SIZE / sizeof(uint64_t));

    uint64_t *dst = mem_for(tid, fanout * sizeof(uint64_t));

    /* 1. Count tuples per partition */
    slice_t *slice = in->head;
    while (slice) {
        if (slice->is_remote) {
            uint8_t *remote_bytes = (uint8_t *)slice->tuples;
            for (size_t t = 0; t < n_threads; t++) {
                uint64_t count = remote_thread_hist[t * hist_stride + slice->partition];
                if (count == 0) continue;

                for (size_t i = 0; i < count; i++) {
                    uint8_t *src = remote_bytes + (i * COMPRESSED_TUPLE_SIZE);
                    size_t idx = hash(*(uint32_t*)src, compressed_mask, compressed_shift);
                    hist[idx]++;
                }
                remote_bytes += round_up(count * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);
            }
        } else {
            for (size_t i = 0; i < slice->n_tuples; i++) {
                size_t idx = hash(slice->tuples[i].key, mask, shift_bits);
                hist[idx]++;
            }
        }
        slice = slice->next;
    }

    /* 2. Determine start and end of each partition depending on the counts */
    size_t offset = 0;
    for (size_t i = 0; i < fanout; i++) {
        dst[i] = offset + i * P_TUPLES;
        offset += hist[i];
    }

    /* 3. Copy/Decompress tuples to their corresponding partitions */
    slice = in->head;
    __m128i cr = _mm_set_epi64x(0, (uint8_t)(slice->partition));

    while (slice) {
        if (slice->is_remote) {
            uint8_t *remote_bytes = (uint8_t *)slice->tuples;
            for (size_t t = 0; t < n_threads; t++) {
                // USE compressed_radix instead of partition_idx
                uint64_t count = remote_thread_hist[t * hist_stride + slice->partition];
                if (count == 0) continue;

                for(size_t i = 0; i < count; i++) {
                    uint8_t *src = remote_bytes + (i * COMPRESSED_TUPLE_SIZE);
                    size_t idx = hash(*(uint32_t*)src, compressed_mask, compressed_shift);

                    __m128i t_vec = _mm_loadu_si128((__m128i const *)src);
                    t_vec = _mm_slli_si128(t_vec, 1);
                    t_vec = _mm_or_si128(t_vec, cr);
                    _mm_store_si128((__m128i*)&out[dst[idx]], t_vec);

                    ++dst[idx];
                }
                remote_bytes += round_up(count * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);
            }
        } else {
            for(size_t i = 0; i < slice->n_tuples; i++) {
                size_t idx = hash(slice->tuples[i].key, mask, shift_bits);
                out[ dst[idx] ] = slice->tuples[i];
                ++dst[idx];
            }
        }
        slice = slice->next;
    }
}

static void
serial_radix_partition(task_t * const task,
                       task_queue_t *queue,
                       uint64_t const shift_bits,
                       uint64_t const radix_bits,
                       size_t tid,
                       size_t n_threads,
                       uint64_t * restrict remote_thread_hist_r,
                       uint64_t * restrict remote_thread_hist_s)
{
    size_t offset_r = 0;
    size_t offset_s = 0;
    size_t const fanout = 1 << radix_bits;

    tuple_t *tmp_r = mem_for(tid, task->r_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
    tuple_t *tmp_s = mem_for(tid, task->s_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
    uint64_t *hist_r = mem_for(tid, (fanout + 1) * sizeof(uint64_t));
    uint64_t *hist_s = mem_for(tid, (fanout + 1) * sizeof(uint64_t));

    radix_partition(&task->slices_r, tmp_r, hist_r, shift_bits, radix_bits, tid, n_threads, remote_thread_hist_r);
    radix_partition(&task->slices_s, tmp_s, hist_s, shift_bits, radix_bits, tid, n_threads, remote_thread_hist_s);

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

static void compute_local_hist(arg_t *args) {
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
    cache_wb(my_hist_s, fanout * sizeof(uint64_t), true);
}

static void publish_node_hist(arg_t *args) {
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
    cache_wb(my_node_hist_s, fanout * sizeof(uint64_t), true);
}

static void compute_part_assign(arg_t *args) {
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
    printf("Got partition assignment:\n");
    printf("load0: %ld, load1: %ld, total: %ld\n", load0, load1, total_tuples);
    printf("node0: %.4lf, node1: %.4lf)\n", ((double)load0) / ((double)total_tuples), ((double)load1) / ((double)total_tuples));
#endif
}

static uint64_t *compute_part_offs(part_t *part) {
size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    size_t const hist_stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));

    uint64_t *offs = mem_for(part->my_tid, fanout * sizeof(uint64_t));
    uint8_t  *part_assign = mem_p1_part_assign();

    uint64_t local_sum = 0;         // Offsets in local memory (tuples)
    uint64_t global_cxl_sum = 0;    // Offsets in shared CXL memory (bytes)
    size_t local_idx = 0;

    for (size_t p = 0; p < fanout; p++) {
        uint8_t owner = part_assign[p];

        if (owner == part->my_nid) {
            // Compute base local offset
            offs[p] = local_sum + (local_idx * PADDING_TUPLES);

            // CONDENSE: The coordinator thread captures this base node-wide offset directly
            if (part->my_tid == COORDINATION_THREAD) {
                part->local_offs[local_idx] = offs[p];
            }

            local_sum += part->node_hist[p];

            // Advance past preceding threads on THIS node
            for (size_t t = 0; t < part->my_tid; t++) {
                offs[p] += part->thread_hist[(part->my_nid * part->n_threads + t) * hist_stride + p];
            }
            local_idx++;
        }

        // ALWAYS calculate and increment global CXL space to keep all nodes fully synchronized
        for (size_t n = 0; n < part->n_nodes; n++) {
            if (n == owner) continue; // Owner writes locally, not to CXL

            for (size_t t = 0; t < part->n_threads; t++) {
                uint64_t count = part->thread_hist[(n * part->n_threads + t) * hist_stride + p];
                size_t bytes = round_up(count * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);

                // If this is our remote contribution, assign our offset
                if (owner != part->my_nid && n == part->my_nid && t == part->my_tid) {
                    offs[p] = global_cxl_sum;
                }
                global_cxl_sum += bytes;
            }
        }
    }

    // CONDENSE: Store the final guard array bound element exclusively from the coordinator thread
    if (part->my_tid == COORDINATION_THREAD) {
        part->local_offs[local_idx] = local_sum + (local_idx * PADDING_TUPLES);
    }

    return offs;
}

static inline void store512(uint8_t *src, uint8_t *dst) {
    __m512i v = _mm512_load_si512((__m512i const *)src);
    _mm512_store_si512((__m512i *)dst, v);
}

static void stream_tuples_to_parts(part_t *part, uint64_t *offs) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    uint64_t const ignore_bits = 0;
    uint64_t const mask = (fanout - 1) << ignore_bits;

    uint8_t *part_assign = mem_p1_part_assign();

    wc_buffer_t *wc_bufs = mem_for(part->my_tid, fanout * sizeof(wc_buffer_t));
    for (size_t p = 0; p < fanout; p++) {
        if (part_assign[p] != part->my_nid) {
            wc_bufs[p].cxl_byte_offset = offs[p];
            wc_bufs[p].bytes_used = 0;
        }
    }

    tuple_t const *rel = part->rel.tuples;
    tuple_t *local_tmp = part->local_tmp;
    uint8_t *remote_tmp = (uint8_t *)(part->remote_tmp);

    for(size_t i = 0; i < part->rel.n_tuples; i++) {
        size_t p = hash(rel[i].key, mask, ignore_bits);

        if (part_assign[p] == part->my_nid) {
            // Local write: regular store
            size_t idx = offs[p]++;
            __m128i t = _mm_load_si128((__m128i const *)&rel[i]);
            _mm_store_si128((__m128i *)&local_tmp[idx], t);
        } else {
            // Remote write: compression, write-combining store
            wc_buffer_t *wc = &wc_bufs[p];

            // SUPER FAST PACKING: Overlapping scalar stores
            uint8_t *dest = wc->buf + wc->bytes_used;
            *(uint64_t*)dest = rel[i].key >> 8; // compressed key
            *(uint64_t*)(dest + 7) = rel[i].rid;

            wc->bytes_used += COMPRESSED_TUPLE_SIZE;

            // If we've crossed the 64-byte threshold, flush exactly one cache line
            if (wc->bytes_used >= CACHELINE_SIZE) {
                uint8_t *cxl_dest = remote_tmp + wc->cxl_byte_offset;

                store512(wc->buf, cxl_dest);

                wc->cxl_byte_offset += CACHELINE_SIZE;
                wc->bytes_used -= CACHELINE_SIZE;

                // Shift the spill (max 14 bytes) to the front of the buffer
                __m128i s = _mm_loadu_si128((__m128i *)&wc->buf[CACHELINE_SIZE]);
                _mm_storeu_si128((__m128i *)&wc->buf[0], s);
            }
        }
    }

    // Flush any remaining partial tuples in the buffers
    for (size_t p = 0; p < fanout; p++) {
        if (part_assign[p] != part->my_nid && wc_bufs[p].bytes_used > 0) {
            uint8_t *cxl_dest = remote_tmp + wc_bufs[p].cxl_byte_offset;
            memcpy(cxl_dest, wc_bufs[p].buf, wc_bufs[p].bytes_used);
        }
    }

    local_barrier(part->barrier);

    // Flush caches
    if (part->my_tid == COORDINATION_THREAD) {
        cache_wb(remote_tmp, part->total_tuples * sizeof(tuple_t), true);
    }
}

static void parallel_radix_partition(part_t * const part) {
    uint64_t *offs = compute_part_offs(part);

    stream_tuples_to_parts(part, offs);
}

static void prepare_part_tasks(arg_t *args) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    size_t const hist_stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));
    uint8_t *part_assign = mem_p1_part_assign();

    uint64_t *local_offs_r = mem_p1_local_offs_r();
    uint64_t *local_offs_s = mem_p1_local_offs_s();

    // Base pointers for all thread histograms across all nodes
    uint64_t *all_hists_r = cxl_p1_thread_hist_r();
    uint64_t *all_hists_s = cxl_p1_thread_hist_s();

    tuple_t *local_tmp_r = mem_p1_local_tmp_r();
    tuple_t *local_tmp_s = mem_p1_local_tmp_s();

    // Shared CXL buffers (no longer node-specific)
    uint8_t *remote_tmp_r = (uint8_t *)cxl_p1_remote_tmp_r();
    uint8_t *remote_tmp_s = (uint8_t *)cxl_p1_remote_tmp_s();

    uint64_t global_cxl_sum_r = 0;
    uint64_t global_cxl_sum_s = 0;
    size_t local_idx = 0;

    for (size_t p = 0; p < fanout; p++) {
        uint8_t owner = part_assign[p];

        // Calculate the total remote bytes/tuples for this partition across ALL non-owner nodes
        uint64_t p_remote_bytes_r = 0, p_remote_tuples_r = 0;
        uint64_t p_remote_bytes_s = 0, p_remote_tuples_s = 0;

        for (size_t n = 0; n < args->n_nodes; n++) {
            if (n == owner) continue;
            for (size_t t = 0; t < args->n_threads; t++) {
                uint64_t count_r = all_hists_r[(n * args->n_threads + t) * hist_stride + p];
                p_remote_bytes_r += round_up(count_r * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);
                p_remote_tuples_r += count_r;

                uint64_t count_s = all_hists_s[(n * args->n_threads + t) * hist_stride + p];
                p_remote_bytes_s += round_up(count_s * COMPRESSED_TUPLE_SIZE, CACHELINE_SIZE);
                p_remote_tuples_s += count_s;
            }
        }

        // Only create tasks for partitions assigned to this node
        if (owner == args->my_nid) {
            size_t local_r = local_offs_r[local_idx + 1] - local_offs_r[local_idx] - PADDING_TUPLES;
            size_t local_s = local_offs_s[local_idx + 1] - local_offs_s[local_idx] - PADDING_TUPLES;

            if ((local_r + p_remote_tuples_r > 0) && (local_s + p_remote_tuples_s > 0)) {
                task_t *task = task_queue_get_slot(args->part_queue);
                BUG_ON(!task);

                // Add Local R slice
                if (local_r > 0) {
                    slice_t *slice = slice_alloc();
                    slice->tuples = &local_tmp_r[local_offs_r[local_idx]];
                    slice->n_tuples = local_r;
                    slice->is_remote = false;
                    slice->partition = p;
                    slice_list_add(&task->slices_r, slice);
                }

                // Add Single Contiguous Remote R slice
                if (p_remote_tuples_r > 0) {
                    slice_t *slice = slice_alloc();
                    slice->tuples = (tuple_t *)&remote_tmp_r[global_cxl_sum_r];
                    slice->n_tuples = p_remote_tuples_r;
                    slice->is_remote = true;
                    slice->partition = p;
                    slice_list_add(&task->slices_r, slice);
                }

                // Add Local S slice
                if (local_s > 0) {
                    slice_t *slice = slice_alloc();
                    slice->tuples = &local_tmp_s[local_offs_s[local_idx]];
                    slice->n_tuples = local_s;
                    slice->is_remote = false;
                    slice->partition = p;
                    slice_list_add(&task->slices_s, slice);
                }

                // Add Single Contiguous Remote S slice
                if (p_remote_tuples_s > 0) {
                    slice_t *slice = slice_alloc();
                    slice->tuples = (tuple_t *)&remote_tmp_s[global_cxl_sum_s];
                    slice->n_tuples = p_remote_tuples_s;
                    slice->is_remote = true;
                    slice->partition = p;
                    slice_list_add(&task->slices_s, slice);
                }

                task->r_total_tuples = local_r + p_remote_tuples_r;
                task->s_total_tuples = local_s + p_remote_tuples_s;
                task_queue_add(args->part_queue, task);
            }
            local_idx++;
        }

        // Always increment the global offset trackers regardless of assignment
        global_cxl_sum_r += p_remote_bytes_r;
        global_cxl_sum_s += p_remote_bytes_s;
    }
}

void *join_worker(void *arg) {
    arg_t *args = (arg_t *) arg;
    bool const is_coordinator_thread = (args->my_tid == COORDINATION_THREAD);

    local_barrier(args->barrier);

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

    size_t const stride = round_up(FANOUT_PASS1, CACHELINE_SIZE / sizeof(uint64_t));

    part_t part;

    part.my_tid     = args->my_tid;
    part.n_threads  = args->n_threads;
    part.my_nid     = args->my_nid;
    part.n_nodes    = args->n_nodes;
    part.barrier    = args->barrier;

    /* Partition R */
    part.rel          = args->r;
    part.total_tuples = args->r_total_tuples;
    part.thread_hist  = cxl_p1_thread_hist_r();
    part.node_hist    = &cxl_p1_node_hist_r()[args->my_nid * stride];
    part.local_offs   = mem_p1_local_offs_r();
    part.local_tmp    = mem_p1_local_tmp_r();
    part.remote_tmp   = cxl_p1_remote_tmp_r();

    parallel_radix_partition(&part);

    /* Partition S */
    part.rel          = args->s;
    part.total_tuples = args->s_total_tuples;
    part.thread_hist  = cxl_p1_thread_hist_s();
    part.node_hist    = &cxl_p1_node_hist_s()[args->my_nid * stride];
    part.local_offs   = mem_p1_local_offs_s();
    part.local_tmp    = mem_p1_local_tmp_s();
    part.remote_tmp   = cxl_p1_remote_tmp_s();

    parallel_radix_partition(&part);

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
        printf("2nd pass: #tasks = %ld\n", part_queue->size);
    }
    local_barrier(args->barrier);
#endif
    size_t const hist_stride = round_up(FANOUT_PASS1, CACHELINE_SIZE / sizeof(uint64_t));
    size_t remote_nid = (args->my_nid + 1) % 2;

    uint64_t *remote_thread_hist_r = &cxl_p1_thread_hist_r()[remote_nid * args->n_threads * hist_stride];
    uint64_t *remote_thread_hist_s = &cxl_p1_thread_hist_s()[remote_nid * args->n_threads * hist_stride];

    task_t *part_task;
    while ((part_task = task_queue_get_atomic(part_queue))) {
        uint64_t shift = N_RADIX_BITS_PASS1;
        uint64_t radix = N_RADIX_BITS_PASS2;
        size_t tid = args->my_tid;
        serial_radix_partition(part_task, join_queue, shift, radix, tid, args->n_threads, remote_thread_hist_r, remote_thread_hist_s);
    }

    /* Wait until parallel threads add all join tasks */
    local_barrier(args->barrier);

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

uint64_t join_relations(relation_t *r, relation_t *s, param_t *params) {
    uint64_t matches = 0;

    /* Threads */
    pthread_t threads[params->n_threads];
    arg_t args[params->n_threads];
    cpu_set_t set;
    pthread_barrier_t barrier;
    BUG_ON(pthread_barrier_init(&barrier, NULL, params->n_threads));
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    /* Histograms */
    uint64_t *hist_r = cxl_p1_thread_hist_r();
    uint64_t *hist_s = cxl_p1_thread_hist_s();

    /* Partition Buffer */
    tuple_t *tmp_r = cxl_p1_remote_tmp_r();
    tuple_t *tmp_s = cxl_p1_remote_tmp_s();

    /* Queues for partition and join tasks */
    task_queue_t *part_queue = task_queue_init(FANOUT_PASS1);
    BUG_ON(!part_queue);
    task_queue_t *join_queue = task_queue_init(1 << N_RADIX_BITS);
    BUG_ON(!join_queue);
    // TODO: use whatever is bigger from above
    slice_allocator_init(FANOUT_PASS1 * params->n_nodes + (1 << N_RADIX_BITS));

    /* Assign slices of R & S for each thread */
    size_t r_slice = r->n_tuples / params->n_threads;
    size_t s_slice = s->n_tuples / params->n_threads;

    for (size_t i = 0; i < params->n_threads; i++) {
        bool last = (i == params->n_threads - 1);

        args[i].r.tuples = r->tuples + i * r_slice;
        args[i].r.n_tuples = last ? (r->n_tuples - i * r_slice) : r_slice;
        args[i].hist_r = hist_r;
        args[i].tmp_r = tmp_r;
        args[i].r_total_tuples = r->n_tuples;

        args[i].s.tuples = s->tuples + i * s_slice;
        args[i].s.n_tuples = last ? (s->n_tuples - i * s_slice) : s_slice;
        args[i].hist_s = hist_s;
        args[i].tmp_s = tmp_s;
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
        BUG_ON(pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set));
        BUG_ON(pthread_create(&threads[i], &attr, join_worker, (void *)&args[i]));
    }

    /* Wait for threads to finish */
    for (size_t i = 0; i < params->n_threads; i++) {
        pthread_join(threads[i], NULL);
        matches += args[i].matches;
    }
    pthread_attr_destroy(&attr);

#if PERF
    print_timing(&args[0].timing, params, matches);
#endif

    /* Clean-up */
    task_queue_free(part_queue);
    task_queue_free(join_queue);
    slice_allocator_free();

    return matches;
}
