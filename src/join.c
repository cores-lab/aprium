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
typedef struct {
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
} arg_t;

/* Parallel radix partition pass arguments */
typedef struct {
    alignas(CACHELINE_SIZE)
    relation_t rel;
    size_t total_tuples;
    size_t my_tid;
    size_t my_nid;
    pthread_barrier_t *barrier;
    uint64_t *thread_hist;
    uint64_t *node_hist;
    uint64_t *local_offs;
    uint64_t *remote_offs;
    size_t offset_stride;
    tuple_t *local_tmp;
    tuple_t *remote_tmp;
} part_t;

/* Software write-combining buffer per partition (L1 cache optimized) */
typedef struct {
    uint8_t buf[2 * CACHELINE_SIZE];        // Safely fits up to 78 bytes + 16-byte SIMD shift margin
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
                size_t tid)
{
    size_t const fanout = 1 << radix_bits;
    uint64_t const mask = (fanout - 1) << shift_bits;
    uint64_t compressed_shift = shift_bits - 8;
    uint64_t compressed_mask = (fanout - 1) << compressed_shift;

    uint64_t *dst = mem_for(tid, fanout * sizeof(uint64_t));

    /* 1. Count tuples per partition */
    slice_t *slice = in->head;
    while (slice) {
        if (slice->is_compressed) {
            uint8_t *remote_bytes = (uint8_t *)slice->tuples;
            for (size_t i = 0; i < slice->n_tuples; i++) {
                uint8_t *src = remote_bytes + (i * COMPRESSED_TUPLE_SIZE);
                size_t idx = hash(*(uint32_t*)src, compressed_mask, compressed_shift);
                hist[idx]++;
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
    __m128i cr = _mm_set_epi64x(0, slice->compressed_radix);

    while (slice) {
        if (slice->is_compressed) {
            uint8_t *remote_bytes = (uint8_t *)slice->tuples;
            for(size_t i = 0; i < slice->n_tuples; i++) {
                uint8_t *src = remote_bytes + (i * COMPRESSED_TUPLE_SIZE);
                size_t idx = hash(*(uint32_t*)src, compressed_mask, compressed_shift);

                // 1. Load 16 bytes (15 bytes of tuple + 1 garbage byte from next tuple)
                __m128i t = _mm_loadu_si128((const __m128i*)src);

                // 2. Shift entire 128-bit vector left by 1 byte
                // This drops the garbage byte off the end, aligns the 15-byte tuple
                // to the top 15 bytes, and sets byte 0 to 0x00
                t = _mm_slli_si128(t, 1);

                // 3. Drop the compressed radix into the 0x00 hole at byte 0
                t = _mm_or_si128(t, cr);

                // 4. Store the reconstructed 16-byte tuple
                _mm_storeu_si128((__m128i*)&out[dst[idx]], t);
                ++dst[idx];
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
                       size_t tid)
{
    size_t offset_r = 0;
    size_t offset_s = 0;
    size_t const fanout = 1 << radix_bits;

    tuple_t *tmp_r = mem_for(tid, task->r_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
    tuple_t *tmp_s = mem_for(tid, task->s_total_tuples * sizeof(tuple_t) + (FANOUT_PASS2 + 1) * P_BYTES);
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

static void compute_local_hist(arg_t *args) {
    uint64_t const ignore_bits = 0;
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    uint64_t const mask = (fanout - 1) << ignore_bits;

    size_t stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));

    uint64_t *my_hist_r = &mem_p1_thread_hist_r()[args->my_tid * stride];
    uint64_t *my_hist_s = &mem_p1_thread_hist_s()[args->my_tid * stride];

    for (size_t t = 0; t < args->r.n_tuples; t++) {
        size_t p = hash(args->r.tuples[t].key, mask, ignore_bits);
        my_hist_r[p]++;
    }

    for (size_t t = 0; t < args->s.n_tuples; t++) {
        size_t p = hash(args->s.tuples[t].key, mask, ignore_bits);
        my_hist_s[p]++;
    }
}

static void publish_node_hist(arg_t *args) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;

    size_t stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));

    uint64_t *my_node_hist_r = &cxl_p1_node_hist_r()[args->my_nid * stride];
    uint64_t *my_node_hist_s = &cxl_p1_node_hist_s()[args->my_nid * stride];

    for (size_t t = 0; t < args->n_threads; t++) {
        uint64_t *t_hist_r = &mem_p1_thread_hist_r()[t * stride];
        uint64_t *t_hist_s = &mem_p1_thread_hist_s()[t * stride];

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
    size_t const stride = round_up(fanout, CACHELINE_SIZE / sizeof(uint64_t));

    uint64_t *offs = mem_for(part->my_tid, fanout * sizeof(uint64_t));
    uint64_t *node_hist = part->node_hist;
    uint8_t  *part_assign = mem_p1_part_assign();

    uint64_t local_sum = 0; // counts in tuples
    uint64_t remote_sum = 0; // counts in compressed tuples
    size_t local_idx = 0;

    for (size_t p = 0; p < fanout; p++) {
        if (part_assign[p] == part->my_nid) {
            offs[p] = local_sum + (local_idx * PADDING_TUPLES);
            local_sum += node_hist[p];
            local_idx++;
        } else {
            offs[p] = remote_sum;
            remote_sum += node_hist[p];
        }
    }

    // coordinator skips this -> has global offs
    for (size_t t = 0; t < part->my_tid; t++) {
        uint64_t *thread_hist = &part->thread_hist[t * stride];
        for (size_t p = 0; p < fanout; p++) {
            if (part_assign[p] == part->my_nid) {
                offs[p] += thread_hist[p];
            } else {
                offs[p] += thread_hist[p];
            }
        }
    }

    return offs;
}

static void publish_global_offs(part_t *part, uint64_t *offs) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    size_t const stride = round_up(fanout + 1, CACHELINE_SIZE / sizeof(uint64_t));

    uint64_t *local_offs = part->local_offs;
    uint64_t *my_remote_offs = &part->remote_offs[part->my_nid * stride];

    uint64_t *node_hist = part->node_hist;
    uint8_t  *part_assign = mem_p1_part_assign();

    uint64_t local_sum = 0;
    uint64_t remote_sum = 0;
    size_t local_idx = 0;
    size_t remote_idx = 0;

    for (size_t p = 0; p < fanout; p++) {
        if (part_assign[p] == part->my_nid) {
            local_offs[local_idx++] = offs[p];
            local_sum += node_hist[p];
        } else {
            my_remote_offs[remote_idx++] = offs[p];
            remote_sum += node_hist[p];
        }
    }

    local_offs[local_idx] = local_sum + (local_idx * PADDING_TUPLES);
    my_remote_offs[remote_idx] = remote_sum;

    cache_wb(my_remote_offs, stride * sizeof(uint64_t), true);
}

static inline void store512_unaligned(uint8_t *src, uint8_t *dst) {
#if defined(__AVX512F__)
    __m512i v = _mm512_loadu_si512((__m512i const *)src);
    _mm512_storeu_si512((__m512i *)dst, v);
#elif defined(__AVX2__)
    __m256i v0 = _mm256_loadu_si256((__m256i const *)(src + 0));
    __m256i v1 = _mm256_loadu_si256((__m256i const *)(src + 32));
    _mm256_storeu_si256((__m256i *)(dst + 0), v0);
    _mm256_storeu_si256((__m256i *)(dst + 32), v1);
#else
    __m128i v0 = _mm_loadu_si128((__m128i const *)(src + 0));
    __m128i v1 = _mm_loadu_si128((__m128i const *)(src + 16));
    __m128i v2 = _mm_loadu_si128((__m128i const *)(src + 32));
    __m128i v3 = _mm_loadu_si128((__m128i const *)(src + 48));
    _mm_storeu_si128((__m128i *)(dst + 0), v0);
    _mm_storeu_si128((__m128i *)(dst + 16), v1);
    _mm_storeu_si128((__m128i *)(dst + 32), v2);
    _mm_storeu_si128((__m128i *)(dst + 48), v3);
#endif
}

static void stream_tuples_to_parts(part_t *part, uint64_t *offs) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    uint64_t const ignore_bits = 0;
    uint64_t const mask = (fanout - 1) << ignore_bits;
    
    uint8_t *part_assign = mem_p1_part_assign();

    wc_buffer_t *wc_bufs = mem_for(part->my_tid, fanout * sizeof(wc_buffer_t));
    for (size_t p = 0; p < fanout; p++) {
        if (part_assign[p] != part->my_nid) {
            wc_bufs[p].cxl_byte_offset = offs[p] * COMPRESSED_TUPLE_SIZE;
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
            __m128i t = _mm_loadu_si128((__m128i const *)&rel[i]);
            _mm_storeu_si128((__m128i *)&local_tmp[idx], t);
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

                store512_unaligned(wc->buf, cxl_dest);

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

    if (part->my_tid == COORDINATION_THREAD) {
        publish_global_offs(part, offs);
    }

    stream_tuples_to_parts(part, offs);
}

static void prepare_part_tasks(arg_t *args) {
    size_t const fanout = 1 << N_RADIX_BITS_PASS1;
    size_t const stride = round_up(fanout + 1, CACHELINE_SIZE / sizeof(uint64_t));
    uint8_t *part_assign = mem_p1_part_assign();
    // TODO: this assumes 2 nodes
    size_t remote_nid = (args->my_nid + 1) % 2;

    uint64_t *local_offs_r = mem_p1_local_offs_r();
    uint64_t *local_offs_s = mem_p1_local_offs_s();
    uint64_t *remote_offs_r = &cxl_p1_remote_offs_r()[remote_nid * stride];
    uint64_t *remote_offs_s = &cxl_p1_remote_offs_s()[remote_nid * stride];

    cache_inv(remote_offs_r, stride * sizeof(uint64_t), false);
    cache_inv(remote_offs_s, stride * sizeof(uint64_t), true);

    tuple_t *local_tmp_r = mem_p1_local_tmp_r();
    tuple_t *local_tmp_s = mem_p1_local_tmp_s();
    uint8_t *remote_tmp_r = (uint8_t *)&cxl_p1_remote_tmp_r()[remote_nid * args->tmp_stride];
    uint8_t *remote_tmp_s = (uint8_t *)&cxl_p1_remote_tmp_s()[remote_nid * args->tmp_stride];

    size_t idx = 0;
    for (size_t p = 0; p < fanout; p++) {
        if (part_assign[p] != args->my_nid) {
            continue;
        }

        size_t local_r = local_offs_r[idx + 1] - local_offs_r[idx] - PADDING_TUPLES;
        size_t local_s = local_offs_s[idx + 1] - local_offs_s[idx] - PADDING_TUPLES;
        size_t remote_r = remote_offs_r[idx + 1] - remote_offs_r[idx];
        size_t remote_s = remote_offs_s[idx + 1] - remote_offs_s[idx];

        size_t count_r = local_r + remote_r;
        size_t count_s = local_s + remote_s;

        if (count_r == 0 || count_s == 0) {
            idx++;
            continue;
        }

        task_t *task = task_queue_get_slot(args->part_queue);
        BUG_ON(!task);

        uint8_t compressed_radix = (uint8_t)p;

        if (local_r > 0) {
            slice_t *slice = slice_alloc();
            BUG_ON(!slice);
            slice->tuples = &local_tmp_r[local_offs_r[idx]];
            slice->n_tuples = local_r;
            slice->is_compressed = false;
            slice->compressed_radix = compressed_radix;
            slice_list_add(&task->slices_r, slice);
        }

        if (remote_r > 0) {
            slice_t *slice = slice_alloc();
            BUG_ON(!slice);
            slice->tuples = (tuple_t *)&remote_tmp_r[remote_offs_r[idx] * COMPRESSED_TUPLE_SIZE];
            slice->n_tuples = remote_r;
            slice->is_compressed = true;
            slice->compressed_radix = compressed_radix;
            slice_list_add(&task->slices_r, slice);
        }

        if (local_s > 0) {
            slice_t *slice = slice_alloc();
            BUG_ON(!slice);
            slice->tuples = &local_tmp_s[local_offs_s[idx]];
            slice->n_tuples = local_s;
            slice->is_compressed = false;
            slice->compressed_radix = compressed_radix;
            slice_list_add(&task->slices_s, slice);
        }

        if (remote_s > 0) {
            slice_t *slice = slice_alloc();
            BUG_ON(!slice);
            slice->tuples = (tuple_t *)&remote_tmp_s[remote_offs_s[idx] * COMPRESSED_TUPLE_SIZE];
            slice->n_tuples = remote_s;
            slice->is_compressed = true;
            slice->compressed_radix = compressed_radix;
            slice_list_add(&task->slices_s, slice);
        }

        task->r_total_tuples = count_r;
        task->s_total_tuples = count_s;
        task_queue_add(args->part_queue, task);

        idx++;
    }
}

void *join_worker(void *arg) {
    arg_t *args = (arg_t *) arg;
    bool const is_coordinator_thread = (args->my_tid == COORDINATION_THREAD);

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

    //global_barrier(args->my_tid, args->barrier);

#if PERF
    args->timing.start = timestamp();
#endif

    size_t const stride = round_up(FANOUT_PASS1, CACHELINE_SIZE / sizeof(uint64_t));

    part_t part;

    part.my_tid     = args->my_tid;
    part.my_nid     = args->my_nid;
    part.barrier    = args->barrier;

    /* Partition R */
    part.rel          = args->r;
    part.total_tuples = args->r_total_tuples;
    part.thread_hist  = mem_p1_thread_hist_r();
    part.node_hist    = &cxl_p1_node_hist_r()[args->my_nid * stride];
    part.local_offs   = mem_p1_local_offs_r();
    part.remote_offs  = cxl_p1_remote_offs_r();
    part.local_tmp    = mem_p1_local_tmp_r();
    part.remote_tmp   = &cxl_p1_remote_tmp_r()[args->my_nid * args->tmp_stride];

    parallel_radix_partition(&part);

    /* Partition S */
    part.rel          = args->s;
    part.total_tuples = args->s_total_tuples;
    part.thread_hist  = mem_p1_thread_hist_s();
    part.node_hist    = &cxl_p1_node_hist_s()[args->my_nid * stride];
    part.local_offs   = mem_p1_local_offs_s();
    part.remote_offs  = cxl_p1_remote_offs_s();
    part.local_tmp    = mem_p1_local_tmp_s();
    part.remote_tmp   = &cxl_p1_remote_tmp_s()[args->my_nid * args->tmp_stride];

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

    // TODO: here we assume symmetrical relations
    size_t slice;
    size_t bigger = params->r_size > params->s_size ? params->r_size : params->s_size;
    slice = bigger;
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

#if PERF
    print_timing(params->r_size, params->s_size, params->n_threads, matches, &args[0].timing);
#endif

    /* Clean-up */
    task_queue_free(part_queue);
    task_queue_free(join_queue);
    slice_allocator_free();

    return matches;
}

