#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "types.h"

typedef struct {
    size_t partition;
    size_t r_total_tuples;
    size_t s_total_tuples;
    /* idx: 0 = local slice, 1 = remote slice */
    relation_t slices_r[2];
    relation_t slices_s[2];
} task_t;

typedef struct {
    task_t **tasks;
    size_t size;
    atomic_size_t push_idx;
    atomic_size_t pop_idx;
} task_queue_t;

static task_queue_t *task_queue_init(size_t size) {
    task_queue_t *queue = malloc(sizeof(task_queue_t));
    BUG_ON(!queue);
    queue->tasks = malloc(size * sizeof(task_t *));
    BUG_ON(!queue->tasks);

    queue->size = size;
    atomic_init(&queue->push_idx, 0);
    atomic_init(&queue->push_idx, 0);
    return queue;
}

static void task_queue_free(task_queue_t *queue) {
    free(queue->tasks);
    free(queue);
}

static void task_queue_add(task_queue_t *queue, task_t *task) {
    size_t idx = atomic_fetch_add_explicit(&queue->push_idx, 1, memory_order_relaxed);
    BUG_ON(idx >= queue->size);
    queue->tasks[idx] = task;
}

static void task_queue_add_atomic(task_queue_t *queue, task_t *task) {
    size_t idx = atomic_fetch_add(&queue->push_idx, 1);
    BUG_ON(idx >= queue->size);
    queue->tasks[idx] = task;
}

static task_t *task_queue_get_atomic(task_queue_t *queue) {
    size_t idx = atomic_fetch_add(&queue->pop_idx, 1);
    if (idx >= atomic_load_explicit(&queue->push_idx, memory_order_relaxed)) {
        return NULL;
    }
    return queue->tasks[idx];
}
