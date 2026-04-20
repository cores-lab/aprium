#pragma once

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "types.h"
#include "slice_list.h"

typedef struct task task_t;
typedef struct task_queue task_queue_t;

struct task {
    slice_list_t slices_r;
    size_t r_total_tuples;
    slice_list_t slices_s;
    size_t s_total_tuples;
    task_t *next;
};

struct task_queue {
    pthread_mutex_t lock;
    task_t *head;
    task_t *tasks;
    size_t max_size;
    atomic_size_t next_free;
#if DEBUG
    size_t size;
#endif
};

static task_queue_t *task_queue_init(size_t size) {
    task_queue_t *queue = malloc(sizeof(task_queue_t));
    BUG_ON(!queue);

    pthread_mutex_init(&queue->lock, NULL);
    queue->head = NULL;

    size_t max_size = (size_t)(size * OVERALLOC);
    queue->tasks = calloc(max_size, sizeof(task_t));
    BUG_ON(!queue->tasks);

    queue->max_size = max_size;
    atomic_init(&queue->next_free, 0);
#if DEBUG
    queue->size = 0;
#endif

    return queue;
}

static void task_queue_free(task_queue_t *queue) {
    pthread_mutex_destroy(&queue->lock);
    free(queue->tasks);
    free(queue);
}

static task_t *task_queue_get_atomic(task_queue_t *queue) {
    pthread_mutex_lock(&queue->lock);
    task_t *t = queue->head;
    if (t) {
        queue->head = t->next;
#if DEBUG
        queue->size--;
#endif
    }
    pthread_mutex_unlock(&queue->lock);
    return t;
}

static void task_queue_add(task_queue_t *queue, task_t *task) {
    task->next = queue->head;
    queue->head = task;
#if DEBUG
    queue->size++;
#endif
}

static void task_queue_add_atomic(task_queue_t *queue, task_t *task) {
    pthread_mutex_lock(&queue->lock);
    task_queue_add(queue, task);
    pthread_mutex_unlock(&queue->lock);
}

static task_t *task_queue_get_slot(task_queue_t *queue) {
    if (queue->next_free >= queue->max_size) {
        return NULL;
    }
    return &queue->tasks[queue->next_free++];
}

static task_t *task_queue_get_slot_atomic(task_queue_t *queue) {
    size_t idx = atomic_fetch_add(&queue->next_free, 1);
    if (idx >= queue->max_size) {
        return NULL;
    }
    return &queue->tasks[idx];
}
