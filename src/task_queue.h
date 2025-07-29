#pragma once

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "types.h"

typedef struct task task_t;
typedef struct task_queue task_queue_t;

struct task {
    relation_t r;
    relation_t tmp_r;
    relation_t s;
    relation_t tmp_s;
    task_t *next;
};

typedef struct task_queue {
    pthread_mutex_t lock;
    task_t *head;
    task_t *tasks;
    size_t max_size;
    atomic_size_t next_free;
#if DEBUG
    size_t size;
#endif
} task_queue_t;

static task_queue_t *task_queue_init(size_t max_size);
static void task_queue_free(task_queue_t *queue);

static inline task_t *task_queue_get_atomic(task_queue_t *queue);
static inline void task_queue_add_atomic(task_queue_t *queue, task_t *task);
static inline void task_queue_add(task_queue_t *queue, task_t *task);
static inline task_t *task_queue_get_slot_atomic(task_queue_t * queue);
static inline task_t *task_queue_get_slot(task_queue_t *queue);

task_queue_t *task_queue_init(size_t max_size) {
    task_queue_t *queue = malloc(sizeof(task_queue_t));
    if (!queue) {
        return NULL;
    }

    pthread_mutex_init(&queue->lock, NULL);
    queue->head = NULL;

    queue->tasks = calloc(max_size, sizeof(task_t));
    if (!queue->tasks) {
        free(queue);
        return NULL;
    }

    queue->max_size = max_size;
    atomic_init(&queue->next_free, 0);
#if DEBUG
    queue->size = 0;
#endif

    return queue;
}

void task_queue_free(task_queue_t *queue) {
    pthread_mutex_destroy(&queue->lock);
    free(queue->tasks);
    free(queue);
}

static inline task_t *task_queue_get_atomic(task_queue_t *queue) {
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

static inline void task_queue_add_atomic(task_queue_t *queue, task_t *task) {
    pthread_mutex_lock(&queue->lock);
    task_queue_add(queue, task);
    pthread_mutex_unlock(&queue->lock);
}

static inline void task_queue_add(task_queue_t *queue, task_t *task) {
    task->next = queue->head;
    queue->head = task;
#if DEBUG
    queue->size++;
#endif
}

static inline task_t *task_queue_get_slot(task_queue_t *queue) {
    size_t idx = queue->next_free;
    if (idx >= queue->max_size) {
        return NULL;
    }
    queue->next_free = idx + 1;
    return &queue->tasks[idx];
}

static inline task_t *task_queue_get_slot_atomic(task_queue_t *queue) {
    size_t idx = atomic_fetch_add(&queue->next_free, 1);
    if (idx >= queue->max_size) {
        return NULL;
    }
    return &queue->tasks[idx];
}

