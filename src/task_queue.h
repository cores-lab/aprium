#pragma once

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "types.h"

typedef struct task task_t;
typedef struct task_queue task_queue_t;

struct task {
    relation_t rel_r;
    relation_t tmp_r;
    relation_t rel_s;
    relation_t tmp_s;
    task_t *next;
};

typedef struct task_queue {
    pthread_mutex_t lock;
    task_t *head;
    task_t *tasks;
    size_t size;
    atomic_size_t next_free;
} task_queue_t;

task_queue_t *task_queue_init(size_t size);
void task_queue_free(task_queue_t *queue);

inline task_t *task_queue_get_atomic(task_queue_t *queue);
inline void task_queue_add_atomic(task_queue_t *queue, task_t *task);
inline void task_queue_add(task_queue_t *queue, task_t *task);
inline task_t *task_queue_get_slot_atomic(task_queue_t * queue);
inline task_t *task_queue_get_slot(task_queue_t *queue);

inline task_queue_t *task_queue_init(size_t size) {
    task_queue_t *queue = malloc(sizeof(task_queue_t));
    if (!queue) {
        return NULL;
    }

    pthread_mutex_init(&queue->lock, NULL);
    queue->head = NULL;

    queue->tasks = calloc(size, sizeof(task_t));
    if (!queue->tasks) {
        free(queue);
        return NULL;
    }

    queue->size = size;
    atomic_init(&queue->next_free, 0);

    return queue;
}

inline void task_queue_free(task_queue_t *queue) {
    pthread_mutex_destroy(&queue->lock);
    free(queue->tasks);
    free(queue);
}

inline task_t *task_queue_get_atomic(task_queue_t *queue) {
    pthread_mutex_lock(&queue->lock);
    task_t *t = queue->head;
    if (t) {
        queue->head = t->next;
    }
    pthread_mutex_unlock(&queue->lock);
    return t;
}

inline void task_queue_add_atomic(task_queue_t *queue, task_t *task) {
    pthread_mutex_lock(&queue->lock);
    task_queue_add(queue, task);
    pthread_mutex_unlock(&queue->lock);
}

inline void task_queue_add(task_queue_t *queue, task_t *task) {
    task->next = queue->head;
    queue->head = task;
}

inline task_t *task_queue_get_slot(task_queue_t *queue) {
    size_t idx = queue->next_free;
    if (idx >= queue->size) {
        return NULL;
    }
    queue->next_free = idx + 1;
    return &queue->tasks[idx];
}

inline task_t *task_queue_get_slot_atomic(task_queue_t *queue) {
    size_t idx = atomic_fetch_add(&queue->next_free, 1);
    if (idx >= queue->size) {
        return NULL;
    }
    return &queue->tasks[idx];
}

