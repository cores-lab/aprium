#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "generate.h"
#include "config.h"

static int init_done = 0;

void create_relation(relation_t *relation, size_t size) {
    if (!init_done) {
        srand(time(NULL));
        init_done = 1;
    }
    void *ptr = malloc(size * sizeof(tuple_t) + RELATION_PADDING);
    BUG_ON(!ptr);
    relation->tuples = ptr;
    relation->n_tuples = size;

    for (size_t i = 0; i < size; i++) {
        relation->tuples[i].key = i;
    }

    for (size_t i = size - 1; i > 0; i--) {
        size_t j = ((double) rand() / ((double) RAND_MAX + 1)) * i;
        uint64_t tmp = relation->tuples[i].key;
        relation->tuples[i].key = relation->tuples[j].key;
        relation->tuples[j].key = tmp;
    }

    for (size_t i = 0; i < size; i++) {
        relation->tuples[i].rid = i;
    }
}

void delete_relation(relation_t *relation) {
    free(relation->tuples);
}

