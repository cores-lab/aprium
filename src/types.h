#pragma once

#include <stddef.h>
#include <stdint.h>

typedef struct tuple tuple_t;
typedef struct relation relation_t;

struct tuple {
    uint64_t key;
    uint64_t rid;
};

struct relation {
    tuple_t *tuples;
    size_t n_tuples;
};

