#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    uint64_t key;
    uint64_t rid;
} tuple_t;

// TODO: move slice definition here

typedef struct {
    tuple_t *tuples;
    size_t n_tuples;
} relation_t;

