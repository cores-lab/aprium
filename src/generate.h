#pragma once

#include "types.h"
#include "cli.h"

void get_slices(relation_t *slice_r, relation_t *slice_s, param_t *params);
void release_slices(relation_t *slice_r, relation_t *slice_s);
