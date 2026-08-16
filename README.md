# Accelerated Parallel Radix-Join over Incoherent Unified Memory

This repository contains the artifact accompanying the VLDB 2027 submission "[APRIUM: Accelerated Parallel Radix-Join
over Incoherent Unified Memory](https://vldb.org/2027/)".

## Structure

The artifact is split into two parts:

- `bench/`: helpful benchmarking scripts
- `src/`: APRIUM algorithm source code

## Requirements

To build the artifact, you will need:

- gcc
- GNU make

We have verified compatibility with the following versions of these tools:

```bash
$ gcc --version
gcc (GCC) 16.1.1 20260515 (Red Hat 16.1.1-2)
$ make --version
GNU Make 4.4.1
```

To run the artifact, you will need:

- 2 machines connected to a (non-coherent) CXL memory pool

## Reproduction

### Minimum Working Example

To run a minimum working example, perform the following steps:

### Paper Results

To reproduce the results from the paper, perform the following steps:

## Citation

If you use this artifact, please cite the paper.

```bibtex
@inproceedings{aprium-2027,
author    = {Lumme, Moritz and Berger, Daniel and Friedman, Michal},
title     = {APRIUM: Accelerated Parallel Radix-Join over Incoherent Unified Memory},
booktitle = {TBD},
pages     = {TBD},
publisher = {TBD},
year      = {2027},
url       = {TBD},
doi       = {TBD},
}
```