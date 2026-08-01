#!/usr/bin/env sh
set -eu

# Are we root?
if [ "$(id -u)" != "0" ]; then
    printf "Error: This script must be run as root.\n" >&2
    printf "       Usage: sudo sh %s\n" "$0" >&2
    exit 1
fi

# Disable SMT.
SMTCTL="/sys/devices/system/cpu/smt/control"
if [ -f "$SMTCTL" ] && [ "$(cat "$SMTCTL")" = "on" ]; then
    echo off > "$SMTCTL"
fi

# For each core,
for CORE in /sys/devices/system/cpu/cpu[0-9]*; do
    [ -d "$CORE" ] || continue

    # skip if offline,
    if [ -f "$CORE/online" ] && [ "$(cat "$CORE/online")" = "0" ]; then
        continue
    fi

    # pin to max frequency,
    if [ -d "$CORE/cpufreq" ]; then
        MAXFREQ="$(cat "$CORE/cpufreq/cpuinfo_max_freq")"
        echo performance > "$CORE/cpufreq/scaling_governor"
        echo "$MAXFREQ"  > "$CORE/cpufreq/scaling_max_freq"
        echo "$MAXFREQ"  > "$CORE/cpufreq/scaling_min_freq"
    fi

    # and disable sleep states.
    if [ -d "$CORE/cpuidle" ]; then
        for STATE in "$CORE"/cpuidle/state[0-9]*; do
            [ -d "$STATE" ] || continue
            NAME="$(basename "$STATE")"
            if [ "$NAME" = "state0" ]; then
                echo 0 > "$STATE/disable"
            else
                echo 1 > "$STATE/disable"
            fi
        done
    fi
done

# Disable turbo boost.
BOOST="/sys/devices/system/cpu/cpufreq/boost"
echo 0 > "$BOOST"

# Make perf usable.
echo -1 > /proc/sys/kernel/perf_event_paranoid
echo 0 > /proc/sys/kernel/nmi_watchdog