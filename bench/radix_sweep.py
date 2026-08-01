#!/usr/bin/env python3

import os
import sys
import subprocess
import re
import argparse
import socket
import time
import threading

# ==== CONFIGURATION ===========================================================
SERVER_IP = "10.0.0.1"
SERVER_PORT = 50051
CONFIG = "./src/config.h"
N_CLIENTS = 2
N_TUPLES = 2147483648  # 32 GiB
N_REPS = 10
N_THREADS = 32

RADIX_BITS = []
for total_bits in range(16, 23):
    min_bits = (N_THREADS - 1).bit_length() # (2*N_THREADS - 1) to ensure every thread gets work!
    for p1 in range(min_bits, total_bits - min_bits + 1):
        p2 = total_bits - p1
        max_bits = 12  # Limit TLB thrashing
        if p1 <= max_bits and p2 <= max_bits:
            RADIX_BITS.append((p1, p2))
# ==============================================================================

def server_loop():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", SERVER_PORT))
            s.listen()
        except OSError as e:
            print(f"[Barrier Server] Failed to bind to port {SERVER_PORT}: {e}")
            return

        while True:
            clients = [s.accept()[0] for _ in range(N_CLIENTS)]
            for c in clients:
                try:
                    c.sendall(b"R")
                except Exception:
                    pass
                finally:
                    c.close()

def barrier():
    while True:
        try:
            with socket.create_connection((SERVER_IP, SERVER_PORT), timeout=5) as s:
                if s.recv(1) == b"R":
                    return
        except Exception:
            time.sleep(1)

def update_config(key, value):
    if not os.path.exists(CONFIG):
        print(f"Error: Config file '{CONFIG}' not found.")
        sys.exit(1)

    with open(CONFIG, "r") as f:
        content = f.read()

    pattern = rf"^(#define\s+{key}\s+).*$"
    replacement = rf"\g<1>{value}"

    new_content, count = re.subn(pattern, replacement, content, flags=re.MULTILINE)

    if count == 0:
        print(f"Warning: Macro '{key}' was not found in {CONFIG}. No changes made.")

    with open(CONFIG, "w") as f:
        f.write(new_content)

def main():
    parser = argparse.ArgumentParser(description="Distributed Radix Bits Sweep")
    parser.add_argument("--out", type=str, required=True, help="Output directory for results")
    parser.add_argument("--node-id", type=int, required=True, help="ID of this node")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    output_csv = os.path.join(args.out, "radix_sweep.csv")

    if args.node_id == 0:
        print(f"Initializing Barrier Server on port {SERVER_PORT}...")
        threading.Thread(target=server_loop, daemon=True).start()
        time.sleep(1)

    print(f"Total configurations to test: {len(RADIX_BITS)}")

    # Initialize CSV header
    with open(output_csv, "w") as csv_file:
        csv_file.write("r_tuples,s_tuples,fill,fill_param,n_threads,radix_bits_pass1,radix_bits_pass2,n_tuples,total,part_distr,part_assign,part_local,build_probe,per_tuple\n")

    for p1, p2 in RADIX_BITS:
        gib = N_TUPLES * 16 // 1024 // 1024 // 1024
        print(f"\n==== Target Radix Bits: Pass1 = {p1}, Pass2 = {p2} (Total: {p1 + p2}) (Size: {gib} GiB) ====")

        print("Updating config...")
        update_config("N_RADIX_BITS_PASS1", str(p1))
        update_config("N_RADIX_BITS_PASS2", str(p2))

        print("Compiling project...")
        try:
            subprocess.run(["make", "clean"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["make", "-j"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            print(f"Compilation failed for Pass1={p1}, Pass2={p2}. Error: {e}")
            sys.exit(1)
        barrier()

        print(f"Running benchmark {N_REPS} times...")
        for rep in range(N_REPS):
            print(f"[{rep+1:02d}/{N_REPS}] Synchronizing barrier... ", end="", flush=True)
            barrier()
            print("Executing... ", end="", flush=True)

            cmd = [
                "./bin/prj",
                "-r", str(N_TUPLES),
                "-s", str(N_TUPLES),
                "-u",
                "-t", str(N_THREADS),
                "-n", str(N_CLIENTS),
                "-i", str(args.node_id)
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                print(f"FAILED (Exit code {result.returncode})")
                print(f"Error: {result.stderr.strip()}")
                continue

            with open(output_csv, mode="a") as csv_file:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        csv_file.write(line + "\n")

            print("OK")

    print(f"\nBenchmarking complete! Results saved to: {output_csv}")

if __name__ == "__main__":
    main()