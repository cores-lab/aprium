#!/usr/bin/env python3

import os
import sys
import subprocess
import argparse
import socket
import time
import threading

# ==== CONFIGURATION ===========================================================
SERVER_IP = "10.0.0.1"
SERVER_PORT = 50051
N_CLIENTS = 2
N_REPS = 10
N_THREADS = 32
GIB_TUPLES = 67108864 # 1 GiB
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

def main():
    parser = argparse.ArgumentParser(description="Distributed Relation Size Sweep")
    parser.add_argument("--out", type=str, required=True, help="Output directory for results")
    parser.add_argument("--node-id", type=int, required=True, help="ID of this node")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    output_csv = os.path.join(args.out, "relation_sweep.csv")

    if args.node_id == 0:
        print(f"Initializing Barrier Server on port {SERVER_PORT}...")
        threading.Thread(target=server_loop, daemon=True).start()
        time.sleep(1)

    print("Compiling project...")
    try:
        subprocess.run(["make", "clean"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["make", "-j$(nproc)"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        print(f"Compilation failed. Error: {e}")
        sys.exit(1)

    with open(output_csv, "w") as csv_file:
        csv_file.write("r_tuples,s_tuples,fill,fill_param,n_threads,radix_bits_pass1,radix_bits_pass2,n_tuples,total,part_distr,part_assign,part_local,build_probe,per_tuple\n")

    s = []
    # Symmetric Pattern (1 GiB to 64 GiB)
    s += [((2**i) * GIB_TUPLES, (2**i) * GIB_TUPLES) for i in range(0, 7)]

    # Streaming Stress Pattern (Fixed Build R, Scaling Probe S)
    s += [(1 * GIB_TUPLES, (2**i) * GIB_TUPLES) for i in range(0, 7)]

    # Cache Stress Pattern (Scaling Build R, Fixed Probe S)
    s += [((2**i) * GIB_TUPLES, 64 * GIB_TUPLES) for i in range(0, 7)]

    # Asymmetric Pattern (Fixed Footprint = 64 GiB)
    s += [
        (32 * GIB_TUPLES, 32 * GIB_TUPLES),
        (16 * GIB_TUPLES, 48 * GIB_TUPLES),
        (8 * GIB_TUPLES,  56 * GIB_TUPLES),
        (4 * GIB_TUPLES,  60 * GIB_TUPLES),
        (2 * GIB_TUPLES,  62 * GIB_TUPLES),
        (1 * GIB_TUPLES,  63 * GIB_TUPLES),
    ]

    print(f"Total scenarios to test: {len(s)}")

    for r_tuples, s_tuples in s:
        r_gib = (r_tuples * 16) / (1024 ** 3)
        s_gib = (s_tuples * 16) / (1024 ** 3)
        print(f"\n==== Target Sizes: R = {r_gib:.2f} GiB, S = {s_gib:.2f} GiB ====")

        print(f"Running benchmark {N_REPS} times...")
        for rep in range(N_REPS):
            print(f"[{rep+1:02d}/{N_REPS}] Synchronizing barrier... ", end="", flush=True)
            barrier()
            print("Executing... ", end="", flush=True)

            cmd = [
                "./bin/aprium",
                "-r", str(r_tuples),
                "-s", str(s_tuples),
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