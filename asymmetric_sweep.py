#!/usr/bin/env python3

import socket
import threading
import subprocess
import sys
import time

# --- Configuration ---
SERVER_IP    = "10.10.20.211"
SERVER_PORT  = 9999
N_CLIENTS    = 2
N_REPS       = 5  # Modified to 5 reps per size
CSV_FILE     = "results_asymmetric_sweep.csv"
IS_SERVER    = "--server" in sys.argv
NODE_ID      = 0 if IS_SERVER else 1

# Base tuple volume unit (67108864 tuples * 16 bytes = 1 GiB)
GIB_TUPLES   = 67108864

# --- Server Operations ---
def server_loop():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((SERVER_IP, SERVER_PORT))
            s.listen()
        except OSError:
            return  # Fallback if port is in use or IP mismatch

        while True:
            # Block until exactly N_CLIENTS connect, then release them all
            clients = [s.accept()[0] for _ in range(N_CLIENTS)]
            for c in clients:
                c.sendall(b"R")
                c.close()

# --- Client Operations ---
def barrier():
    while True:
        try:
            with socket.create_connection((SERVER_IP, SERVER_PORT), timeout=5) as s:
                if s.recv(1) == b"R":
                    return
        except Exception:
            time.sleep(1) # Retry

def run_benchmark_group(f, scenarios, group_title):
    """Helper function to cleanly iterate and inject non-CSV logging data."""
    print(f"\n[*] Starting {group_title}")
    f.write(f"# ==========================================\n")
    f.write(f"# {group_title}\n")
    f.write(f"# ==========================================\n")
    
    for r_tuples, s_tuples, label in scenarios:
        print(f"    Running {label}...")
        f.write(f"# --- {label} (R: {r_tuples}, S: {s_tuples}) ---\n")
        f.flush()
        
        cmd = f"./bin/prj -r {r_tuples} -s {s_tuples} -t 64 -n {N_CLIENTS} -i {NODE_ID}".split()
        
        for rep in range(N_REPS):
            barrier() # Lockstep sync across cluster nodes
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                sys.exit(f"[!] Binary failed with exit code {result.returncode}\n{result.stderr}")
            
            # Append execution outputs
            for line in result.stdout.strip().split('\n'):
                if line: 
                    f.write(line + "\n")
            f.flush()

def run_sweep():
    # Build a single initial compilation check without clean or config alterations
    print("[*] Performing baseline project compilation...")
    if subprocess.run(["make", "-j"], stdout=subprocess.DEVNULL).returncode != 0:
        print("[!] Initial compilation failed. Proceeding assuming binary exists.")

    with open(CSV_FILE, "w") as f:
        # Standard header row
        f.write("r_tuples,s_tuples,fill,fill_param,n_threads,radix_bits,n_tuples,total,part_distr,part_assign,part_local,build_probe,per_tuple\n")
        
        # -------------------------------------------------------------------------
        # Scenario 1: Dimension-Fact Pattern (Fixed Build R, Scaling Probe S)
        # -------------------------------------------------------------------------
        s1 = [
            (1 * GIB_TUPLES, 1 * GIB_TUPLES, "S1: R=1GiB, S=1GiB"),
            (1 * GIB_TUPLES, 4 * GIB_TUPLES, "S1: R=1GiB, S=4GiB"),
            (1 * GIB_TUPLES, 16 * GIB_TUPLES, "S1: R=1GiB, S=16GiB"),
            (1 * GIB_TUPLES, 32 * GIB_TUPLES, "S1: R=1GiB, S=32GiB"),
            (1 * GIB_TUPLES, 64 * GIB_TUPLES, "S1: R=1GiB, S=64GiB"),
        ]
        run_benchmark_group(f, s1, "SCENARIO 1: Dimension-Fact Pattern (Fixed Build, Scaling Probe)")

        # -------------------------------------------------------------------------
        # Scenario 2: Cache Stress Test Pattern (Scaling Build R, Fixed Probe S)
        # -------------------------------------------------------------------------
        s2 = [
            (GIB_TUPLES // 4, 64 * GIB_TUPLES, "S2: R=256MiB, S=64GiB"),
            (1 * GIB_TUPLES,  64 * GIB_TUPLES, "S2: R=1GiB, S=64GiB"),
            (4 * GIB_TUPLES,  64 * GIB_TUPLES, "S2: R=4GiB, S=64GiB"),
            (16 * GIB_TUPLES, 64 * GIB_TUPLES, "S2: R=16GiB, S=64GiB"),
            (64 * GIB_TUPLES, 64 * GIB_TUPLES, "S2: R=64GiB, S=64GiB"),
        ]
        run_benchmark_group(f, s2, "SCENARIO 2: Cache Stress Test Pattern (Scaling Build, Fixed Probe)")

        # -------------------------------------------------------------------------
        # Scenario 3: Asymmetry Spectrum Pattern (Fixed Total Footprint = 64 GiB)
        # -------------------------------------------------------------------------
        s3 = [
            (32 * GIB_TUPLES, 32 * GIB_TUPLES, "S3: Ratio 1:1 (R=32GiB, S=32GiB)"),
            (16 * GIB_TUPLES, 48 * GIB_TUPLES, "S3: Ratio 1:3 (R=16GiB, S=48GiB)"),
            (8 * GIB_TUPLES,  56 * GIB_TUPLES, "S3: Ratio 1:7 (R=8GiB, S=56GiB)"),
            (4 * GIB_TUPLES,  60 * GIB_TUPLES, "S3: Ratio 1:15 (R=4GiB, S=60GiB)"),
            (2 * GIB_TUPLES,  62 * GIB_TUPLES, "S3: Ratio 1:31 (R=2GiB, S=62GiB)"),
            (1 * GIB_TUPLES,  63 * GIB_TUPLES, "S3: Ratio 1:63 (R=1GiB, S=63GiB)"),
        ]
        run_benchmark_group(f, s3, "SCENARIO 3: Asymmetry Spectrum Pattern (Fixed Total Volume = 64GiB)")

# --- Entry Point ---
if __name__ == "__main__":
    if IS_SERVER:
        threading.Thread(target=server_loop, daemon=True).start()
        time.sleep(1) # Let the background barrier socket bind completely

    try:
        run_sweep()
    except KeyboardInterrupt:
        print("\n[!] Aborted.")
    finally:
        print("Done.")
