#!/usr/bin/env python3

import socket
import threading
import subprocess
import sys
import time
import re

# --- Configuration ---
SERVER_IP    = "10.10.20.211"
SERVER_PORT  = 9999
N_CLIENTS    = 2
N_REPS       = 10
MIN_ZIPF     = 0.0
MAX_ZIPF     = 1.5
ZIPF_STEP    = 0.1
N_TUPLES     = 536870912
CONFIG_FILE  = "./src/config.h"
CSV_FILE     = "results_zipf_8gib.csv"
IS_SERVER    = "--server" in sys.argv
NODE_ID      = 0 if IS_SERVER else 1

# --- Server Operations ---
def server_loop():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((SERVER_IP, SERVER_PORT))
            s.listen()
        except OSError:
            return 

        while True:
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
            time.sleep(1)

def update_config(zipf_val):
    zipf_str = f"{zipf_val:.1f}"
    with open(CONFIG_FILE, 'r') as f:
        cfg = re.sub(r'^#define ZIPF\s+[0-9]*\.?[0-9]+', f'#define ZIPF {zipf_str}', f.read(), flags=re.MULTILINE)
    with open(CONFIG_FILE, 'w') as f:
        f.write(cfg)

def run_sweep():
    with open(CSV_FILE, "w") as f:
        f.write("r_tuples,s_tuples,fill,fill_param,n_threads,radix_bits,n_tuples,total,part_distr,part_assign,part_local,build_probe,per_tuple\n")

    num_steps = int(round((MAX_ZIPF - MIN_ZIPF) / ZIPF_STEP)) + 1
    
    for i in range(num_steps):
        zipf_val = MIN_ZIPF + (i * ZIPF_STEP)
        print(f"[*] Testing ZIPF = {zipf_val:.1f}")
        update_config(zipf_val)

        # Compile
        subprocess.run(["make", "clean"], stdout=subprocess.DEVNULL)
        if subprocess.run(["make", "-j"], stdout=subprocess.DEVNULL).returncode != 0:
            print(f"[!] Compilation failed for ZIPF {zipf_val:.1f}. Skipping.")
            continue

        # Execute at the fixed relation size
        cmd = f"./bin/prj -r {N_TUPLES} -s {N_TUPLES} -t 64 -n {N_CLIENTS} -i {NODE_ID}".split()

        for rep in range(N_REPS):
            barrier() # Lockstep sync across nodes

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                sys.exit(f"[!] Binary failed with exit code {result.returncode}\n{result.stderr}")

            # Append standard output
            with open(CSV_FILE, "a") as f:
                for line in result.stdout.strip().split('\n'):
                    if line: f.write(line + "\n")

# --- Entry Point ---
if __name__ == "__main__":
    if IS_SERVER:
        threading.Thread(target=server_loop, daemon=True).start()
        time.sleep(1)

    try:
        run_sweep()
    except KeyboardInterrupt:
        print("\n[!] Aborted.")
    finally:
        # Restore a sane default
        update_config(0.0)
        print("Done.")
