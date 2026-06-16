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
N_STEPS      = 7
N_REPS       = 10
MIN_BITS     = 16
MAX_BITS     = 24
CONFIG_FILE  = "./src/config.h"
CSV_FILE     = "results_sweep.csv"
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

def update_config(bits):
    with open(CONFIG_FILE, 'r') as f:
        cfg = re.sub(r'^#define N_RADIX_BITS\s+\d+', f'#define N_RADIX_BITS {bits}', f.read(), flags=re.MULTILINE)
    with open(CONFIG_FILE, 'w') as f:
        f.write(cfg)

def run_sweep():
    with open(CSV_FILE, "w") as f:
        f.write("r_tuples,s_tuples,fill,fill_param,n_threads,radix_bits,n_tuples,total,part_distr,part_assign,part_local,build_probe,per_tuple\n")

    for bits in range(MIN_BITS, MAX_BITS + 1):
        print(f"[*] Testing N_RADIX_BITS = {bits}")
        update_config(bits)

        # Compile
        subprocess.run(["make", "clean"], stdout=subprocess.DEVNULL)
        if subprocess.run(["make", "-j"], stdout=subprocess.DEVNULL).returncode != 0:
            print(f"[!] Compilation failed. Skipping.")
            continue

        # Execute
        for step in range(N_STEPS):
            n_tuples = 67108864 * (2 ** step)

            cmd = f"./bin/prj -r {n_tuples} -s {n_tuples} -t 64 -n {N_CLIENTS} -i {NODE_ID}".split()

            for rep in range(N_REPS):
                barrier() # Lockstep sync

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
        time.sleep(1) # Give the server socket a brief moment to bind

    try:
        run_sweep()
    except KeyboardInterrupt:
        print("\n[!] Aborted.")
    finally:
        update_config(18) # Restore default config
        print("Done.")
