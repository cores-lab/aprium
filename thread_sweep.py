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
N_REPS       = 10
N_TUPLES     = 536870912
CSV_FILE     = "results_thread_sweep_cxl.csv"
IS_SERVER    = "--server" in sys.argv
NODE_ID      = 0 if IS_SERVER else 1

# Thread counts to sweep: 1, 2, 4, then +4 until 64
THREAD_COUNTS = [1, 2, 4] + list(range(8, 65, 4))

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

def run_sweep():
    with open(CSV_FILE, "w") as f:
        f.write("r_tuples,s_tuples,fill,fill_param,n_threads,radix_bits,n_tuples,total,part_distr,part_assign,part_local,build_probe,per_tuple\n")

    # Compile ONCE before the sweep
    subprocess.run(["make", "clean"], stdout=subprocess.DEVNULL)
    if subprocess.run(["make", "-j"], stdout=subprocess.DEVNULL).returncode != 0:
        sys.exit("[!] Compilation failed. Exiting.")

    for threads in THREAD_COUNTS:
        print(f"[*] Testing THREADS = {threads}")

        # Execute at the fixed relation size, passing the current thread count
        cmd = f"./bin/prj -r {N_TUPLES} -s {N_TUPLES} -t {threads} -n {N_CLIENTS} -i {NODE_ID}".split()

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
        print("Done.")
