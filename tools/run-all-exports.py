import os
import sys
import subprocess
import time
from datetime import datetime
import signal

# Define the directory where all the scripts are located
script_dir = '/Users/victorproust/Documents/Work/Priority/Script'

# List of scripts to run in the specified order
scripts_to_run = [
    "10. Script for Product All v8.py",
    "11. Script for Financial Parameters for Parts v1.py",
    "12. Script for MRP for Parts v1.py",
    "20. Script for Vendor All v2.py",
    "21. Script for Financial Parameters for Vendors v1.py",
    "22. Script for Vendor Price Lists v2.py",
    "30. Script for Customer All v14.py",
    "31. Script for Financial Parameters for Customers v2.py",
    "32. Script for Customer Price Lists v5.py"
]

# Function to get current timestamp for logging
def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Function to run each script with improved output and timeout handling
def run_scripts():
    total_scripts = len(scripts_to_run)
    successful = 0
    failed = 0
    summary = []

    print(f"\n[{get_timestamp()}] Starting execution of {total_scripts} scripts...\n")

    for i, script in enumerate(scripts_to_run, 1):
        script_path = os.path.join(script_dir, script)
        
        # Check if script exists
        if not os.path.exists(script_path):
            print(f"[{get_timestamp()}] ERROR: Script '{script}' not found at {script_path}. Skipping.")
            summary.append(f"Script '{script}': Skipped (Not Found)")
            failed += 1
            continue

        print(f"[{get_timestamp()}] ({i}/{total_scripts}) Running script: {script}")
        start_time = time.time()

        try:
            # Run the script with a timeout (e.g., 300 seconds = 5 minutes)
            result = subprocess.run(
                [sys.executable, script_path],
                check=True,
                capture_output=True,
                text=True,
                timeout=300  # Adjust timeout as needed
            )
            elapsed_time = time.time() - start_time
            print(f"[{get_timestamp()}] Script '{script}' completed successfully in {elapsed_time:.2f} seconds.")
            if result.stdout:
                print("Output:\n", result.stdout.strip())
            if result.stderr:
                print("Warnings/Errors:\n", result.stderr.strip())
            summary.append(f"Script '{script}': Success (Completed in {elapsed_time:.2f} seconds)")
            successful += 1

        except subprocess.TimeoutExpired:
            print(f"[{get_timestamp()}] ERROR: Script '{script}' timed out after 300 seconds.")
            summary.append(f"Script '{script}': Failed (Timeout)")
            failed += 1

        except subprocess.CalledProcessError as e:
            elapsed_time = time.time() - start_time
            print(f"[{get_timestamp()}] ERROR: Script '{script}' failed with exit code {e.returncode} in {elapsed_time:.2f} seconds.")
            print("Standard Output:\n", e.stdout.strip() if e.stdout else "No output")
            print("Standard Error:\n", e.stderr.strip() if e.stderr else "No error output")
            summary.append(f"Script '{script}': Failed (Exit Code {e.returncode})")
            failed += 1

        except Exception as e:
            elapsed_time = time.time() - start_time
            print(f"[{get_timestamp()}] UNEXPECTED ERROR: Script '{script}' failed in {elapsed_time:.2f} seconds: {e}")
            summary.append(f"Script '{script}': Failed (Unexpected Error: {e})")
            failed += 1

        # Ensure output is flushed to terminal in real-time
        sys.stdout.flush()
        print("-" * 50)

    # Print summary
    print(f"\n[{get_timestamp()}] Execution Summary:")
    print(f"Total Scripts: {total_scripts}")
    print(f"Successful: {successful}")
    print(f"Failed/Skipped: {failed}")
    print("\nDetailed Summary:")
    for line in summary:
        print(line)
    print(f"\n[{get_timestamp()}] All scripts processed.\n")

# Handle keyboard interrupt gracefully
def signal_handler(sig, frame):
    print(f"\n[{get_timestamp()}] Execution interrupted by user.")
    sys.exit(1)

# Run the function
if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    run_scripts()