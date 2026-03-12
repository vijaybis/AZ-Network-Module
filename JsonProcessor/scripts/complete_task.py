#!/usr/bin/env python3
"""
Azure Batch - Rebuild and Test Script
This script:
1. Recreates the batch pool (if needed).
2. Submits a new test job.
3. Shows monitoring commands.
"""

import os
import sys
import json
import subprocess
from datetime import datetime


def run_command(command):
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, shell=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        sys.exit(1)


def load_config(config_path):
    """Load configuration from a JSON file."""
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        print("Please create config/config.json from config/config.sample.json")
        sys.exit(1)

    with open(config_path, "r") as f:
        return json.load(f)


def recreate_pool(config):
    """Recreate the Azure Batch pool."""
    print("Recreating Batch pool...")
    try:
        run_command("python scripts/create_batch_pool.py")
        print("Pool recreated successfully.")
    except Exception as e:
        print(f"Failed to recreate pool: {e}")
        print("Continuing with existing pool...")


def submit_job(config):
    """Submit a test job to Azure Batch."""
    print("Submitting test job...")

    # Generate synthetic data if no samples exist
    sample_dir = "samples"
    if not os.path.exists(sample_dir) or len(os.listdir(sample_dir)) == 0:
        print("No sample files found. Generating synthetic data...")
        run_command("python scripts/generate_data.py --output samples/ --count 3")

    # Upload files to Azure Storage
    print("Uploading test files to storage...")
    run_command("python scripts/upload_storage.py --container batch-input --path samples/")

    # Submit the batch job
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    job_id = f"test-{timestamp}"
    pool_id = config["azure"]["batch"]["pool_id"]

    print(f"Submitting batch job with ID: {job_id}")
    run_command(f"python scripts/submit_task.py --pool-id {pool_id} --job-id {job_id}")

    print(f"Job submitted successfully: {job_id}")
    return job_id


def show_monitoring_commands(config, job_id):
    """Show commands to monitor the job and download results."""
    batch_account_name = config["azure"]["batch"]["account_name"]
    batch_account_url = config["azure"]["batch"]["account_url"]

    print("")
    print("Monitoring Commands")
    print("===================")
    print(f"Monitor job status:")
    print(f"  az batch job show --job-id {job_id} --account-name {batch_account_name} --account-endpoint {batch_account_url}")
    print("")
    print("List all tasks:")
    print(f"  az batch task list --job-id {job_id} --account-name {batch_account_name} --account-endpoint {batch_account_url} --output table")
    print("")
    print("Get task output (replace task-0 with actual task ID):")
    print(f"  az batch task file download --job-id {job_id} --task-id task-0 --file-path stdout.txt --destination logs/stdout.txt --account-name {batch_account_name} --account-endpoint {batch_account_url}")
    print("")
    print("Get task errors:")
    print(f"  az batch task file download --job-id {job_id} --task-id task-0 --file-path stderr.txt --destination logs/stderr.txt --account-name {batch_account_name} --account-endpoint {batch_account_url}")
    print("")
    print("Download results when complete:")
    print(f"  python scripts/download_results.py --output results/")
    print("")
    print("Next Steps:")
    print("1. Wait 2-3 minutes for tasks to start.")
    print("2. Use the monitoring commands above to check status.")
    print("3. Check Azure Portal → Batch Account → Jobs → {job_id} for detailed status.")
    print("4. Look for task output files to verify container execution.")
    print("")


def main():
    # Load configuration
    config = load_config(config_path) # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "config", "config.json")

    # Recreate pool if requested
    recreate_pool(config)

    # Submit a test job
    job_id = submit_job(config)

    # Show monitoring commands
    show_monitoring_commands(config, job_id)


if __name__ == "__main__":
    main()