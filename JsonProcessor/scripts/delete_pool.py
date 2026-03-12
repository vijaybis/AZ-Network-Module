#!/usr/bin/env python3
"""
Azure Batch Cleanup Script
This script deletes a specific Azure Batch pool and all jobs running in that pool.
Configuration is loaded from a `config.json` file located in the adjacent `config` folder.
"""

import os
import sys
import json
import subprocess
from typing import Dict, Any


def run_command(command: str) -> str:
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, shell=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running command: {command}")
        print(f"   stdout: {e.stdout}")
        print(f"   stderr: {e.stderr}")
        sys.exit(1)


def load_config() -> Dict[str, Any]:
    """Load configuration from config.json file."""
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the path to the config.json file in the adjacent config folder
    config_path = os.path.join(script_dir, "..", "config", "config.json")

    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_path}")
        print("Please create a valid config/config.json file.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in configuration file: {e}")
        sys.exit(1)


def check_pool_exists(account_name: str, pool_id: str) -> bool:
    """Check if a pool with the given ID already exists."""
    print(f"🔍 Checking if pool '{pool_id}' exists...")
    command = f"az batch pool show --account-name {account_name} --pool-id {pool_id}"
    try:
        run_command(command)
        print(f"✅ Pool '{pool_id}' exists.")
        return True
    except subprocess.CalledProcessError:
        print(f"❌ Pool '{pool_id}' does not exist.")
        return False


def list_jobs(account_name: str, pool_id: str) -> list:
    """List all jobs associated with the specified pool."""
    print(f"🔍 Listing jobs for pool '{pool_id}'...")
    command = f"az batch job list --account-name {account_name} --query \"[?poolInfo.poolId=='{pool_id}'].id\" -o json"
    output = run_command(command)
    try:
        job_ids = json.loads(output)  # Parse the output as a JSON list
        if not job_ids:
            print(f"✅ No jobs found for pool '{pool_id}'.")
        else:
            print(f"📋 Found {len(job_ids)} job(s) for pool '{pool_id}': {job_ids}")
        return job_ids
    except json.JSONDecodeError:
        print("❌ Failed to parse job list. Please check the output format.")
        sys.exit(1)


def delete_jobs(account_name: str, job_ids: list):
    """Delete all jobs associated with the specified pool."""
    for job_id in job_ids:
        print(f"🗑️ Deleting job '{job_id}'...")
        command = f"az batch job delete --account-name {account_name} --job-id {job_id} --yes"
        run_command(command)
        print(f"✅ Job '{job_id}' deleted.")


def delete_pool(account_name: str, pool_id: str):
    """Delete the specified Batch pool."""
    print(f"🗑️ Deleting pool '{pool_id}'...")
    command = f"az batch pool delete --account-name {account_name} --pool-id {pool_id} --yes"
    run_command(command)
    print(f"✅ Pool '{pool_id}' deleted.")


def main():
    # Load configuration
    config = load_config()
    azure_config = config['azure']

    batch_account_name = azure_config['batch']['account_name']
    pool_id = azure_config['batch']['pool_id']

    print("🚀 Starting Azure Batch Cleanup Script...")
    print("=" * 50)

    # Step 1: Check if the pool exists
    if check_pool_exists(batch_account_name, pool_id):
        # Step 2: List jobs associated with the pool
        job_ids = list_jobs(batch_account_name, pool_id)

        # Step 3: Delete the jobs
        if job_ids:
            delete_jobs(batch_account_name, job_ids)

        # Step 4: Delete the pool
        delete_pool(batch_account_name, pool_id)
    else:
        print(f"❌ Pool '{pool_id}' does not exist. No action needed.")

    print("🎉 Cleanup completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    main()