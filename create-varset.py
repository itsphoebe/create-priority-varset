"""
create-varset.py

This script automates the management of global priority variable sets (varsets) across all organizations in a Terraform Enterprise (TFE) instance. 
It supports creating, updating, and deleting a specific global priority varset for each organization, and synchronizes the variables within the varset according to a predefined configuration.

Features:
- Creates a global, priority varset with a specified name and description for each organization.
- Adds predefined variables to the varset, with support for variable attributes such as sensitivity, category, and HCL flag.
- Updates existing varsets by comparing current variables with the desired configuration, adding new variables, updating changed ones, and deleting variables not present in the desired list.
- Deletes the global priority varset from each organization if requested.
- Handles API authentication via an admin token, via prompting or environment variable: TFE_ADMIN_TOKEN.
- Supports dry-run mode to preview changes without making modifications.
- Uses multithreading to process multiple organizations concurrently for improved performance.
- Logs detailed output to both the console and a log file.
- Reports total script runtime at completion.
- Generates a CSV report (`varset_report_%Y%m%d_%H%M%S.csv`) summarizing all actions, changes, and errors for each organization and variable.

Usage Flow:
1. Create:  
   Start by running the script with `--mode create` to create the global priority varset and add the initial set of variables to each organization.
2. Update:  
   When you want to synchronize or change the variables in the varset (add, update, or remove variables), run the script with `--mode update`.
3. Delete:  
   If you need to remove the global priority varset from organizations, run the script with `--mode delete`.

Usage:
    python create-varset.py --mode [create|update|delete] --config CONFIG_FILE [--orgs ORGS] [--dry-run] [--log-level LEVEL] [--max-workers N]

Arguments:
    --mode: Operation mode. 
        - 'create': Create the global priority varset and add variables (default).
        - 'update': Synchronize variables in the varset with the desired configuration.
        - 'delete': Delete the global priority varset from each organization.
    --config: Path to YAML config file (required).
    --orgs: Path to a file with org names (one per line) or comma-separated list of org names.
    --dry-run: Show what would change, but do not make any changes.
    --log-level: Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    --max-workers: Number of concurrent threads to use for processing organizations (default: 5).

Environment:
    - Requires an admin API token for authentication.
    - Set the token using the TFE_ADMIN_TOKEN environment variable, or you will be prompted securely.

Notes:
    - Sensitive variable values cannot be read back from the API; updates may overwrite them.
    - Designed for administrative use; use with caution in production environments.
    - Everything will be logged to both the console and 'execution.log'.
    - A CSV report of all actions and errors will be written to 'varset_report_%Y%m%d_%H%M%S.csv'.
    - Organization selection precedence:
        1. If the optional --orgs flag is provided:
            - If the value is a path to a file, each line in the file is treated as an organization name.
            - Otherwise, the value is parsed as a comma-separated list of organization names (e.g, org1,org2,org3).
        2. If --orgs is not provided, but the config file (provided via --config) contains an 'organizations' key, those organizations are used.
        3. If neither of the above are provided, the script will fetch and process all organizations available in the TFE instance.
"""

import getpass
import requests
import time
import argparse
import yaml
import os
import logging
import concurrent.futures
import csv
import threading
import sys
import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

tfe_url = None 
varset_name = None
varset_description = None
varset_vars = None

api_prefix = "/api/v2/"
admin_token = ""
headers = {}
report_rows = []
report_lock = threading.Lock()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("execution.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.INFO)

# Read in config file
def load_config(config_path="config.yaml"):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
        validate_config(config)
    return config

# Validate config file. Required keys: tfe_url, varset_name, varset_vars
def validate_config(config):
    required = ["tfe_url", "varset_name", "varset_vars"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")

# Write row to CSV report
def log_report(org, action, varset_id=None, variable=None, status="success", message=""):
    with report_lock:
        report_rows.append({
            "org": org,
            "action": action,
            "variable set ID": varset_id or "",
            "variable": variable or "",
            "status": status,
            "message": message
        })

# Get list of orgs with pagination
def list_orgs():
    orgs = []
    page_number = 1
    page_size = 100 # 100 is the max page size

    while True:
        try:
            url = f"{tfe_url}{api_prefix}organizations?page[number]={page_number}&page[size]={page_size}"
            response = session.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Create list with orgs from paged response
            page_orgs = [org["id"] for org in data["data"]]

            # If no more orgs are returned, break loop
            if not page_orgs:
                break

            orgs.extend(page_orgs)
            logger.info(f"Retrieved {len(page_orgs)} orgs from page {page_number}")

            # Stop if no more pages
            if not data.get("links", {}).get("next"):
                break

            page_number += 1

        except requests.exceptions.RequestException as e:
            logger.error(f"Error listing orgs on page {page_number}: {e}")
            break
    
    return orgs

# Create a global priority varset for each org
def create_global_priority_varset(org_name, dry_run=False):
    url = f"{tfe_url}{api_prefix}organizations/{org_name}/varsets"
    payload = {
        "data": {
            "type": "varsets",
            "attributes": {
                "name": varset_name,
                "description": varset_description,
                "global": True,
                "priority": True
            }
        }
    }

    if dry_run:
        existing_id = get_global_priority_varset_id(org_name)
        if existing_id:
            logger.info(f"[DRY RUN] Varset '{varset_name}' already exists for org {org_name} (id: {existing_id}). Would not create.")
        else:
            logger.info(f"[DRY RUN] Would create varset for org {org_name} with payload: {payload}")
            for var in varset_vars:
                logger.info(f"[DRY RUN] Would add variable {var['key']} to varset for org {org_name}")
        return

    try:
        response = session.post(url, headers=headers, json=payload)
        if response.status_code == 201:
            varset_id = response.json()["data"]["id"]
            logger.info(f"Varset created for org {org_name} with ID {varset_id}")
            log_report(org_name, "create_varset", varset_id, status="success", message=f"Varset created for org {org_name} with ID {varset_id}")

            for var in varset_vars:
                add_variable(org_name, varset_id, var, dry_run=dry_run)

        elif response.status_code == 422:
            if response.json()["errors"][0]["detail"] == "Name has already been taken":
                logger.warning(f"! Varset {varset_name} already exists for org {org_name}")
                log_report(org_name, "create_varset", status="skipped", message="Already exists")
            else:
                logger.error(f"! Problem creating varset for org {org_name}: {response.status_code} - {response.text}")
                log_report(org_name, "create_varset", status="error", message=response.text)
        else:
            logger.error(f"! Organization {org_name} not found: {response.status_code} - {response.text}")
            log_report(org_name, "create_varset", status="error", message=response.text)
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to create global priority varset failed: {e}")
        log_report(org_name, "create_varset", status="error", message=str(e))

# Add a variable to the varset, will be adding variables based on the varset_vars list
def add_variable(org_name, varset_id, var, dry_run=False):
    url = f"{tfe_url}{api_prefix}varsets/{varset_id}/relationships/vars"
    payload = {
        "data": {
            "type": "vars",
            "attributes": {
                "key": var["key"],
                "value": var.get("value", ""),
                "description": var.get("description", ""),
                "sensitive": var.get("sensitive", False),
                "category": var.get("category", "terraform"),
                "hcl": var.get("hcl", False)
            }
        }
    }

    if dry_run:
        logger.info(f"[DRY RUN] Would add variable {var['key']} to varset {varset_id} with payload: {payload}")
        return

    try:
        response = session.post(url, headers=headers, json=payload)
        if response.status_code == 201:
            logger.info(f"+ Variable {var['key']} added to varset {varset_id}")
            log_report(org_name, "add_variable", varset_id, variable=var["key"], status="success", message=f"Variable {var['key']} added to varset {varset_id}")
        elif response.status_code == 422:
            logger.error(f"! Problem adding variable {var['key']} to varset {varset_id}: {response.status_code} - {response.text}")
            log_report(org_name, "add_variable", varset_id, variable=var["key"], status="error", message=response.text)
        else:
            logger.error(f"! Varset {varset_id} not found: {response.status_code} - {response.text}")
            log_report(org_name, "add_variable", varset_id, variable=var["key"], status="error", message=response.text)
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to add variable to varset failed: {e}")
        log_report(org_name, "add_variable", varset_id, status="error", message=str(e))

def delete_global_priority_varset(org_name, dry_run=False):
    varset_id = get_global_priority_varset_id(org_name)
    if not varset_id:
        logger.warning(f"No global priority varset found for org {org_name}")
        log_report(org_name, "delete_varset", varset_id, status="error",message=f"No global priority varset found for org {org_name}")
        return

    url = f"{tfe_url}{api_prefix}varsets/{varset_id}"
    if dry_run:
        logger.info(f"[DRY RUN] Would delete varset {varset_id} for org {org_name}")
        return

    try: 
        response = session.delete(url, headers=headers)
        if response.status_code == 204:
            logger.info(f"- Varset {varset_id} deleted for org {org_name}")
            log_report(org_name, "delete_varset", varset_id, status="success", message=f"Varset {varset_id} deleted for org {org_name}")
        elif response.status_code == 404:
            logger.error(f"! Varset {varset_id} not found for org {org_name}: {response.status_code} - {response.text}")
            log_report(org_name, "delete_variable", varset_id, status="error", message=response.text)
        else:
            logger.error(f"! Error deleting varset {varset_id} for org {org_name}: {response.status_code} - {response.text}")
            log_report(org_name, "delete_variable", varset_id, status="error", message=response.text)
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to delete varset failed: {e}")
        log_report(org_name, "delete_variable", varset_id, status="error", message=str(e))

def delete_variable(org_name, varset_id, var_id, var_name, dry_run=False):
    url = f"{tfe_url}{api_prefix}varsets/{varset_id}/relationships/vars/{var_id}"
    if dry_run:
        logger.info(f"[DRY RUN] Would delete variable {var_name} (id {var_id}) from varset {varset_id}")
        return

    try:
        response = session.delete(url, headers=headers)
        response.raise_for_status()

        if response.status_code == 204:
            logger.info(f"- Variable {var_name} was deleted from varset because it was not in the desired list")
            log_report(org_name, "delete_variable", varset_id, variable=var_name, status="success", message=f"Variable {var_name} was deleted from varset because it was not in the desired list")
        else:
            logger.error(f"! Error deleting variable {var_name}: {response.status_code} - {response.text}")
            log_report(org_name, "delete_variable", varset_id, variable=var_name, status="error", message=response.text)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request to update variable failed: {e}")
        log_report(org_name, "delete_variable", varset_id, variable=var_name, status="error", message=str(e))

def update_variable(org_name, varset_id, var_id, desired, dry_run=False):
    url = f"{tfe_url}{api_prefix}varsets/{varset_id}/relationships/vars/{var_id}"
    payload = {
        "data": {
            "type": "vars",
            "attributes": {
                "key": desired["key"],
                "value": desired.get("value", ""),
                "description": desired.get("description", ""),
                "sensitive": desired.get("sensitive", False),
                "category": desired.get("category", "terraform"),
                "hcl": desired.get("hcl", False)
            }
        }
    }

    if dry_run:
        logger.info(f"[DRY RUN] Would update variable {desired['key']} in varset {varset_id} with payload: {payload}")
        return

    try:
        response = session.patch(url, headers=headers, json=payload)
        response.raise_for_status()

        if response.status_code == 200:
            logger.info(f"~ Variable {desired['key']} updated")
            log_report(org_name, "update_variable", varset_id, variable=desired["key"], status="success", message=f"Variable {desired['key']} updated")
        elif response.status_code == 404:
            logger.error(f"! Varset {varset_id} not found: {response.status_code} - {response.text}")
            log_report(org_name, "update_variable", varset_id, variable=desired["key"], status="error", message=response.text)
        else:
            logger.error(f"! Error updating variable {desired['key']}: {response.status_code} - {response.text}")
            log_report(org_name, "update_variable", varset_id, variable=desired["key"], status="error", message=response.text)
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to update variable failed: {e}")
        log_report(org_name, "update_variable", varset_id, variable=desired["key"], status="error", message=str(e))

def check_diffs_variables_in_varset(org_name, varset_id, varset_vars, dry_run=False):
    current_vars = get_variables_in_varset(varset_id)
    current_dict = {var["attributes"]["key"]: var for var in current_vars}
    desired_dict = {var["key"]: var for var in varset_vars}

    for desired in varset_vars:
        key = desired["key"]
        current = current_dict.get(key)

        # If new variable, add to varset
        if not current:
            add_variable(org_name, varset_id, desired, dry_run=dry_run)
            continue
        # Otherwise, see if we need to update any attributes of an existing variable
        current_attrs = current["attributes"]
        needs_update = any([
            desired["value"] != current_attrs.get("value"),
            desired.get("description", "") != current_attrs.get("description", ""),
            desired.get("sensitive", False) != current_attrs.get("sensitive", False),
            desired.get("category", "terraform") != current_attrs.get("category", "terraform"),
            desired.get("hcl", False) != current_attrs.get("hcl", False),
        ])

        if needs_update:
            update_variable(org_name, varset_id, current["id"], desired, dry_run=dry_run)
        else:
            logger.info(f"No updates found to be made on variable: {current_attrs.get('key')}")
            log_report(org_name, "update_variable", varset_id, variable=current_attrs.get("key"), status="skipped", message=f"No updates found to be made on variable: {current_attrs.get('key')}")
    
    # Delete any variables that are not in the desired list
    for key, var in current_dict.items():
        if key not in desired_dict:
            delete_variable(org_name, varset_id, var["id"], key, dry_run=dry_run)

def update_global_priority_varset(org_name, dry_run=False):
    varset_id = get_global_priority_varset_id(org_name)
    if not varset_id:
        logger.warning(f"! No global priority varset found to update")
        log_report(org_name, "update_varset", varset_id, status="error", message=f"No global priority varset found to update")
        return
    check_diffs_variables_in_varset(org_name, varset_id, varset_vars, dry_run=dry_run)

# Get the varset ID for the global priority varset
def get_global_priority_varset_id(org_name):
    page_number = 1
    page_size = 20 # 100 is the max page size, dont expect as many varsets

    while True:
        url = f"{tfe_url}{api_prefix}organizations/{org_name}/varsets?page[number]={page_number}&page[size]={page_size}"
        try:
            response = session.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            varsets = response.json().get("data", [])

            # Look for the matching varset
            for varset in varsets:
                attrs = varset["attributes"]
                if attrs.get("name") == varset_name and attrs.get("global") and attrs.get("priority"):
                    return varset["id"]
                
            # Stop if no more pages
            if not data.get("links", {}).get("next"):
                break
            page_number += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to get global priority varset failed: {e}")
            break

    return None

# Get variables in a varset
def get_variables_in_varset(varset_id):
    url = f"{tfe_url}{api_prefix}varsets/{varset_id}/relationships/vars"
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to get variables in varset failed: {e}")
        return []

def process_org(org, mode, dry_run):
    logger.info(f"Processing org: {org}")
    if mode == "create":
        logger.info(f"Creating global priority varset for org {org}...")
        create_global_priority_varset(org, dry_run=dry_run)
    elif mode == "delete":
        logger.info(f"Deleting varset for org {org}...")
        delete_global_priority_varset(org, dry_run=dry_run)
    elif mode == "update":
        logger.info(f"Updating varset for org {org}...")
        update_global_priority_varset(org, dry_run=dry_run)
    time.sleep(0.5)  # To avoid API rate limits

# Create a requests session with retries (max 6 retries, exponential backoff)
# Covers 429 for rate limits
def get_requests_session_with_retries(retries=6, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# Create a global session object
session = get_requests_session_with_retries()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create global priority varset for each org in TFE")
    parser.add_argument("--mode", choices=["create", "delete", "update"], default="create", help="Optional: 'create', 'delete', or 'update' the global priority varset. Default is 'create'.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change, but do not make any changes.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--orgs", help="Comma-separated list of org names or path to a file with org names (one per line)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level (default: INFO)")
    parser.add_argument("--max-workers", type=int, default=5, help="Number of concurrent threads (default: 5)")
    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level.upper())
    # Load config file
    config = load_config(args.config)
    tfe_url = config["tfe_url"]
    varset_name = config["varset_name"]
    varset_description = config.get("varset_description", "")                           
    varset_vars = config["varset_vars"]

    # Set admin token from environment variable or prompt user
    admin_token = os.getenv("TFE_ADMIN_TOKEN") or getpass.getpass("Enter your admin token: ")
    
    start_time = time.time()

    headers = {
        "Authorization": f"Bearer {admin_token}",
        "Content-Type": "application/vnd.api+json"
    }

    # Determine organizations to process
    organizations = None
    if args.orgs:
        if os.path.isfile(args.orgs):
            with open(args.orgs, "r") as f:
                organizations = [line.strip() for line in f if line.strip()]
        else:
            organizations = [org.strip() for org in args.orgs.split(",") if org.strip()]
    elif "organizations" in config:
        organizations = config["organizations"]
    else:
        organizations = list_orgs()

    logger.info(f"Found {len(organizations)} orgs")
    logger.info(f"Orgs: {organizations}")

    # Confirm delete action if not dry run
    if args.mode == "delete" and not args.dry_run:
        confirm = input(
            "\nWARNING: You are about to DELETE the global priority varset from "
            f"{len(organizations)} organizations. This action is irreversible.\n"
            "Type 'yes' to continue: "
        )
        if confirm.strip().lower() != "yes":
            print("Aborted by user.")
            sys.exit(0)

    # Process each organization in parallel, max 5 threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [
            executor.submit(process_org, org, args.mode, args.dry_run)
            for org in organizations
        ]
        for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            try:
                future.result()
                logger.info(f"[{i}/{len(organizations)}] Finished processing org")
            except Exception as exc:
                logger.error(f"Error processing org: {exc}")

    # Write CSV report
    if report_rows:
        report_filename = f"varset_report_{datetime.datetime.now():%Y%m%d_%H%M%S}.csv"
        with open(report_filename, "w", newline="") as csvfile:
            fieldnames = ["org", "action", "variable set ID", "variable", "status", "message"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in report_rows:
                writer.writerow(row)
        logger.info(f"CSV report written to {report_filename}")

    errors = [row for row in report_rows if row["status"] != "success"]
    if errors:
        logger.warning(f"{len(errors)} actions failed or were skipped. See {report_filename} for details.")
    else:
        logger.info("All actions completed successfully.")

    end_time = time.time()
    logger.info(f"Total script runtime: {end_time - start_time:.2f} seconds ({(end_time - start_time) / 60:.2f} minutes)")