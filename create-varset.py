"""
create-varset.py
This script automates the management of global priority variable sets (varsets) across all organizations in a Terraform Enterprise (TFE) instance. It supports creating, updating, and deleting a specific global priority varset for each organization, and synchronizes the variables within the varset according to a predefined configuration.
Features:
- Lists all organizations in the TFE instance using paginated API requests.
- Creates a global, priority varset with a specified name and description for each organization.
- Adds a set of predefined variables to the varset, with support for variable attributes such as sensitivity, category, and HCL flag.
- Updates existing varsets by comparing current variables with the desired configuration, adding new variables, updating changed ones, and deleting variables not present in the desired list.
- Deletes the global priority varset from each organization if requested.
- Handles API authentication via an admin token, and manages API rate limits with configurable sleep intervals.
Usage:
    python create-varset.py --mode [create|update|delete]
Arguments:
    --mode: Operation mode. 
        - 'create': Create the global priority varset and add variables (default).
        - 'update': Synchronize variables in the varset with the desired configuration.
        - 'delete': Delete the global priority varset from each organization.
    --dry-run: Show what would change, but do not make any changes.
Environment:
    - Requires an admin API token for authentication.
Note:
    - Sensitive variable values cannot be read back from the API; updates may overwrite them.
    - Designed for administrative use; use with caution in production environments.
"""

import getpass
import requests
import time
import argparse
import yaml
import os

tfe_url = None
varset_name = None
varset_description = None
varset_vars = None
# Use yaml config file instead! 
# Variables to configure
# tfe_url = "https://tfe-migrate-from.phoebe-lee.sbx.hashidemos.io"
# varset_name = "global-proxy-override"
# varset_description = "Global proxy override varset for proxy"
# varset_vars = [
#     {
#       "key": "proxy",
#       "value": "https://proxy.example.com:8080",
#       "description": "Proxy for this and that",
#       "sensitive": False,
#       "category": "terraform",
#       "hcl": False
#     },
#     {
#       "key": "key_example",
#       "value": "61e400d5ccffb3782f215344481e6c82"
#     }
# ]

api_prefix = "/api/v2/"
admin_token = ""
headers = {}

# Read in config
def load_config(config_path="config.yaml"):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

# Get list of orgs with pagination
def list_orgs():
    orgs = []
    page_number = 1
    page_size = 100 # 100 is the max page size

    while True:
        try:
            url = f"{tfe_url}{api_prefix}organizations?page[number]={page_number}&page[size]={page_size}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Create list with orgs from paged response
            page_orgs = [org["id"] for org in data["data"]]

            # If no more orgs are returned, break loop
            if not page_orgs:
                break

            orgs.extend(page_orgs)
            print(f"Retrieved {len(page_orgs)} orgs from page {page_number}")

            # Stop if no more pages
            if not data.get("links", {}).get("next"):
                break

            page_number += 1

        except requests.exceptions.RequestException as e:
            print(f"Error listing orgs on page {page_number}: {e}")
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
            print(f"[DRY RUN] Varset '{varset_name}' already exists for org {org_name} (id: {existing_id}). Would not create.")
        else:
            print(f"[DRY RUN] Would create varset for org {org_name} with payload: {payload}")
            for var in varset_vars:
                print(f"[DRY RUN] Would add variable {var['key']} to varset for org {org_name}")
        return

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 201:
            varset_id = response.json()["data"]["id"]
            print(f"Varset created for org {org_name} with ID {varset_id}")

            for var in varset_vars:
                add_variable(varset_id, var, dry_run=dry_run)

        elif response.status_code == 422:
            if response.json()["errors"][0]["detail"] == "Name has already been taken":
                print(f"! Varset {varset_name} already exists for org {org_name}")
            else:
                print(f"! Problem creating varset for org {org_name}: {response.status_code} - {response.text}")
        else:
            print(f"! Organization {org_name} not found: {response.status_code} - {response.text}")
    
    except requests.exceptions.RequestException as e:
        print(f"Request to create global priority varset failed: {e}")

# Add a variable to the varset, will be adding variables based on the varset_vars list
def add_variable(varset_id, var, dry_run=False):
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
        print(f"[DRY RUN] Would add variable {var['key']} to varset {varset_id} with payload: {payload}")
        return

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 201:
            print(f"+ Variable {var['key']} added to varset {varset_id}")
        elif response.status_code == 422:
            print(f"! Problem adding variable {var['key']} to varset {varset_id}: {response.status_code} - {response.text}")
        else:
            print(f"! Varset {varset_id} not found: {response.status_code} - {response.text}")
    
    except requests.exceptions.RequestException as e:
        print(f"Request to add variable to varset failed: {e}")

def delete_global_priority_varset(org_name, dry_run=False):
    varset_id = get_global_priority_varset_id(org_name)
    if not varset_id:
        print(f"No global priority varset found for org {org_name}")
        return

    url = f"{tfe_url}{api_prefix}varsets/{varset_id}"
    if dry_run:
        print(f"[DRY RUN] Would delete varset {varset_id} for org {org_name}")
        return

    try: 
        response = requests.delete(url, headers=headers)
        if response.status_code == 204:
            print(f"- Varset {varset_id} deleted for org {org_name}")
        elif response.status_code == 404:
            print(f"! Varset {varset_id} not found for org {org_name}: {response.status_code} - {response.text}")
        else:
            print(f"! Error deleting varset {varset_id} for org {org_name}: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request to delete varset failed: {e}")

def delete_variable(varset_id, var_id, var_name, dry_run=False):
    url = f"{tfe_url}{api_prefix}varsets/{varset_id}/relationships/vars/{var_id}"
    if dry_run:
        print(f"[DRY RUN] Would delete variable {var_name} (id {var_id}) from varset {varset_id}")
        return

    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()

        if response.status_code == 204:
            print(f"- Variable {var_name} was deleted from varset because it was not in the desired list")
        else:
            print(f"! Error deleting variable {var_name}: {response.status_code} - {response.text}")
    
    except requests.exceptions.RequestException as e:
        print(f"Request to update variable failed: {e}")

def update_variable(varset_id, var_id, desired, dry_run=False):
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
        print(f"[DRY RUN] Would update variable {desired['key']} in varset {varset_id} with payload: {payload}")
        return

    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()

        if response.status_code == 200:
            print(f"~ Variable {desired['key']} updated")
        elif response.status_code == 404:
            print(f"! Varset {varset_id} not found: {response.status_code} - {response.text}")
        else:
            print(f"! Error updating variable {desired['key']}: {response.status_code} - {response.text}")
    
    except requests.exceptions.RequestException as e:
        print(f"Request to update variable failed: {e}")

def check_diffs_variables_in_varset(varset_id, varset_vars, dry_run=False):
    current_vars = get_variables_in_varset(varset_id)
    current_dict = {var["attributes"]["key"]: var for var in current_vars}
    desired_dict = {var["key"]: var for var in varset_vars}

    for desired in varset_vars:
        key = desired["key"]
        current = current_dict.get(key)

        # If new variable, add to varset
        if not current:
            add_variable(varset_id, desired, dry_run=dry_run)
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
            update_variable(varset_id, current["id"], desired, dry_run=dry_run)
        else:
            print(f"No updates found to be made on variable: {current_attrs.get('key')}")
    
    # Delete any variables that are not in the desired list
    for key, var in current_dict.items():
        if key not in desired_dict:
            delete_variable(varset_id, var["id"], key, dry_run=dry_run)

def update_global_priority_varset(org, dry_run=False):
    varset_id = get_global_priority_varset_id(org)
    if not varset_id:
        print(f"! No global priority varset found to update")
        return
    check_diffs_variables_in_varset(varset_id, varset_vars, dry_run=dry_run)

# Get the varset ID for the global priority varset
def get_global_priority_varset_id(org_name):
    page_number = 1
    page_size = 20 # 100 is the max page size, dont expect as many varsets

    while True:
        url = f"{tfe_url}{api_prefix}organizations/{org_name}/varsets?page[number]={page_number}&page[size]={page_size}"
        try:
            response = requests.get(url, headers=headers)
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
            print(f"Request to get global priority varset failed: {e}")
            break

    return None

# Get variables in a varset
def get_variables_in_varset(varset_id):
    url = f"{tfe_url}{api_prefix}varsets/{varset_id}/relationships/vars"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        print(f"Request to get variables in varset failed: {e}")
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create global priority varset for each org in TFE")
    parser.add_argument("--mode", choices=["create", "delete", "update"], default="create", help="Optional: 'create', 'delete', or 'update' the global priority varset. Default is 'create'.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change, but do not make any changes.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--orgs", help="Comma-separated list of org names or path to a file with org names (one per line)")
    args = parser.parse_args()

    # Load config file
    config = load_config(args.config)
    tfe_url = config["tfe_url"]
    varset_name = config["varset_name"]
    varset_description = config.get("varset_description", "")                           
    varset_vars = config["varset_vars"]

    admin_token = getpass.getpass("Enter your admin token (output will be hidden): ")
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

    print(f"Found {len(organizations)} orgs")
    print(f"Orgs: {organizations}")

    for i, org in enumerate(organizations, start=1):
        print(f"\n[{i}/{len(organizations)}] Processing org: {org}")
        if args.mode == "create":
            print(f"Creating global priority varset for org {org}...")
            create_global_priority_varset(org, dry_run=args.dry_run)

        elif args.mode == "delete":
            print(f"Deleting varset for org {org}...")
            delete_global_priority_varset(org, dry_run=args.dry_run)

        elif args.mode == "update":
            print(f"Updating varset for org {org}...")
            update_global_priority_varset(org, dry_run=args.dry_run)
             
        time.sleep(0.5)  # Tiny sleep to avoid hitting API rate limits