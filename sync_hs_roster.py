#!/usr/bin/env python3
# ==============================================================================
# ONE-TIME HS ROSTER TO PLP SYNC SCRIPT
# ==============================================================================
#
# PURPOSE:
# This script reads the HS Course Roster board, finds the courses linked in
# each student's subitems, and ensures those courses are also linked in the
# appropriate columns on the corresponding student's PLP item.
#
# This should be run BEFORE the main PLP full sync script.
#
# ==============================================================================

import os
import json
import requests
import time
from collections import defaultdict

# ==============================================================================
# CENTRALIZED CONFIGURATION
# ==============================================================================
# This script requires the same environment variables as your other scripts.
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"

PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")

HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID")

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
except (json.JSONDecodeError, TypeError):
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}

# ==============================================================================
# MONDAY.COM UTILITIES
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
    try:
        response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Monday.com API Error: {e}")
        return None

def get_linked_ids_from_connect_column_value(value_data):
    if not value_data: return set()
    try:
        parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data) if isinstance(value_data, str) else {}
        if "linkedPulseIds" in parsed_value:
            return {int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"] if "linkedPulseId" in item}
    except (json.JSONDecodeError, TypeError):
        pass
    return set()

def bulk_add_to_connect_column(item_id, board_id, connect_column_id, course_ids_to_add):
    """Efficiently adds multiple items to a connect boards column."""
    # First, get the currently linked items on the PLP
    query_current = f'query {{ items(ids:[{item_id}]) {{ column_values(ids:["{connect_column_id}"]) {{ value }} }} }}'
    result = execute_monday_graphql(query_current)
    current_linked_items = set()
    try:
        col_val = result['data']['items'][0]['column_values']
        if col_val:
            current_linked_items = get_linked_ids_from_connect_column_value(col_val[0]['value'])
    except (TypeError, KeyError, IndexError):
        pass
        
    # Add the new courses to the existing set
    updated_linked_items = current_linked_items.union(course_ids_to_add)
    
    # If no change, do nothing
    if updated_linked_items == current_linked_items:
        return True

    # Prepare the value for the mutation
    connect_value = {"linkedPulseIds": [{"linkedPulseId": int(lid)} for lid in sorted(list(updated_linked_items))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    
    mutation = f'mutation {{ change_column_value (board_id: {board_id}, item_id: {item_id}, column_id: "{connect_column_id}", value: {graphql_value}) {{ id }} }}'
    
    print(f"    SYNCING: Adding {len(course_ids_to_add - current_linked_items)} courses to column {connect_column_id} on PLP item {item_id}.")
    return execute_monday_graphql(mutation) is not None

def get_all_board_items(board_id):
    """Fetches all item IDs from a board, handling pagination."""
    all_items = []
    cursor = None
    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        query = f"""
            query {{
                boards(ids: {board_id}) {{
                    items_page (limit: 100, {cursor_str}) {{
                        cursor
                        items {{ id name }}
                    }}
                }}
            }}
        """
        result = execute_monday_graphql(query)
        if not result or 'data' not in result: break
        try:
            page_info = result['data']['boards'][0]['items_page']
            all_items.extend(page_info['items'])
            cursor = page_info.get('cursor')
            if not cursor: break
            print(f"  Fetched {len(all_items)} items from board {board_id}...")
        except (KeyError, IndexError):
            print(f"ERROR: Could not parse items from board {board_id}.")
            break
    return all_items

# ==============================================================================
# CORE SYNC LOGIC
# ==============================================================================

def sync_hs_roster_item(parent_item, dry_run=True):
    """Processes a single parent item from the HS Roster board."""
    parent_item_id = parent_item['id']
    parent_item_name = parent_item['name']
    print(f"\n--- Processing Student: {parent_item_name} (ID: {parent_item_id}) ---")

    # 1. Find the linked PLP item
    plp_query = f'query {{ items(ids:[{parent_item_id}]) {{ column_values(ids:["{HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID}"]) {{ value }} }} }}'
    plp_result = execute_monday_graphql(plp_query)
    try:
        plp_linked_ids = get_linked_ids_from_connect_column_value(plp_result['data']['items'][0]['column_values'][0]['value'])
        if not plp_linked_ids:
            print("  SKIPPING: No PLP item is linked.")
            return
        plp_item_id = list(plp_linked_ids)[0]
    except (TypeError, KeyError, IndexError):
        print("  SKIPPING: Could not find linked PLP item.")
        return

    # 2. Get all subitems for the parent item
    subitems_query = f"""
        query {{
            items (ids: [{parent_item_id}]) {{
                subitems {{
                    id
                    column_values(ids: ["{HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID}", "{HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}"]) {{
                        id
                        text
                        value
                    }}
                }}
            }}
        }}
    """
    subitems_result = execute_monday_graphql(subitems_query)
    
    # 3. Aggregate all courses by their target PLP column
    plp_updates = defaultdict(set)
    try:
        subitems = subitems_result['data']['items'][0]['subitems']
        if not subitems:
            print("  INFO: No subitems to process.")
            return
            
        for subitem in subitems:
            subitem_cols = {cv['id']: cv for cv in subitem['column_values']}
            
            dropdown_val = subitem_cols.get(HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID)
            courses_val = subitem_cols.get(HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID)
            
            if dropdown_val and courses_val:
                category = dropdown_val['text']
                
                # ========= NEW LOGIC STARTS HERE =========
                target_plp_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(category)
                # If the specific category isn't found, try to find the 'Other' category
                if not target_plp_col_id:
                    print(f"    INFO: Category '{category}' not found. Attempting to use 'Other'.")
                    target_plp_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Other")
                # ========= NEW LOGIC ENDS HERE =========

                if target_plp_col_id:
                    course_ids = get_linked_ids_from_connect_column_value(courses_val['value'])
                    if course_ids:
                        plp_updates[target_plp_col_id].update(course_ids)

    except (TypeError, KeyError, IndexError):
        print("  ERROR: Could not process subitems.")
        return

    # 4. Perform the updates on the PLP item
    if not plp_updates:
        print("  INFO: No valid courses found in subitems to sync.")
        return

    print(f"  Found courses to sync for PLP item {plp_item_id}.")
    if dry_run:
        for col_id, courses in plp_updates.items():
            print(f"    DRY RUN: Would add {len(courses)} courses to PLP column {col_id}.")
        return

    for col_id, courses in plp_updates.items():
        bulk_add_to_connect_column(plp_item_id, int(PLP_BOARD_ID), col_id, courses)
        time.sleep(1) # Respect API limits between updates
        
# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================

if __name__ == '__main__':
    DRY_RUN = True # SET TO FALSE TO EXECUTE CHANGES
    
    print("======================================================")
    print("=== STARTING HS ROSTER TO PLP FULL SYNC SCRIPT ===")
    print("======================================================")
    if DRY_RUN:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!               DRY RUN MODE IS ON               !!!")
        print("!!!  No actual changes will be made to your data.  !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    all_hs_roster_items = get_all_board_items(HS_ROSTER_BOARD_ID)
    total_items = len(all_hs_roster_items)
    print(f"\nFound {total_items} total students on the HS Roster board to process.")

    for i, item in enumerate(all_hs_roster_items):
        try:
            sync_hs_roster_item(item, dry_run=DRY_RUN)
        except Exception as e:
            print(f"FATAL ERROR processing item {item.get('id', 'N/A')}: {e}")
        
        # We only need the sleep for real runs
        if not DRY_RUN:
            time.sleep(2)

    print("\n======================================================")
    print("=== SCRIPT FINISHED                                ===")
    print("======================================================")
