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
    """
    Processes a single parent item from the HS Roster board using the final,
    additive, multi-step categorization logic.
    """
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

    # 2. Get all subitems and their initial categories
    subitems_query = f"""
        query {{
            items (ids: [{parent_item_id}]) {{
                subitems {{
                    id
                    column_values(ids: ["{HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID}", "{HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}"]) {{ id text value }}
                }}
            }}
        }}
    """
    subitems_result = execute_monday_graphql(subitems_query)
    
    initial_course_categories = {}
    try:
        subitems = subitems_result['data']['items'][0]['subitems']
        for subitem in subitems:
            subitem_cols = {cv['id']: cv for cv in subitem['column_values']}
            category = subitem_cols.get(HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID, {}).get('text')
            courses_val = subitem_cols.get(HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID, {}).get('value')
            if category and courses_val:
                course_ids = get_linked_ids_from_connect_column_value(courses_val)
                for course_id in course_ids:
                    initial_course_categories[course_id] = category
    except (TypeError, KeyError, IndexError):
        print("  ERROR: Could not process subitems.")
        return

    all_course_ids = list(initial_course_categories.keys())
    if not all_course_ids:
        print("  INFO: No courses found in subitems.")
        return

    # 3. Efficiently query the secondary category status for all courses at once
    secondary_category_col_id = "dropdown_mkq0r2av"
    secondary_category_query = f"""
        query {{
            items (ids: {all_course_ids}) {{
                id
                column_values(ids: ["{secondary_category_col_id}"]) {{ text }}
            }}
        }}
    """
    secondary_category_results = execute_monday_graphql(secondary_category_query)
    secondary_category_map = {}
    try:
        for item in secondary_category_results['data']['items']:
            if item.get('column_values'):
                secondary_category_map[int(item['id'])] = item['column_values'][0].get('text')
    except (TypeError, KeyError, IndexError):
        pass

    # 4. Apply the final logic to aggregate PLP updates
    plp_updates = defaultdict(set)
    for course_id, initial_category in initial_course_categories.items():
        
        # Rule 1: Primary categorization from HS Roster subitem
        if initial_category == "Math":
            target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Math")
            if target_col_id: plp_updates[target_col_id].add(course_id)

        if initial_category == "English":
            target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ELA")
            if target_col_id: plp_updates[target_col_id].add(course_id)

        # Rule 2: Secondary categorization from All Courses board
        secondary_category = secondary_category_map.get(course_id)
        
        if secondary_category == "ACE":
            target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ACE")
            if target_col_id: plp_updates[target_col_id].add(course_id)
        
        if secondary_category not in ["ACE", "Connect"]:
            target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Other/Elective")
            if target_col_id: plp_updates[target_col_id].add(course_id)

    # 5. Perform the updates on the PLP item
    if not plp_updates:
        print("  INFO: No valid courses found to sync after categorization.")
        return

    print(f"  Found courses to sync for PLP item {plp_item_id}.")
    if dry_run:
        for col_id, courses in plp_updates.items():
            print(f"    DRY RUN: Would add {len(courses)} courses to PLP column {col_id}.")
        return

    for col_id, courses in plp_updates.items():
        bulk_add_to_connect_column(plp_item_id, int(PLP_BOARD_ID), col_id, courses)
        time.sleep(1)
        
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
