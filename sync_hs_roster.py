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
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID") # Add missing All Courses board ID for querying.

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
    Correctly filters out any subitem marked as "Spring" before processing.
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

    # 2. Get all subitems and build the initial category map, IGNORING Spring subitems
    HS_ROSTER_SUBITEM_TERM_COLUMN_ID = "color6"
    subitems_query = f"""
        query {{
            items (ids: [{parent_item_id}]) {{
                subitems {{
                    id name
                    column_values(ids: ["{HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID}", "{HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}", "{HS_ROSTER_SUBITEM_TERM_COLUMN_ID}"]) {{ id text value }}
                }}
            }}
        }}
    """
    subitems_result = execute_monday_graphql(subitems_query)
    
    # Stores all course IDs found in a dictionary, keyed by their category label.
    course_ids_by_label = defaultdict(set)
    try:
        subitems = subitems_result['data']['items'][0]['subitems']
        for subitem in subitems:
            subitem_cols = {cv['id']: cv for cv in subitem['column_values']}
            
            # Check the Term column first. If it's "Spring", skip this entire subitem.
            term_val = subitem_cols.get(HS_ROSTER_SUBITEM_TERM_COLUMN_ID, {}).get('text')
            if term_val == "Spring":
                print(f"  SKIPPING: Subitem '{subitem['name']}' is marked as Spring.")
                continue # Move to the next subitem
            
            # If the subitem is not Spring, process it.
            category_text = subitem_cols.get(HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID, {}).get('text', '')
            courses_val = subitem_cols.get(HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID, {}).get('value')

            if category_text and courses_val:
                # Handle multiple subjects in a single dropdown by splitting the text
                labels = [label.strip() for label in category_text.split(',')]
                course_ids = get_linked_ids_from_connect_column_value(courses_val)
                for label in labels:
                    if label:
                        course_ids_by_label[label].update(course_ids)

    except (TypeError, KeyError, IndexError):
        print("  ERROR: Could not process subitems.")
        return

    all_course_ids = set().union(*course_ids_by_label.values())
    if not all_course_ids:
        print("  INFO: No non-Spring courses found to process.")
        return

    # 3. Efficiently query the secondary category for all courses at once
    secondary_category_col_id = "dropdown_mkq0r2av"
    secondary_category_query = f"query {{ items (ids: {list(all_course_ids)}) {{ id column_values(ids: [\"{secondary_category_col_id}\"]) {{ text }} }} }}"
    secondary_category_results = execute_monday_graphql(secondary_category_query)
    secondary_category_map = {int(item['id']): item['column_values'][0].get('text') for item in secondary_category_results.get('data', {}).get('items', []) if item.get('column_values')}
    
    # 4. Apply the final logic for valid courses only
    plp_updates = defaultdict(set)
    # Process courses based on both primary and secondary categories
    for category_label, course_ids in course_ids_by_label.items():
        # Get the primary column ID from the map
        target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(category_label)
        
        # Route unmapped subjects to "Other/Elective"
        if not target_col_id:
            other_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Other/Elective")
            if other_col_id:
                print(f"  WARNING: Subject '{category_label}' doesn't map to a PLP column. Routing to 'Other/Elective'.")
                target_col_id = other_col_id
            else:
                print(f"  WARNING: Subject '{category_label}' not mapped and 'Other/Elective' is not configured. Skipping.")
                continue
        
        # Add all courses for this label to the determined column
        plp_updates[target_col_id].update(course_ids)
        
    # Corrected logic: Iterate through all found courses and add them to the secondary category columns if applicable.
    for course_id in all_course_ids:
        secondary_category = secondary_category_map.get(course_id)
        if secondary_category == "ACE":
            ace_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ACE")
            if ace_col_id: plp_updates[ace_col_id].add(course_id)
        elif secondary_category == "Connect":
            # The original code's logic was confusing. A simple elif is clearer.
            # Assuming Connect classes also have a primary category, so no special handling needed here.
            pass
        else: # Handle courses with a different or no secondary category
            # A course gets routed to "Other/Elective" in the primary loop if its subject isn't mapped.
            # This logic block is now removed to avoid redundant entries.
            pass

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
    DRY_RUN = False # SET TO FALSE TO EXECUTE CHANGES
    
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
