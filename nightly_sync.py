#!/usr/bin/env python3
# ==============================================================================
# NIGHTLY PLP & HS ROSTER SYNC SCRIPT
#
# PURPOSE:
# This script intelligently syncs changes from Monday.com to Canvas.
# It only processes students who are new or have been updated since the
# last successful sync, making it ideal for a nightly run.
#
# EXECUTION ORDER:
# 1. Runs the HS Roster to PLP sync logic.
# 2. Runs the full PLP to Canvas sync logic.
# ==============================================================================

import os
import json
import requests
import time
from datetime import datetime, timezone
from collections import defaultdict
import mysql.connector
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist
import unicodedata

# ==============================================================================
# 1. CENTRALIZED CONFIGURATION (Merged from both scripts)
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"

# Database
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = os.environ.get("DB_PORT", 3306)

# Board and Column IDs
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
ALL_STAFF_BOARD_ID = os.environ.get("ALL_STAFF_BOARD_ID")
CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")

PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID")
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN")
PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID = os.environ.get("PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
MASTER_STUDENT_CANVAS_ID_COLUMN = "text_mktgs1ax"
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
ALL_CLASSES_CANVAS_ID_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_ID_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")
CANVAS_TO_STAFF_CONNECT_COLUMN_ID = os.environ.get("CANVAS_TO_STAFF_CONNECT_COLUMN_ID")
MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID = os.environ.get("MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID")
MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID = os.environ.get("MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID")
MASTER_STUDENT_TOR_COLUMN_ID = os.environ.get("MASTER_STUDENT_TOR_COLUMN_ID")

CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
except (json.JSONDecodeError, TypeError):
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}

# ==============================================================================
# 2. MONDAY.COM & CANVAS UTILITIES (Merged and improved)
# ==============================================================================

# --- MONDAY.COM ---
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
    # This smart version includes retries for rate limits and network errors
    max_retries = 4
    delay = 2
    for attempt in range(max_retries):
        try:
            response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS, timeout=30)
            if response.status_code == 429:
                print(f"WARNING: Rate limit hit. Waiting {delay} seconds...")
                time.sleep(delay)
                delay *= 2
                continue
            response.raise_for_status()
            json_response = response.json()
            if "errors" in json_response:
                print(f"ERROR: Monday GraphQL Error: {json_response['errors']}")
                return None
            return json_response
        except requests.exceptions.RequestException as e:
            print(f"WARNING: Monday HTTP Request Error: {e}. Retrying...")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                print("ERROR: Final retry failed.")
                return None
    return None

def get_all_board_items(board_id, columns_to_fetch=None):
    """Fetches all items from a board, including specified columns and updated_at."""
    all_items = []
    cursor = None
    
    # NEW: Build column query part dynamically
    column_query = " ".join(columns_to_fetch) if columns_to_fetch else ""

    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        query = f"""
            query {{
                boards(ids: {board_id}) {{
                    items_page (limit: 50, {cursor_str}) {{
                        cursor
                        items {{ id name updated_at {column_query} }}
                    }}
                }}
            }}"""
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
    
# ... (Copy ALL other utility functions from full_sync.py here) ...
# get_item_name, get_user_name, get_column_value, delete_item, create_subitem, etc.
# initialize_canvas_api, find_canvas_user, create_canvas_user, etc.
# ADD THE bulk_add_to_connect_column function from sync_hs_roster.py here too.


# ==============================================================================
# 3. CORE SYNC LOGIC (Functions from both scripts)
# ==============================================================================

def _run_hs_roster_sync_for_student(hs_roster_item, dry_run=True):
    # This is the logic from your sync_hs_roster.py's sync_hs_roster_item function
    # ... (Copy the full body of the sync_hs_roster_item function here) ...
    pass

def _run_plp_sync_for_student(plp_item_id, dry_run=True):
    # This is the logic from your full_sync.py's sync_single_plp_item function
    # ... (Copy the full body of the sync_single_plp_item function here) ...
    pass


# ==============================================================================
# 4. SCRIPT EXECUTION (New change-detection logic)
# ==============================================================================

if __name__ == '__main__':
    DRY_RUN = False 
    TARGET_USER_NAME = "Sarah Bruce" # For subitem cleanup
    
    print("======================================================")
    print("=== STARTING NIGHTLY DELTA SYNC SCRIPT           ===")
    print("======================================================")
    
    db = None
    cursor = None
    try:
        # --- 1. Database Connection ---
        print("INFO: Connecting to the database...")
        db = mysql.connector.connect(...) # Your full DB connection details
        cursor = db.cursor()

        # --- 2. Fetch last sync times for all previously processed students ---
        print("INFO: Fetching last sync times for processed students...")
        cursor.execute("SELECT student_id, last_synced_at FROM processed_students")
        processed_map = {row[0]: row[1] for row in cursor.fetchall()}
        print(f"INFO: Found {len(processed_map)} students in the database.")

        # --- 3. Fetch all students from Monday.com with their last update time ---
        print("INFO: Fetching all PLP board items from Monday.com...")
        # We need the connect column to find the HS Roster item
        columns_to_get = [f'column_values(ids:["{HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID}"]) {{ value }}']
        all_plp_items = get_all_board_items(PLP_BOARD_ID, columns_to_fetch=columns_to_get)

        # --- 4. Filter for only new or updated students ---
        print("INFO: Filtering for new or updated students...")
        items_to_process = []
        for item in all_plp_items:
            item_id = int(item['id'])
            # Monday.com's UTC timestamp needs to be made timezone-aware to compare
            updated_at_str = item['updated_at'].split('.')[0] # remove milliseconds
            updated_at = datetime.strptime(updated_at_str, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
            
            last_synced = processed_map.get(item_id)
            if last_synced:
                last_synced = last_synced.replace(tzinfo=timezone.utc)

            if not last_synced or updated_at > last_synced:
                items_to_process.append(item)
        
        total_to_process = len(items_to_process)
        print(f"INFO: Found {total_to_process} students that are new or have been updated.")

        # --- 5. Process each changed student ---
        # Note: You'll need to adapt this part to find the HS Roster item from the PLP item
        # This is a conceptual loop. The logic to find the corresponding HS Roster item needs to be added.
        for i, plp_item in enumerate(items_to_process, 1):
            plp_item_id = int(plp_item['id'])
            print(f"\n===== Processing Student {i}/{total_to_process} (PLP ID: {plp_item_id}) =====")
            
            try:
                # Find the corresponding HS Roster item first
                # This is a simplification; you'll need to query based on the connect column
                hs_roster_item = find_hs_roster_item_for_plp(plp_item) # You'll need to write this helper

                if hs_roster_item:
                    print("--- Phase 1: Syncing HS Roster to PLP ---")
                    _run_hs_roster_sync_for_student(hs_roster_item, dry_run=DRY_RUN)
                
                print("--- Phase 2: Syncing PLP to Canvas ---")
                _run_plp_sync_for_student(plp_item_id, dry_run=DRY_RUN)

                # --- 6. If successful, update the timestamp in the database ---
                if not DRY_RUN:
                    print(f"INFO: Sync successful. Updating timestamp for PLP item {plp_item_id}.")
                    update_query = """
                        INSERT INTO processed_students (student_id, last_synced_at)
                        VALUES (%s, NOW())
                        ON DUPLICATE KEY UPDATE last_synced_at = NOW()
                    """
                    cursor.execute(update_query, (plp_item_id,))
                    db.commit()

            except Exception as e:
                print(f"FATAL ERROR processing PLP item {plp_item_id}: {e}")
        
    except Exception as e:
        print(f"A critical error occurred: {e}")
    finally:
        if db and db.is_connected():
            cursor.close()
            db.close()
            print("\nINFO: Database connection closed.")

    print("\n======================================================")
    print("=== SCRIPT FINISHED                                ===")
    print("======================================================")
