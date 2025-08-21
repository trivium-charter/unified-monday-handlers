import os
import json
import requests
import time
from collections import defaultdict
from datetime import datetime

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
DRY_RUN = False
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
MONDAY_API_URL = "https://api.monday.com/v2"
TARGET_USER_NAME = "Sarah Bruce" # User to target for deletions
PLP_CONNECT_COLUMNS_MAP = {
    "ELA Curriculum": "board_relation_mkqnxyjd",
    "Math Curriculum": "board_relation_mkqnbtaf",
    "ACE Curriculum": "board_relation_mkqn34pg",
    "Other/Elective Curriculum": "board_relation_mkr54dtg"
}
PLP_PEOPLE_COLUMNS_MAP = {
    "TOR Assignments": "person",
    "Case Manager Assignments": "multiple_person_mks1hqnj",
    "Connect Teacher Assignments": "multiple_person_mks1hzcz",
    "ACE Teacher Assignments": "multiple_person_mks1w5fc"
}

# ==============================================================================
# 2. MONDAY.COM HELPER FUNCTIONS
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
    max_retries = 4; delay = 2
    for attempt in range(max_retries):
        try:
            response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS, timeout=30)
            if response.status_code == 429: print(f"WARNING: Rate limit hit. Waiting {delay} seconds..."); time.sleep(delay); delay *= 2; continue
            response.raise_for_status()
            json_response = response.json()
            if "errors" in json_response: print(f"ERROR: Monday GraphQL Error: {json_response['errors']}"); return None
            return json_response
        except requests.exceptions.RequestException as e:
            print(f"WARNING: Monday HTTP Request Error: {e}. Retrying...")
            if attempt < max_retries - 1: time.sleep(delay); delay *= 2
            else: print("ERROR: Final retry failed."); return None
    return None

def get_user_id_by_name(user_name):
    query = f'query {{ users(kind: all) {{ id name }} }}'
    result = execute_monday_graphql(query)
    try:
        for user in result['data']['users']:
            if user['name'].lower() == user_name.lower(): return user['id']
    except (KeyError, IndexError, TypeError): pass
    return None

def get_all_plp_items():
    all_items = []
    cursor = None
    connect_column_ids = [f'"{col_id}"' for col_id in PLP_CONNECT_COLUMNS_MAP.values() if col_id]
    people_column_ids = [f'"{col_id}"' for col_id in PLP_PEOPLE_COLUMNS_MAP.values() if col_id]
    all_column_ids = connect_column_ids + people_column_ids
    if not all_column_ids: print("FATAL ERROR: No PLP Column IDs are configured."); exit()
    while True:
        cursor_str = f', cursor: "{cursor}"' if cursor else ""
        # Added creator { id } to the subitems query
        query = f'query {{ boards(ids: {PLP_BOARD_ID}) {{ items_page(limit: 50{cursor_str}) {{ cursor items {{ id name column_values(ids: [{", ".join(all_column_ids)}]) {{ id value text }} subitems {{ id name creator {{ id }} }} }} }} }} }}'
        result = execute_monday_graphql(query)
        if not result or 'data' not in result: break
        try:
            page_info = result['data']['boards'][0]['items_page']
            all_items.extend(page_info['items'])
            cursor = page_info.get('cursor');
            if not cursor: break
            print(f"  Fetched {len(all_items)} student items...")
        except (KeyError, IndexError): print(f"ERROR: Could not parse items from board {PLP_BOARD_ID}."); break
    return all_items

def delete_subitem(subitem_id):
    mutation = f'mutation {{ delete_item (item_id: {subitem_id}) {{ id }} }}'
    return execute_monday_graphql(mutation)

# ==============================================================================
# 3. MAIN CLEANUP LOGIC
# ==============================================================================
if __name__ == '__main__':
    if DRY_RUN:
        print("\n" + "="*50)
        print("=== SCRIPT IS RUNNING IN DRY RUN MODE ===")
        print("="*50 + "\n")

    print(f"Step 1: Finding user ID for '{TARGET_USER_NAME}' to target for deletions...")
    target_user_id = get_user_id_by_name(TARGET_USER_NAME)
    if not target_user_id:
        print(f"FATAL ERROR: Could not find user '{TARGET_USER_NAME}'.")
        exit()
    print(f"Found user ID: {target_user_id}")

    print("\nStep 2: Fetching all student items...")
    all_students = get_all_plp_items()
    print(f"Found {len(all_students)} students to process.")

    people_id_to_cat_map = {v: k for k, v in PLP_PEOPLE_COLUMNS_MAP.items()}

    for i, student in enumerate(all_students, 1):
        student_id = student['id']
        student_name = student['name']
        print(f"\n--- Processing Student {i}/{len(all_students)}: {student_name} (ID: {student_id}) ---")
        
        # --- TARGETED DELETION LOGIC ---
        print("  -> Running cleanup checks...")
        all_student_subitems = student.get('subitems', [])
        subitem_names = {s['name'] for s in all_student_subitems}
        subitems_to_delete = []

        # Rule 1: Delete specific empty staff subitems created by the target user
        staff_roles_to_check = ["Case Manager Assignments", "Connect Teacher Assignments"]
        for role in staff_roles_to_check:
            is_assigned = any(
                people_id_to_cat_map.get(cv['id']) == role and cv.get('text')
                for cv in student.get('column_values', [])
            )
            if role in subitem_names and not is_assigned:
                for subitem in all_student_subitems:
                    # ADDED CHECK: Only delete if created by the target user
                    if subitem.get('name') == role and str(subitem.get('creator', {}).get('id')) == str(target_user_id):
                        subitems_to_delete.append(subitem['id'])
                        print(f"     INFO: Marked empty '{role}' subitem for deletion (Creator: {TARGET_USER_NAME}).")
                        break
        
        # Rule 2: Handle "Other Curriculum" if it was created by the target user
        if "Other Curriculum" in subitem_names and "Other/Elective Curriculum" in subitem_names:
            for subitem in all_student_subitems:
                 # ADDED CHECK: Only delete if created by the target user
                if subitem.get('name') == "Other Curriculum" and str(subitem.get('creator', {}).get('id')) == str(target_user_id):
                    subitems_to_delete.append(subitem['id'])
                    print(f"     INFO: Marked legacy 'Other Curriculum' subitem for deletion (Creator: {TARGET_USER_NAME}).")
                    break
        
        # Rule 3: Handle exact duplicates created by the target user
        subitem_name_map = defaultdict(list)
        for subitem in all_student_subitems:
            # Only consider subitems created by the target user for the duplicate check
            if str(subitem.get('creator', {}).get('id')) == str(target_user_id):
                name = subitem.get('name')
                subitem_id_val = subitem.get('id')
                if name and subitem_id_val and subitem_id_val not in subitems_to_delete:
                    subitem_name_map[name].append(subitem_id_val)

        for name, ids in subitem_name_map.items():
            if len(ids) > 1:
                ids_to_delete = ids[1:]
                subitems_to_delete.extend(ids_to_delete)
                print(f"     INFO: Found exact duplicate of '{name}'. Marked {len(ids_to_delete)} for deletion (Creator: {TARGET_USER_NAME}).")

        # --- Execute Deletion ---
        if not subitems_to_delete:
            print(f"  No subitems marked for deletion.")
        else:
            unique_ids_to_delete = list(set(subitems_to_delete))
            if not DRY_RUN:
                print(f"  -> Deleting {len(unique_ids_to_delete)} subitems...")
                for subitem_id_to_delete in unique_ids_to_delete:
                    delete_subitem(subitem_id_to_delete)
                    time.sleep(0.5)
            else:
                print(f"  -> DRY RUN: Would delete {len(unique_ids_to_delete)} subitems based on cleanup rules.")

    print("\n=====================================")
    print("=== Subitem cleanup script finished. ===")
    print("=====================================\n")
