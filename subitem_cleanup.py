 import os
import json
import requests
import time
from collections import defaultdict
from datetime import datetime

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================

# --- IMPORTANT ---
# Set to False to perform the actual creation and deletion.
DRY_RUN = False

# Your Monday.com API Key and Board ID from environment variables
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
MONDAY_API_URL = "https://api.monday.com/v2"

# --- SET THE TARGET USER ---
PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID = os.environ.get("PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID")
TARGET_USER_NAME = "Sarah Bruce"

# --- CONFIGURE YOUR PLP COLUMNS (Based on your provided variables) ---
PLP_CONNECT_COLUMNS_MAP = {
    "ELA Curriculum": "board_relation_mkqnxyjd",
    "Math Curriculum": "board_relation_mkqnbtaf",
    "ACE Curriculum": "board_relation_mkqn34pg",
    "Other Curriculum": "board_relation_mkr54dtg"
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

# <<< CORRECTED FUNCTION
def get_all_plp_items():
    all_items = []
    cursor = None
    
    connect_column_ids = [f'"{col_id}"' for col_id in PLP_CONNECT_COLUMNS_MAP.values() if col_id]
    people_column_ids = [f'"{col_id}"' for col_id in PLP_PEOPLE_COLUMNS_MAP.values() if col_id]
    all_column_ids = connect_column_ids + people_column_ids
    
    if not all_column_ids:
        print("FATAL ERROR: No PLP Column IDs are configured in the script. Exiting.")
        exit()

    while True:
        cursor_str = f', cursor: "{cursor}"' if cursor else ""
        # CORRECTED QUERY: Explicitly requests both 'value' and 'text' for all columns
        query = f"""
            query {{
                boards(ids: {PLP_BOARD_ID}) {{
                    items_page(limit: 50{cursor_str}) {{
                        cursor
                        items {{
                            id
                            name
                            column_values(ids: [{", ".join(all_column_ids)}]) {{
                                id
                                value
                                text
                            }}
                            subitems {{
                                id
                                creator {{ id }}
                            }}
                        }}
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
            print(f"  Fetched {len(all_items)} student items...")
        except (KeyError, IndexError):
            print(f"ERROR: Could not parse items from board {PLP_BOARD_ID}.")
            break
    return all_items

def get_item_names(item_ids):
    if not item_ids: return {}
    query = f"query {{ items(ids: {list(item_ids)}) {{ id name }} }}"
    result = execute_monday_graphql(query)
    try: return {int(item['id']): item['name'] for item in result['data']['items']}
    except (TypeError, KeyError, IndexError): return {}

def create_subitem(parent_item_id, subitem_name, column_values=None):
    column_values_str = json.dumps(column_values) if column_values else "{}"
    mutation = f'mutation {{ create_subitem (parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {json.dumps(column_values_str)}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return result['data']['create_subitem'].get('id') if result and 'data' in result and result['data'].get('create_subitem') else None

def create_monday_update(item_id, update_text):
    mutation = f'mutation {{ create_update (item_id: {item_id}, body: {json.dumps(update_text)}) {{ id }} }}'
    return execute_monday_graphql(mutation)

def delete_subitem(subitem_id):
    mutation = f'mutation {{ delete_item (item_id: {subitem_id}) {{ id }} }}'
    return execute_monday_graphql(mutation)

def find_or_create_subitem(parent_item_id, subitem_name, column_values=None):
    query = f'query {{ items(ids:[{parent_item_id}]) {{ subitems {{ id name }} }} }}'
    result = execute_monday_graphql(query)
    try:
        for subitem in result['data']['items'][0]['subitems']:
            if subitem.get('name') == subitem_name:
                print(f"  INFO: Found existing subitem '{subitem_name}' (ID: {subitem['id']}).")
                return subitem['id']
    except (KeyError, IndexError, TypeError): pass
    print(f"  INFO: No existing subitem named '{subitem_name}'. Creating it.")
    return create_subitem(parent_item_id, subitem_name, column_values=column_values)

# ==============================================================================
# 3. MAIN CLEANUP LOGIC
# ==============================================================================
if __name__ == '__main__':
    if DRY_RUN:
        print("\n" + "="*50)
        print("=== SCRIPT IS RUNNING IN DRY RUN MODE ===")
        print("="*50 + "\n")

    if not PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID:
        print("FATAL ERROR: The PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID environment variable is not set.")
        exit()

    print(f"Step 1: Finding user ID for '{TARGET_USER_NAME}'...")
    target_user_id = get_user_id_by_name(TARGET_USER_NAME)
    if not target_user_id:
        print(f"FATAL ERROR: Could not find user '{TARGET_USER_NAME}'.")
        exit()
    print(f"Found user ID: {target_user_id}")
    
    print("\nStep 2: Fetching all student items...")
    all_students = get_all_plp_items()
    print(f"Found {len(all_students)} students to process.")

    consolidated_subitem_names = set(PLP_CONNECT_COLUMNS_MAP.keys()) | set(PLP_PEOPLE_COLUMNS_MAP.keys())
    curriculum_entry_type = {PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Curriculum"]}}
    staff_entry_type = {PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Staff"]}}

    connect_id_to_cat_map = {v: k for k, v in PLP_CONNECT_COLUMNS_MAP.items()}
    people_id_to_cat_map = {v: k for k, v in PLP_PEOPLE_COLUMNS_MAP.items()}

    for i, student in enumerate(all_students, 1):
        student_id = student['id']
        student_name = student['name']
        print(f"\n--- Processing Student {i}/{len(all_students)}: {student_name} (ID: {student_id}) ---")

        # --- Build and log curriculum from connect columns ---
        current_curriculum = defaultdict(set)
        all_linked_ids = set()
        for col_val in student.get('column_values', []):
            category_name = connect_id_to_cat_map.get(col_val['id'])
            if category_name:
                try:
                    linked_ids = {int(item["linkedPulseId"]) for item in json.loads(col_val.get('value', '{}') or '{}').get("linkedPulseIds", [])}
                    current_curriculum[category_name].update(linked_ids)
                    all_linked_ids.update(linked_ids)
                except (json.JSONDecodeError, TypeError, KeyError, AttributeError):
                    continue
        
        item_id_to_name_map = get_item_names(all_linked_ids)

        for category_name, item_ids in current_curriculum.items():
            if not item_ids: continue
            course_names = sorted([f"'{item_id_to_name_map.get(item_id, f'Item {item_id}')}'" for item_id in item_ids])
            log_message = f"Current curriculum as of {datetime.now().strftime('%Y-%m-%d')}:\n" + "\n".join([f"- {name}" for name in course_names])
            
            if not DRY_RUN:
                subitem_id = find_or_create_subitem(student_id, category_name, column_values=curriculum_entry_type)
                if subitem_id:
                    update_result = create_monday_update(subitem_id, log_message)
                    if not update_result:
                        print(f"  ERROR: Failed to post update to new subitem '{category_name}' (ID: {subitem_id})")
                    time.sleep(1)
                else:
                    print(f"  ERROR: Could not find or create subitem for '{category_name}'")
            else:
                print(f"  -> DRY RUN: Would find or create subitem '{category_name}' with Entry Type 'Curriculum'.")
                print(f"     DRY RUN: Would post an update with {len(course_names)} courses.")

        # --- Build and log staff assignments from people columns ---
        for col_val in student.get('column_values', []):
            category_name = people_id_to_cat_map.get(col_val['id'])
            if category_name and col_val.get('text'):
                staff_names = col_val['text']
                log_message = f"Current assignment as of {datetime.now().strftime('%Y-%m-%d')}:\n- {staff_names}"
                
                if not DRY_RUN:
                    subitem_id = find_or_create_subitem(student_id, category_name, column_values=staff_entry_type)
                    if subitem_id:
                        update_result = create_monday_update(subitem_id, log_message)
                        if not update_result:
                            print(f"  ERROR: Failed to post update to subitem '{category_name}' (ID: {subitem_id})")
                        time.sleep(1)
                    else:
                        print(f"  ERROR: Could not find or create subitem for '{category_name}'")
                else:
                    print(f"  -> DRY RUN: Would find or create subitem '{category_name}' with Entry Type 'Staff'.")
                    print(f"     DRY RUN: Would post an update with assignment: {staff_names}")

        # --- Delete all old subitems created by the target user ---
        subitems_to_delete = [
            s['id'] for s in student.get('subitems', []) 
            if str(s.get('creator', {}).get('id')) == str(target_user_id) 
            and s.get('name') not in consolidated_subitem_names
        ]
        
        if not subitems_to_delete:
            print(f"  No old subitems from {TARGET_USER_NAME} to delete.")
            continue
            
        if not DRY_RUN:
            print(f"  -> Deleting {len(subitems_to_delete)} old subitems created by {TARGET_USER_NAME}...")
            for subitem_id in subitems_to_delete:
                delete_subitem(subitem_id)
                time.sleep(0.5)
        else:
            print(f"  -> DRY RUN: Would delete {len(subitems_to_delete)} old subitems created by {TARGET_USER_NAME}.")
    
    print("\n=====================================")
    print("=== Subitem cleanup script finished. ===")
    print("=====================================\n")
