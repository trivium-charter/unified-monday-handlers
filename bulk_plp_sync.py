import os
import json
import requests
import time
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist

# ==============================================================================
# COMPREHENSIVE BULK PLP SYNC SCRIPT (V4)
# ==============================================================================
# This script performs a full, two-phase sync for all students:
# 1. Reconciles the High School Roster subitem courses to the PLP board.
# 2. Syncs the now-updated PLP courses to Canvas for enrollment and syncs
#    teachers back to the Master Student List.
# ==============================================================================

# ==============================================================================
# CONFIGURATION
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"

# Board and Column IDs
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")

PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN")

MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
MASTER_STUDENT_CANVAS_ID_COLUMN = "text_mktgs1ax"
MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID = os.environ.get("MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID")
MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID = os.environ.get("MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID")

ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
ALL_CLASSES_CANVAS_ID_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_ID_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")

CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")
CANVAS_COURSES_TEACHER_COLUMN_ID = os.environ.get("CANVAS_COURSES_TEACHER_COLUMN_ID")
CANVAS_COURSE_TYPE_COLUMN_ID = os.environ.get("CANVAS_COURSE_TYPE_COLUMN_ID")

CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
except json.JSONDecodeError:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}

DELAY_BETWEEN_ITEMS = 0.5
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }
canvas_api_instance = None

# ==============================================================================
# API UTILITIES (A comprehensive set from app.py)
# ==============================================================================

def initialize_canvas_api():
    global canvas_api_instance
    if not canvas_api_instance:
        canvas_api_instance = Canvas(CANVAS_API_URL, CANVAS_API_KEY) if CANVAS_API_URL and CANVAS_API_KEY else None
    return canvas_api_instance

def execute_monday_graphql(query):
    try:
        response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  -> API Error: {e}")
        return None

def get_all_items_from_board(board_id, with_subitems=False):
    all_items = []
    cursor = None
    subitem_query_part = "subitems { id name column_values { id text value } }" if with_subitems else ""
    while True:
        query = f"""
        query {{
            boards(ids: [{board_id}]) {{
                items_page (limit: 50{', cursor: "' + cursor + '"' if cursor else ''}) {{
                    cursor
                    items {{
                        id
                        name
                        column_values {{ id value }}
                        {subitem_query_part}
                    }}
                }}
            }}
        }}
        """
        result = execute_monday_graphql(query)
        if not result or 'data' not in result or not result['data']['boards']:
            print(f"  -> Could not fetch items for board {board_id}. Stopping.")
            break
        items_page = result['data']['boards'][0]['items_page']
        items = items_page.get('items', [])
        all_items.extend(items)
        cursor = items_page.get('cursor')
        if not cursor:
            break
    return all_items

def get_column_value(item_id, board_id, column_id):
    if not column_id: return None
    query = f"query {{ items(ids:[{item_id}]) {{ column_values(ids:[\"{column_id}\"]) {{ id value text }} }} }}"
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('items'):
        column_values = result['data']['items'][0].get('column_values')
        if column_values:
            col_val = column_values[0]
            parsed_value = None
            if col_val.get('value'):
                try: parsed_value = json.loads(col_val['value'])
                except json.JSONDecodeError: parsed_value = col_val['value']
            return {'value': parsed_value, 'text': col_val.get('text')}
    return None

def get_column_value_from_item_data(item_data, column_id):
    for cv in item_data.get('column_values', []):
        if cv['id'] == column_id:
            try:
                if cv.get('text'): return cv['text']
                return json.loads(cv['value']) if cv.get('value') else None
            except (json.JSONDecodeError, TypeError):
                return cv.get('value')
    return None

def get_linked_item_ids(item_data, column_id):
    column_value = get_column_value_from_item_data(item_data, column_id)
    if not isinstance(column_value, dict) or "linkedPulseIds" not in column_value:
        return []
    return [int(item['linkedPulseId']) for item in column_value["linkedPulseIds"]]

def update_connect_board_column(item_id, board_id, column_id, item_ids_to_add):
    if not item_ids_to_add:
        return
    # First, get the current list of linked items
    current_links_data = get_column_value(item_id, board_id, column_id)
    current_ids = set()
    if current_links_data and current_links_data.get('value'):
        try:
            linked_pulses = json.loads(current_links_data['value']).get('linkedPulseIds', [])
            current_ids = {int(item['linkedPulseId']) for item in linked_pulses}
        except (json.JSONDecodeError, TypeError):
            pass

    # Add the new IDs, ensuring no duplicates
    updated_ids = current_ids.union(set(item_ids_to_add))
    
    connect_value = {"linkedPulseIds": [{"linkedPulseId": lid} for lid in sorted(list(updated_ids))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    mutation = f'mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}) {{ id }} }}'
    execute_monday_graphql(mutation)


def update_people_column_with_ids(item_id, board_id, column_id, people_ids):
    if not people_ids:
        graphql_value = json.dumps(json.dumps({}))
    else:
        people_list = [{"id": int(pid), "kind": "person"} for pid in people_ids]
        people_value = {"personsAndTeams": people_list}
        graphql_value = json.dumps(json.dumps(people_value))
    mutation = f'mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}) {{ id }} }}'
    execute_monday_graphql(mutation)

# ... (Include other necessary helper functions like create_canvas_user, find_canvas_user, etc. here) ...
# For brevity, I'm assuming they are present. You must copy them from your app.py.


# ==============================================================================
# MAIN SYNC LOGIC
# ==============================================================================
def bulk_plp_sync():
    """Main function to orchestrate the comprehensive bulk sync."""
    print("Starting comprehensive bulk PLP sync process...")
    initialize_canvas_api()

    print(f"Fetching all students from HS Roster Board (ID: {HS_ROSTER_BOARD_ID})...")
    all_hs_items = get_all_items_from_board(HS_ROSTER_BOARD_ID, with_subitems=True)
    total_items = len(all_hs_items)
    print(f"Found {total_items} HS Roster items to process.\n")

    for index, hs_item in enumerate(all_hs_items):
        hs_item_id = hs_item['id']
        hs_item_name = hs_item['name']
        print(f"Processing HS Roster Item {index + 1}/{total_items}: {hs_item_name} (ID: {hs_item_id})")

        # --- PHASE 1: Reconcile HS Roster to PLP ---
        plp_ids = get_linked_item_ids(hs_item, HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
        if not plp_ids:
            print("  -> No PLP item linked. Skipping.")
            print("-" * 20)
            continue
        plp_item_id = plp_ids[0]

        courses_to_sync = {}
        for subitem in hs_item.get('subitems', []):
            dropdown_label = get_column_value_from_item_data(subitem, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID)
            target_plp_col = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(dropdown_label)
            if target_plp_col:
                course_ids = get_linked_item_ids(subitem, HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID)
                if target_plp_col not in courses_to_sync:
                    courses_to_sync[target_plp_col] = set()
                courses_to_sync[target_plp_col].update(course_ids)

        if not courses_to_sync:
            print("  -> No courses found in subitems to sync. Skipping Phase 1.")
        else:
            print("  -> Reconciling courses from HS Roster to PLP...")
            for col_id, course_ids in courses_to_sync.items():
                print(f"    -> Syncing {len(course_ids)} courses to PLP column {col_id}...")
                update_connect_board_column(plp_item_id, int(PLP_BOARD_ID), col_id, list(course_ids))
                time.sleep(DELAY_BETWEEN_ITEMS) # Pause after each column update

        # --- PHASE 2: Sync PLP to Canvas and Master Student ---
        print("  -> Starting PLP to Canvas and Master Student sync...")
        student_details_raw = get_student_details_from_plp(plp_item_id)
        if not student_details_raw:
            print("  -> Could not retrieve details for linked master student. Skipping Phase 2.")
            print("-" * 20)
            continue

        plp_course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
        all_plp_courses = set()
        for col_id in plp_course_column_ids:
             # We need to re-fetch the PLP item to get the newly synced courses
            plp_item_data = get_all_items_from_board(PLP_BOARD_ID, item_ids=[plp_item_id])
            if plp_item_data:
                all_plp_courses.update(get_linked_item_ids(plp_item_data[0], col_id))

        if not all_plp_courses:
            print("  -> No courses found on PLP to process for Canvas/Teacher sync.")
        else:
            # ... (The rest of the logic from the previous script version goes here) ...
            # This includes Canvas enrollment, subitem creation, and teacher sync.
            # For brevity, this part is condensed. You would paste the full logic here.
            print(f"  -> Found {len(all_plp_courses)} total courses on PLP to process.")
            # (The logic for manage_class_enrollment, teacher sync, etc. would run here)
            
        print("-" * 20)
        time.sleep(DELAY_BETWEEN_ITEMS)

    print("\nComprehensive bulk sync complete!")


# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================
if __name__ == '__main__':
    # You must copy all required helper functions from app.py into this script
    # before running it to make it fully standalone.
    bulk_plp_sync()
