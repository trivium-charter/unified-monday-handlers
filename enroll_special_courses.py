#!/usr/bin/env python3
# ==============================================================================
# ONE-TIME JUMPSTART & STUDY HALL ENROLLMENT SCRIPT (Final Version)
# ==============================================================================
import os
import json
import requests
import time
import re
from collections import defaultdict
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist

# ==============================================================================
# CENTRALIZED CONFIGURATION
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"

# --- BOARD AND COLUMN IDs ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")

PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_JUMPSTART_SH_CONNECT_COLUMN = "board_relation_mktqp08q"

MASTER_STUDENT_TOR_COLUMN_ID = os.environ.get("MASTER_STUDENT_TOR_COLUMN_ID") 
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_CANVAS_ID_COLUMN = "text_mktgs1ax"
MASTER_STUDENT_GRADE_COLUMN_ID = "color_mksy8hcw"

ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")
CANVAS_BOARD_STUDY_HALL_COLUMN_ID = "color_mktqgt0t"

# --- MONDAY.COM ITEM IDs for Special Courses ---
# This dictionary was missing
SPECIAL_COURSE_MONDAY_IDS = {
    "Jumpstart": 9717398551,
    "ACE Study Hall": 9717398779,
    "Connect English Study Hall": 9717398717,
    "Connect Math Study Hall": 9717398109,
    "Prep Math and ELA Study Hall": 9717398063,
    "EL Support Study Hall": 10046 # As on your screenshot, using Canvas ID as placeholder
}

# --- CANVAS COURSE IDs for Enrollment ---
SPECIAL_COURSE_CANVAS_IDS = {
    "Jumpstart": 10069,
    "ACE Study Hall": 10128,
    "Connect English Study Hall": 10109,
    "Connect Math Study Hall": 9966,
    "Prep Math and ELA Study Hall": 9960,
    "EL Support Study Hall": 10046
}

# ==============================================================================
# UTILITY FUNCTIONS
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

def get_column_value(item_id, column_id):
    if not item_id or not column_id: return None
    query = f'query {{ items (ids: [{item_id}]) {{ column_values (ids: ["{column_id}"]) {{ id text value type }} }} }}'
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('items'):
        try:
            column_list = result['data']['items'][0].get('column_values', [])
            if not column_list: return None
            col_val = column_list[0]
            parsed_value = col_val.get('value')
            if isinstance(parsed_value, str):
                try: parsed_value = json.loads(parsed_value)
                except json.JSONDecodeError: pass
            return {'value': parsed_value, 'text': col_val.get('text')}
        except (IndexError, KeyError): return None
    return None

def get_people_ids_from_value(value_data):
    if not value_data: return set()
    if isinstance(value_data, str):
        try:
            value_data = json.loads(value_data)
        except json.JSONDecodeError:
            return set()
    persons_and_teams = value_data.get('personsAndTeams', [])
    return {person['id'] for person in persons_and_teams if 'id' in person}
    
def get_all_board_items(board_id):
    all_items = []
    cursor = None
    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        query = f'query {{ boards(ids: {board_id}) {{ items_page (limit: 100, {cursor_str}) {{ cursor items {{ id name }} }} }} }}'
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

def get_linked_ids_from_connect_column_value(value_data):
    if not value_data: return set()
    try:
        parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data) if isinstance(value_data, str) else {}
        if "linkedPulseIds" in parsed_value:
            return {int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"] if "linkedPulseId" in item}
    except (json.JSONDecodeError, TypeError): pass
    return set()

def get_user_name(user_id):
    if user_id is None: return None
    query = f"query {{ users(ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result.get('data', {}).get('users'):
        return result['data']['users'][0].get('name')
    return None

def bulk_add_to_connect_column(item_id, board_id, connect_column_id, course_ids_to_add):
    query_current = f'query {{ items(ids:[{item_id}]) {{ column_values(ids:["{connect_column_id}"]) {{ value }} }} }}'
    result = execute_monday_graphql(query_current)
    current_linked_items = set()
    try:
        col_val = result['data']['items'][0]['column_values']
        if col_val: current_linked_items = get_linked_ids_from_connect_column_value(col_val[0]['value'])
    except (TypeError, KeyError, IndexError): pass
    updated_linked_items = current_linked_items.union(course_ids_to_add)
    if updated_linked_items == current_linked_items: return True
    connect_value = {"linkedPulseIds": [{"linkedPulseId": int(lid)} for lid in sorted(list(updated_linked_items))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    mutation = f'mutation {{ change_column_value (board_id: {board_id}, item_id: {item_id}, column_id: "{connect_column_id}", value: {graphql_value}) {{ id }} }}'
    return execute_monday_graphql(mutation) is not None

def initialize_canvas_api():
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY) if CANVAS_API_URL and CANVAS_API_KEY else None

def find_canvas_user(student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    if student_details.get('canvas_id'):
        try: return canvas_api.get_user(student_details['canvas_id'])
        except (ResourceDoesNotExist, ValueError): pass 
    if student_details.get('email'):
        try: return canvas_api.get_user(student_details['email'], 'login_id')
        except ResourceDoesNotExist: pass
    if student_details.get('ssid'):
        try: return canvas_api.get_user(student_details['ssid'], 'sis_user_id')
        except ResourceDoesNotExist: pass
    if student_details.get('email'):
        try:
            users = [u for u in canvas_api.get_account(1).get_users(search_term=student_details['email'])]
            if len(users) == 1: return users[0]
        except (ResourceDoesNotExist, CanvasException): pass
    return None

def create_canvas_user(student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        account = canvas_api.get_account(1)
        user_payload = {
            'user': {'name': student_details['name'], 'terms_of_use': True},
            'pseudonym': {'unique_id': student_details['email'], 'sis_user_id': student_details['ssid'], 'send_confirmation': False},
            'communication_channel': {'type': 'email', 'address': student_details['email'], 'skip_confirmation': True}
        }
        return account.create_user(**user_payload)
    except CanvasException as e:
        print(f"  ERROR: Canvas user creation failed for {student_details.get('name')}: {e}")
        return None

def create_section_if_not_exists(course, section_name):
    try:
        for section in course.get_sections():
            if section.name.lower() == section_name.lower():
                return section
        return course.create_course_section(course_section={'name': section_name})
    except CanvasException as e:
        print(f"  ERROR: Canvas section creation/check failed for {section_name}: {e}")
        return None

def enroll_student_in_section(course, user, section):
    try:
        course.enroll_user(user, 'StudentEnrollment', enrollment={'course_section_id': section.id, 'notify': False})
        return "Success"
    except Conflict: return "Already Enrolled"
    except CanvasException as e:
        print(f"  ERROR: Failed to enroll user {user.id} in section {section.id}. Details: {e}")
        return "Failed"

def enroll_student(canvas_course_id, section_name, student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api:
        print("  ERROR: Canvas API not initialized.")
        return
    
    user = find_canvas_user(student_details)
    if not user:
        print(f"  INFO: Canvas user not found for {student_details.get('email', 'N/A')}. Creating new user.")
        user = create_canvas_user(student_details)
    
    if not user:
        print(f"  ERROR: Could not find or create Canvas user for {student_details.get('name')}")
        return

    try:
        course = canvas_api.get_course(canvas_course_id)
        section = create_section_if_not_exists(course, section_name)
        if section:
            result = enroll_student_in_section(course, user, section)
            print(f"  Enrollment in '{course.name}' (Section: {section_name}): {result}")
    except ResourceDoesNotExist:
        print(f"  ERROR: Canvas course with ID {canvas_course_id} not found.")

# ==============================================================================
# CORE SYNC LOGIC
# ==============================================================================
def process_student_special_enrollments(plp_item, dry_run=True):
    plp_item_id = int(plp_item['id'])
    print(f"\n--- Processing Student: {plp_item['name']} (PLP ID: {plp_item_id}) ---")

    # 1. Gather Data
    master_link_val = get_column_value(plp_item_id, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_link_val or not master_link_val.get('value'):
        print("  SKIPPING: No Master Student item linked.")
        return
    master_id_set = get_linked_ids_from_connect_column_value(master_link_val['value'])
    if not master_id_set:
        print("  SKIPPING: No Master Student item linked.")
        return
    master_id = list(master_id_set)[0]
    
    master_details_query = f'query {{ items(ids:[{master_id}]) {{ name column_values(ids:["{MASTER_STUDENT_TOR_COLUMN_ID}", "{MASTER_STUDENT_GRADE_COLUMN_ID}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}"]) {{ id text value }} }} }}'
    master_result = execute_monday_graphql(master_details_query)
    
    tor_last_name = "Orientation"
    student_details = {}
    grade = 0
    
    if not master_result or not master_result.get('data', {}).get('items'):
        print(f"  WARNING: Could not fetch details for master item {master_id}.")
        return

    item_details = master_result['data']['items'][0]
    cols = {cv['id']: cv for cv in item_details.get('column_values', [])}
    
    student_details['name'] = item_details.get('name', '')
    student_details['email'] = cols.get(MASTER_STUDENT_EMAIL_COLUMN, {}).get('text', '')
    student_details['ssid'] = cols.get(MASTER_STUDENT_SSID_COLUMN, {}).get('text', '')
    student_details['canvas_id'] = cols.get(MASTER_STUDENT_CANVAS_ID_COLUMN, {}).get('text', '')

    grade_text = cols.get(MASTER_STUDENT_GRADE_COLUMN_ID, {}).get('text', '0')
    grade_match = re.search(r'\d+', grade_text)
    if grade_match: grade = int(grade_match.group())

    tor_val_str = cols.get(MASTER_STUDENT_TOR_COLUMN_ID, {}).get('value')
    if tor_val_str:
        try:
            # --- THIS BLOCK IS THE FIX ---
            # It now correctly uses the get_people_ids_from_value helper, which is
            # designed for the multiple-person column format the API is sending.
            tor_val_dict = json.loads(tor_val_str)
            tor_ids = get_people_ids_from_value(tor_val_dict)
            if tor_ids:
                tor_id = list(tor_ids)[0]
                tor_full_name = get_user_name(tor_id)
                if tor_full_name: tor_last_name = tor_full_name.split()[-1]
        except (json.JSONDecodeError, TypeError):
            print(f"  WARNING: Could not parse TOR value for master item {master_id}.")
    
    plp_links_to_add = set()

    # 2. Process Jumpstart
    jumpstart_canvas_id = SPECIAL_COURSE_CANVAS_IDS.get("Jumpstart")
    if jumpstart_canvas_id:
        print(f"  Processing Jumpstart enrollment, section: {tor_last_name}")
        if not dry_run:
            enroll_student(jumpstart_canvas_id, tor_last_name, student_details)
        jumpstart_item_id = SPECIAL_COURSE_MONDAY_IDS.get("Jumpstart")
        if jumpstart_item_id: plp_links_to_add.add(jumpstart_item_id)

    # 3. Process Study Hall
    course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
    all_regular_course_ids = set()
    for col_id in course_column_ids:
        column_data = get_column_value(plp_item_id, col_id)
        if column_data: all_regular_course_ids.update(get_linked_ids_from_connect_column_value(column_data.get('value')))
    
    target_sh_name = None
    sh_section_name = None
    if all_regular_course_ids:
        ids_str = ','.join(map(str, all_regular_course_ids))
        canvas_links_query = f'query {{ items(ids:[{ids_str}]) {{ id name column_values(ids:["{ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID}"]) {{ value }} }} }}'
        canvas_links_result = execute_monday_graphql(canvas_links_query)
        
        canvas_id_to_course_name_map = {}
        if canvas_links_result and canvas_links_result.get('data', {}).get('items'):
            for item in canvas_links_result['data']['items']:
                if item.get('column_values'):
                    canvas_ids = get_linked_ids_from_connect_column_value(item['column_values'][0].get('value'))
                    for cid in canvas_ids:
                        canvas_id_to_course_name_map[cid] = item.get('name')

        all_canvas_item_ids = set(canvas_id_to_course_name_map.keys())
        if all_canvas_item_ids:
            ids_str = ','.join(map(str, all_canvas_item_ids))
            sh_status_query = f'query {{ items(ids:[{ids_str}]) {{ id column_values(ids:["{CANVAS_BOARD_STUDY_HALL_COLUMN_ID}"]) {{ text }} }} }}'
            sh_status_result = execute_monday_graphql(sh_status_query)
            if sh_status_result and sh_status_result.get('data', {}).get('items'):
                for item in sh_status_result['data']['items']:
                    if item.get('column_values'):
                        sh_name = item['column_values'][0].get('text')
                        if sh_name and sh_name in SPECIAL_COURSE_CANVAS_IDS:
                            target_sh_name = sh_name
                            sh_section_name = canvas_id_to_course_name_map.get(item['id'], "General")
                            break
    
    if target_sh_name:
        target_sh_canvas_id = SPECIAL_COURSE_CANVAS_IDS.get(target_sh_name)
        if target_sh_canvas_id:
            print(f"  Processing {target_sh_name} enrollment, section: {sh_section_name}")
            if not dry_run:
                enroll_student(target_sh_canvas_id, sh_section_name, student_details)
            sh_item_id = SPECIAL_COURSE_MONDAY_IDS.get(target_sh_name)
            if sh_item_id: plp_links_to_add.add(sh_item_id)
    else:
        print("  INFO: No Study Hall enrollment rule matched.")

    # 4. Update the PLP connect column
    if plp_links_to_add:
        print(f"  Action: Linking {len(plp_links_to_add)} courses to PLP column {PLP_JUMPSTART_SH_CONNECT_COLUMN}.")
        if not dry_run:
            bulk_add_to_connect_column(plp_item_id, int(PLP_BOARD_ID), PLP_JUMPSTART_SH_CONNECT_COLUMN, plp_links_to_add)
# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================
if __name__ == '__main__':
    DRY_RUN = False
    print("======================================================")
    print("=== STARTING JUMPSTART & STUDY HALL SYNC SCRIPT ===")
    print("======================================================")
    if DRY_RUN:
        print("\n!!! DRY RUN MODE IS ON !!!")

    all_plp_items = get_all_board_items(PLP_BOARD_ID)
    total_items = len(all_plp_items)
    print(f"\nFound {total_items} total students to process.")

    for i, item in enumerate(all_plp_items):
        try:
            process_student_special_enrollments(item, dry_run=DRY_RUN)
        except Exception as e:
            print(f"FATAL ERROR processing item {item.get('id', 'N/A')}: {e}")
            import traceback
            traceback.print_exc()
        
        # --- THIS LINE IS THE FIX ---
        if not DRY_RUN:
            time.sleep(2)

    print("\n======================================================")
    print("=== SCRIPT FINISHED                                ===")
    print("======================================================")
