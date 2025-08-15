# ==============================================================================
# ONE-TIME SCRIPT TO CREATE SPECIAL CANVAS SECTIONS (COMPLETE VERSION)
# ==============================================================================
import os
import json
import requests
import time
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist
import unicodedata
import re

# ==============================================================================
# CONFIGURATION
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
MASTER_STUDENT_TOR_COLUMN_ID = os.environ.get("MASTER_STUDENT_TOR_COLUMN_ID")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
MASTER_STUDENT_CANVAS_ID_COLUMN = "text_mktgs1ax"

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
except (json.JSONDecodeError, TypeError):
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}

# The 10 special courses
ROSTER_ONLY_COURSES = {10298, 10297, 10299, 10300, 10301}
ROSTER_AND_CREDIT_COURSES = {10097, 10002, 10092, 10164, 10198}
ALL_SPECIAL_COURSES = ROSTER_ONLY_COURSES.union(ROSTER_AND_CREDIT_COURSES)

# ==============================================================================
# UTILITY FUNCTIONS (Copied from nightly_sync.py and app.py)
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
    max_retries = 4; delay = 2
    for attempt in range(max_retries):
        try:
            response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS, timeout=30)
            if response.status_code == 429:
                print(f"WARNING: Rate limit hit. Waiting {delay} seconds..."); time.sleep(delay); delay *= 2; continue
            response.raise_for_status()
            json_response = response.json()
            if "errors" in json_response:
                print(f"ERROR: Monday GraphQL Error: {json_response['errors']}")
                return None
            return json_response
        except requests.exceptions.RequestException as e:
            print(f"WARNING: Monday HTTP Request Error: {e}. Retrying...")
            if attempt < max_retries - 1: time.sleep(delay); delay *= 2
            else: print("ERROR: Final retry failed."); return None
    return None

def get_all_board_items(board_id):
    all_items = []; cursor = None
    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        query = f"""query {{ boards(ids: {board_id}) {{ items_page(limit: 100, {cursor_str}) {{ cursor items {{ id name }} }} }} }}"""
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

def get_item_name(item_id, board_id):
    query = f"query {{ items(ids: [{item_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    try: return result['data']['items'][0].get('name')
    except (TypeError, KeyError, IndexError): return None

def get_user_name(user_id):
    if user_id is None: return None
    query = f"query {{ users(ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    try: return result['data']['users'][0].get('name')
    except (TypeError, KeyError, IndexError): return None

def get_column_value(item_id, board_id, column_id):
    if not item_id or not column_id: return None
    query = f'query {{ items (ids: [{item_id}]) {{ column_values (ids: ["{column_id}"]) {{ text value }} }} }}'
    result = execute_monday_graphql(query)
    try:
        col_val = result['data']['items'][0]['column_values'][0]
        parsed_value = json.loads(col_val.get('value')) if col_val.get('value') else None
        return {'value': parsed_value, 'text': col_val.get('text')}
    except (TypeError, KeyError, IndexError, json.JSONDecodeError): return None

def get_linked_ids_from_connect_column_value(value_data):
    if not value_data: return set()
    try:
        parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data)
        if "linkedPulseIds" in parsed_value: return {int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"]}
    except (json.JSONDecodeError, TypeError): pass
    return set()

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    column_data = get_column_value(item_id, board_id, connect_column_id)
    return get_linked_ids_from_connect_column_value(column_data.get('value')) if column_data else set()

def get_people_ids_from_value(value_data):
    if not value_data: return set()
    if isinstance(value_data, str):
        try: value_data = json.loads(value_data)
        except json.JSONDecodeError: return set()
    persons_and_teams = value_data.get('personsAndTeams', [])
    return {person['id'] for person in persons_and_teams if 'id' in person and person.get('kind') == 'person'}

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

def create_section_if_not_exists(course, section_name):
    try:
        for section in course.get_sections():
            if section.name.lower() == section_name.lower():
                return section
        return course.create_course_section(course_section={'name': section_name})
    except CanvasException as e:
        print(f"  -> ERROR: Canvas section creation/check failed: {e}")
        return None

def enroll_student_in_section(course, user, section):
    try:
        course.enroll_user(user, 'StudentEnrollment', enrollment={'course_section_id': section.id, 'notify': False})
        print(f"    -> SUCCESS: Enrolled '{user.name}' in section '{section.name}'.")
    except Conflict:
        print(f"    -> INFO: '{user.name}' already enrolled in section '{section.name}'.")
    except CanvasException as e:
        print(f"    -> ERROR: Failed to enroll '{user.name}' in section '{section.name}': {e}")

def get_roster_teacher_name(master_student_id):
    tor_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_TOR_COLUMN_ID)
    if tor_val and tor_val.get('value'):
        tor_ids = get_people_ids_from_value(tor_val['value'])
        if tor_ids:
            tor_full_name = get_user_name(list(tor_ids)[0])
            if tor_full_name: return tor_full_name.split()[-1]
    return None

def get_student_details_from_plp(plp_item_id):
    try:
        query = f'query {{ items (ids: [{plp_item_id}]) {{ column_values (ids: ["{PLP_TO_MASTER_STUDENT_CONNECT_COLUMN}"]) {{ value }} }} }}'
        result = execute_monday_graphql(query)
        connect_value = json.loads(result['data']['items'][0]['column_values'][0]['value'])
        master_student_id = [item['linkedPulseId'] for item in connect_value.get('linkedPulseIds', [])][0]
        details_query = f'query {{ items (ids: [{master_student_id}]) {{ name column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}"]) {{ id text }} }} }}'
        details_result = execute_monday_graphql(details_query)
        item_details = details_result['data']['items'][0]
        column_map = {cv['id']: cv.get('text', '') for cv in item_details.get('column_values', [])}
        return {
            'name': item_details.get('name'),
            'ssid': column_map.get(MASTER_STUDENT_SSID_COLUMN, ''),
            'email': column_map.get(MASTER_STUDENT_EMAIL_COLUMN, ''),
            'canvas_id': column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN, ''),
            'master_id': master_student_id
        }
    except (TypeError, KeyError, IndexError, json.JSONDecodeError):
        return None

# ==============================================================================
# MAIN SCRIPT LOGIC
# ==============================================================================
def run_one_time_section_fix():
    print("Starting the one-time section fix script...")
    canvas_api = initialize_canvas_api()
    if not canvas_api:
        print("ERROR: Canvas API not initialized. Halting.")
        return

    print(f"Fetching all students from PLP Board: {PLP_BOARD_ID}")
    all_plp_items = get_all_board_items(PLP_BOARD_ID)
    print(f"Found {len(all_plp_items)} total students to check.")

    for i, plp_item in enumerate(all_plp_items):
        plp_item_id = int(plp_item['id'])
        print(f"\n--- Processing student {i+1}/{len(all_plp_items)} (PLP ID: {plp_item_id}) ---")

        student_details = get_student_details_from_plp(plp_item_id)
        if not student_details or not student_details.get('master_id'):
            print("  -> SKIPPING: Could not get complete student details.")
            continue

        canvas_user = find_canvas_user(student_details)
        if not canvas_user:
            print(f"  -> SKIPPING: Could not find user {student_details['name']} in Canvas.")
            continue
            
        roster_teacher_name = get_roster_teacher_name(student_details['master_id'])
        if not roster_teacher_name:
            print("  -> WARNING: Could not determine Roster Teacher. Defaulting to 'Unassigned'.")
            roster_teacher_name = "Unassigned"

        for category, col_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
            linked_course_ids = get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), col_id)
            
            for course_id in linked_course_ids:
                if course_id in ALL_SPECIAL_COURSES:
                    try:
                        canvas_course = canvas_api.get_course(course_id)
                        print(f"  -> Found special course: '{canvas_course.name}'")

                        section_teacher = create_section_if_not_exists(canvas_course, roster_teacher_name)
                        if section_teacher:
                            enroll_student_in_section(canvas_course, canvas_user, section_teacher)

                        if course_id in ROSTER_AND_CREDIT_COURSES:
                            course_item_name = get_item_name(course_id, int(ALL_COURSES_BOARD_ID)) or ""
                            credit_section_name = "2.5 Credits" if "2.5" in course_item_name else "5 Credits"
                            
                            section_credit = create_section_if_not_exists(canvas_course, credit_section_name)
                            if section_credit:
                                enroll_student_in_section(canvas_course, canvas_user, section_credit)
                    
                    except ResourceDoesNotExist:
                        print(f"  -> ERROR: Course with ID {course_id} not found in Canvas.")
                    except Exception as e:
                        print(f"  -> An unexpected error occurred: {e}")
        
        time.sleep(0.5)

    print("\nScript finished!")

if __name__ == '__main__':
    run_one_time_section_fix()
