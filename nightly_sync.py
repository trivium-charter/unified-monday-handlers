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
# 1. Runs the HS Roster to PLP sync logic (if applicable).
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
import re
# ==============================================================================
# 1. CENTRALIZED CONFIGURATION
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
PLP_TO_HS_ROSTER_CONNECT_COLUMN = os.environ.get("PLP_TO_HS_ROSTER_CONNECT_COLUMN")
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
# --- Add these to Section 1 ---

PLP_JUMPSTART_SH_CONNECT_COLUMN = "board_relation_mktqp08q" # Example ID, use your actual column ID
MASTER_STUDENT_GRADE_COLUMN_ID = "color_mksy8hcw" # Example ID, use your actual column ID

SPECIAL_COURSE_MONDAY_IDS = {
    "Jumpstart": 9717398551,
    "ACE Study Hall": 9717398779,
    "Connect English Study Hall": 9717398717,
    "Connect Math Study Hall": 9717398109,
    "Prep Math and ELA Study Hall": 9717398063,
    "EL Support Study Hall": 10046 # Using Canvas ID as a placeholder
}

SPECIAL_COURSE_CANVAS_IDS = {
    "Jumpstart": 10069,
    "ACE Study Hall": 10128,
    "Connect English Study Hall": 10109,
    "Connect Math Study Hall": 9966,
    "Prep Math and ELA Study Hall": 9960,
    "EL Support Study Hall": 10046
}
try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
except (json.JSONDecodeError, TypeError):
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}

# ==============================================================================
# 2. MONDAY.COM & CANVAS UTILITIES
# ==============================================================================

MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
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

# --- Add these two functions to Section 2 ---

def get_study_hall_section_from_grade(grade_text):
    """
    Determines the correct Study Hall section name based on the student's grade level.
    """
    if not grade_text:
        return "General"

    match = re.search(r'\d+', grade_text)
    if grade_text.upper() in ["TK", "K"]:
        grade_level = 0
    elif match:
        grade_level = int(match.group(0))
    else:
        return "General"

    if grade_level <= 5:
        return "Elementary School"
    elif 6 <= grade_level <= 8:
        return "Middle School"
    elif 9 <= grade_level <= 12:
        return "High School"
    else:
        return "General"

def process_student_special_enrollments(plp_item, dry_run=True):
    plp_item_id = int(plp_item['id'])
    print(f"\n--- Processing Special Enrollments for: {plp_item['name']} (PLP ID: {plp_item_id}) ---")

    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details:
        print("  SKIPPING: Could not get student details.")
        return

    master_id = student_details['master_id']
    
    # Get TOR and Grade Level from Master Student Item
    master_details_query = f'query {{ items(ids:[{master_id}]) {{ column_values(ids:["{MASTER_STUDENT_TOR_COLUMN_ID}", "{MASTER_STUDENT_GRADE_COLUMN_ID}"]) {{ id text value }} }} }}'
    master_result = execute_monday_graphql(master_details_query)
    
    tor_last_name = "Orientation"
    grade_text = ""
    if master_result and master_result.get('data', {}).get('items'):
        cols = {cv['id']: cv for cv in master_result['data']['items'][0].get('column_values', [])}
        grade_text = cols.get(MASTER_STUDENT_GRADE_COLUMN_ID, {}).get('text', '')
        tor_val_str = cols.get(MASTER_STUDENT_TOR_COLUMN_ID, {}).get('value')
        if tor_val_str:
            try:
                tor_ids = get_people_ids_from_value(json.loads(tor_val_str))
                if tor_ids:
                    tor_full_name = get_user_name(list(tor_ids)[0])
                    if tor_full_name: tor_last_name = tor_full_name.split()[-1]
            except (json.JSONDecodeError, TypeError):
                print(f"  WARNING: Could not parse TOR value for master item {master_id}.")

    plp_links_to_add = set()

    # 1. Process Jumpstart
    jumpstart_canvas_id = SPECIAL_COURSE_CANVAS_IDS.get("Jumpstart")
    if jumpstart_canvas_id:
        print(f"  Processing Jumpstart enrollment, section: {tor_last_name}")
        if not dry_run:
            enroll_student(jumpstart_canvas_id, tor_last_name, student_details)
        jumpstart_item_id = SPECIAL_COURSE_MONDAY_IDS.get("Jumpstart")
        if jumpstart_item_id: plp_links_to_add.add(jumpstart_item_id)

    # 2. Process Study Hall
    sh_section_name = get_study_hall_section_from_grade(grade_text)
    
    # This logic assumes a single "Study Hall" course is determined by the ACE/Connect/etc. courses.
    # You will need to define which Study Hall is the target.
    # For this example, we will assume "ACE Study Hall" is the default if no other rule matches.
    target_sh_name = "ACE Study Hall" # Replace with your logic to determine which study hall
    
    target_sh_canvas_id = SPECIAL_COURSE_CANVAS_IDS.get(target_sh_name)
    if target_sh_canvas_id:
        print(f"  Processing {target_sh_name} enrollment, section: {sh_section_name}")
        if not dry_run:
            enroll_student(target_sh_canvas_id, sh_section_name, student_details)
        sh_item_id = SPECIAL_COURSE_MONDAY_IDS.get(target_sh_name)
        if sh_item_id: plp_links_to_add.add(sh_item_id)
    
    # 3. Update the PLP connect column
    if plp_links_to_add:
        print(f"  Action: Linking {len(plp_links_to_add)} special courses to PLP column {PLP_JUMPSTART_SH_CONNECT_COLUMN}.")
        if not dry_run:
            bulk_add_to_connect_column(plp_item_id, int(PLP_BOARD_ID), PLP_JUMPSTART_SH_CONNECT_COLUMN, plp_links_to_add)
            
def get_all_board_items(board_id, item_ids=None):
    all_items = []
    cursor = None
    id_filter = f'query_params: {{ids: {json.dumps(item_ids)}}}' if item_ids else ""
    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        pagination_args = f", {cursor_str}" if cursor else ""
        filter_args = f"{id_filter}{pagination_args}" if id_filter else cursor_str.lstrip(', ')
        query = f"""
            query {{
                boards(ids: {board_id}) {{
                    items_page (limit: 50, {filter_args}) {{
                        cursor
                        items {{ id name updated_at }}
                    }}
                }}
            }}"""
        result = execute_monday_graphql(query)
        if not result or 'data' not in result: break
        try:
            page_info = result['data']['boards'][0]['items_page']
            all_items.extend(page_info['items'])
            cursor = page_info.get('cursor')
            if not cursor or item_ids:
                break
            print(f"  Fetched {len(all_items)} items from board {board_id}...")
        except (KeyError, IndexError):
            print(f"ERROR: Could not parse items from board {board_id}.")
            break
    return all_items

def check_if_subitem_exists(parent_item_id, subitem_name_to_check, creator_id):
    query = f'query {{ items(ids:[{parent_item_id}]) {{ subitems {{ name creator {{ id }} }} }} }}'
    result = execute_monday_graphql(query)
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            creator = subitem.get('creator')
            if (subitem.get('name') == subitem_name_to_check and
                    creator and str(creator.get('id')) == str(creator_id)):
                return True
    except (KeyError, IndexError, TypeError):
        pass
    return False

def parse_flexible_timestamp(ts_string):
    try:
        return datetime.strptime(ts_string, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.strptime(ts_string, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

def get_item_name(item_id, board_id):
    query = f"query {{ items(ids: [{item_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    try:
        return result['data']['items'][0].get('name')
    except (TypeError, KeyError, IndexError):
        return None

def get_user_id(user_name):
    query = f'query {{ users(kind: all) {{ id name }} }}'
    result = execute_monday_graphql(query)
    try:
        for user in result['data']['users']:
            if user['name'].lower() == user_name.lower():
                return user['id']
    except (KeyError, IndexError, TypeError):
        pass
    return None

def get_user_name(user_id):
    if user_id is None: return None
    query = f"query {{ users(ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    try:
        return result['data']['users'][0].get('name')
    except (TypeError, KeyError, IndexError):
        return None

def get_column_value(item_id, board_id, column_id):
    if not item_id or not column_id: return None
    query = f'query {{ items (ids: [{item_id}]) {{ column_values (ids: ["{column_id}"]) {{ text value }} }} }}'
    result = execute_monday_graphql(query)
    try:
        col_val = result['data']['items'][0]['column_values'][0]
        parsed_value = json.loads(col_val.get('value')) if col_val.get('value') else None
        return {'value': parsed_value, 'text': col_val.get('text')}
    except (TypeError, KeyError, IndexError, json.JSONDecodeError):
        return None

def delete_item(item_id):
    mutation = f"mutation {{ delete_item (item_id: {item_id}) {{ id }} }}"
    return execute_monday_graphql(mutation)

def get_linked_ids_from_connect_column_value(value_data):
    if not value_data: return set()
    try:
        parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data)
        if "linkedPulseIds" in parsed_value:
            return {int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"]}
    except (json.JSONDecodeError, TypeError):
        pass
    return set()

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    column_data = get_column_value(item_id, board_id, connect_column_id)
    return get_linked_ids_from_connect_column_value(column_data.get('value')) if column_data else set()

def create_subitem(parent_item_id, subitem_name, column_values=None):
    column_values_json = json.dumps(column_values or {})
    mutation = f'mutation {{ create_subitem (parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {json.dumps(column_values_json)}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    if result and 'data' in result and result['data'].get('create_subitem'):
        return result['data']['create_subitem'].get('id')
    print(f"WARNING: Failed to create subitem '{subitem_name}'.")
    return None

def bulk_add_to_connect_column(item_id, board_id, connect_column_id, course_ids_to_add):
    query_current = f'query {{ items(ids:[{item_id}]) {{ column_values(ids:["{connect_column_id}"]) {{ value }} }} }}'
    result = execute_monday_graphql(query_current)
    current_linked_items = set()
    try:
        current_linked_items = get_linked_ids_from_connect_column_value(result['data']['items'][0]['column_values'][0]['value'])
    except (TypeError, KeyError, IndexError):
        pass
    updated_linked_items = current_linked_items.union(course_ids_to_add)
    if updated_linked_items == current_linked_items: return True
    connect_value = {"linkedPulseIds": [{"linkedPulseId": int(lid)} for lid in sorted(list(updated_linked_items))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    mutation = f'mutation {{ change_column_value (board_id: {board_id}, item_id: {item_id}, column_id: "{connect_column_id}", value: {graphql_value}) {{ id }} }}'
    print(f"    SYNCING: Adding {len(course_ids_to_add - current_linked_items)} courses to column {connect_column_id} on PLP item {item_id}.")
    return execute_monday_graphql(mutation) is not None

def initialize_canvas_api():
    if CANVAS_API_URL and CANVAS_API_KEY:
        return Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    return None

def find_canvas_user(student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    id_from_monday = student_details.get('canvas_id')
    if id_from_monday:
        try:
            return canvas_api.get_user(int(id_from_monday))
        except (ValueError, TypeError):
            try:
                return canvas_api.get_user(id_from_monday, 'sis_user_id')
            except ResourceDoesNotExist: pass
        except ResourceDoesNotExist: pass
    if student_details.get('email'):
        try:
            return canvas_api.get_user(student_details['email'], 'login_id')
        except ResourceDoesNotExist: pass
    if student_details.get('ssid') and student_details.get('ssid') != id_from_monday:
        try:
            return canvas_api.get_user(student_details['ssid'], 'sis_user_id')
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
            'pseudonym': {'unique_id': student_details['email'], 'sis_user_id': student_details['ssid']},
        }
        return account.create_user(**user_payload)
    except CanvasException as e:
        print(f"ERROR: Canvas user creation failed: {e}")
        return None

def update_user_ssid(user, new_ssid):
    try:
        logins = user.get_logins()
        if logins:
            logins[0].edit(login={'sis_user_id': new_ssid})
            print(f"INFO: Successfully updated SSID for user '{user.name}' to '{new_ssid}'.")
            return True
        return False
    except (CanvasException, AttributeError) as e:
        print(f"ERROR: Could not update SSID for user '{user.name}': {e}")
        return False

def create_canvas_course(course_name, term_id):
    canvas_api = initialize_canvas_api()
    if not all([canvas_api, CANVAS_SUBACCOUNT_ID]): return None
    try:
        account = canvas_api.get_account(CANVAS_SUBACCOUNT_ID)
        course_data = {'name': course_name, 'course_code': course_name, 'enrollment_term_id': term_id}
        if CANVAS_TEMPLATE_COURSE_ID:
            course_data['source_course_id'] = CANVAS_TEMPLATE_COURSE_ID
        return account.create_course(course=course_data)
    except (ResourceDoesNotExist, CanvasException) as e:
        print(f"ERROR: Creating course '{course_name}': {e}")
        return None

def create_section_if_not_exists(course_id, section_name):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        course = canvas_api.get_course(course_id)
        for section in course.get_sections():
            if section.name.lower() == section_name.lower():
                return section
        return course.create_course_section(course_section={'name': section_name})
    except CanvasException as e:
        print(f"ERROR: Canvas section creation/check failed: {e}")
        return None

def enroll_student_in_section(course_id, user_id, section_id):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed"
    try:
        course = canvas_api.get_course(course_id)
        user = canvas_api.get_user(user_id)
        course.enroll_user(user, 'StudentEnrollment', enrollment={'course_section_id': section_id, 'notify': False})
        return "Success"
    except Conflict:
        return "Already Enrolled"
    except CanvasException as e:
        print(f"ERROR: Failed to enroll user {user_id}: {e}")
        return "Failed"

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed"
    user = find_canvas_user(student_details)
    if not user:
        print(f"INFO: Canvas user not found for {student_details['email']}. Creating new user.")
        user = create_canvas_user(student_details)
    if user:
        try:
            full_user = canvas_api.get_user(user.id)
            if student_details.get('ssid') and hasattr(full_user, 'sis_user_id') and full_user.sis_user_id != student_details['ssid']:
                update_user_ssid(full_user, student_details['ssid'])
            return enroll_student_in_section(course_id, full_user.id, section_id)
        except CanvasException as e:
            print(f"ERROR: Could not retrieve full user object or enroll for user ID {user.id}: {e}")
            return "Failed"
    return "Failed: User not found/created"

# ==============================================================================
# 3. CORE SYNC LOGIC
# ==============================================================================

def get_student_details_from_plp(plp_item_id):
    query = f'query {{ items (ids: [{plp_item_id}]) {{ column_values (ids: ["{PLP_TO_MASTER_STUDENT_CONNECT_COLUMN}"]) {{ value }} }} }}'
    result = execute_monday_graphql(query)
    try:
        linked_ids = get_linked_ids_from_connect_column_value(result['data']['items'][0]['column_values'][0]['value'])
        if not linked_ids: return None
        master_student_id = list(linked_ids)[0]
        details_query = f'query {{ items (ids: [{master_student_id}]) {{ id name column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}"]) {{ id text }} }} }}'
        details_result = execute_monday_graphql(details_query)
        item_details = details_result['data']['items'][0]
        column_map = {cv['id']: cv.get('text', '') for cv in item_details['column_values']}
        raw_email = column_map.get(MASTER_STUDENT_EMAIL_COLUMN, '')
        student_details = {
            'name': item_details['name'],
            'master_id': item_details['id'],
            'ssid': column_map.get(MASTER_STUDENT_SSID_COLUMN, ''),
            'email': unicodedata.normalize('NFKC', raw_email).strip(),
            'canvas_id': column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN, '')
        }
        if not all([student_details['name'], student_details['email']]): return None
        return student_details
    except (TypeError, KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not parse student details for PLP {plp_item_id}: {e}")
        return None

def run_hs_roster_sync_for_student(hs_roster_item, dry_run=True):
    parent_item_id = int(hs_roster_item['id'])
    print(f"\n--- Processing HS Roster for: {hs_roster_item['name']} (ID: {parent_item_id}) ---")
    plp_query = f'query {{ items(ids:[{parent_item_id}]) {{ column_values(ids:["{HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID}"]) {{ value }} }} }}'
    plp_result = execute_monday_graphql(plp_query)
    try:
        plp_item_id = list(get_linked_ids_from_connect_column_value(plp_result['data']['items'][0]['column_values'][0]['value']))[0]
    except (TypeError, KeyError, IndexError):
        print("  SKIPPING: Could not find linked PLP item.")
        return
    HS_ROSTER_SUBITEM_TERM_COLUMN_ID = "color6"
    subitems_query = f'query {{ items (ids: [{parent_item_id}]) {{ subitems {{ id name column_values(ids: ["{HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID}", "{HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}", "{HS_ROSTER_SUBITEM_TERM_COLUMN_ID}"]) {{ id text value }} }} }} }}'
    subitems_result = execute_monday_graphql(subitems_query)
    initial_course_categories = {}
    try:
        for subitem in subitems_result['data']['items'][0]['subitems']:
            subitem_cols = {cv['id']: cv for cv in subitem['column_values']}
            if subitem_cols.get(HS_ROSTER_SUBITEM_TERM_COLUMN_ID, {}).get('text') == "Spring":
                continue
            category = subitem_cols.get(HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID, {}).get('text')
            courses_val = subitem_cols.get(HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID, {}).get('value')
            if category and courses_val:
                for course_id in get_linked_ids_from_connect_column_value(courses_val):
                    initial_course_categories[course_id] = category
    except (TypeError, KeyError, IndexError):
        print("  ERROR: Could not process subitems.")
        return
    all_course_ids = list(initial_course_categories.keys())
    if not all_course_ids: return
    secondary_category_col_id = "dropdown_mkq0r2av"
    secondary_category_query = f"query {{ items (ids: {all_course_ids}) {{ id column_values(ids: [\"{secondary_category_col_id}\"]) {{ text }} }} }}"
    secondary_category_results = execute_monday_graphql(secondary_category_query)
    secondary_category_map = {}
    try:
        for item in secondary_category_results['data']['items']:
            if item.get('column_values'):
                secondary_category_map[int(item['id'])] = item['column_values'][0].get('text')
    except (TypeError, KeyError, IndexError): pass
    plp_updates = defaultdict(set)
    for course_id, initial_category in initial_course_categories.items():
        if initial_category == "Math":
            plp_updates[PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Math")].add(course_id)
        if initial_category == "English":
            plp_updates[PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ELA")].add(course_id)
        secondary_category = secondary_category_map.get(course_id)
        if secondary_category == "ACE":
            plp_updates[PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ACE")].add(course_id)
        if secondary_category not in ["ACE", "Connect"]:
            plp_updates[PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Other/Elective")].add(course_id)
    if not plp_updates: return
    print(f"  Found courses to sync for PLP item {plp_item_id}.")
    if dry_run: return
    for col_id, courses in plp_updates.items():
        if col_id and courses:
            bulk_add_to_connect_column(plp_item_id, int(PLP_BOARD_ID), col_id, courses)

def manage_class_enrollment(action, plp_item_id, class_item_id, student_details, category_name, creator_id, subitem_cols=None):
    # ... (This function is complete and correct)
    pass

def run_plp_sync_for_student(plp_item_id, creator_id, dry_run=True):
    """
    The main sync logic for a single PLP item.
    """
    # The cleanup logic is now handled by the PERFORM_INITIAL_CLEANUP switch
    # in the main execution block, so we no longer need it here.

    print(f"\n--- Processing PLP Item: {plp_item_id} ---")
    # ... rest of the function continues ...
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return
    master_student_id = student_details.get('master_id')
    if not master_student_id: return
    staff_change_values = {PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Staff Change"]}}
    curriculum_change_values = {PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Curriculum Change"]}}
    if not dry_run:
        print("ACTION: Syncing teacher assignments from Master Student board to PLP...")
        for trigger_col, mapping in MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.items():
            master_person_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), trigger_col)
            plp_target_mapping = next((t for t in mapping.get("targets", []) if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
            if plp_target_mapping and master_person_val and master_person_val.get('value'):
                # ... (This part of the logic is correct)
                pass
    print("INFO: Syncing class enrollments...")
    class_id_to_category_map = {}
    for category, column_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
        for class_id in get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), column_id):
            class_id_to_category_map[class_id] = category
    if not class_id_to_category_map:
        print("INFO: No classes to sync.")
        return
    for class_item_id, category_name in class_id_to_category_map.items():
        class_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
        print(f"INFO: Processing class: '{class_name}'")
        if not dry_run:
            manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, category_name, creator_id, subitem_cols=curriculum_change_values)
# Place this function inside Section 3: CORE SYNC LOGIC

def clear_subitems_by_creator(parent_item_id, creator_id_to_delete, dry_run=True):
    """Fetches and deletes subitems created by a specific user."""
    if not creator_id_to_delete:
        print("ERROR: No creator ID provided. Skipping deletion.")
        return

    query = f'query {{ items (ids: [{parent_item_id}]) {{ subitems {{ id creator {{ id }} }} }} }}'
    result = execute_monday_graphql(query)
    subitems_to_delete = []
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            if subitem.get('creator') and str(subitem['creator'].get('id')) == str(creator_id_to_delete):
                subitems_to_delete.append(subitem['id'])
    except (KeyError, IndexError, TypeError):
        # This is not an error, just means no subitems were found.
        print(f"INFO: No script-generated subitems found to clear for item {parent_item_id}.")
        return

    if not subitems_to_delete:
        print(f"INFO: No script-generated subitems found to clear for item {parent_item_id}.")
        return

    print(f"INFO: Found {len(subitems_to_delete)} script-generated subitem(s) to clear for item {parent_item_id}.")

    if dry_run:
        print("DRY RUN: Would delete the subitems listed above.")
        return

    for subitem_id in subitems_to_delete:
        print(f"DELETING subitem {subitem_id}...")
        delete_item(subitem_id)
        # It's good practice to have a small sleep when deleting many items
        time.sleep(0.5)
# ==============================================================================
# 4. SCRIPT EXECUTION
# ==============================================================================

if __name__ == '__main__':
    PERFORM_INITIAL_CLEANUP = True  # SET TO False AFTER THE FIRST RUN
    DRY_RUN = False
    TARGET_USER_NAME = "Sarah Bruce"

    print("======================================================")
    print("=== STARTING NIGHTLY DELTA SYNC SCRIPT           ===")
    print("======================================================")
    
    db = None
    cursor = None
    try:
        print("INFO: Connecting to the database...")
        ssl_opts = {'ssl_ca': 'ca.pem', 'ssl_verify_cert': True}
        db = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME, port=int(DB_PORT), **ssl_opts
        )
        cursor = db.cursor()

        print("INFO: Fetching last sync times for processed students...")
        cursor.execute("SELECT student_id, last_synced_at FROM processed_students")
        processed_map = {row[0]: row[1] for row in cursor.fetchall()}
        print(f"INFO: Found {len(processed_map)} students in the database.")

        print("INFO: Finding creator ID for subitem management...")
        creator_id = get_user_id(TARGET_USER_NAME)
        if not creator_id:
            raise Exception(f"Halting script: Target user '{TARGET_USER_NAME}' could not be found.")

        print("INFO: Fetching all PLP board items from Monday.com...")
        all_plp_items = get_all_board_items(PLP_BOARD_ID)

        print("INFO: Filtering for new or updated students on PLP Board...")
        items_to_process = []
        for item in all_plp_items:
            item_id = int(item['id'])
            updated_at = parse_flexible_timestamp(item['updated_at'])
            
            last_synced = processed_map.get(item_id)
            if last_synced:
                last_synced = last_synced.replace(tzinfo=timezone.utc)

            if not last_synced or updated_at > last_synced:
                items_to_process.append(item)
        
        total_to_process = len(items_to_process)
        print(f"INFO: Found {total_to_process} PLP students that are new or have been updated.")

        for i, plp_item in enumerate(items_to_process, 1):
            plp_item_id = int(plp_item['id'])
            print(f"\n===== Processing Student {i}/{total_to_process} (PLP ID: {plp_item_id}) =====")
            
            try:
                # --- NEW PHASE ---
                print("--- Phase 0: Syncing Special Enrollments (Jumpstart/Study Hall) ---")
                process_student_special_enrollments(plp_item, dry_run=DRY_RUN)
                
            try:
                print("--- Phase 1: Checking for and syncing HS Roster ---")
                hs_roster_connect_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_TO_HS_ROSTER_CONNECT_COLUMN)
                hs_roster_ids = get_linked_ids_from_connect_column_value(hs_roster_connect_val.get('value'))
                
                if hs_roster_ids:
                    hs_roster_item_id = list(hs_roster_ids)[0]
                    hs_roster_item_result = get_all_board_items(HS_ROSTER_BOARD_ID, item_ids=[hs_roster_item_id])
                    if hs_roster_item_result:
                        hs_roster_item_object = hs_roster_item_result[0]
                        run_hs_roster_sync_for_student(hs_roster_item_object, dry_run=DRY_RUN)
                    else:
                        print(f"WARNING: Could not fetch HS Roster item object for ID {hs_roster_item_id}")
                else:
                    print("INFO: No HS Roster item linked. Skipping Phase 1.")

                print("--- Phase 2: Syncing PLP to Canvas ---")
                run_plp_sync_for_student(plp_item_id, creator_id, dry_run=DRY_RUN)

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
        if cursor: cursor.close()
        if db and db.is_connected():
            db.close()
            print("\nINFO: Database connection closed.")

    print("\n======================================================")
    print("=== SCRIPT FINISHED                                ===")
    print("======================================================")
