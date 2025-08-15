run#!/usr/bin/env python3
# ==============================================================================
# NIGHTLY PLP & HS ROSTER SYNC SCRIPT (FINAL, COMPLETE, AND CORRECTED)
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
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = os.environ.get("DB_PORT", 3306)
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
PLP_JUMPSTART_SH_CONNECT_COLUMN = "board_relation_mktqp08q"
MASTER_STUDENT_GRADE_COLUMN_ID = "color_mksy8hcw"
SPECIAL_COURSE_MONDAY_IDS = { "Jumpstart": 9717398551, "ACE Study Hall": 9717398779, "Connect English Study Hall": 9717398717, "Connect Math Study Hall": 9717398109, "Prep Math and ELA Study Hall": 9717398063, "EL Support Study Hall": 10046 }
SPECIAL_COURSE_CANVAS_IDS = { "Jumpstart": 10069, "ACE Study Hall": 10128, "Connect English Study Hall": 10109, "Connect Math Study Hall": 9966, "Prep Math and ELA Study Hall": 9960, "EL Support Study Hall": 10046 }
try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
except (json.JSONDecodeError, TypeError):
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}

# ==============================================================================
# 2. MONDAY.COM & CANVAS UTILITIES (ALL DEFINED FIRST)
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

def get_item_name(item_id, board_id):
    query = f"query {{ items(ids: [{item_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    try: return result['data']['items'][0].get('name')
    except (TypeError, KeyError, IndexError): return None

def get_all_board_items(board_id, item_ids=None, group_id=None):
    all_items = []; cursor = None
    items_page_source = f'groups(ids: ["{group_id}"]) {{ items_page' if group_id else 'items_page'
    id_filter = f'query_params: {{ids: {json.dumps(item_ids)}}}' if item_ids else ""
    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        pagination_args = f", {cursor_str}" if cursor else ""
        filter_args = f"{id_filter}{pagination_args}" if id_filter else cursor_str.lstrip(', ')
        query = f"""query {{ boards(ids: {board_id}) {{ {items_page_source}(limit: 50{pagination_args if group_id else ''}) {{ cursor items {{ id name updated_at }} }} {'}' if group_id else ''} }} }}""" if group_id else f"""query {{ boards(ids: {board_id}) {{ {items_page_source} (limit: 50, {filter_args}) {{ cursor items {{ id name updated_at }} }} }} }}"""
        result = execute_monday_graphql(query)
        if not result or 'data' not in result: break
        try:
            page_info = result['data']['boards'][0]['groups'][0]['items_page'] if group_id else result['data']['boards'][0]['items_page']
            all_items.extend(page_info['items'])
            cursor = page_info.get('cursor')
            if not cursor or item_ids: break
            print(f"  Fetched {len(all_items)} items from board {board_id}...")
        except (KeyError, IndexError):
            print(f"ERROR: Could not parse items from board {board_id}.")
            break
    return all_items

def get_user_id(user_name):
    query = f'query {{ users(kind: all) {{ id name }} }}'
    result = execute_monday_graphql(query)
    try:
        for user in result['data']['users']:
            if user['name'].lower() == user_name.lower(): return user['id']
    except (KeyError, IndexError, TypeError): pass
    return None

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
    try: current_linked_items = get_linked_ids_from_connect_column_value(result['data']['items'][0]['column_values'][0]['value'])
    except (TypeError, KeyError, IndexError): pass
    updated_linked_items = current_linked_items.union(course_ids_to_add)
    if updated_linked_items == current_linked_items: return True
    connect_value = {"linkedPulseIds": [{"linkedPulseId": int(lid)} for lid in sorted(list(updated_linked_items))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    mutation = f'mutation {{ change_column_value (board_id: {board_id}, item_id: {item_id}, column_id: "{connect_column_id}", value: {graphql_value}) {{ id }} }}'
    print(f"    SYNCING: Adding {len(course_ids_to_add - current_linked_items)} courses to column {connect_column_id} on PLP item {item_id}.")
    return execute_monday_graphql(mutation) is not None

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    parsed_new_value = new_people_value if isinstance(new_people_value, dict) else json.loads(new_people_value) if isinstance(new_people_value, str) else {}
    persons_and_teams = parsed_new_value.get('personsAndTeams', [])
    if target_column_type == "person":
        person_id = persons_and_teams[0].get('id') if persons_and_teams else None
        graphql_value = json.dumps(json.dumps({"personId": person_id} if person_id else {}))
    elif target_column_type == "multiple-person":
        people_list = [{"id": p.get('id'), "kind": "person"} for p in persons_and_teams if 'id' in p]
        graphql_value = json.dumps(json.dumps({"personsAndTeams": people_list}))
    else: return False
    mutation = f"""mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{people_column_id}", value: {graphql_value}) {{ id }} }}"""
    return execute_monday_graphql(mutation) is not None

def initialize_canvas_api():
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY) if CANVAS_API_URL and CANVAS_API_KEY else None

def find_canvas_user(student_details, cursor):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    plp_item_id = student_details.get('plp_id')
    if plp_item_id:
        cursor.execute("SELECT canvas_id FROM processed_students WHERE student_id = %s", (plp_item_id,))
        result = cursor.fetchone()
        if result and result[0]:
            print(f"  INFO: Found cached Canvas ID {result[0]} for student.")
            try: return canvas_api.get_user(result[0])
            except ResourceDoesNotExist: print(f"  WARNING: Cached Canvas ID {result[0]} was not found. Searching again.")
    id_from_monday = student_details.get('canvas_id')
    if id_from_monday:
        try: return canvas_api.get_user(int(id_from_monday))
        except (ValueError, TypeError):
            try: return canvas_api.get_user(id_from_monday, 'sis_user_id')
            except ResourceDoesNotExist: pass
        except ResourceDoesNotExist: pass
    if student_details.get('email'):
        try: return canvas_api.get_user(student_details['email'], 'login_id')
        except ResourceDoesNotExist: pass
    if student_details.get('ssid') and student_details.get('ssid') != id_from_monday:
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
        user_payload = { 'user': {'name': student_details['name'], 'terms_of_use': True}, 'pseudonym': {'unique_id': student_details['email'], 'sis_user_id': student_details['ssid']}, }
        return account.create_user(**user_payload)
    except CanvasException as e:
        print(f"ERROR: Canvas user creation failed: {e}")
        raise

def update_user_ssid(user, new_ssid):
    try:
        logins = user.get_logins()
        if logins:
            logins[0].edit(login={'sis_user_id': new_ssid})
            return True
        return False
    except (CanvasException, AttributeError) as e:
        print(f"ERROR: Could not update SSID for user '{user.name}': {e}")
        return False

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
    except Conflict: return "Already Enrolled"
    except CanvasException as e:
        print(f"ERROR: Failed to enroll user {user_id}: {e}")
        return "Failed"

def get_study_hall_section_from_grade(grade_text):
    if not grade_text: return "General"
    match = re.search(r'\d+', grade_text)
    if grade_text.upper() in ["TK", "K"]: grade_level = 0
    elif match: grade_level = int(match.group(0))
    else: return "General"
    if grade_level <= 5: return "Elementary School"
    elif 6 <= grade_level <= 8: return "Middle School"
    elif 9 <= grade_level <= 12: return "High School"
    else: return "General"

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
    except (KeyError, IndexError, TypeError): pass
    return False

def parse_flexible_timestamp(ts_string):
    try: return datetime.strptime(ts_string, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    except ValueError: return datetime.strptime(ts_string, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

# ==============================================================================
# 3. CORE LOGIC FUNCTIONS
# ==============================================================================
def enroll_or_create_and_enroll(course_id, section_id, student_details, db_cursor):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed"
    user = find_canvas_user(student_details, db_cursor)
    if not user:
        print(f"INFO: Canvas user not found for {student_details['email']}. Attempting to create new user.")
        try:
            user = create_canvas_user(student_details)
        except CanvasException as e:
            if "sis_user_id" in str(e) and "is already in use" in str(e):
                print(f"INFO: User creation failed because SIS ID is in use. Searching again for existing user.")
                user = find_canvas_user(student_details, db_cursor)
            else:
                print(f"ERROR: A critical error occurred during user creation: {e}")
                user = None
    if user:
        try:
            full_user = canvas_api.get_user(user.id)
            db_cursor.execute("UPDATE processed_students SET canvas_id = %s WHERE student_id = %s", (str(full_user.id), student_details['plp_id']))
            if student_details.get('ssid') and hasattr(full_user, 'sis_user_id') and full_user.sis_user_id != student_details['ssid']:
                update_user_ssid(full_user, student_details['ssid'])
            return enroll_student_in_section(course_id, full_user.id, section_id)
        except CanvasException as e:
            print(f"ERROR: Could not retrieve full user object or enroll for user ID {user.id}: {e}")
            return "Failed"
    print(f"ERROR: Could not find or create a Canvas user for {student_details.get('name')}. Final enrollment failed.")
    return "Failed"

def get_student_details_from_plp(plp_item_id):
    print(f"  [DIAGNOSTIC] Starting detail fetch for PLP item: {plp_item_id}")
    try:
        query = f'query {{ items (ids: [{plp_item_id}]) {{ column_values (ids: ["{PLP_TO_MASTER_STUDENT_CONNECT_COLUMN}"]) {{ value }} }} }}'
        result = execute_monday_graphql(query)
        column_value = result['data']['items'][0]['column_values'][0]['value']
        if not column_value:
            print("  [DIAGNOSTIC] FAILED: 'Connect to Master' column is empty.")
            return None
        connect_column_value = json.loads(column_value)
        linked_ids = [item['linkedPulseId'] for item in connect_column_value.get('linkedPulseIds', [])]
        if not linked_ids:
            print("  [DIAGNOSTIC] FAILED: 'Connect to Master' column is linked, but the linked item list is empty.")
            return None
        master_student_id = linked_ids[0]
        print(f"  [DIAGNOSTIC] Found Master Student ID: {master_student_id}")
    except (TypeError, KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"  [DIAGNOSTIC] FAILED: Could not get the Master Student ID. Error: {e}")
        return None
    try:
        details_query = f'query {{ items (ids: [{master_student_id}]) {{ id name column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}"]) {{ id text }} }} }}'
        details_result = execute_monday_graphql(details_query)
        item_details = details_result['data']['items'][0]
        student_name = item_details.get('name')
        if not student_name:
            print(f"  [DIAGNOSTIC] FAILED: Master Student item {master_student_id} has no name.")
            return None
        print(f"  [DIAGNOSTIC] Found Name: {student_name}")
        column_map = {cv['id']: cv.get('text', '') for cv in item_details.get('column_values', [])}
        raw_email = column_map.get(MASTER_STUDENT_EMAIL_COLUMN)
        if not raw_email:
            print(f"  [DIAGNOSTIC] FAILED: Master Student item {master_student_id} is missing an email address.")
            return None
        print(f"  [DIAGNOSTIC] Found Email: {raw_email}")
        email = unicodedata.normalize('NFKC', raw_email).strip()
        ssid = column_map.get(MASTER_STUDENT_SSID_COLUMN, '')
        canvas_id = column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN, '')
        print("  [DIAGNOSTIC] Successfully gathered all required details.")
        return {'name': student_name, 'ssid': ssid, 'email': email, 'canvas_id': canvas_id, 'master_id': item_details['id'], 'plp_id': plp_item_id}
    except (TypeError, KeyError, IndexError) as e:
        print(f"  [DIAGNOSTIC] FAILED: Could not parse details from the Master Student board. Error: {e}")
        return None

def process_student_special_enrollments(plp_item, db_cursor, dry_run=True):
    plp_item_id = int(plp_item['id'])
    print(f"\n--- Processing Special Enrollments for: {plp_item['name']} (PLP ID: {plp_item_id}) ---")
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details:
        print("  SKIPPING: Could not get student details.")
        return
    master_id = student_details['master_id']
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
    jumpstart_canvas_id = SPECIAL_COURSE_CANVAS_IDS.get("Jumpstart")
    if jumpstart_canvas_id:
        print(f"  Processing Jumpstart enrollment, section: {tor_last_name}")
        if not dry_run:
            section = create_section_if_not_exists(jumpstart_canvas_id, tor_last_name)
            if section:
                result = enroll_or_create_and_enroll(jumpstart_canvas_id, section.id, student_details, db_cursor)
                print(f"  -> Enrollment status: {result}")
    sh_section_name = get_study_hall_section_from_grade(grade_text)
    target_sh_name = "ACE Study Hall"
    target_sh_canvas_id = SPECIAL_COURSE_CANVAS_IDS.get(target_sh_name)
    if target_sh_canvas_id:
        print(f"  Processing {target_sh_name} enrollment, section: {sh_section_name}")
        if not dry_run:
            section = create_section_if_not_exists(target_sh_canvas_id, sh_section_name)
            if section:
                result = enroll_or_create_and_enroll(target_sh_canvas_id, section.id, student_details, db_cursor)
                print(f"  -> Enrollment status: {result}")

# In nightly_sync (1).py, replace the entire run_hs_roster_sync_for_student function

# In nightly_sync (1).py, replace the entire run_hs_roster_sync_for_student function

def run_hs_roster_sync_for_student(hs_roster_item, dry_run=True):
    parent_item_id = int(hs_roster_item['id'])
    print(f"\n--- Processing HS Roster for: {hs_roster_item['name']} (ID: {parent_item_id}) ---")
    
    plp_query = f'query {{ items(ids:[{parent_item_id}]) {{ column_values(ids:["{HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID}"]) {{ value }} }} }}'
    plp_result = execute_monday_graphql(plp_query)
    try: plp_item_id = list(get_linked_ids_from_connect_column_value(plp_result['data']['items'][0]['column_values'][0]['value']))[0]
    except (TypeError, KeyError, IndexError):
        print("  SKIPPING: Could not find linked PLP item.")
        return
        
    HS_ROSTER_SUBITEM_TERM_COLUMN_ID = "color6"
    subitems_query = f'query {{ items (ids: [{parent_item_id}]) {{ subitems {{ id name column_values(ids: ["{HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID}", "{HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}", "{HS_ROSTER_SUBITEM_TERM_COLUMN_ID}"]) {{ id text value }} }} }} }}'
    subitems_result = execute_monday_graphql(subitems_query)
    
    course_data = defaultdict(lambda: {'primary_category': '', 'course_ids': set()})
    try:
        subitems = subitems_result['data']['items'][0]['subitems']
        for subitem in subitems:
            subitem_cols = {cv['id']: cv for cv in subitem['column_values']}
            
            term_val = subitem_cols.get(HS_ROSTER_SUBITEM_TERM_COLUMN_ID, {}).get('text')
            if term_val == "Spring":
                print(f"  SKIPPING: Subitem '{subitem['name']}' is marked as Spring.")
                continue
            
            category_text = subitem_cols.get(HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID, {}).get('text', '')
            courses_val = subitem_cols.get(HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID, {}).get('value')
            if category_text and courses_val:
                labels = [label.strip() for label in category_text.split(',')]
                course_ids = get_linked_ids_from_connect_column_value(courses_val)
                for label in labels:
                    if label:
                        for course_id in course_ids:
                             course_data[course_id]['primary_category'] = label
                             course_data[course_id]['course_ids'].add(course_id)
    except (TypeError, KeyError, IndexError):
        print("  ERROR: Could not process subitems.")
        return

    all_course_ids = list(course_data.keys())
    if not all_course_ids: 
        print("  INFO: No non-Spring courses found to process.")
        return

    secondary_category_col_id = "dropdown_mkq0r2av"
    secondary_category_query = f"query {{ items (ids: {all_course_ids}) {{ id column_values(ids: [\"{secondary_category_col_id}\"]) {{ text }} }} }}"
    secondary_category_results = execute_monday_graphql(secondary_category_query)
    secondary_category_map = {int(item['id']): item['column_values'][0].get('text') for item in secondary_category_results.get('data', {}).get('items', []) if item.get('column_values')}
    
    plp_updates = defaultdict(set)
    for course_id, data in course_data.items():
        primary_category = data.get('primary_category', '')
        secondary_category = secondary_category_map.get(course_id, '')

        # Handle ACE and ELA/Other/Elective
        if secondary_category == "ACE":
            ace_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ACE")
            if ace_col_id:
                plp_updates[ace_col_id].add(course_id)

            if primary_category in ["ELA", "Other/Elective"]:
                primary_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(primary_category)
                if primary_col_id:
                    plp_updates[primary_col_id].add(course_id)
        # Handle all other categories, including unmapped ones
        else:
            target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(primary_category)
            if target_col_id:
                plp_updates[target_col_id].add(course_id)
            else:
                other_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Other/Elective")
                if other_col_id:
                    print(f"  WARNING: Subject '{primary_category}' doesn't map to a PLP column. Routing to 'Other/Elective'.")
                    plp_updates[other_col_id].add(course_id)
                else:
                    print(f"  WARNING: Subject '{primary_category}' not mapped and 'Other/Elective' is not configured. Skipping.")

    if not plp_updates:
        print("  INFO: No valid courses found to sync after categorization.")
        return
        
    print(f"  Found courses to sync for PLP item {plp_item_id}.")
    if dry_run:
        for col_id, courses in plp_updates.items():
            print(f"    DRY RUN: Would add {len(courses)} courses to PLP column {col_id}.")
        return

    for col_id, courses in plp_updates.items():
        if col_id and courses:
            bulk_add_to_connect_column(plp_item_id, int(PLP_BOARD_ID), col_id, courses)
            time.sleep(1)

def manage_class_enrollment(action, plp_item_id, class_item_id, student_details, category_name, creator_id, db_cursor, subitem_cols=None, dry_run=True):
    subitem_cols = subitem_cols or {}
    class_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
    if action == "enroll":
        print(f"  ACTION: Pushing enrollment for '{class_name}' to Canvas.")
        if not dry_run:
            linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
            if linked_canvas_item_ids:
                canvas_item_id = list(linked_canvas_item_ids)[0]
                course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
                canvas_course_id = None
                if course_id_val:
                    canvas_course_id = course_id_val.get('text')
                    if not canvas_course_id: canvas_course_id = course_id_val.get('value')
                if canvas_course_id:
                    section = create_section_if_not_exists(canvas_course_id, "All")
                    if section: enroll_or_create_and_enroll(canvas_course_id, section.id, student_details, db_cursor)
        subitem_title = f"Added {category_name} '{class_name}'"
        if not check_if_subitem_exists(plp_item_id, subitem_title, creator_id):
            print(f"  INFO: Subitem log is missing. Creating it.")
            if not dry_run: create_subitem(plp_item_id, subitem_title, column_values=subitem_cols)
        else:
            print(f"  INFO: Subitem log already exists.")
    elif action == "unenroll":
        subitem_title = f"Removed {category_name} '{class_name}'"
        print(f"  INFO: Unenrolling student and creating log: '{subitem_title}'")
        create_subitem(plp_item_id, subitem_title, column_values=subitem_cols)

def sync_teacher_assignments(master_student_id, plp_item_id, dry_run=True):
    print("ACTION: Syncing teacher assignments from Master Student board to PLP...")
    for source_col_id, mapping in MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.items():
        master_person_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), source_col_id)
        source_person_ids = get_people_ids_from_value(master_person_val.get('value')) if master_person_val else set()
        plp_target_mapping = next((t for t in mapping.get("targets", []) if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
        if plp_target_mapping:
            target_col_id = plp_target_mapping.get("target_column_id")
            target_col_type = plp_target_mapping.get("target_column_type")
            current_plp_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), target_col_id)
            current_person_ids = get_people_ids_from_value(current_plp_val.get('value')) if current_plp_val else set()
            if source_person_ids != current_person_ids:
                print(f"  -> Change detected for {mapping.get('name', 'Staff')}. Updating PLP column {target_col_id}.")
                if not dry_run:
                    update_people_column(plp_item_id, int(PLP_BOARD_ID), target_col_id, master_person_val.get('value'), target_col_type)
            else:
                print(f"  -> No change needed for {mapping.get('name', 'Staff')}. Values are already in sync.")

def run_plp_sync_for_student(plp_item_id, creator_id, db_cursor, dry_run=True):
    print(f"\n--- Processing PLP Item: {plp_item_id} ---")
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return
    master_student_id = student_details.get('master_id')
    if not master_student_id: return
    curriculum_change_values = {PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Curriculum Change"]}}
    print("INFO: Syncing class enrollments...")
    class_id_to_category_map = {}
    for category, column_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
        for class_id in get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), column_id):
            class_id_to_category_map[class_id] = category
    if not class_id_to_category_map:
        print("INFO: No classes to sync.")
    for class_item_id, category_name in class_id_to_category_map.items():
        class_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
        print(f"INFO: Processing class: '{class_name}'")
        manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, category_name, creator_id, db_cursor, subitem_cols=curriculum_change_values, dry_run=dry_run)
    sync_teacher_assignments(master_student_id, plp_item_id, dry_run=dry_run)

def reconcile_subitems(plp_item_id, creator_id, db_cursor, dry_run=True):
    print(f"--- Reconciling All Subitems & Enrollments for PLP Item: {plp_item_id} ---")
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details or not student_details.get('master_id'):
        print("  SKIPPING: Could not get complete student details for reconciliation.")
        return
    print("  -> Reconciling course enrollments...")
    class_id_to_category_map = {}
    for category, column_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
        for class_id in get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), column_id):
            class_id_to_category_map[class_id] = category
    if not class_id_to_category_map:
        print("    INFO: No classes are linked.")
    else:
        for class_item_id, category_name in class_id_to_category_map.items():
            class_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
            print(f"    ACTION: Processing enrollment for '{class_name}'.")
            expected_subitem_name = f"Added {category_name} '{class_name}'"
            if not dry_run:
                linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
                if linked_canvas_item_ids:
                    canvas_item_id = list(linked_canvas_item_ids)[0]
                    course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
                    canvas_course_id = None
                    if course_id_val:
                        canvas_course_id = course_id_val.get('text')
                        if not canvas_course_id: canvas_course_id = course_id_val.get('value')
                    if canvas_course_id:
                        section = create_section_if_not_exists(canvas_course_id, "All")
                        if section: enroll_or_create_and_enroll(canvas_course_id, section.id, student_details, db_cursor)
                else:
                    print(f"    INFO: '{class_name}' is a non-Canvas course.")
            if not check_if_subitem_exists(plp_item_id, expected_subitem_name, creator_id):
                print(f"    INFO: Subitem '{expected_subitem_name}' is missing. Creating it.")
                if not dry_run: create_subitem(plp_item_id, expected_subitem_name, column_values={PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Curriculum Change"]}})
            else:
                print(f"    INFO: Subitem '{expected_subitem_name}' already exists.")
    print("  -> Reconciling staff assignments...")
    master_student_id = student_details['master_id']
    for trigger_col, mapping in MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.items():
        staff_role_name = mapping.get("name", "Staff")
        master_person_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), trigger_col)
        if master_person_val and master_person_val.get('value'):
            person_ids = get_people_ids_from_value(master_person_val.get('value'))
            if person_ids:
                person_id = list(person_ids)[0]
                person_name = get_user_name(person_id)
                if person_name:
                    expected_staff_subitem = f"{staff_role_name}: {person_name}"
                    if not check_if_subitem_exists(plp_item_id, expected_staff_subitem, creator_id):
                        print(f"    INFO: Subitem '{expected_staff_subitem}' is missing. Creating it.")
                        if not dry_run: create_subitem(plp_item_id, expected_staff_subitem, column_values={PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Staff Change"]}})
                    else:
                        print(f"    INFO: Subitem '{expected_staff_subitem}' already exists.")

# ==============================================================================
# 4. SCRIPT EXECUTION
# ==============================================================================
if __name__ == '__main__':
    PERFORM_INITIAL_CLEANUP = False
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
        db = mysql.connector.connect( host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=int(DB_PORT), **ssl_opts )
        cursor = db.cursor()
        print("INFO: Fetching last sync times for processed students...")
        cursor.execute("SELECT student_id, last_synced_at, canvas_id FROM processed_students")
        processed_map = {row[0]: {'last_synced': row[1], 'canvas_id': row[2]} for row in cursor.fetchall()}
        print(f"INFO: Found {len(processed_map)} students in the database.")
        print("INFO: Finding creator ID for subitem management...")
        creator_id = get_user_id(TARGET_USER_NAME)
        if not creator_id: raise Exception(f"Halting script: Target user '{TARGET_USER_NAME}' could not be found.")
        print("INFO: Fetching all PLP board items from Monday.com...")
        all_plp_items = get_all_board_items(PLP_BOARD_ID)
        print("INFO: Filtering for new or updated students on PLP Board...")
        items_to_process = []
        for item in all_plp_items:
            item_id = int(item['id'])
            updated_at = parse_flexible_timestamp(item['updated_at'])
            sync_data = processed_map.get(item_id)
            last_synced = sync_data['last_synced'] if sync_data else None
            if last_synced: last_synced = last_synced.replace(tzinfo=timezone.utc)
            if not last_synced or updated_at > last_synced:
                items_to_process.append(item)
        total_to_process = len(items_to_process)
        print(f"INFO: Found {total_to_process} PLP students that are new or have been updated.")
        
        for i, plp_item in enumerate(items_to_process, 1):
            plp_item_id = int(plp_item['id'])
            print(f"\n===== Processing Student {i}/{total_to_process} (PLP ID: {plp_item_id}) =====")
            try:
                print("--- Phase 0: Syncing Special Enrollments (Jumpstart/Study Hall) ---")
                process_student_special_enrollments(plp_item, cursor, dry_run=DRY_RUN)
                print("--- Phase 1: Checking for and syncing HS Roster ---")
                hs_roster_connect_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_TO_HS_ROSTER_CONNECT_COLUMN)
                hs_roster_ids = get_linked_ids_from_connect_column_value(hs_roster_connect_val.get('value')) if hs_roster_connect_val else set()
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
                run_plp_sync_for_student(plp_item_id, creator_id, cursor, dry_run=DRY_RUN)
                if not DRY_RUN:
                    print(f"INFO: Sync successful. Updating timestamp for PLP item {plp_item_id}.")
                    update_query = ''' INSERT INTO processed_students (student_id, last_synced_at) VALUES (%s, NOW()) ON DUPLICATE KEY UPDATE last_synced_at = NOW() '''
                    cursor.execute(update_query, (plp_item_id,))
                    db.commit()
            except Exception as e:
                print(f"FATAL ERROR processing PLP item {plp_item_id}: {e}")

        print("\n======================================================")
        print("=== STARTING FINAL RECONCILIATION RUN          ===")
        print("======================================================")
        total_all_students = len(all_plp_items)
        print(f"INFO: Reconciling subitems for all {total_all_students} students...")
        for i, plp_item in enumerate(all_plp_items, 1):
            plp_item_id = int(plp_item['id'])
            print(f"\n===== Reconciling Student {i}/{total_all_students} (PLP ID: {plp_item_id}) =====")
            try:
                reconcile_subitems(plp_item_id, creator_id, cursor, dry_run=DRY_RUN)
                if not DRY_RUN:
                    print(f"INFO: Reconciliation successful. Updating timestamp for PLP item {plp_item_id}.")
                    update_query = ''' INSERT INTO processed_students (student_id, last_synced_at) VALUES (%s, NOW()) ON DUPLICATE KEY UPDATE last_synced_at = NOW() '''
                    cursor.execute(update_query, (plp_item_id,))
                    db.commit()
            except Exception as e:
                print(f"FATAL ERROR during reconciliation for PLP item {plp_item_id}: {e}")
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
