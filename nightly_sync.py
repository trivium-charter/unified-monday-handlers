#!/usr/bin/env python3
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
HS_ROSTER_TRACK_COLUMN_ID = "status7"
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
TA_SUB_EMAIL = "sub@triviumcharter.org"
TA_AIDE_EMAIL = "aide@triviumcharter.org"

# <<< ADDED FROM app.py: Configuration variables for teacher sync
ALL_STAFF_EMAIL_COLUMN_ID = os.environ.get("ALL_STAFF_EMAIL_COLUMN_ID")
ALL_STAFF_SIS_ID_COLUMN_ID = os.environ.get("ALL_STAFF_SIS_ID_COLUMN_ID")
ALL_STAFF_CANVAS_ID_COLUMN = "text_mktg7h6"
ALL_STAFF_INTERNAL_ID_COLUMN = "text_mkthjxht"

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
except (json.JSONDecodeError, TypeError):
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}

# The 10 special courses
ROSTER_ONLY_COURSES = {10298, 10297, 10299, 10300, 10301}
ROSTER_AND_CREDIT_COURSES = {10097, 10002, 10092, 10164, 10198}
ALL_SPECIAL_COURSES = ROSTER_ONLY_COURSES.union(ROSTER_AND_CREDIT_COURSES)

# Map for STAFF "People" columns on the PLP board
PLP_PEOPLE_COLUMNS_MAP = {
    "TOR Assignments": "person",
    "Case Manager Assignments": "multiple_person_mks1hqnj",
    "Connect Teacher Assignments": "multiple_person_mks1hzcz",
    "ACE Teacher Assignments": "multiple_person_mks1w5fc"
}

# ==============================================================================
# 2. MONDAY.COM & CANVAS UTILITIES (ALL DEFINED FIRST)
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def is_middle_or_high_school(grade_text):
    """Checks if a student is in middle or high school (grades 6-12)."""
    if not grade_text: return False
    # Handle TK and K explicitly as not middle/high
    if grade_text.upper() in ["TK", "K"]:
        return False
    match = re.search(r'\d+', grade_text)
    if match:
        grade_level = int(match.group(0))
        return 6 <= grade_level <= 12
    return False

def is_high_school_student(grade_text):
    """Checks if a student is in high school based on their grade text."""
    if not grade_text: return False
    match = re.search(r'\d+', grade_text)
    if match:
        grade_level = int(match.group(0))
        return 9 <= grade_level <= 12
    return False

def delete_item(item_id):
    """Deletes a Monday.com item (or subitem)."""
    mutation = f"mutation {{ delete_item (item_id: {item_id}) {{ id }} }}"
    print(f"  -> DELETING Monday.com item: {item_id}")
    result = execute_monday_graphql(mutation)
    return result is not None

def create_monday_update(item_id, update_text):
    """Creates an update (comment) on a Monday.com item."""
    mutation = f'mutation {{ create_update (item_id: {item_id}, body: {json.dumps(update_text)}) {{ id }} }}'
    return execute_monday_graphql(mutation)

def get_item_names(item_ids):
    """Efficiently gets names for a list of item IDs."""
    if not item_ids:
        return {}
    query = f"query {{ items(ids: {list(item_ids)}) {{ id name }} }}"
    result = execute_monday_graphql(query)
    try:
        return {int(item['id']): item['name'] for item in result['data']['items']}
    except (TypeError, KeyError, IndexError):
        return {}

def get_logged_items_from_updates(subitem_id):
    """
    Reads the most recent 'Current state' update to determine the logged state of items.
    Returns a set of item names (e.g., "'Course A'", "'Staff B'").
    """
    if not subitem_id:
        return set()
    query = f"query {{ items(ids: [{subitem_id}]) {{ updates(limit: 50) {{ body }} }} }}"
    result = execute_monday_graphql(query)
    
    try:
        updates = result['data']['items'][0]['updates']
        # Updates are newest first, so we don't need to reverse
        for update in updates:
            body = update.get('body', '')
            # Check for the key phrases that declare the final state
            if "curriculum is now:" in body or "assignment is now:" in body:
                # Find the part of the string after the key phrase
                state_string = ""
                if "curriculum is now:" in body:
                    state_string = body.split("curriculum is now:")[1]
                elif "assignment is now:" in body:
                    state_string = body.split("assignment is now:")[1]
                
                # Use a regular expression to find all items enclosed in single quotes
                # This is robust and handles names with spaces or special characters
                logged_items = re.findall(r"'([^']*)'", state_string)
                # Return the set of names, formatted with quotes to match the source of truth
                return {f"'{item}'" for item in logged_items}
                
    except (TypeError, KeyError, IndexError):
        pass
        
    # If no state-declaring update is found, return an empty set
    return set()
    
def find_or_create_subitem(parent_item_id, subitem_name, column_values=None, dry_run=False):
    """
    Finds a subitem by name. If it doesn't exist, it creates it.
    Returns a tuple: (subitem_id, was_created_boolean).
    """
    # First, try to find the subitem by name
    query = f'query {{ items(ids:[{parent_item_id}]) {{ subitems {{ id name }} }} }}'
    result = execute_monday_graphql(query)
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            if subitem.get('name') == subitem_name:
                # Found existing subitem
                return subitem['id'], False
    except (KeyError, IndexError, TypeError):
        pass

    # If not found, create it
    print(f"  INFO: No existing subitem named '{subitem_name}'. Creating it.")
    if not dry_run:
        new_id = create_subitem(parent_item_id, subitem_name, column_values=column_values)
        return new_id, True
    else:
        print(f"  -> DRY RUN: Would create subitem '{subitem_name}'.")
        # Return a placeholder and True to simulate creation
        return "dry_run_placeholder_id", True
        
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
    """Fetches all item IDs from a board, handling pagination."""
    all_items = []
    cursor = None
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

def get_roster_teacher_name(master_student_id):
    tor_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_TOR_COLUMN_ID)
    if tor_val and tor_val.get('value'):
        tor_ids = get_people_ids_from_value(tor_val['value'])
        if tor_ids:
            tor_full_name = get_user_name(list(tor_ids)[0])
            if tor_full_name: return tor_full_name.split()[-1]
    return "Orientation" # Default value for nightly sync

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
    """Efficiently adds multiple items to a connect boards column."""
    query_current = f'query {{ items(ids:[{item_id}]) {{ column_values(ids:["{connect_column_id}"]) {{ value }} }} }}'
    result = execute_monday_graphql(query_current)
    current_linked_items = set()
    try:
        col_val = result['data']['items'][0]['column_values']
        if col_val:
            current_linked_items = get_linked_ids_from_connect_column_value(col_val[0]['value'])
    except (TypeError, KeyError, IndexError):
        pass
        
    updated_linked_items = current_linked_items.union(course_ids_to_add)
    
    if updated_linked_items == current_linked_items:
        return True

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

def find_canvas_teacher(teacher_details):
    """
    Finds a Canvas user based on provided details without checking their role.
    Prioritizes Canvas ID, then SIS ID, then email.
    """
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None

    # Search by internal Canvas ID
    if teacher_details.get('canvas_id'):
        try:
            return canvas_api.get_user(teacher_details['canvas_id'])
        except (ResourceDoesNotExist, ValueError):
            pass

    # Search by SIS ID
    if teacher_details.get('sis_id'):
        try:
            return canvas_api.get_user(teacher_details['sis_id'], 'sis_user_id')
        except ResourceDoesNotExist:
            pass

    # Search by email
    if teacher_details.get('email'):
        try:
            return canvas_api.get_user(teacher_details['email'], 'login_id')
        except ResourceDoesNotExist:
            pass

    # Broader email search (fallback)
    if teacher_details.get('email'):
        try:
            users = [u for u in canvas_api.get_account(1).get_users(search_term=teacher_details['email'])]
            if len(users) == 1:
                return users[0]
        except (ResourceDoesNotExist, CanvasException):
            pass
            
    return None

def find_or_create_canvas_user(student_details, db_cursor):
    """Finds an existing Canvas user or creates a new one, returning the user object."""
    user = find_canvas_user(student_details, db_cursor)
    if user:
        return user
    
    print(f"INFO: Canvas user not found for {student_details['email']}. Attempting to create new user.")
    try:
        user = create_canvas_user(student_details, db_cursor=db_cursor)
        if user:
             # If creation succeeds, update the DB cache immediately
            db_cursor.execute("UPDATE processed_students SET canvas_id = %s WHERE student_id = %s", (str(user.id), student_details['plp_id']))
        return user
    except CanvasException as e:
        if ("sis_user_id" in str(e) and "is already in use" in str(e)) or \
           ("unique_id" in str(e) and "ID already in use" in str(e)):
            print(f"INFO: User creation failed because ID is in use. Searching again for existing user.")
            return find_canvas_user(student_details, db_cursor)
        else:
            print(f"ERROR: A critical error occurred during user creation: {e}")
            return None

def create_canvas_user(user_details, role='student', db_cursor=None):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        account = canvas_api.get_account(1)
        user_payload = {
            'user': {'name': user_details['name'], 'terms_of_use': True},
            'pseudonym': {
                'unique_id': user_details['email'],
                'sis_user_id': user_details.get('sis_id') or user_details.get('ssid') or user_details['email'],
                'login_id': user_details['email'],
                'authentication_provider_id': '112'
            },
            'communication_channel': {
                'type': 'email',
                'address': user_details['email'],
                'skip_confirmation': True
            }
        }
        new_user = account.create_user(**user_payload)
        return new_user
    except CanvasException as e:
        print(f"ERROR: Canvas user creation failed for {user_details['email']}: {e}")
        if ("sis_user_id" in str(e) and "is already in use" in str(e)) or \
           ("unique_id" in str(e) and "ID already in use" in str(e)):
            print(f"INFO: User creation failed because ID is in use. Attempting to find existing user.")
            return find_canvas_teacher(user_details) if role == 'teacher' else find_canvas_user(user_details, db_cursor)
        raise


def update_user_ssid(user, new_ssid):
    """
    CORRECTED: Fetches the full user object to ensure .get_logins() is available.
    """
    try:
        canvas_api = initialize_canvas_api()
        # This line ensures the full, detailed user object is fetched
        full_user_obj = canvas_api.get_user(user.id)
        logins = full_user_obj.get_logins()
        if logins:
            login_to_update = logins[0]
            login_to_update.edit(login={'sis_user_id': new_ssid})
            return True
        return False
    except (CanvasException, AttributeError) as e:
        print(f"ERROR: API error updating SSID for user ID '{user.id}': {e}")
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

def enroll_user_in_course(course_id, user_id, role='StudentEnrollment'):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed: Canvas API not initialized"
    try:
        course = canvas_api.get_course(course_id)
        user = canvas_api.get_user(user_id)
        enrollment = course.enroll_user(user, role, enrollment_state='active', notify=False)
        return "Success" if enrollment else "Failed"
    except Conflict: return "Already Enrolled"
    except CanvasException as e:
        print(f"ERROR: Failed to enroll user {user_id} with role {role} in course {course_id}. Details: {e}")
        return "Failed"

def unenroll_student_from_course(course_id, student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return False
    # Create a dummy cursor for the find_canvas_user call as it's not essential here
    class DummyCursor:
        def execute(self, *args): pass
        def fetchone(self): return None
    user = find_canvas_user(student_details, cursor=DummyCursor())
    if not user: return True
    try:
        course = canvas_api.get_course(course_id)
        for enrollment in course.get_enrollments(user_id=user.id):
            if enrollment.role == 'StudentEnrollment':
                print(f"  -> Deactivating enrollment {enrollment.id} for user {user.id} in course {course_id}")
                enrollment.deactivate(task='conclude')
        return True
    except CanvasException as e:
        print(f"ERROR: Canvas unenrollment failed: {e}")
        return False

def enroll_teacher_in_course(course_id, teacher_details, role='TeacherEnrollment'):
    canvas_api = initialize_canvas_api()
    if not canvas_api:
        return "Failed: Canvas API not initialized"

    teacher_name = teacher_details.get('name', teacher_details.get('email', 'Unknown'))
    
    user_to_enroll = find_canvas_teacher(teacher_details)

    if not user_to_enroll:
        print(f"  INFO: Teacher '{teacher_name}' not found. Attempting to create.")
        try:
            user_to_enroll = create_canvas_user(teacher_details, role='teacher', db_cursor=None)
        except CanvasException as e:
            if ("sis_user_id" in str(e) and "is already in use" in str(e)) or \
               ("unique_id" in str(e) and "ID already in use" in str(e)):
                print(f"  INFO: Create failed, user exists. Searching again.")
                user_to_enroll = find_canvas_teacher(teacher_details)
            else:
                return f"Failed: Could not create teacher '{teacher_name}'. Error: {e}"

    if not user_to_enroll:
        return f"Failed: Could not find or create teacher '{teacher_name}' with the provided details."
    
    try:
        course = canvas_api.get_course(course_id)
        course.enroll_user(user_to_enroll, role, enrollment_state='active', notify=False)
        return "Success"
    except ResourceDoesNotExist:
        return f"Failed: Course with ID '{course_id}' not found in Canvas."
    except Conflict:
        return "Already Enrolled"
    except CanvasException as e:
        return f"Failed: {e}"

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
def get_canvas_section_name(plp_item_id, class_item_id, class_name, student_details, course_to_track_map, class_id_to_category_map, id_to_name_map):
    """
    Determines the correct Canvas section name for a student based on a clear priority order.
    """
    # Rule for Connect Math Study Hall
    if "Connect Math Study Hall" in class_name:
        for c_id, category in class_id_to_category_map.items():
            course_name = id_to_name_map.get(c_id, "")
            if category == "Math" and "Connect" in course_name:
                return course_name
        return "General Math Connect"

    # Rule for Connect English Study Hall
    if "Connect English Study Hall" in class_name:
        for c_id, category in class_id_to_category_map.items():
            course_name = id_to_name_map.get(c_id, "")
            if category == "ELA" and "Connect" in course_name:
                return course_name
        return "General English Connect"

    # Rule for Prep Math and ELA Study Hall
    if "Prep Math and ELA Study Hall" in class_name:
        prep_subjects = []
        for c_id, category in class_id_to_category_map.items():
            course_name = id_to_name_map.get(c_id, "")
            if category in ["Math", "ELA"] and "Connect" in course_name:
                prep_subjects.append(course_name)
        if not prep_subjects:
            return "General Prep"
        return " & ".join(sorted(prep_subjects))

    # Default for K-8 students or if no other rules apply
    section_name = "General Enrollment"
    
    if is_high_school_student(student_details.get('grade_text')):
        # PRIORITY 1: Check M-Series/Op2 column on the PLP board
        m_series_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_M_SERIES_LABELS_COLUMN)
        m_series_text = m_series_val.get('text') if m_series_val else None
        
        if m_series_text:
            section_name = m_series_text
        else:
            # PRIORITY 2: Fallback to the HS Roster "Track" status column
            section_name = course_to_track_map.get(class_item_id, "General Enrollment")
            
    return section_name

def enroll_or_create_and_enroll(course_id, section_id, student_details, db_cursor):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed"
    user = find_or_create_canvas_user(student_details, db_cursor)
    if user:
        try:
            # Re-fetch the full user object to ensure all attributes are present
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

def sync_study_hall_enrollment(course_id, student_details, target_section_name, db_cursor, dry_run=False):
    """
    Ensures a student is enrolled in the correct study hall section and removed from all others.
    """
    if dry_run:
        print(f"  DRY RUN: Would sync student {student_details['email']} to section '{target_section_name}' in course {course_id}.")
        return

    canvas_api = initialize_canvas_api()
    if not canvas_api: return

    user = find_or_create_canvas_user(student_details, db_cursor)
    if not user:
        print(f"  ERROR: Could not find or create Canvas user for {student_details['email']}. Skipping section sync.")
        return
    
    try:
        course = canvas_api.get_course(course_id)
        target_section = create_section_if_not_exists(course_id, target_section_name)
        if not target_section: return

        enrollments = course.get_enrollments(user_id=user.id)
        is_correctly_enrolled = False
        
        for enrollment in enrollments:
            if enrollment.role == 'StudentEnrollment':
                if enrollment.course_section_id == target_section.id:
                    is_correctly_enrolled = True
                    print(f"  INFO: Student already in correct section '{target_section_name}'.")
                else:
                    # Get section name for logging before removing
                    try:
                        old_section = canvas.get_section(enrollment.course_section_id)
                        print(f"  ACTION: Removing student from incorrect section '{old_section.name}'.")
                    except ResourceDoesNotExist:
                        print(f"  ACTION: Removing student from old/deleted section ID {enrollment.course_section_id}.")
                    enrollment.deactivate(task='conclude')

        if not is_correctly_enrolled:
            print(f"  ACTION: Enrolling student in correct section '{target_section_name}'.")
            enroll_student_in_section(course.id, user.id, target_section.id)

    except CanvasException as e:
        print(f"ERROR: Failed during study hall section sync for user {user.id} in course {course_id}. Details: {e}")

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
        details_query = f'query {{ items (ids: [{master_student_id}]) {{ id name column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}", "{MASTER_STUDENT_GRADE_COLUMN_ID}"]) {{ id text }} }} }}'
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
        grade_text = column_map.get(MASTER_STUDENT_GRADE_COLUMN_ID, '')
        print("  [DIAGNOSTIC] Successfully gathered all required details.")
        return {'name': student_name, 'ssid': ssid, 'email': email, 'canvas_id': canvas_id, 'master_id': item_details['id'], 'plp_id': plp_item_id, 'grade_text': grade_text}
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
    grade_text = student_details.get('grade_text', '')

    master_details_query = f'query {{ items(ids:[{master_id}]) {{ column_values(ids:["{MASTER_STUDENT_TOR_COLUMN_ID}"]) {{ id text value }} }} }}'
    master_result = execute_monday_graphql(master_details_query)
    tor_last_name = "Orientation"
    if master_result and master_result.get('data', {}).get('items'):
        cols = {cv['id']: cv for cv in master_result['data']['items'][0].get('column_values', [])}
        tor_val_str = cols.get(MASTER_STUDENT_TOR_COLUMN_ID, {}).get('value')
        if tor_val_str:
            try:
                tor_ids = get_people_ids_from_value(json.loads(tor_val_str))
                if tor_ids:
                    tor_full_name = get_user_name(list(tor_ids)[0])
                    if tor_full_name: tor_last_name = tor_full_name.split()[-1]
            except (json.JSONDecodeError, TypeError):
                print(f"  WARNING: Could not parse TOR value for master item {master_id}.")
    
    # --- JUMPSTART ENROLLMENT ---
    jumpstart_canvas_id = SPECIAL_COURSE_CANVAS_IDS.get("Jumpstart")
    if jumpstart_canvas_id:
        print(f"  Processing Jumpstart enrollment, section: {tor_last_name}")
        sync_study_hall_enrollment(jumpstart_canvas_id, student_details, tor_last_name, db_cursor, dry_run=dry_run)

    # --- ACE STUDY HALL ENROLLMENT/UNENROLLMENT ---
    ace_sh_canvas_id = SPECIAL_COURSE_CANVAS_IDS.get("ACE Study Hall")
    if ace_sh_canvas_id:
        if is_middle_or_high_school(grade_text):
            print(f"  Processing ACE Study Hall enrollment for 6-12th grader, section: {tor_last_name}")
            sync_study_hall_enrollment(ace_sh_canvas_id, student_details, tor_last_name, db_cursor, dry_run=dry_run)
        else:
            # Unenroll K-5 students if they are found in the course.
            print(f"  -> Student grade '{grade_text}' is not 6-12. Ensuring they are UNENROLLED from ACE Study Hall.")
            if not dry_run:
                unenroll_student_from_course(ace_sh_canvas_id, student_details)

def run_hs_roster_sync_for_student(hs_roster_item, dry_run=True):
    parent_item_id = int(hs_roster_item['id'])
    print(f"\n--- Processing HS Roster for: {hs_roster_item['name']} (ID: {parent_item_id}) ---")

    plp_query = f'query {{ items(ids:[{parent_item_id}]) {{ column_values(ids:["{HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID}"]) {{ value }} }} }}'
    plp_result = execute_monday_graphql(plp_query)
    try:
        plp_linked_ids = get_linked_ids_from_connect_column_value(plp_result['data']['items'][0]['column_values'][0]['value'])
        if not plp_linked_ids:
            print("  SKIPPING: Could not find a linked PLP item.")
            return
        plp_item_id = list(plp_linked_ids)[0]
    except (TypeError, KeyError, IndexError):
        print("  SKIPPING: Could not find linked PLP item.")
        return

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

    course_data = defaultdict(lambda: {'primary_categories': set(), 'secondary_category': ''})
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
                for course_id in course_ids:
                    for label in labels:
                        if label:
                            course_data[course_id]['primary_categories'].add(label)
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
        primary_categories = data.get('primary_categories', set())
        secondary_category = secondary_category_map.get(course_id, '')

        is_ace_course = secondary_category == "ACE"

        if is_ace_course:
            ace_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ACE")
            if ace_col_id:
                plp_updates[ace_col_id].add(course_id)
            
            for category in primary_categories:
                if category in ["ELA", "Other/Elective"]:
                    primary_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(category)
                    if primary_col_id:
                        plp_updates[primary_col_id].add(course_id)
        
        else:
            for category in primary_categories:
                target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(category)
                if target_col_id:
                    plp_updates[target_col_id].add(course_id)
                else:
                    other_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Other/Elective")
                    if other_col_id:
                        print(f"  WARNING: Subject '{category}' doesn't map to a PLP column. Routing to 'Other/Elective'.")
                        plp_updates[other_col_id].add(course_id)
                    else:
                        print(f"  WARNING: Subject '{category}' not mapped and 'Other/Elective' is not configured. Skipping.")

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

def manage_class_enrollment(action, plp_item_id, class_item_id, student_details, section_name, category_name, creator_id, db_cursor, dry_run=True):
    class_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
    linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)

    if not linked_canvas_item_ids:
        print(f"  INFO: '{class_name}' is a non-Canvas course. Skipping Canvas action.")
        return

    canvas_item_id = list(linked_canvas_item_ids)[0]
    course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = course_id_val.get('text') if course_id_val else None

    if not canvas_course_id:
        print(f"  WARNING: Canvas Course ID not found for course '{class_name}'. Skipping Canvas action.")
        return

    if action == "enroll":
        print(f"  ACTION: Pushing enrollment for '{class_name}' to Canvas section '{section_name}'.")
        if not dry_run:
            if class_item_id in ALL_SPECIAL_COURSES:
                student_canvas_user = find_canvas_user(student_details, db_cursor)
                if student_details.get('master_id') and student_canvas_user:
                    roster_teacher_name = get_roster_teacher_name(student_details['master_id']) or "Unassigned"
                    section_teacher = create_section_if_not_exists(canvas_course_id, roster_teacher_name)
                    if section_teacher:
                        enroll_student_in_section(canvas_course_id, student_canvas_user.id, section_teacher.id)
                    if class_item_id in ROSTER_AND_CREDIT_COURSES:
                        course_item_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or ""
                        credit_section_name = "2.5 Credits" if "2.5" in course_item_name else "5 Credits"
                        section_credit = create_section_if_not_exists(canvas_course_id, credit_section_name)
                        if section_credit:
                            enroll_student_in_section(canvas_course_id, student_canvas_user.id, section_credit.id)
            else:
                section = create_section_if_not_exists(canvas_course_id, section_name)
                if section:
                    enroll_or_create_and_enroll(canvas_course_id, section.id, student_details, db_cursor)

    elif action == "unenroll":
        print(f"  ACTION: Pushing unenrollment for '{class_name}' to Canvas.")
        if not dry_run:
            unenroll_student_from_course(canvas_course_id, student_details)


def sync_teacher_assignments(master_student_id, plp_item_id, dry_run=True):
    """Ensures the People columns on the PLP board match the Master Student board."""
    print("  -> Syncing staff assignments from Master Student to PLP...")
    for source_col_id, mapping in MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.items():
        master_person_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), source_col_id)
        
        plp_target_mapping = next((t for t in mapping.get("targets", []) if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
        if plp_target_mapping:
            target_col_id = plp_target_mapping.get("target_column_id")
            target_col_type = plp_target_mapping.get("target_column_type")
            
            # This is a simplified update logic for the nightly sync
            if not dry_run:
                update_people_column(plp_item_id, int(PLP_BOARD_ID), target_col_id, master_person_val.get('value'), target_col_type)

def run_plp_sync_for_student(plp_item_id, creator_id, db_cursor, dry_run=True):
    print(f"\n--- Processing PLP Item: {plp_item_id} ---")
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return
    master_student_id = student_details.get('master_id')
    if not master_student_id: return
    
    # --- GET FULL CONTEXT FOR SECTIONING ---
    class_id_to_category_map = {}
    for category, column_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
        for class_id in get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), column_id):
            class_id_to_category_map[class_id] = category
    id_to_name_map = get_item_names(class_id_to_category_map.keys())

    course_to_track_map = {}
    if is_high_school_student(student_details.get('grade_text')):
        hs_roster_ids = get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), PLP_TO_HS_ROSTER_CONNECT_COLUMN)
        if hs_roster_ids:
            hs_roster_id = list(hs_roster_ids)[0]
            subitems_query = f"""query {{ items(ids: [{hs_roster_id}]) {{ subitems {{
                column_values(ids: ["{HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}", "{HS_ROSTER_TRACK_COLUMN_ID}"]) {{ id text value }}
            }} }} }}"""
            subitems_result = execute_monday_graphql(subitems_query)
            if subitems_result:
                try:
                    subitems = subitems_result['data']['items'][0]['subitems']
                    for subitem in subitems:
                        track_name = ''
                        linked_course_ids = set()
                        for cv in subitem['column_values']:
                            if cv['id'] == HS_ROSTER_TRACK_COLUMN_ID:
                                track_name = cv['text']
                            elif cv['id'] == HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID:
                                linked_course_ids = get_linked_ids_from_connect_column_value(cv['value'])
                        if track_name:
                            for course_id in linked_course_ids:
                                course_to_track_map[course_id] = track_name
                except (KeyError, IndexError):
                    pass

    # --- Sync Class Enrollments ---
    print("INFO: Syncing class enrollments...")
    if not class_id_to_category_map:
        print("INFO: No classes to sync.")
        
    for class_item_id, category_name in class_id_to_category_map.items():
        class_name = id_to_name_map.get(class_item_id, "")
        print(f"INFO: Processing class: '{class_name}'")
        section_name = get_canvas_section_name(plp_item_id, class_item_id, class_name, student_details, course_to_track_map, class_id_to_category_map, id_to_name_map)
        manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, section_name, category_name, creator_id, db_cursor, dry_run=dry_run)
        
    sync_teacher_assignments(master_student_id, plp_item_id, dry_run=dry_run)

def deduplicate_subitems_for_student(plp_item_id, creator_id_to_check, dry_run=True):
    """Finds and removes specific duplicate subitems created by a target user."""
    print(f"  -> Checking for duplicate subitems for PLP item {plp_item_id}...")
    query = f'query {{ items(ids:[{plp_item_id}]) {{ subitems {{ id name creator {{ id }} }} }} }}'
    result = execute_monday_graphql(query)
    
    try:
        subitems = result['data']['items'][0]['subitems']
    except (KeyError, IndexError, TypeError):
        print("     No subitems found to check.")
        return

    subitems_by_name = defaultdict(list)
    for item in subitems:
        subitems_by_name[item.get('name')].append(item)

    items_to_delete = set()

    # Case 1: Specific 'Other Curriculum' vs 'Other/Elective'
    other_curr_items = subitems_by_name.get("Other Curriculum", [])
    other_elec_items = subitems_by_name.get("Other/Elective", [])

    if other_curr_items and other_elec_items:
        for item in other_curr_items:
                 creator = item.get('creator') or {}
                 if str(creator.get('id')) == str(creator_id_to_check):
                     items_to_delete.add(int(item['id']))
                     print(f"     MARKING FOR DELETION: 'Other Curriculum' (ID: {item['id']}) created by target user, and 'Other/Elective' exists.")

    # Case 2: General duplicates created by the target user
    for name, items in subitems_by_name.items():
        if len(items) > 1:
            # filter for items created by Sarah Bruce
            items_by_creator = [item for item in items if str((item.get('creator') or {}).get('id')) == str(creator_id_to_check)]
            # If there's more than one item with the same name by the target creator, delete all but the first one
            if len(items_by_creator) > 1:
                for item_to_delete in items_by_creator[1:]: # Keep the first, delete the rest
                    items_to_delete.add(int(item_to_delete['id']))
                    print(f"     MARKING FOR DELETION: Duplicate '{name}' (ID: {item_to_delete['id']}) created by target user.")
    
    if not items_to_delete:
        print("     No duplicate subitems found to remove.")
        return

    if not dry_run:
        for item_id in items_to_delete:
            delete_item(item_id)
            time.sleep(1) # Be kind to the API
    else:
        print(f"     DRY RUN: Would delete {len(items_to_delete)} subitems: {list(items_to_delete)}")

def reconcile_subitems(plp_item_id, creator_id, db_cursor, dry_run=True):
    print(f"--- Reconciling All Data and Logs for PLP Item: {plp_item_id} ---")
    
    # Run deduplication first before reconciling logs
    deduplicate_subitems_for_student(plp_item_id, creator_id, dry_run=dry_run)

    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details or not student_details.get('master_id'):
        print("  SKIPPING: Could not get complete student details for reconciliation.")
        return

    # --- Reconcile Courses ---
    print("  -> Verifying course enrollments and logs...")
    for category, column_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
        source_of_truth_ids = get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), column_id)
        
        # --- NEW: Only create subitem if the column has courses in it ---
        if source_of_truth_ids:
            id_to_name_map = get_item_names(source_of_truth_ids)
            source_of_truth_names = {f"'{name}'" for name in id_to_name_map.values()}
            
            subitem_name = f"{category} Curriculum"
            subitem_id, was_created = find_or_create_subitem(plp_item_id, subitem_name, dry_run=dry_run)
            if not subitem_id: continue

            logged_names = get_logged_items_from_updates(subitem_id)
            
            if source_of_truth_names != logged_names:
                current_names_str = ", ".join(sorted(list(source_of_truth_names))) or "Blank"
                update_text = f"Reconciliation sync: Current {category} curriculum is now: {current_names_str}."
                
                if not dry_run:
                    create_monday_update(subitem_id, update_text)
                    print(f"  -> Discrepancy found for '{subitem_name}'. Posting update.")
                else:
                    print(f"     DRY RUN: Discrepancy found for '{subitem_name}'. Would post update: {update_text}")

    # --- Reconcile Staff Assignments ---
    print("  -> Verifying PLP staff assignments and logs...")
    sync_teacher_assignments(student_details['master_id'], plp_item_id, dry_run=dry_run)

    for subitem_name, column_id in PLP_PEOPLE_COLUMNS_MAP.items():
        staff_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), column_id)

        # --- NEW: Only create subitem if the column has a person assigned ---
        if staff_val and staff_val.get('text'):
            source_of_truth_staff = {f"'{name.strip()}'" for name in staff_val.get('text', '').split(',')}
            
            subitem_id, was_created = find_or_create_subitem(plp_item_id, subitem_name, dry_run=dry_run)
            if not subitem_id: continue

            logged_staff = get_logged_items_from_updates(subitem_id)

            if source_of_truth_staff != logged_staff:
                current_staff_str = ", ".join(sorted(list(source_of_truth_staff))) or "Blank"
                col_name = subitem_name.replace(" Assignments", "")
                update_text = f"Reconciliation sync: Current {col_name} assignment is now: {current_staff_str}."

                if not dry_run:
                    create_monday_update(subitem_id, update_text)
                    print(f"  -> Discrepancy found for '{subitem_name}'. Posting update.")
                else:
                    print(f"     DRY RUN: Discrepancy found for '{subitem_name}'. Would post update: {update_text}")

def sync_canvas_teachers_and_tas(db_cursor, dry_run=True):
    """
    Syncs teachers from Monday.com Canvas Courses board to Canvas,
    and adds fixed TA accounts to all Canvas classes.
    """
    print("\n======================================================")
    print("=== STARTING CANVAS TEACHER AND TA SYNC          ===")
    print("======================================================")

    canvas_course_items_query = f"""
        query {{
            boards(ids: {CANVAS_BOARD_ID}) {{
                items_page(limit: 500) {{
                    cursor
                    items {{
                        id name
                        column_values(ids: ["{CANVAS_COURSE_ID_COLUMN_ID}", "{CANVAS_TO_STAFF_CONNECT_COLUMN_ID}"]) {{
                            id text value
                        }}
                    }}
                }}
            }}
        }}
    """
    all_canvas_course_items = []
    cursor = None
    while True:
        current_query = canvas_course_items_query
        if cursor:
            current_query = current_query.replace('limit: 500)', f'limit: 500, cursor: "{cursor}"')
        
        result = execute_monday_graphql(current_query)
        if not result or 'data' not in result or not result['data']['boards']:
            break

        page_info = result['data']['boards'][0]['items_page']
        all_canvas_course_items.extend(page_info['items'])
        cursor = page_info.get('cursor')
        if not cursor:
            break
        print(f"  Fetched {len(all_canvas_course_items)} Canvas course items...")

    print(f"Found {len(all_canvas_course_items)} Canvas courses on Monday.com to process.")

    for i, canvas_item in enumerate(all_canvas_course_items, 1):
        canvas_item_id = int(canvas_item['id'])
        canvas_item_name = canvas_item['name']
        print(f"\n===== Processing Canvas Course {i}/{len(all_canvas_course_items)}: '{canvas_item_name}' (Monday ID: {canvas_item_id}) =====")

        column_values = {cv['id']: cv for cv in canvas_item.get('column_values', [])}
        canvas_course_id_val = column_values.get(CANVAS_COURSE_ID_COLUMN_ID, {}).get('text')
        
        if not canvas_course_id_val:
            print(f"  WARNING: Canvas Course ID not found for Monday item {canvas_item_id}. Skipping teacher/TA sync for this course.")
            continue

        canvas_course_id = int(canvas_course_id_val)

        print("  -> Syncing assigned teachers...")
        linked_staff_ids = get_linked_ids_from_connect_column_value(column_values.get(CANVAS_TO_STAFF_CONNECT_COLUMN_ID, {}).get('value'))
        
        if linked_staff_ids:
            for staff_monday_id in linked_staff_ids:
                staff_details_query = f"""
                    query {{
                        items (ids: [{staff_monday_id}]) {{
                            name
                            column_values(ids: ["{ALL_STAFF_EMAIL_COLUMN_ID}", "{ALL_STAFF_SIS_ID_COLUMN_ID}", "{ALL_STAFF_CANVAS_ID_COLUMN}", "{ALL_STAFF_INTERNAL_ID_COLUMN}"]) {{
                                id text
                            }}
                        }}
                    }}
                """
                staff_result = execute_monday_graphql(staff_details_query)
                if staff_result and staff_result.get('data', {}).get('items'):
                    staff_item = staff_result['data']['items'][0]
                    staff_name = staff_item.get('name')
                    staff_col_map = {cv['id']: cv.get('text') for cv in staff_item.get('column_values', [])}
                    
                    teacher_details = {
                        'name': staff_name,
                        'email': staff_col_map.get(ALL_STAFF_EMAIL_COLUMN_ID),
                        'sis_id': staff_col_map.get(ALL_STAFF_SIS_ID_COLUMN_ID),
                        'canvas_id': staff_col_map.get(ALL_STAFF_CANVAS_ID_COLUMN),
                        'internal_id': staff_col_map.get(ALL_STAFF_INTERNAL_ID_COLUMN)
                    }
                    if teacher_details['email']:
                        print(f"    Attempting to enroll teacher: {teacher_details['name']} ({teacher_details['email']})")
                        if not dry_run:
                            enroll_status = enroll_teacher_in_course(canvas_course_id, teacher_details, role='TeacherEnrollment')
                            print(f"    -> Enrollment status for {teacher_details['name']}: {enroll_status}")
                    else:
                        print(f"    WARNING: Teacher {staff_name} (Monday ID: {staff_monday_id}) missing email. Skipping enrollment.")
                else:
                    print(f"    WARNING: Could not retrieve details for staff ID {staff_monday_id}. Skipping enrollment.")
        else:
            print("  INFO: No specific teachers linked on Monday.com for this Canvas course.")

        if not dry_run:
            time.sleep(2)

    print("\n======================================================")
    print("=== CANVAS TEACHER AND TA SYNC FINISHED          ===")
    print("======================================================")

# In nightly_sync.py

if __name__ == '__main__':
    # Set this to True to run the script on ALL students, not just recently updated ones.
    # Should only be used for a one-time full sync after a cleanup.
    FORCE_FULL_SYNC = False
    
    DRY_RUN = False
    TARGET_USER_NAME = "Sarah Bruce"

    print("======================================================")
    print("=== STARTING NIGHTLY DELTA SYNC SCRIPT           ===")
    print("======================================================")
    if DRY_RUN:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!               DRY RUN MODE IS ON               !!!")
        print("!!!  No actual changes will be made to your data.  !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    db = None
    cursor = None
    try:
        print("INFO: Connecting to the database...")
        db = mysql.connector.connect( host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=int(DB_PORT) )
        cursor = db.cursor()
        print("INFO: Fetching last sync times for processed students...")
        cursor.execute("CREATE TABLE IF NOT EXISTS processed_students (student_id BIGINT PRIMARY KEY, last_synced_at TIMESTAMP, canvas_id VARCHAR(255))")
        cursor.execute("SELECT student_id, last_synced_at, canvas_id FROM processed_students")
        processed_map = {row[0]: {'last_synced': row[1], 'canvas_id': row[2]} for row in cursor.fetchall()}
        print(f"INFO: Found {len(processed_map)} students in the database.")

        creator_id = get_user_id(TARGET_USER_NAME)
        if not creator_id: raise Exception(f"Halting script: Target user '{TARGET_USER_NAME}' could not be found.")

        print("INFO: Fetching all PLP board items from Monday.com...")
        all_plp_items = get_all_board_items(PLP_BOARD_ID)
        
        items_to_process = []
        if FORCE_FULL_SYNC:
            print("\n*** FORCE FULL SYNC IS ENABLED. PROCESSING ALL STUDENTS. ***\n")
            items_to_process = all_plp_items
        else:
            plp_ids_to_process = set()

            print("INFO: Filtering for students with updated PLP items...")
            for item in all_plp_items:
                item_id = int(item['id'])
                updated_at = parse_flexible_timestamp(item['updated_at'])
                sync_data = processed_map.get(item_id)
                last_synced = sync_data['last_synced'].replace(tzinfo=timezone.utc) if sync_data and sync_data['last_synced'] else None
                
                if not last_synced or updated_at > last_synced:
                    plp_ids_to_process.add(item_id)

            print("INFO: Fetching all HS Roster items to check for recent updates...")
            all_hs_roster_items = get_all_board_items(HS_ROSTER_BOARD_ID)
            
            print("INFO: Filtering for PLP items linked to updated HS Rosters...")
            for hs_item in all_hs_roster_items:
                hs_item_id = int(hs_item['id'])
                linked_plp_ids = get_linked_items_from_board_relation(hs_item_id, int(HS_ROSTER_BOARD_ID), HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
                if linked_plp_ids:
                    plp_id = list(linked_plp_ids)[0]
                    sync_data = processed_map.get(plp_id)
                    last_synced = sync_data['last_synced'].replace(tzinfo=timezone.utc) if sync_data and sync_data['last_synced'] else None
                    hs_updated_at = parse_flexible_timestamp(hs_item['updated_at'])

                    if not last_synced or hs_updated_at > last_synced:
                        plp_ids_to_process.update(linked_plp_ids)
            
            if plp_ids_to_process:
                all_items_on_board = {int(i['id']): i for i in all_plp_items}
                items_to_process = [all_items_on_board[pid] for pid in plp_ids_to_process if pid in all_items_on_board]
        
        total_to_process = len(items_to_process)
        print(f"INFO: Found {total_to_process} unique students to process.")

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
                    hs_roster_item_object = next((item for item in all_hs_roster_items if int(item['id']) == hs_roster_item_id), None)
                    if hs_roster_item_object:
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

        # Always run Teacher/TA Sync after student processing
        sync_canvas_teachers_and_tas(cursor, dry_run=DRY_RUN)

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
