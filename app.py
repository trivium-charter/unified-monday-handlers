# ==============================================================================
# FINAL CONSOLIDATED APPLICATION (All Original Logic Restored and Bugs Fixed)
# ==============================================================================
import os
import json
import requests
import time
from datetime import datetime
from flask import Flask, request, jsonify
from celery import Celery
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist
from collections import defaultdict
import unicodedata
import re

# ==============================================================================
# CENTRALIZED CONFIGURATION
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"
CELERY_BROKER_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_CANVAS_SYNC_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_COLUMN_ID")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE", "Done")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
PLP_TO_HS_ROSTER_CONNECT_COLUMN = os.environ.get("PLP_TO_HS_ROSTER_CONNECT_COLUMN") 
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
MASTER_STUDENT_CANVAS_ID_COLUMN = "text_mktgs1ax"
MASTER_STUDENT_TOR_COLUMN_ID = os.environ.get("MASTER_STUDENT_TOR_COLUMN_ID")
MASTER_STUDENT_GRADE_COLUMN_ID = "color_mksy8hcw"
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
ALL_CLASSES_CANVAS_ID_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_ID_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
HS_ROSTER_TRACK_COLUMN_ID = "status7"
HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID")
ALL_STAFF_BOARD_ID = os.environ.get("ALL_STAFF_BOARD_ID")
ALL_STAFF_EMAIL_COLUMN_ID = os.environ.get("ALL_STAFF_EMAIL_COLUMN_ID")
ALL_STAFF_SIS_ID_COLUMN_ID = os.environ.get("ALL_STAFF_SIS_ID_COLUMN_ID")
ALL_STAFF_PERSON_COLUMN_ID = os.environ.get("ALL_STAFF_PERSON_COLUMN_ID")
ALL_STAFF_CANVAS_ID_COLUMN = "text_mktg7h6"
ALL_STAFF_INTERNAL_ID_COLUMN = "text_mkthjxht"
IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID")
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
SPED_TO_IEPAP_CONNECT_COLUMN_ID = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN_ID")
CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")
CANVAS_TO_STAFF_CONNECT_COLUMN_ID = os.environ.get("CANVAS_TO_STAFF_CONNECT_COLUMN_ID")
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

SPECIAL_COURSE_CANVAS_IDS = { "Jumpstart": 10069, "ACE Study Hall": 10128, "Connect English Study Hall": 10109, "Connect Math Study Hall": 9966, "Prep Math and ELA Study Hall": 9960, "EL Support Study Hall": 10046 }

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}"))
    LOG_CONFIGS = json.loads(os.environ.get("MONDAY_LOGGING_CONFIGS", "[]"))
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}"))
except json.JSONDecodeError:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}
    LOG_CONFIGS = []
    MASTER_STUDENT_PEOPLE_COLUMNS = {}

ROSTER_ONLY_COURSES = {10298, 10297, 10299, 10300, 10301}
ROSTER_AND_CREDIT_COURSES = {10097, 10002, 10092, 10164, 10198}
ALL_SPECIAL_COURSES = ROSTER_ONLY_COURSES.union(ROSTER_AND_CREDIT_COURSES)


# ==============================================================================
# MONDAY.COM UTILITIES
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

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
                logged_items = re.findall(r"'([^']*)'", state_string)
                # Return the set of names, formatted with quotes to match the source of truth
                return {f"'{item}'" for item in logged_items}
                
    except (TypeError, KeyError, IndexError):
        pass
        
    # If no state-declaring update is found, return an empty set
    return set()

def find_or_create_subitem(parent_item_id, subitem_name, column_values=None):
    """
    Finds a subitem by name. If it doesn't exist, it creates it with column values.
    Returns the ID of the subitem.
    """
    # First, try to find the subitem by name
    query = f'query {{ items(ids:[{parent_item_id}]) {{ subitems {{ id name }} }} }}'
    result = execute_monday_graphql(query)
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            if subitem.get('name') == subitem_name:
                # Found existing subitem, return its ID
                return subitem['id']
    except (KeyError, IndexError, TypeError):
        pass # If any error, proceed to create

    # If not found, create it
    print(f"  INFO: No existing subitem named '{subitem_name}'. Creating it.")
    return create_subitem(parent_item_id, subitem_name, column_values=column_values)
    
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

def get_user_email(user_id):
    if user_id is None: return None
    query = f"query {{ users(ids: [{user_id}]) {{ email }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('users'):
        return result['data']['users'][0].get('email')
    return None

def get_item_name(item_id, board_id):
    query = f"query {{ boards(ids: {board_id}) {{ items_page(query_params: {{ids: [{item_id}]}}) {{ items {{ name }} }} }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            return board['items_page']['items'][0].get('name')
    return None

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


        
def get_user_name(user_id):
    if user_id is None or user_id == -4: return None
    query = f"query {{ users(ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('users'):
        return result['data']['users'][0].get('name')
    return None

def get_roster_teacher_name(master_student_id):
    tor_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_TOR_COLUMN_ID)
    if tor_val and tor_val.get('value'):
        tor_ids = get_people_ids_from_value(tor_val['value'])
        if tor_ids:
            tor_full_name = get_user_name(list(tor_ids)[0])
            if tor_full_name: return tor_full_name.split()[-1]
    return None

def get_column_value(item_id, board_id, column_id):
    if not item_id or not column_id: return None
    query = f"""query {{ items (ids: [{item_id}]) {{ column_values (ids: ["{column_id}"]) {{ id text value type }} }} }}"""
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

def find_item_by_person(board_id, person_column_id, person_id):
    query = f"""query {{ items_page_by_column_values ( board_id: {board_id}, columns: [{{ column_id: "{person_column_id}", column_values: ["{{\\"ids\\":[ {person_id} ]}}"] }}] ) {{ items {{ id }} }} }}"""
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('items_page_by_column_values', {}).get('items'):
        items = result['data']['items_page_by_column_values']['items']
        if items: return items[0]['id']
    return None

def update_item_name(item_id, board_id, new_name):
    column_values_obj = {"name": new_name}
    graphql_value = json.dumps(json.dumps(column_values_obj))
    mutation = f"mutation {{ change_multiple_column_values(board_id: {board_id}, item_id: {item_id}, column_values: {graphql_value}) {{ id }} }}"
    return execute_monday_graphql(mutation) is not None
    

def change_column_value_generic(board_id, item_id, column_id, value):
    graphql_value = json.dumps(str(value))
    mutation = f"""mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}) {{ id }} }} """
    return execute_monday_graphql(mutation) is not None

def get_people_ids_from_value(value_data):
    if not value_data: return set()
    if isinstance(value_data, str):
        try: value_data = json.loads(value_data)
        except json.JSONDecodeError: return set()
    persons_and_teams = value_data.get('personsAndTeams', [])
    return {person['id'] for person in persons_and_teams if 'id' in person}

def get_linked_ids_from_connect_column_value(value_data):
    if not value_data: return set()
    parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data) if isinstance(value_data, str) else {}
    if "linkedPulseIds" in parsed_value:
        return {int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"] if "linkedPulseId" in item}
    return set()

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    column_data = get_column_value(item_id, board_id, connect_column_id)
    return get_linked_ids_from_connect_column_value(column_data.get('value')) if column_data else set()

def update_connect_board_column(item_id, board_id, connect_column_id, item_to_link_id, action="add"):
    current_linked_items = get_linked_items_from_board_relation(item_id, board_id, connect_column_id)
    target_item_id_int = int(item_to_link_id)
    if action == "add": updated_linked_items = current_linked_items | {target_item_id_int}
    elif action == "remove": updated_linked_items = current_linked_items - {target_item_id_int}
    else: return False
    connect_value = {"linkedPulseIds": [{"linkedPulseId": lid} for lid in sorted(list(updated_linked_items))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    mutation = f"mutation {{ change_column_value (board_id: {board_id}, item_id: {item_id}, column_id: \"{connect_column_id}\", value: {graphql_value}) {{ id }} }}"
    return execute_monday_graphql(mutation) is not None

def create_subitem(parent_item_id, subitem_name, column_values=None):
    values_for_api = {col_id: val for col_id, val in (column_values or {}).items()}
    column_values_json = json.dumps(values_for_api)
    mutation = f"mutation {{ create_subitem (parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {json.dumps(column_values_json)}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result['data']['create_subitem'].get('id') if result and 'data' in result and result['data'].get('create_subitem') else None

def create_item(board_id, item_name, column_values=None):
    column_values_str = json.dumps(column_values or {})
    mutation = f"mutation {{ create_item (board_id: {board_id}, item_name: {json.dumps(item_name)}, column_values: {json.dumps(column_values_str)}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result['data']['create_item'].get('id') if result and 'data' in result and result['data'].get('create_item') else None

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    new_persons_and_teams = new_people_value.get('personsAndTeams', [])

    # Get the ID of the person being assigned from the webhook event
    new_person_id = None
    if new_persons_and_teams:
        new_person_id = new_persons_and_teams[0].get('id')

    # Get the IDs of the people currently in the column on Monday.com
    current_col_val = get_column_value(item_id, board_id, people_column_id)
    current_people_ids = set()
    if current_col_val and current_col_val.get('value'):
        current_people_ids = get_people_ids_from_value(current_col_val['value'])

    # --- NEW: Check if an update is needed before proceeding ---
    if target_column_type == "person":
        if new_person_id and current_people_ids == {new_person_id}:
            print(f"  -> INFO: Person {new_person_id} is already assigned. No update needed.")
            return True # Exit because the correct person is already there
        if not new_person_id and not current_people_ids:
            print("  -> INFO: Column is already empty. No update needed.")
            return True # Exit because the column is already empty
            
    elif target_column_type == "multiple-person":
        # For multiple person, the logic is additive, so we only check if the person is already there
        if new_person_id in current_people_ids:
            print(f"  -> INFO: Person {new_person_id} is already in the list. No update needed.")
            return True # Exit because the person is already in the list

    # If an update is needed, proceed with the original logic
    print(f"  -> INFO: Updating person column {people_column_id}...")
    if not new_persons_and_teams:
        final_value = {} # Clear the column if the new value is empty
    else:
        if target_column_type == "person":
            final_value = {"persons": [int(new_person_id)]}
        elif target_column_type == "multiple-person":
            current_people_ids.add(new_person_id)
            updated_people_list = [{"id": int(pid), "kind": "person"} for pid in current_people_ids]
            final_value = {"personsAndTeams": updated_people_list}
        else:
            return False

    graphql_value = json.dumps(json.dumps(final_value))
    mutation = f"mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: \"{people_column_id}\", value: {graphql_value}) {{ id }} }}"
    return execute_monday_graphql(mutation) is not None

def create_monday_update(item_id, update_text):
    formatted_text = json.dumps(update_text)
    mutation = f"mutation {{ create_update (item_id: {item_id}, body: {formatted_text}) {{ id }} }}"
    return execute_monday_graphql(mutation)

def check_if_subitem_exists_by_name(parent_item_id, subitem_name_to_check):
    """
    Checks if a subitem for a specific course already exists,
    regardless of its category prefix (e.g., 'Added Math' vs 'Added ACE').
    """
    try:
        course_part = "'" + subitem_name_to_check.split("'", 1)[1]
    except IndexError:
        course_part = subitem_name_to_check

    query = f'query {{ items(ids:[{parent_item_id}]) {{ subitems {{ name }} }} }}'
    result = execute_monday_graphql(query)
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            if subitem.get('name', '').endswith(course_part):
                return True
    except (KeyError, IndexError, TypeError):
        pass
    return False

# ==============================================================================
# CANVAS UTILITIES
# ==============================================================================
def initialize_canvas_api():
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY) if CANVAS_API_URL and CANVAS_API_KEY else None

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

def find_canvas_teacher(teacher_details):
    """
    Finds a Canvas user based on provided details without checking their role.
    Prioritizes Canvas ID, then SIS ID, then email.
    Handles cases where canvas_id might be a username instead of an integer.
    """
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None

    canvas_id = teacher_details.get('canvas_id')
    
    if canvas_id:
        try:
            return canvas_api.get_user(int(canvas_id))
        except (ValueError, TypeError):
            try:
                return canvas_api.get_user(canvas_id, 'login_id')
            except ResourceDoesNotExist:
                pass 
        except ResourceDoesNotExist:
            pass 

    if teacher_details.get('sis_id'):
        try:
            return canvas_api.get_user(teacher_details['sis_id'], 'sis_user_id')
        except ResourceDoesNotExist:
            pass

    if teacher_details.get('email'):
        try:
            return canvas_api.get_user(teacher_details['email'], 'login_id')
        except ResourceDoesNotExist:
            pass

    if teacher_details.get('email'):
        try:
            users = [u for u in canvas_api.get_account(1).get_users(search_term=teacher_details['email'])]
            if len(users) == 1:
                return users[0]
        except (ResourceDoesNotExist, CanvasException):
            pass
            
    return None

def create_canvas_user(user_details, role='student'):
    """Creates a Canvas user and includes a final check to prevent duplicates."""
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        account = canvas_api.get_account(1)
        user_payload = {
            'user': {'name': user_details['name'], 'terms_of_use': True},
            'pseudonym': {
                'unique_id': user_details['email'],
                'sis_user_id': user_details.get('sis_id') or user_details.get('ssid') or user_details.get('email'),
                'login_id': user_details['email'],
                'authentication_provider_id': '112'
            },
            'communication_channel': {
                'type': 'email',
                'address': user_details['email'],
                'skip_confirmation': True
            }
        }
        return account.create_user(**user_payload)
    except CanvasException as e:
        if "is already in use" in str(e) or "ID already in use" in str(e):
            print(f"INFO: User creation failed because ID is in use. Searching again for existing user.")
            return find_canvas_teacher(user_details) if role == 'teacher' else find_canvas_user(student_details)
        print(f"ERROR: A critical error occurred during user creation: {e}")
        raise

def update_user_ssid(user, new_ssid):
    """
    CORRECTED: Fetches the full user object to ensure .get_logins() is available.
    """
    try:
        canvas_api = initialize_canvas_api()
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

def create_canvas_course(course_name, term_id):
    canvas_api = initialize_canvas_api()
    if not all([canvas_api, CANVAS_SUBACCOUNT_ID, CANVAS_TEMPLATE_COURSE_ID]): return None
    try: account = canvas_api.get_account(CANVAS_SUBACCOUNT_ID)
    except ResourceDoesNotExist: return None
    base_sis_name = ''.join(e for e in course_name if e.isalnum()).replace(' ', '_').lower()
    base_sis_id = f"{base_sis_name}_{term_id}"
    max_attempts = 10
    for attempt in range(max_attempts):
        sis_id_to_try = base_sis_id if attempt == 0 else f"{base_sis_id}_{attempt}"
        course_data = { 'name': course_name, 'course_code': course_name, 'enrollment_term_id': f"sis_term_id:{term_id}", 'sis_course_id': sis_id_to_try, 'source_course_id': CANVAS_TEMPLATE_COURSE_ID }
        try:
            new_course = account.create_course(course=course_data)
            return new_course
        except CanvasException as e:
            if hasattr(e, 'status_code') and e.status_code == 400 and 'is already in use' in str(e).lower():
                continue
            else: return None
    return None

def create_section_if_not_exists(course_id, section_name):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        course = canvas_api.get_course(course_id)
        existing_section = next((s for s in course.get_sections() if s.name.lower() == section_name.lower()), None)
        return existing_section or course.create_course_section(course_section={'name': section_name})
    except CanvasException as e:
        print(f"ERROR: Canvas section creation failed: {e}")
        return None

def enroll_student_in_section(course_id, user_id, section_id):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed: Canvas API not initialized"
    try:
        course = canvas_api.get_course(course_id)
        user = canvas_api.get_user(user_id)
        enrollment = course.enroll_user(user, 'StudentEnrollment', enrollment_state='active', course_section_id=section_id, notify=False)
        return "Success" if enrollment else "Failed"
    except Conflict: return "Already Enrolled"
    except CanvasException as e:
        print(f"ERROR: Failed to enroll user {user_id} in section {section_id}. Details: {e}")
        return "Failed"

def unenroll_student_from_course(course_id, student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return False
    user = find_canvas_user(student_details)
    if not user: return True
    try:
        course = canvas_api.get_course(course_id)
        for enrollment in course.get_enrollments(user_id=user.id):
            enrollment.deactivate(task='conclude')
        return True
    except CanvasException as e:
        print(f"ERROR: Canvas unenrollment failed: {e}")
        return False

def enroll_teacher_in_course(course_id, teacher_details, role='TeacherEnrollment'):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed: Canvas API not initialized"
    teacher_name = teacher_details.get('name', teacher_details.get('email', 'Unknown'))
    user_to_enroll = find_canvas_teacher(teacher_details)
    if not user_to_enroll: return f"Failed: User '{teacher_name}' not found in Canvas with provided IDs."
    try:
        course = canvas_api.get_course(course_id)
        course.enroll_user(user_to_enroll, role, enrollment_state='active', notify=False)
        return "Success"
    except ResourceDoesNotExist: return f"Failed: Course with ID '{course_id}' not found in Canvas."
    except Conflict: return "Already Enrolled"
    except CanvasException as e: return f"Failed: {e}"

def get_teacher_person_value_from_canvas_board(canvas_item_id):
    linked_staff_ids = get_linked_items_from_board_relation(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_TO_STAFF_CONNECT_COLUMN_ID)
    if not linked_staff_ids: return None
    staff_item_id = list(linked_staff_ids)[0]
    person_col_val = get_column_value(staff_item_id, int(ALL_STAFF_BOARD_ID), ALL_STAFF_PERSON_COLUMN_ID)
    return person_col_val.get('value') if person_col_val else None

# ==============================================================================
# CORE LOGIC FUNCTIONS
# ==============================================================================

# Make sure 'import re' is at the top of the script

def get_canvas_section_name(plp_item_id, class_item_id, class_name, student_details, course_to_track_map, class_id_to_category_map, id_to_name_map):
    """
    Determines the correct Canvas section name for a student. (FULLY CORRECTED)
    """
    # === PRIORITY 1: Handle Special Study Hall Sectioning ===
    if "Connect Math Study Hall" in class_name:
        for c_id, category in class_id_to_category_map.items():
            course_name = id_to_name_map.get(c_id, "")
            if category == "Math" and "Connect" in course_name:
                return course_name # Use the actual Connect Math class name
        return "General Math Connect" # Fallback

    if "Connect English Study Hall" in class_name:
        for c_id, category in class_id_to_category_map.items():
            course_name = id_to_name_map.get(c_id, "")
            if category == "ELA" and "Connect" in course_name:
                return course_name # Use the actual Connect ELA class name
        return "General English Connect" # Fallback

    if "Prep Math and ELA Study Hall" in class_name:
        prep_subjects = []
        for c_id, category in class_id_to_category_map.items():
            course_name = id_to_name_map.get(c_id, "")
            if category in ["Math", "ELA"] and "Prep" in course_name:
                prep_subjects.append(course_name)
        if not prep_subjects:
            return "General Prep" # Fallback
        return " & ".join(sorted(prep_subjects)) # Use the actual Prep class names

    # === PRIORITY 2: Check M-Series/Op2 Column for ALL Students ===
    m_series_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_M_SERIES_LABELS_COLUMN)
    m_series_text = m_series_val.get('text') if m_series_val else None
    
    if m_series_text:
        match = re.search(r'M\d|Op\d', m_series_text) # Find patterns like M3 or Op2
        if match:
            return match.group(0) # If found, use it immediately for any student

    # === PRIORITY 3: High School Specific Fallback ===
    if is_high_school_student(student_details.get('grade_text')):
        # This only runs if the M-Series check above fails
        return course_to_track_map.get(class_item_id, "General Enrollment")

    # === PRIORITY 4: Default for all other cases ===
    return "General Enrollment"

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed"
    user = find_canvas_user(student_details)
    if not user:
        print(f"INFO: Canvas user not found for {student_details['email']}. Attempting to create new user.")
        try:
            user = create_canvas_user(student_details)
        except CanvasException as e:
            if ("sis_user_id" in str(e) and "is already in use" in str(e)) or \
               ("unique_id" in str(e) and "ID already in use" in str(e)):
                print(f"INFO: User creation failed because ID is in use. Searching again for existing user.")
                user = find_canvas_user(student_details)
            else:
                print(f"ERROR: A critical error occurred during user creation: {e}")
                user = None
    if user:
        try:
            full_user = canvas_api.get_user(user.id)
            # *** NEW: Explicitly check for active enrollment before proceeding ***
            course_obj = canvas_api.get_course(course_id)
            enrollments = course_obj.get_enrollments(user_id=full_user.id)
            for enrollment in enrollments:
                if enrollment.course_section_id == section_id and enrollment.enrollment_state == 'active':
                    print(f"  -> INFO: Student is already active in section {section_id}. No action needed.")
                    return "Already Enrolled"

            if student_details.get('ssid') and hasattr(full_user, 'sis_user_id') and full_user.sis_user_id != student_details['ssid']:
                update_user_ssid(full_user, student_details['ssid'])
            return enroll_student_in_section(course_id, full_user.id, section_id)
        except CanvasException as e:
            print(f"ERROR: Could not retrieve full user object or enroll for user ID {user.id}: {e}")
            return "Failed"
    print(f"ERROR: Could not find or create a Canvas user for {student_details.get('name')}. Final enrollment failed.")
    return "Failed"

def get_student_details_from_plp(plp_item_id):
    query = f"""query {{ items (ids: [{plp_item_id}]) {{ column_values (ids: ["{PLP_TO_MASTER_STUDENT_CONNECT_COLUMN}"]) {{ value }} }} }}"""
    result = execute_monday_graphql(query)
    try:
        connect_column_value = json.loads(result['data']['items'][0]['column_values'][0]['value'])
        linked_ids = [item['linkedPulseId'] for item in connect_column_value.get('linkedPulseIds', [])]
        if not linked_ids: return None
        master_student_id = linked_ids[0]
        details_query = f"""query {{ items (ids: [{master_student_id}]) {{ name column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}", "{MASTER_STUDENT_GRADE_COLUMN_ID}"]) {{ id text }} }} }}"""
        details_result = execute_monday_graphql(details_query)
        item_details = details_result['data']['items'][0]
        student_name = item_details['name']
        column_map = {cv['id']: cv.get('text') for cv in item_details.get('column_values', []) if isinstance(cv, dict) and 'id' in cv}
        ssid = column_map.get(MASTER_STUDENT_SSID_COLUMN, '')
        email = column_map.get(MASTER_STUDENT_EMAIL_COLUMN, '')
        canvas_id = column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN, '')
        grade_text = column_map.get(MASTER_STUDENT_GRADE_COLUMN_ID, '')
        if not all([student_name, email]): return None
        return {'name': student_name, 'ssid': ssid, 'email': email, 'canvas_id': canvas_id, 'master_id': master_student_id, 'grade_text': grade_text}
    except (TypeError, KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not parse student details from Monday.com response: {e}")
        return None

def manage_class_enrollment(action, plp_item_id, class_item_id, student_details, section_name="All"):
    """Handles ONLY the Canvas enrollment or unenrollment action."""
    class_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"

    linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    if not linked_canvas_item_ids:
        print(f"  INFO: '{class_name}' is not a Canvas course. Skipping Canvas action.")
        return

    canvas_item_id = list(linked_canvas_item_ids)[0]
    course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = course_id_val.get('text') if course_id_val else None

    if not canvas_course_id:
        print(f"  WARNING: Canvas Course ID not found for '{class_name}'. Skipping Canvas action.")
        return

    if action == "enroll":
        print(f"  -> Pushing enrollment for '{class_name}' to Canvas section '{section_name}'.")
        section = create_section_if_not_exists(canvas_course_id, section_name)
        if section:
            enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)

    elif action == "unenroll":
        print(f"  -> Pushing unenrollment for '{class_name}' to Canvas.")
        unenroll_student_from_course(canvas_course_id, student_details)

# ==============================================================================
# CELERY APP DEFINITION & TASKS
# ==============================================================================
broker_use_ssl_config = {'ssl_cert_reqs': 'required'} if CELERY_BROKER_URL.startswith('rediss://') else {}
celery_app = Celery('tasks', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND, include=[__name__])
if broker_use_ssl_config:
    celery_app.conf.broker_use_ssl = broker_use_ssl_config
    celery_app.conf.redis_backend_use_ssl = broker_use_ssl_config
celery_app.conf.timezone = 'America/Los_Angeles'
celery_app.conf.broker_transport_options = { 'health_check_interval': 30, 'socket_keepalive': True, }
celery_app.conf.broker_connection_retry_on_startup = True

@celery_app.task(name='app.process_general_webhook')
def process_general_webhook(event_data, config_rule):
    log_type, params = config_rule.get("log_type"), config_rule.get("params", {})
    board_id, item_id = event_data.get('boardId'), event_data.get('pulseId')
    if log_type == "NameReformat":
        target_col_id, current_name = params.get('target_text_column_id'), get_item_name(item_id, board_id)
        if not all([target_col_id, current_name]): return
        parts = current_name.strip().split()
        if len(parts) >= 2: change_column_value_generic(board_id, item_id, target_col_id, f"{parts[-1]}, {' '.join(parts[:-1])}")
    elif log_type == "CopyToItemName":
        source_col_id = params.get('source_column_id')
        if not source_col_id: return
        column_data = get_column_value(item_id, board_id, source_col_id)
        if column_data and column_data.get('text'): update_item_name(item_id, board_id, column_data['text'])
    elif log_type == "ConnectBoardChange":
        current_ids, previous_ids = get_linked_ids_from_connect_column_value(event_data.get('value')), get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
        changer, date, prefix, linked_board_id = get_user_name(event_data.get('userId')) or "automation", datetime.now().strftime('%Y-%m-%d'), params.get('subitem_name_prefix', ''), params.get('linked_board_id')
        subitem_cols = {params['entry_type_column_id']: {"labels": [str(params['subitem_entry_type'])]}} if params.get('entry_type_column_id') and params.get('subitem_entry_type') else {}
        for link_id in (current_ids - previous_ids):
            name = get_item_name(link_id, linked_board_id)
            if name: create_subitem(item_id, f"Added {prefix} '{name}' on {date} by {changer}", subitem_cols)
        for link_id in (previous_ids - current_ids):
            name = get_item_name(link_id, linked_board_id)
            if name: create_subitem(item_id, f"Removed {prefix} '{name}' on {date} by {changer}", subitem_cols)

@celery_app.task(name='app.process_canvas_full_sync_from_status')
def process_canvas_full_sync_from_status(event_data):
    if event_data.get('value', {}).get('label', {}).get('text', '') != PLP_CANVAS_SYNC_STATUS_VALUE:
        return
        
    plp_item_id = event_data.get('pulseId')
    changer_name = get_user_name(event_data.get('userId')) or "Full Sync Automation"
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details:
        return

    # --- 1. GATHER ALL CLASSES FROM THE PLP BOARD ---
    class_id_to_category_map = {}
    for category, column_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
        for class_id in get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), column_id):
            class_id_to_category_map[class_id] = category
    id_to_name_map = get_item_names(class_id_to_category_map.keys())
            
    # --- 1B. HANDLE SPECIAL ENROLLMENTS (ACE STUDY HALL) ---
    print(f"INFO: Checking special enrollments for PLP ID {plp_item_id}.")
    grade_text = student_details.get('grade_text', '')
    tor_last_name = get_roster_teacher_name(student_details.get('master_id')) or "Orientation"
    ace_sh_canvas_id = SPECIAL_COURSE_CANVAS_IDS.get("ACE Study Hall")
    
    if ace_sh_canvas_id:
        if is_middle_or_high_school(grade_text):
            print(f"  -> Student is 6-12th grade. Enrolling in ACE Study Hall section '{tor_last_name}'.")
            section = create_section_if_not_exists(ace_sh_canvas_id, tor_last_name)
            if section:
                enroll_or_create_and_enroll(ace_sh_canvas_id, section.id, student_details)
        else:
            print(f"  -> Student is K-5th grade. Unenrolling from ACE Study Hall.")
            unenroll_student_from_course(ace_sh_canvas_id, student_details)

    # --- 2. PRE-COMPUTE HS SECTION NAMES ---
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

    # --- 3. PERFORM ALL CANVAS ENROLLMENTS ---
    print(f"INFO: Starting Full Canvas Sync for PLP ID {plp_item_id}. Enrolling in {len(class_id_to_category_map)} courses.")
    for class_item_id, category_name in class_id_to_category_map.items():
        linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
        if linked_canvas_item_ids:
            canvas_item_id = list(linked_canvas_item_ids)[0]
            course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
            canvas_course_id = course_id_val.get('text') if course_id_val else None
            
            if canvas_course_id:
                class_name = id_to_name_map.get(class_item_id, "")
                section_name = get_canvas_section_name(plp_item_id, class_item_id, class_name, student_details, course_to_track_map, class_id_to_category_map, id_to_name_map)
                manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, section_name=section_name)

    # --- 4. POST CONSOLIDATED MONDAY.COM LOGS ---
    print(f"INFO: Posting consolidated logs for PLP ID {plp_item_id}.")
    
    category_to_class_ids_map_logs = defaultdict(list)
    for class_id, category in class_id_to_category_map.items():
        category_to_class_ids_map_logs[category].append(class_id)
        
    for category, class_ids in category_to_class_ids_map_logs.items():
        subitem_name = f"{category} Curriculum"
        subitem_id = find_or_create_subitem(plp_item_id, subitem_name)
        if subitem_id:
            current_names_str = ", ".join([f"'{id_to_name_map.get(cid)}'" for cid in sorted(class_ids) if id_to_name_map.get(cid)]) or "Blank"
            update_text = f"Full Canvas Sync triggered by {changer_name}. Current {category} curriculum is now: {current_names_str}."
            create_monday_update(subitem_id, update_text)

@celery_app.task(name='app.process_canvas_delta_sync_from_course_change')
def process_canvas_delta_sync_from_course_change(event_data):
    plp_item_id = event_data.get('pulseId')
    user_id = event_data.get('userId')
    trigger_column_id = event_data.get('columnId')
    changer_name = get_user_name(user_id) or "automation"
    
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return

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

    # --- LOGGING ---
    current_ids = get_linked_ids_from_connect_column_value(event_data.get('value'))
    previous_ids = get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    added_ids = current_ids - previous_ids
    removed_ids = previous_ids - current_ids
    
    if not added_ids and not removed_ids:
        return

    category = {v: k for k, v in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items()}.get(trigger_column_id, "Other/Elective")
    subitem_name = f"{category} Curriculum"
        
    subitem_id = find_or_create_subitem(plp_item_id, subitem_name)
    if not subitem_id: return

    update_messages = []
    log_id_map = get_item_names(added_ids | removed_ids | current_ids)
    for rid in removed_ids:
        name = log_id_map.get(rid, f"Item {rid}")
        update_messages.append(f"'{name}' was removed by {changer_name}.")
    for aid in added_ids:
        name = log_id_map.get(aid, f"Item {aid}")
        update_messages.append(f"'{name}' was added by {changer_name}.")

    if update_messages:
        current_names_str = ", ".join([f"'{log_id_map.get(cid)}'" for cid in sorted(list(current_ids)) if log_id_map.get(cid)]) or "Blank"
        final_update = "\n".join(update_messages) + f"\nCurrent {category} curriculum is now: {current_names_str}."
        create_monday_update(subitem_id, final_update)
    
    # --- CANVAS ACTIONS ---
    for rid in removed_ids:
        manage_class_enrollment("unenroll", plp_item_id, rid, student_details)

    for aid in added_ids:
        class_name = id_to_name_map.get(aid, "")
        section_name = get_canvas_section_name(plp_item_id, aid, class_name, student_details, course_to_track_map, class_id_to_category_map, id_to_name_map)
        manage_class_enrollment("enroll", plp_item_id, aid, student_details, section_name=section_name)
        
        # --- NEW: Sync teacher from course to Master Student list ---
        class_name_lower = class_name.lower()
        if "ace" in class_name_lower or "connect" in class_name_lower:
            master_student_id = student_details.get('master_id')
            if master_student_id:
                linked_canvas_item_ids = get_linked_items_from_board_relation(aid, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
                if linked_canvas_item_ids:
                    canvas_item_id = list(linked_canvas_item_ids)[0]
                    teacher_person_value = get_teacher_person_value_from_canvas_board(canvas_item_id)
                    if teacher_person_value:
                        target_col_id = None
                        if "ace" in class_name_lower:
                            target_col_id = MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID
                        elif "connect" in class_name_lower:
                            target_col_id = MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID
                        
                        if target_col_id:
                            update_people_column(master_student_id, int(MASTER_STUDENT_BOARD_ID), target_col_id, teacher_person_value, "multiple-person")


@celery_app.task(name='app.process_master_student_person_sync_webhook')
def process_master_student_person_sync_webhook(event_data):
    master_item_id, trigger_column_id, user_id = event_data.get('pulseId'), event_data.get('columnId'), event_data.get('userId')
    current_value_raw, previous_value_raw = event_data.get('value'), event_data.get('previousValue')
    current_ids = get_people_ids_from_value(current_value_raw)
    previous_ids = get_people_ids_from_value(previous_value_raw)
    
    if current_ids == previous_ids: return

    mappings = MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.get(trigger_column_id)
    if not mappings: return
    
    # Syncs people columns to other boards
    for target in mappings["targets"]:
        linked_ids = get_linked_items_from_board_relation(master_item_id, int(MASTER_STUDENT_BOARD_ID), target["connect_column_id"])
        for linked_id in linked_ids:
            update_people_column(linked_id, int(target["board_id"]), target["target_column_id"], current_value_raw, target["target_column_type"])

    # Creates a single, detailed log update on the PLP board
    plp_target = next((t for t in mappings["targets"] if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
    if not plp_target: return
    
    plp_linked_ids = get_linked_items_from_board_relation(master_item_id, int(MASTER_STUDENT_BOARD_ID), plp_target["connect_column_id"])
    if not plp_linked_ids: return
    
    plp_item_id = list(plp_linked_ids)[0]
    changer_name = get_user_name(user_id) or "automation"
    col_name = mappings.get("name", "Staff")
    subitem_name = f"{col_name} Assignments"
    
    subitem_id = find_or_create_subitem(plp_item_id, subitem_name)
    if not subitem_id: return
    
    added_names = [name for name in [get_user_name(pid) for pid in (current_ids - previous_ids)] if name]
    removed_names = [name for name in [get_user_name(pid) for pid in (previous_ids - current_ids)] if name]

    update_messages = []
    for name in removed_names:
        update_messages.append(f"'{name}' was removed by {changer_name}.")
    for name in added_names:
        update_messages.append(f"'{name}' was assigned by {changer_name}.")

    if update_messages:
        current_names_str = get_column_value(master_item_id, int(MASTER_STUDENT_BOARD_ID), trigger_column_id).get('text') or "Blank"
        final_update = "\n".join(update_messages) + f"\nCurrent {col_name} is now: {current_names_str}."
        create_monday_update(subitem_id, final_update)
    

@celery_app.task(name='app.process_plp_course_sync_webhook')
def process_plp_course_sync_webhook(event_data):
    subitem_id, parent_item_id = event_data.get('pulseId'), event_data.get('parentItemId')
    
    tags_column_value = get_column_value(subitem_id, int(event_data.get('boardId')), HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID)
    if not tags_column_value or not tags_column_value.get('text'):
        print("INFO: No subject tags found on the HS Roster subitem. Skipping.")
        return
    
    try:
        tag_labels = {tag.strip() for tag in tags_column_value.get('text', '').split(',')}
    except (AttributeError, KeyError):
        print("ERROR: Could not parse tags from the Subject column.")
        return

    if not tag_labels:
        print("INFO: No subject tag labels found. Skipping.")
        return

    current_courses = get_linked_ids_from_connect_column_value(event_data.get('value'))
    previous_courses = get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    added_courses = current_courses - previous_courses
    removed_courses = previous_courses - current_courses

    if not added_courses and not removed_courses:
        return # No change in linked courses

    plp_linked_ids = get_linked_items_from_board_relation(parent_item_id, int(HS_ROSTER_BOARD_ID), HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
    if not plp_linked_ids:
        print(f"ERROR: Could not find a PLP item linked to HS Roster item {parent_item_id}.")
        return
    plp_item_id = list(plp_linked_ids)[0]
    
    # Stores the target column for each course
    course_to_final_cols = defaultdict(set)
    
    # 1. Get secondary categories for all added courses to make a single API call
    secondary_category_col_id = "dropdown_mkq0r2av"
    if added_courses:
        course_ids_to_query = list(added_courses)
        secondary_category_query = f"query {{ items (ids: {course_ids_to_query}) {{ id column_values(ids: [\"{secondary_category_col_id}\"]) {{ text }} }} }}"
        secondary_category_results = execute_monday_graphql(secondary_category_query)
        secondary_category_map = {int(item['id']): item['column_values'][0].get('text') for item in secondary_category_results.get('data', {}).get('items', []) if item.get('column_values')}
    else:
        secondary_category_map = {}

    # 2. Process primary tags and determine final columns for each added course
    for course_id in added_courses:
        course_secondary = secondary_category_map.get(course_id, '')
        
        is_ace_course = course_secondary == "ACE"

        if is_ace_course:
            ace_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ACE")
            if ace_col_id:
                course_to_final_cols[course_id].add(ace_col_id)
            
            for category in tag_labels:
                if category in ["ELA", "Other/Elective"]:
                    primary_col = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(category)
                    if primary_col:
                        course_to_final_cols[course_id].add(primary_col)
        
        else:
            for category in tag_labels:
                target_col = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(category)
                
                if target_col:
                    course_to_final_cols[course_id].add(target_col)
                else:
                    other_col = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Other/Elective")
                    if other_col:
                        print(f"WARNING: Tag '{category}' doesn't map to a PLP column. Routing to 'Other/Elective'.")
                        course_to_final_cols[course_id].add(other_col)
                    else:
                        print(f"WARNING: Tag '{category}' not mapped and 'Other/Elective' is not configured. Skipping.")
    
    # 3. Add courses to the determined columns
    for course_id, col_ids in course_to_final_cols.items():
        for col_id in set(col_ids):
            update_connect_board_column(plp_item_id, int(PLP_BOARD_ID), col_id, course_id, "add")

    # 4. Handle removed courses
    for course_id in removed_courses:
        possible_cols = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.values()
        for col_id in possible_cols:
            update_connect_board_column(plp_item_id, int(PLP_BOARD_ID), col_id, course_id, "remove")

    downstream_event = {'pulseId': plp_item_id, 'userId': event_data.get('userId')}
    process_canvas_delta_sync_from_course_change.delay(downstream_event)


@celery_app.task(name='app.process_teacher_enrollment_webhook')
def process_teacher_enrollment_webhook(event_data):
    course_item_id = event_data.get('pulseId')
    board_id = event_data.get('boardId')
    canvas_course_id_val = get_column_value(course_item_id, board_id, CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = canvas_course_id_val.get('text') if canvas_course_id_val else None
    if not canvas_course_id:
        create_monday_update(course_item_id, "Enrollment Failed: Canvas Course ID is missing on the course item.")
        return
    added_staff_item_ids = get_linked_ids_from_connect_column_value(event_data.get('value')) - get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    if not added_staff_item_ids: return
    for staff_item_id in added_staff_item_ids:
        teacher_name = get_item_name(staff_item_id, int(ALL_STAFF_BOARD_ID)) or f"Staff Item {staff_item_id}"
        email_val = get_column_value(staff_item_id, int(ALL_STAFF_BOARD_ID), ALL_STAFF_EMAIL_COLUMN_ID)
        sis_id_val = get_column_value(staff_item_id, int(ALL_STAFF_BOARD_ID), ALL_STAFF_SIS_ID_COLUMN_ID)
        canvas_id_val = get_column_value(staff_item_id, int(ALL_STAFF_BOARD_ID), ALL_STAFF_CANVAS_ID_COLUMN)
        internal_id_val = get_column_value(staff_item_id, int(ALL_STAFF_BOARD_ID), ALL_STAFF_INTERNAL_ID_COLUMN)
        teacher_details = { 'name': teacher_name, 'email': email_val.get('text') if email_val else None, 'sis_id': sis_id_val.get('text') if sis_id_val else None, 'canvas_id': canvas_id_val.get('text') if canvas_id_val else None, 'internal_id': internal_id_val.get('text') if internal_id_val else None, }
        result = enroll_teacher_in_course(canvas_course_id, teacher_details)
        create_monday_update(course_item_id, f"Enrollment attempt for '{teacher_name}': {result}")

@celery_app.task(name='app.process_sped_students_person_sync_webhook')
def process_sped_students_person_sync_webhook(event_data):
    source_item_id, col_id, col_val = event_data.get('pulseId'), event_data.get('columnId'), event_data.get('value')
    config = SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.get(col_id)
    if not config: return
    linked_ids = get_linked_items_from_board_relation(source_item_id, int(SPED_STUDENTS_BOARD_ID), SPED_TO_IEPAP_CONNECT_COLUMN_ID)
    for linked_id in linked_ids:
        update_people_column(linked_id, int(IEP_AP_BOARD_ID), config["target_column_id"], col_val, config["target_column_type"])

# ==============================================================================
# FLASK WEB APP
# ==============================================================================
app = Flask(__name__)

@app.route('/monday-webhooks', methods=['POST'])
def monday_unified_webhooks():
    data = request.get_json()
    if 'challenge' in data: return jsonify({'challenge': data['challenge']})
    event = data.get('event', {})
    board_id, col_id, webhook_type = str(event.get('boardId')), event.get('columnId'), event.get('type')
    parent_board_id = str(event.get('parentItemBoardId')) if event.get('parentItemBoardId') else None
    if board_id == PLP_BOARD_ID and webhook_type == "update_column_value":
        if col_id == PLP_CANVAS_SYNC_COLUMN_ID:
            process_canvas_full_sync_from_status.delay(event)
            return jsonify({"message": "Canvas Full Sync queued."}), 202
        if col_id in [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',')]:
            process_canvas_delta_sync_from_course_change.delay(event)
            return jsonify({"message": "Canvas Delta Sync queued."}), 202
    if parent_board_id == HS_ROSTER_BOARD_ID and col_id == HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID:
        process_plp_course_sync_webhook.delay(event)
        return jsonify({"message": "PLP Course Sync queued."}), 202
    if board_id == MASTER_STUDENT_BOARD_ID and col_id in MASTER_STUDENT_PEOPLE_COLUMNS:
        process_master_student_person_sync_webhook.delay(event)
        return jsonify({"message": "Master Student Person Sync queued."}), 202
    if board_id == SPED_STUDENTS_BOARD_ID and col_id in SPED_STUDENTS_PEOPLE_COLUMN_MAPPING:
        process_sped_students_person_sync_webhook.delay(event)
        return jsonify({"message": "SpEd Students Person Sync queued."}), 202
    if board_id == CANVAS_BOARD_ID and col_id == CANVAS_TO_STAFF_CONNECT_COLUMN_ID:
        process_teacher_enrollment_webhook.delay(event)
        return jsonify({"message": "Canvas Teacher Enrollment queued."}), 202
    for rule in LOG_CONFIGS:
        if str(rule.get("trigger_board_id")) == board_id:
            if (webhook_type == "update_column_value" and rule.get("trigger_column_id") == col_id) or \
               (webhook_type == "create_pulse" and not rule.get("trigger_column_id")):
                 process_general_webhook.delay(event, rule)
                 return jsonify({"message": f"General task '{rule.get('log_type')}' queued."}), 202
    return jsonify({"status": "ignored"}), 200

@app.route('/')
def home():
    return "Consolidated Webhook Handler is running!", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
