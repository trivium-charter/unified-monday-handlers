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
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
MASTER_STUDENT_CANVAS_ID_COLUMN = "text_mktgs1ax"
MASTER_STUDENT_TOR_COLUMN_ID = os.environ.get("MASTER_STUDENT_TOR_COLUMN_ID")
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
ALL_CLASSES_CANVAS_ID_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_ID_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
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

# The 10 special courses
ROSTER_ONLY_COURSES = {10298, 10297, 10299, 10300, 10301}
ROSTER_AND_CREDIT_COURSES = {10097, 10002, 10092, 10164, 10198}
ALL_SPECIAL_COURSES = ROSTER_ONLY_COURSES.union(ROSTER_AND_CREDIT_COURSES)


# ==============================================================================
# MONDAY.COM UTILITIES
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
    graphql_value = json.dumps(json.dumps({"name": new_name}))
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
    if not new_persons_and_teams: return False
    new_person_id = new_persons_and_teams[0].get('id')
    if not new_person_id: return False
    current_col_val = get_column_value(item_id, board_id, people_column_id)
    current_people_ids = set()
    if current_col_val and current_col_val.get('value'):
        current_people_ids = get_people_ids_from_value(current_col_val['value'])
    current_people_ids.add(new_person_id)
    updated_people_list = [{"id": int(pid), "kind": "person"} for pid in current_people_ids]
    if target_column_type == "person": final_value = {"personId": int(new_person_id)}
    elif target_column_type == "multiple-person": final_value = {"personsAndTeams": updated_people_list}
    else: return False
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
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    if teacher_details.get('canvas_id'):
        try: return canvas_api.get_user(teacher_details['canvas_id'])
        except (ResourceDoesNotExist, ValueError): pass
    if teacher_details.get('internal_id'):
        try: return canvas_api.get_user(teacher_details['internal_id'])
        except (ResourceDoesNotExist, ValueError): pass
    if teacher_details.get('email'):
        try: return canvas_api.get_user(teacher_details['email'], 'login_id')
        except ResourceDoesNotExist: pass
    if teacher_details.get('sis_id'):
        try: return canvas_api.get_user(teacher_details['sis_id'], 'sis_user_id')
        except ResourceDoesNotExist: pass
    if teacher_details.get('email'):
        try:
            users = [u for u in canvas_api.get_account(1).get_users(search_term=teacher_details['email'])]
            if len(users) == 1: return users[0]
        except (ResourceDoesNotExist, CanvasException): pass
    return None

def create_canvas_user(user_details, role='student'):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        account = canvas_api.get_account(1)
        user_payload = {
            'user': {'name': user_details['name'], 'terms_of_use': True},
            'pseudonym': {
                'unique_id': user_details['email'],
                'sis_user_id': user_details.get('sis_id') or user_details['email'],
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
            return find_canvas_teacher(user_details) if role == 'teacher' else find_canvas_user(user_details)
        raise

def update_user_ssid(user, new_ssid):
    try:
        logins = user.get_logins()
        if logins:
            login_to_update = logins[0]
            login_to_update.edit(login={'sis_user_id': new_ssid})
            return True
        return False
    except CanvasException as e:
        print(f"ERROR: API error updating SSID for user '{user.name}': {e}")
    return False

def create_section_if_not_exists(course_id, section_name):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        course = canvas_api.get_course(course_id)
        existing_section = next((s for s in course.get_sections() if s.name.lower() == section_name.lower()), None)
        return existing_section or course.create_course_section(course_section={'name': section_name})
    except CanvasException as e:
        print(f"ERROR: Canvas section creation/check failed: {e}")
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

def manage_class_enrollment(action, plp_item_id, class_item_id, student_details, category_name, creator_id, db_cursor, dry_run=True):
    class_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
    linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    
    if not linked_canvas_item_ids:
        print(f"  INFO: '{class_name}' is a non-Canvas course or no link exists. Skipping enrollment action.")
        return

    canvas_item_id = list(linked_canvas_item_ids)[0]
    course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = course_id_val.get('text') if course_id_val else None

    if not canvas_course_id:
        print(f"  WARNING: Canvas Course ID not found for course '{class_name}'. Skipping enrollment action.")
        return

    if action == "enroll":
        print(f"  ACTION: Pushing enrollment for '{class_name}' to Canvas.")
        canvas_api = initialize_canvas_api()
        student_canvas_user = None
        if not dry_run:
            student_canvas_user = find_canvas_user(student_details, db_cursor)

        if student_canvas_user:
            # --- NEW LOGIC FOR SPECIAL SECTIONS ---
            if int(class_item_id) in ALL_SPECIAL_COURSES:
                print("    -> Applying special section logic.")
                student_master_id = student_details.get('master_id')
                
                if not student_master_id:
                    print("    -> SKIPPING: Could not get student Master ID for special section logic.")
                    return

                roster_teacher_name = get_roster_teacher_name(student_master_id)
                if not roster_teacher_name:
                    print("    -> WARNING: Could not determine Roster Teacher. Defaulting to 'Unassigned'.")
                    roster_teacher_name = "Unassigned"
                
                section_teacher = create_section_if_not_exists(canvas_course_id, roster_teacher_name)
                if section_teacher:
                    if not dry_run: 
                        enrollment_status = enroll_student_in_section(canvas_course_id, student_canvas_user.id, section_teacher.id)
                        print(f"      -> Roster Section Enrollment Status: {enrollment_status}")

                if int(class_item_id) in ROSTER_AND_CREDIT_COURSES:
                    course_item_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or ""
                    credit_section_name = "2.5 Credits" if "2.5" in course_item_name else "5 Credits"
                    
                    section_credit = create_section_if_not_exists(canvas_course_id, credit_section_name)
                    if section_credit:
                        if not dry_run:
                            enrollment_status = enroll_student_in_section(canvas_course_id, student_canvas_user.id, section_credit.id)
                            print(f"      -> Credit Section Enrollment Status: {enrollment_status}")
            else:
                # --- ORIGINAL LOGIC FOR NORMAL COURSES ---
                m_series_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_M_SERIES_LABELS_COLUMN)
                ag_grad_val = get_column_value(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_CLASSES_AG_GRAD_COLUMN)
                m_series_text = (m_series_val.get('text') or "") if m_series_val else ""
                ag_grad_text = (ag_grad_val.get('text') or "") if ag_grad_val else ""
                sections = {"A-G" for s in ["AG"] if s in ag_grad_text} | {"Grad" for s in ["Grad"] if s in ag_grad_text} | {"M-Series" for s in ["M-series"] if s in m_series_text}
                if not sections: sections.add("All")
                for section_name in sections:
                    section = create_section_if_not_exists(canvas_course_id, section_name)
                    if section:
                        if not dry_run: enroll_or_create_and_enroll(canvas_course_id, section.id, student_details, db_cursor)
        else:
            print(f"  INFO: Skipping Canvas enrollment for student '{student_details['name']}' because user was not found or created.")

        subitem_title = f"Added {category_name} '{class_name}'"
        if not check_if_subitem_exists(plp_item_id, subitem_title, creator_id):
            print(f"  INFO: Subitem log is missing. Creating it.")
            if not dry_run: create_subitem(plp_item_id, subitem_title)
        else:
            print(f"  INFO: Subitem log already exists.")

    elif action == "unenroll":
        subitem_title = f"Removed {category_name} '{class_name}'"
        print(f"  INFO: Unenrolling student and creating log: '{subitem_title}'")
        if not dry_run:
            unenroll_student_from_course(canvas_course_id, student_details)
            create_subitem(plp_item_id, subitem_title)


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
        manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, category_name, creator_id, db_cursor, dry_run=dry_run)
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
                if not dry_run: create_subitem(plp_item_id, expected_subitem_name)
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
                        if not dry_run: create_subitem(plp_item_id, expected_staff_subitem)
                    else:
                        print(f"    INFO: Subitem '{expected_staff_subitem}' already exists.")

def sync_canvas_teachers_and_tas(db_cursor, dry_run=True):
    """
    Syncs teachers from Monday.com Canvas Courses board to Canvas,
    and adds fixed TA accounts to all Canvas classes.
    """
    print("\n======================================================")
    print("=== STARTING CANVAS TEACHER AND TA SYNC          ===")
    print("======================================================")

    ta_accounts = [
        {'name': 'Substitute TA', 'email': TA_SUB_EMAIL, 'sis_id': 'TA-SUB'},
        {'name': 'Aide TA', 'email': TA_AIDE_EMAIL, 'sis_id': 'TA-AIDE'}
    ]

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
            current_query = current_query.replace("limit: 500)", f"limit: 500, cursor: \"{cursor}\")")
        
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

    universal_ta_users = []
    for ta_data in ta_accounts:
        ta_user = find_canvas_teacher(ta_data)
        if not ta_user:
            print(f"INFO: Universal TA user {ta_data['email']} not found. Attempting to create.")
            try:
                ta_user = create_canvas_user(ta_data, role='teacher')
            except Exception as e:
                print(f"ERROR: Failed to create universal TA {ta_data['email']}: {e}")
                ta_user = None
        if ta_user:
            universal_ta_users.append(ta_user)
        else:
            print(f"WARNING: Could not find or create universal TA {ta_data['email']}. They will not be enrolled.")

    if not universal_ta_users:
        print("WARNING: No universal TA users available for enrollment. Skipping universal TA sync.")

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

        if universal_ta_users:
            print("  -> Ensuring TA accounts are enrolled...")
            for ta_user in universal_ta_users:
                if not dry_run:
                    enroll_status = enroll_teacher_in_course(canvas_course_id, ta_user, role='TaEnrollment')
                    print(f"    -> Enrollment status for {ta_user.name} ({ta_user.id}) in course {canvas_course_id}: {enroll_status}")
                else:
                    print(f"  DRY RUN: Would enroll universal TA {ta_user.name} ({ta_user.id}) in course {canvas_course_id} as TA.")

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
    if DRY_RUN:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!               DRY RUN MODE IS ON               !!!")
        print("!!!  No actual changes will be made to your data.  !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
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
                    hs_roster_item_object = get_all_board_items(HS_ROSTER_BOARD_ID, item_ids=[hs_roster_item_id])
                    if hs_roster_item_object:
                        hs_roster_item_object = hs_roster_item_object[0]
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
        
        # --- NEW PHASE 3: Sync Teachers and TAs to Canvas Courses ---
        print("\n--- Phase 3: Syncing Canvas Teachers and TAs ---")
        sync_canvas_teachers_and_tas(cursor, dry_run=DRY_RUN)

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
