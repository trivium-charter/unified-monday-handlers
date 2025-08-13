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
# 1. Runs the HS Roster to PLP sync logic.
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

# ==============================================================================
# 1. CENTRALIZED CONFIGURATION (Merged from both scripts)
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
PLP_TO_HS_ROSTER_CONNECT_COLUMN = os.environ.get("PLP_TO_HS_ROSTER_CONNECT_COLUMN") # You may need to create this env var
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

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
except (json.JSONDecodeError, TypeError):
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}

# ==============================================================================
# 2. MONDAY.COM & CANVAS UTILITIES (Merged and improved)
# ==============================================================================

# --- MONDAY.COM ---
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

def get_all_board_items(board_id, columns_to_fetch=None):
    all_items = []
    cursor = None
    column_query = " ".join(columns_to_fetch) if columns_to_fetch else ""
    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        query = f"""
            query {{
                boards(ids: {board_id}) {{
                    items_page (limit: 50, {cursor_str}) {{
                        cursor
                        items {{ id name updated_at {column_query} }}
                    }}
                }}
            }}"""
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

def check_if_subitem_exists(parent_item_id, subitem_name_to_check, creator_id):
    """
    Checks if a subitem with an exact name, created by the automation user,
    already exists for a given parent item.
    """
    query = f'query {{ items(ids:[{parent_item_id}]) {{ subitems {{ name creator {{ id }} }} }} }}'
    result = execute_monday_graphql(query)
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            creator = subitem.get('creator')
            if (subitem.get('name') == subitem_name_to_check and
                    creator and str(creator.get('id')) == str(creator_id)):
                return True # Found a match
    except (KeyError, IndexError, TypeError):
        pass # No subitems or error parsing
    return False # No match found
    
# --- ALL OTHER UTILITY FUNCTIONS FROM YOUR SCRIPTS ---

def get_item_name(item_id, board_id):
    query = f"query {{ boards(ids: {board_id}) {{ items_page(query_params: {{ids: [{item_id}]}}) {{ items {{ name }} }} }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            return board['items_page']['items'][0].get('name')
    return None

def get_user_name(user_id):
    if user_id is None or user_id == -4:
        return None
    query = f"query {{ users(ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('users'):
        return result['data']['users'][0].get('name')
    return None

def get_column_value(item_id, board_id, column_id):
    if not item_id or not column_id:
        return None
    query = f'query {{ items (ids: [{item_id}]) {{ column_values (ids: ["{column_id}"]) {{ id text value type }} }} }}'
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('items'):
        try:
            column_list = result['data']['items'][0].get('column_values', [])
            if not column_list: return None
            col_val = column_list[0]
            parsed_value = col_val.get('value')
            if isinstance(parsed_value, str):
                try:
                    parsed_value = json.loads(parsed_value)
                except json.JSONDecodeError:
                    pass
            return {'value': parsed_value, 'text': col_val.get('text')}
        except (IndexError, KeyError):
            return None
    return None

def delete_item(item_id):
    mutation = f"mutation {{ delete_item (item_id: {item_id}) {{ id }} }}"
    return execute_monday_graphql(mutation)

def change_column_value_generic(board_id, item_id, column_id, value):
    graphql_value = json.dumps(str(value))
    mutation = f'mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}) {{ id }} }}'
    return execute_monday_graphql(mutation) is not None

def get_people_ids_from_value(value_data):
    if not value_data: return set()
    if isinstance(value_data, str):
        try:
            value_data = json.loads(value_data)
        except json.JSONDecodeError:
            return set()
    persons_and_teams = value_data.get('personsAndTeams', [])
    return {person['id'] for person in persons_and_teams if 'id' in person}

def get_linked_ids_from_connect_column_value(value_data):
    if not value_data: return set()
    try:
        parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data)
        if "linkedPulseIds" in parsed_value:
            return {int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"] if "linkedPulseId" in item}
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
    else:
        print(f"WARNING: Failed to create subitem '{subitem_name}'. Result was: {result}")
        return None

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
    if target_column_type == "person":
        final_value = {"personId": int(new_person_id)}
    elif target_column_type == "multiple-person":
        final_value = {"personsAndTeams": updated_people_list}
    else:
        return False
    graphql_value = json.dumps(json.dumps(final_value))
    mutation = f'mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{people_column_id}", value: {graphql_value}) {{ id }} }}'
    return execute_monday_graphql(mutation) is not None

def bulk_add_to_connect_column(item_id, board_id, connect_column_id, course_ids_to_add):
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
    if updated_linked_items == current_linked_items: return True
    connect_value = {"linkedPulseIds": [{"linkedPulseId": int(lid)} for lid in sorted(list(updated_linked_items))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    mutation = f'mutation {{ change_column_value (board_id: {board_id}, item_id: {item_id}, column_id: "{connect_column_id}", value: {graphql_value}) {{ id }} }}'
    print(f"    SYNCING: Adding {len(course_ids_to_add - current_linked_items)} courses to column {connect_column_id} on PLP item {item_id}.")
    return execute_monday_graphql(mutation) is not None


# --- CANVAS ---

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
            user_id = int(id_from_monday)
            return canvas_api.get_user(user_id)
        except (ValueError, TypeError):
            try:
                return canvas_api.get_user(id_from_monday, 'sis_user_id')
            except ResourceDoesNotExist:
                pass
        except ResourceDoesNotExist:
            pass
    if student_details.get('email'):
        try:
            return canvas_api.get_user(student_details['email'], 'login_id')
        except ResourceDoesNotExist:
            pass
    if student_details.get('ssid') and student_details.get('ssid') != id_from_monday:
        try:
            return canvas_api.get_user(student_details['ssid'], 'sis_user_id')
        except ResourceDoesNotExist:
            pass
    if student_details.get('email'):
        try:
            search_results = canvas_api.get_account(1).get_users(search_term=student_details['email'])
            users = [u for u in search_results]
            if len(users) == 1:
                return users[0]
        except (ResourceDoesNotExist, CanvasException):
            pass
    return None

def create_canvas_user(student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        account = canvas_api.get_account(1)
        user_payload = {
            'user': {'name': student_details['name'], 'terms_of_use': True},
            'pseudonym': {'unique_id': student_details['email'], 'sis_user_id': student_details['ssid']},
            'communication_channel': {'type': 'email', 'address': student_details['email'], 'skip_confirmation': True}
        }
        return account.create_user(**user_payload)
    except CanvasException as e:
        print(f"ERROR: Canvas user creation failed: {e}")
        return None

def update_user_ssid(user, new_ssid):
    try:
        logins = user.get_logins()
        if logins:
            primary_login = logins[0]
            primary_login.edit(login={'sis_user_id': new_ssid})
            print(f"INFO: Successfully updated SSID for user '{user.name}' to '{new_ssid}'.")
            return True
        else:
            print(f"WARNING: No logins found for user '{user.name}'. Cannot update SSID.")
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
# 3. CORE SYNC LOGIC (Functions from both scripts)
# ==============================================================================

# Place this function inside Section 3: CORE SYNC LOGIC

def get_student_details_from_plp(plp_item_id):
    """Fetches comprehensive student details via the PLP item connection."""
    query = f"""
    query {{
        items (ids: [{plp_item_id}]) {{
            column_values (ids: ["{PLP_TO_MASTER_STUDENT_CONNECT_COLUMN}"]) {{
                value
            }}
        }}
    }}
    """
    result = execute_monday_graphql(query)
    try:
        connect_column_value = json.loads(result['data']['items'][0]['column_values'][0]['value'])
        linked_ids = [item['linkedPulseId'] for item in connect_column_value.get('linkedPulseIds', [])]
        if not linked_ids:
            return None
        master_student_id = linked_ids[0]

        details_query = f"""
        query {{
            items (ids: [{master_student_id}]) {{
                id
                name
                column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}"]) {{
                    id
                    text
                }}
            }}
        }}
        """
        details_result = execute_monday_graphql(details_query)
        item_details = details_result['data']['items'][0]
        student_name = item_details['name']

        column_map = {cv['id']: cv.get('text') for cv in item_details.get('column_values', []) if isinstance(cv, dict)}

        ssid = column_map.get(MASTER_STUDENT_SSID_COLUMN, '')
        
        # Clean the email to prevent matching errors
        raw_email = column_map.get(MASTER_STUDENT_EMAIL_COLUMN, '')
        email = unicodedata.normalize('NFKC', raw_email).strip()
        canvas_id = column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN, '')

        if not all([student_name, email]):
            return None

        return {'name': student_name, 'ssid': ssid, 'email': email, 'canvas_id': canvas_id, 'master_id': item_details['id']}
    except (TypeError, KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not parse student details from Monday.com response for PLP {plp_item_id}: {e}")
        return None
        
def run_hs_roster_sync_for_student(hs_roster_item, dry_run=True):
    parent_item_id = hs_roster_item['id']
    parent_item_name = hs_roster_item['name']
    print(f"\n--- Processing HS Roster for: {parent_item_name} (ID: {parent_item_id}) ---")
    plp_query = f'query {{ items(ids:[{parent_item_id}]) {{ column_values(ids:["{HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID}"]) {{ value }} }} }}'
    plp_result = execute_monday_graphql(plp_query)
    try:
        plp_linked_ids = get_linked_ids_from_connect_column_value(plp_result['data']['items'][0]['column_values'][0]['value'])
        if not plp_linked_ids:
            print("  SKIPPING: No PLP item is linked.")
            return
        plp_item_id = list(plp_linked_ids)[0]
    except (TypeError, KeyError, IndexError):
        print("  SKIPPING: Could not find linked PLP item.")
        return
    HS_ROSTER_SUBITEM_TERM_COLUMN_ID = "color6"
    subitems_query = f'query {{ items (ids: [{parent_item_id}]) {{ subitems {{ id name column_values(ids: ["{HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID}", "{HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}", "{HS_ROSTER_SUBITEM_TERM_COLUMN_ID}"]) {{ id text value }} }} }} }}'
    subitems_result = execute_monday_graphql(subitems_query)
    initial_course_categories = {}
    try:
        subitems = subitems_result['data']['items'][0]['subitems']
        for subitem in subitems:
            subitem_cols = {cv['id']: cv for cv in subitem['column_values']}
            term_val = subitem_cols.get(HS_ROSTER_SUBITEM_TERM_COLUMN_ID, {}).get('text')
            if term_val == "Spring":
                print(f"  SKIPPING: Subitem '{subitem['name']}' is marked as Spring.")
                continue
            category = subitem_cols.get(HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID, {}).get('text')
            courses_val = subitem_cols.get(HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID, {}).get('value')
            if category and courses_val:
                course_ids = get_linked_ids_from_connect_column_value(courses_val)
                for course_id in course_ids:
                    initial_course_categories[course_id] = category
    except (TypeError, KeyError, IndexError):
        print("  ERROR: Could not process subitems.")
        return
    all_course_ids = list(initial_course_categories.keys())
    if not all_course_ids:
        print("  INFO: No non-Spring courses found to process.")
        return
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
            target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Math")
            if target_col_id: plp_updates[target_col_id].add(course_id)
        if initial_category == "English":
            target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ELA")
            if target_col_id: plp_updates[target_col_id].add(course_id)
        secondary_category = secondary_category_map.get(course_id)
        if secondary_category == "ACE":
            target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("ACE")
            if target_col_id: plp_updates[target_col_id].add(course_id)
        if secondary_category not in ["ACE", "Connect"]:
            target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get("Other/Elective")
            if target_col_id: plp_updates[target_col_id].add(course_id)
    if not plp_updates:
        print("  INFO: No valid courses found to sync after categorization.")
        return
    print(f"  Found courses to sync for PLP item {plp_item_id}.")
    if dry_run:
        for col_id, courses in plp_updates.items():
            print(f"    DRY RUN: Would add {len(courses)} courses to PLP column {col_id}.")
        return
    for col_id, courses in plp_updates.items():
        bulk_add_to_connect_column(plp_item_id, int(PLP_BOARD_ID), col_id, courses)

# Place these functions inside Section 3: CORE SYNC LOGIC

def get_teacher_person_value_from_canvas_board(canvas_item_id):
    """Finds the teacher's 'Person' value from the Canvas and Staff boards."""
    linked_staff_ids = get_linked_items_from_board_relation(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_TO_STAFF_CONNECT_COLUMN_ID)
    if not linked_staff_ids:
        return None
    staff_item_id = list(linked_staff_ids)[0]
    person_col_val = get_column_value(staff_item_id, int(ALL_STAFF_BOARD_ID), ALL_STAFF_PERSON_COLUMN_ID) # This was missing a variable name
    if not person_col_val:
        return None
    return person_col_val.get('value')


def manage_class_enrollment(action, plp_item_id, class_item_id, student_details, category_name, creator_id, subitem_cols=None):
    """Manages class enrollment and creates verbose subitems about its actions."""
    subitem_cols = subitem_cols or {}
    all_courses_item_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"

    linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    
    if not linked_canvas_item_ids:
        print(f"INFO: No Canvas course linked for '{all_courses_item_name}'. Creating log subitem.")
        if action == "enroll":
            create_subitem(plp_item_id, f"Added {category_name} '{all_courses_item_name}' (Non-Canvas)", subitem_cols)
        return

    canvas_item_id = list(linked_canvas_item_ids)[0]
    class_name = get_item_name(canvas_item_id, int(CANVAS_BOARD_ID))
    if not class_name:
        print(f"ERROR: Linked item {canvas_item_id} has no name. Aborting.")
        return

    course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = course_id_val.get('text') if course_id_val else None

    if not canvas_course_id and action == "enroll":
        print(f"INFO: No Canvas ID found for '{class_name}'. Attempting to create course.")
        new_course = create_canvas_course(class_name, CANVAS_TERM_ID)
        if new_course:
            canvas_course_id = new_course.id
            change_column_value_generic(int(CANVAS_BOARD_ID), canvas_item_id, CANVAS_COURSE_ID_COLUMN_ID, str(canvas_course_id))
            if ALL_CLASSES_CANVAS_ID_COLUMN:
                change_column_value_generic(int(ALL_COURSES_BOARD_ID), class_item_id, ALL_CLASSES_CANVAS_ID_COLUMN, str(canvas_course_id))
        else:
            create_subitem(plp_item_id, f"Added {category_name} '{class_name}': FAILED - Could not create Canvas course.", subitem_cols)
            return

    if not canvas_course_id:
        print(f"INFO: No Canvas Course ID for '{class_name}'. Skipping {action}.")
        return

    if action == "enroll":
        m_series_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_M_SERIES_LABELS_COLUMN)
        ag_grad_val = get_column_value(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_CLASSES_AG_GRAD_COLUMN)
        m_series_text = (m_series_val.get('text') or "") if m_series_val else ""
        ag_grad_text = (ag_grad_val.get('text') or "") if ag_grad_val else ""
        sections = {"A-G" for s in ["AG"] if s in ag_grad_text} | {"Grad" for s in ["Grad"] if s in ag_grad_text} | {"M-Series" for s in ["M-series"] if s in m_series_text} or {"All"}

        enrollment_results = []
        for section_name in sections:
            # --- START OF NEW CODE ---
            # First, determine the exact subitem name we would create on success
            # This must exactly match the format you use later
            expected_subitem_name = f"Added {category_name} '{class_name}' (Sections: {section_name}): Success"

            # Now, check if this subitem already exists
            if check_if_subitem_exists(plp_item_id, expected_subitem_name, creator_id):
                print(f"INFO: Subitem '{expected_subitem_name}' already exists. Skipping enrollment.")
                continue # Skip to the next section
            print(f"INFO: Enrolling student in '{class_name}', section '{section_name}'...")
            section = create_section_if_not_exists(canvas_course_id, section_name)
            if section:
                result = enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)
                enrollment_results.append({'section': section_name, 'status': result})

        if enrollment_results:
            section_names = ", ".join([res['section'] for res in enrollment_results])
            all_statuses = {res['status'] for res in enrollment_results}
            final_status = "Failed" if "Failed" in all_statuses else "Success"
            subitem_title = f"Added {category_name} '{class_name}' (Sections: {section_names}): {final_status}"
            create_subitem(plp_item_id, subitem_title, subitem_cols)
            
def run_plp_sync_for_student(plp_item_id, creator_id, dry_run=True):
    print(f"\n--- Processing PLP Item: {plp_item_id} ---")
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details:
        print(f"WARNING: Could not get student details for PLP {plp_item_id}. Skipping.")
        return
    master_student_id = student_details.get('master_id')
    if not master_student_id:
        print(f"ERROR: Could not find Master Student ID for PLP {plp_item_id}. Skipping.")
        return
    staff_change_values = {PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Staff Change"]}}
    curriculum_change_values = {PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Curriculum Change"]}}
    if not dry_run:
        print("ACTION: Syncing teacher assignments from Master Student board to PLP...")
        for trigger_col, mapping in MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.items():
            master_person_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), trigger_col)
            plp_target_mapping = next((t for t in mapping["targets"] if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
            if plp_target_mapping and master_person_val and master_person_val.get('value'):
                update_people_column(plp_item_id, int(PLP_BOARD_ID), plp_target_mapping["target_column_id"], master_person_val['value'], plp_target_mapping["target_column_type"])
                person_ids = get_people_ids_from_value(master_person_val['value'])
                for person_id in person_ids:
                    person_name = get_user_name(person_id)
                    if person_name:
                        log_message = f"{mapping.get('name', 'Staff')} set to {person_name}"
                        create_subitem(plp_item_id, log_message, column_values=staff_change_values)
    print("INFO: Syncing class enrollments...")
    class_id_to_category_map = {}
    for category, column_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
        linked_class_ids = get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), column_id)
        for class_id in linked_class_ids:
            class_id_to_category_map[class_id] = category
    all_class_ids = class_id_to_category_map.keys()
    if not all_class_ids:
        print("INFO: No classes to sync.")
        return
    for class_item_id in all_class_ids:
        class_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
        print(f"INFO: Processing class: '{class_name}'")
        category_name = class_id_to_category_map.get(class_item_id, "Course")
        if not dry_run:
            manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, category_name, creator_id, subitem_cols=curriculum_change_values)

# ==============================================================================
# 4. SCRIPT EXECUTION (Corrected to start from PLP Board)
# ==============================================================================

if __name__ == '__main__':
    DRY_RUN = False
    
    print("======================================================")
    print("=== STARTING NIGHTLY DELTA SYNC SCRIPT           ===")
    print("======================================================")
    
    db = None
    cursor = None
    try:
        # --- 1. Database Connection ---
        print("INFO: Connecting to the database...")
        ssl_opts = {'ssl_ca': 'ca.pem', 'ssl_verify_cert': True}
        db = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME, port=int(DB_PORT), **ssl_opts
        )
        cursor = db.cursor()

        # --- 2. Fetch last sync times for all previously processed PLP students ---
        print("INFO: Fetching last sync times for processed students...")
        cursor.execute("SELECT student_id, last_synced_at FROM processed_students")
        processed_map = {row[0]: row[1] for row in cursor.fetchall()}
        print(f"INFO: Found {len(processed_map)} students in the database.")

        # --- 3. Fetch all PLP board items from Monday.com with their last update time ---
        print("INFO: Fetching all PLP board items from Monday.com...")
        all_plp_items = get_all_board_items(PLP_BOARD_ID)

        # --- 4. Filter for only new or updated PLP students ---
        print("INFO: Filtering for new or updated students on PLP Board...")
        items_to_process = []
        for item in all_plp_items:
            item_id = int(item['id'])
            updated_at_str = item['updated_at']
            updated_at = datetime.strptime(updated_at_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
            
            last_synced = processed_map.get(item_id)
            if last_synced:
                last_synced = last_synced.replace(tzinfo=timezone.utc)

            if not last_synced or updated_at > last_synced:
                items_to_process.append(item)
        
        total_to_process = len(items_to_process)
        print(f"INFO: Found {total_to_process} PLP students that are new or have been updated.")

        # --- 5. Process each changed student ---
        for i, plp_item in enumerate(items_to_process, 1):
            plp_item_id = int(plp_item['id'])
            print(f"\n===== Processing Student {i}/{total_to_process} (PLP ID: {plp_item_id}) =====")
            
            try:
                # --- Phase 1: Syncing HS Roster to PLP (if applicable) ---
                print("--- Phase 1: Checking for and syncing HS Roster ---")
                hs_roster_connect_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_TO_HS_ROSTER_CONNECT_COLUMN)
                hs_roster_ids = get_linked_ids_from_connect_column_value(hs_roster_connect_val.get('value'))
                
                if hs_roster_ids:
                    hs_roster_item_id = list(hs_roster_ids)[0]
                    # We need the full HS Roster item object to pass to the function
                    hs_roster_item_result = get_all_board_items(HS_ROSTER_BOARD_ID, columns_to_fetch=[f'items(ids:[{hs_roster_item_id}])'])
                    if hs_roster_item_result:
                         # This query syntax is a bit simplified; fetching single item might need adjustment
                         # For now, assuming get_all_board_items can filter by ID
                        hs_roster_item_object = next((item for item in hs_roster_item_result if int(item['id']) == hs_roster_item_id), None)
                        if hs_roster_item_object:
                            run_hs_roster_sync_for_student(hs_roster_item_object, dry_run=DRY_RUN)
                        else:
                            print(f"WARNING: Could not fetch HS Roster item object for ID {hs_roster_item_id}")
                else:
                    print("INFO: No HS Roster item linked. Skipping Phase 1.")

                # --- Phase 2: Syncing PLP to Canvas ---
                print("--- Phase 2: Syncing PLP to Canvas ---")
                run_plp_sync_for_student(plp_item_id, creator_id, dry_run=DRY_RUN)

                # --- 6. If successful, update the timestamp in the database ---
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
