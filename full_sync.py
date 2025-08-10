#!/usr/bin/env python3
# ==============================================================================
# ONE-TIME CLEANUP AND SYNC SCRIPT
# ==============================================================================
#
# PURPOSE:
# This script performs a full audit and synchronization for student data between
# Monday.com and Canvas. It is designed to be run as a one-time process to
# correct inconsistencies that may have occurred due to a high volume of
# webhook failures.
#
# EXECUTION PHASES:
# 1.  CLEANUP: It first identifies a specific user by name and deletes all
#     subitems they created on the PLP board. This is intended to remove
#     all incorrect, automatically-generated logs.
# 2.  SYNC & RECREATE: It then iterates through every student on the PLP board
#     and performs two main actions:
#     a. Teacher Sync: It ensures the teachers assigned on the Master Student
#        board are correctly synced to the corresponding PLP item and creates
#        a new, clean subitem log for each assignment.
#     b. Class Sync: It re-processes all class enrollments. For Canvas
#        courses, it ensures the student is enrolled in the correct sections
#        and creates a subitem documenting the success or failure. For
#        non-Canvas classes, it creates a subitem simply logging the addition.
#
# USAGE:
# Configure the DRY_RUN and TARGET_USER_NAME variables at the bottom of the
# script before execution. Run with DRY_RUN = True first to verify the
# script's intended actions without making any actual changes.
#
# ==============================================================================

import os
import json
import requests
import time
from datetime import datetime

# ==============================================================================
# CENTRALIZED CONFIGURATION (Copied from app.py)
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"

PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_CANVAS_SYNC_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_COLUMN_ID")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE", "Done")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN")

MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
MASTER_STUDENT_CANVAS_ID_COLUMN = "text_mktgs1ax" # The student's custom Canvas ID

ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
ALL_CLASSES_CANVAS_ID_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_ID_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")

ALL_STAFF_BOARD_ID = os.environ.get("ALL_STAFF_BOARD_ID")
ALL_STAFF_EMAIL_COLUMN_ID = os.environ.get("ALL_STAFF_EMAIL_COLUMN_ID")
ALL_STAFF_SIS_ID_COLUMN_ID = os.environ.get("ALL_STAFF_SIS_ID_COLUMN_ID")
ALL_STAFF_PERSON_COLUMN_ID = os.environ.get("ALL_STAFF_PERSON_COLUMN_ID")
ALL_STAFF_CANVAS_ID_COLUMN = "text_mktg7h6"       # The teacher's custom Canvas ID
ALL_STAFF_INTERNAL_ID_COLUMN = "text_mkthjxht"  # The teacher's internal Canvas ID

CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")
CANVAS_TO_STAFF_CONNECT_COLUMN_ID = os.environ.get("CANVAS_TO_STAFF_CONNECT_COLUMN_ID")

CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

try:
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
except (json.JSONDecodeError, TypeError):
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}

# ==============================================================================
# MONDAY.COM UTILITIES (Copied from app.py)
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

def get_column_value(item_id, board_id, column_id):
    if not column_id: return None
    query = f"query {{ boards(ids: {board_id}) {{ items_page(query_params: {{ids: [{item_id}]}}) {{ items {{ column_values(ids: [\"{column_id}\"]) {{ id value text }} }} }} }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            item = board['items_page']['items'][0]
            if item.get('column_values'):
                col_val = item['column_values'][0]
                parsed_value = None
                if col_val.get('value'):
                    try: parsed_value = json.loads(col_val['value'])
                    except json.JSONDecodeError: parsed_value = col_val['value']
                return {'value': parsed_value, 'text': col_val.get('text')}
    return None

def change_column_value_generic(board_id, item_id, column_id, value):
    graphql_value = json.dumps(str(value))
    mutation = f"""
        mutation {{
            change_column_value(
                board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}
            ) {{ id }}
        }} """
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
    parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data) if isinstance(value_data, str) else {}
    if "linkedPulseIds" in parsed_value:
        return {int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"] if "linkedPulseId" in item}
    return set()

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    column_data = get_column_value(item_id, board_id, connect_column_id)
    return get_linked_ids_from_connect_column_value(column_data.get('value')) if column_data else set()

def create_subitem(parent_item_id, subitem_name, column_values=None):
    values_for_api = {col_id: val for col_id, val in (column_values or {}).items()}
    column_values_json = json.dumps(values_for_api)
    mutation = f"mutation {{ create_subitem (parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {json.dumps(column_values_json)}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result['data']['create_subitem'].get('id') if result and 'data' in result and result['data'].get('create_subitem') else None

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
    mutation = f"mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: \"{people_column_id}\", value: {graphql_value}) {{ id }} }}"
    return execute_monday_graphql(mutation) is not None

def delete_item(item_id):
    """Deletes an item or subitem."""
    mutation = f"mutation {{ delete_item (item_id: {item_id}) {{ id }} }}"
    return execute_monday_graphql(mutation)

# ==============================================================================
# CANVAS UTILITIES (Copied from app.py)
# ==============================================================================
# In a real-world scenario, you would import these from a shared library
# to avoid code duplication. For this standalone script, they are copied directly.
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist

def initialize_canvas_api():
    if CANVAS_API_URL and CANVAS_API_KEY:
        return Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    return None

def find_canvas_user(student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None

    # Search by explicit Canvas ID first
    if student_details.get('canvas_id'):
        try:
            return canvas_api.get_user(student_details['canvas_id'])
        except (ResourceDoesNotExist, ValueError):
            pass

    # Then by email login
    if student_details.get('email'):
        try:
            return canvas_api.get_user(student_details['email'], 'login_id')
        except ResourceDoesNotExist:
            pass

    # Then by SIS ID
    if student_details.get('ssid'):
        try:
            return canvas_api.get_user(student_details['ssid'], 'sis_user_id')
        except ResourceDoesNotExist:
            pass
            
    # Fallback to broad search by email
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
            'pseudonym': {
                'unique_id': student_details['email'],
                'sis_user_id': student_details['ssid'],
                'send_confirmation': False
            },
            'communication_channel': {
                'type': 'email',
                'address': student_details['email'],
                'skip_confirmation': True
            }
        }
        new_user = account.create_user(**user_payload)
        return new_user
    except CanvasException as e:
        print(f"ERROR: Canvas user creation failed: {e}")
        return None

def update_user_ssid(user, new_ssid):
    try:
        logins = user.get_logins()
        if logins:
            login_to_update = logins[0]
            login_to_update.edit(login={'sis_user_id': new_ssid})
            return True
    except CanvasException as e:
        print(f"ERROR: API error updating SSID for user '{user.name}': {e}")
    return False

def create_canvas_course(course_name, term_id):
    canvas_api = initialize_canvas_api()
    if not all([canvas_api, CANVAS_SUBACCOUNT_ID]):
        print("ERROR: Missing Canvas Sub-Account ID config.")
        return None
    try:
        account = canvas_api.get_account(CANVAS_SUBACCOUNT_ID)
    except ResourceDoesNotExist:
        print(f"ERROR: Canvas Sub-Account with ID '{CANVAS_SUBACCOUNT_ID}' not found.")
        return None

    course_data = {
        'name': course_name,
        'course_code': course_name,
        'enrollment_term_id': term_id,
        'is_template': False
    }

    if CANVAS_TEMPLATE_COURSE_ID:
        course_data['source_course_id'] = CANVAS_TEMPLATE_COURSE_ID

    try:
        print(f"INFO: Trying to create course '{course_name}'.")
        new_course = account.create_course(course=course_data)
        print(f"SUCCESS: Course '{course_name}' created with ID {new_course.id}.")
        return new_course
    except CanvasException as e:
        print(f"ERROR: A critical Canvas API error occurred for course '{course_name}': {e}")
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
    if not canvas_api: return "Failed: Canvas API not initialized"
    try:
        course = canvas_api.get_course(course_id)
        user = canvas_api.get_user(user_id)
        enrollment = course.enroll_user(user, 'StudentEnrollment', enrollment={'course_section_id': section_id, 'notify': False})
        return "Success"
    except Conflict: return "Already Enrolled"
    except CanvasException as e:
        print(f"ERROR: Failed to enroll user {user_id} in section {section_id}. Details: {e}")
        return "Failed"

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    user = find_canvas_user(student_details)
    if not user:
        print(f"INFO: Canvas user not found for {student_details['email']}. Creating new user.")
        user = create_canvas_user(student_details)

    if user:
        if student_details.get('ssid') and hasattr(user, 'sis_user_id') and user.sis_user_id != student_details['ssid']:
            update_user_ssid(user, student_details['ssid'])
        return enroll_student_in_section(course_id, user.id, section_id)

    return "Failed: User not found/created"

# ==============================================================================
# CORE LOGIC FUNCTIONS (Adapted from app.py)
# ==============================================================================

def get_student_details_from_plp(plp_item_id):
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
        email = column_map.get(MASTER_STUDENT_EMAIL_COLUMN, '')
        canvas_id = column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN, '')

        if not all([student_name, email]):
            return None

        return {'name': student_name, 'ssid': ssid, 'email': email, 'canvas_id': canvas_id, 'master_id': item_details['id']}
    except (TypeError, KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not parse student details from Monday.com response for PLP {plp_item_id}: {e}")
        return None

def manage_class_enrollment(plp_item_id, class_item_id, student_details):
    linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    all_courses_item_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
    
    # If the class is not linked to the Canvas Board, it's a non-Canvas class.
    if not linked_canvas_item_ids:
        print(f"INFO: '{all_courses_item_name}' is not a Canvas course. Logging only.")
        create_subitem(plp_item_id, f"Added non-Canvas course '{all_courses_item_name}'")
        return
        
    canvas_item_id = list(linked_canvas_item_ids)[0]
    class_name = get_item_name(canvas_item_id, int(CANVAS_BOARD_ID))
    if not class_name:
        print(f"ERROR: Linked item {canvas_item_id} on Canvas Board {CANVAS_BOARD_ID} has no name. Aborting.")
        return

    course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = course_id_val.get('text') if course_id_val else None

    if not canvas_course_id:
        print(f"INFO: No Canvas ID found on Monday item {canvas_item_id}. Creating course for '{class_name}'.")
        new_course = create_canvas_course(class_name, CANVAS_TERM_ID)
        if new_course:
            canvas_course_id = new_course.id
            change_column_value_generic(int(CANVAS_BOARD_ID), canvas_item_id, CANVAS_COURSE_ID_COLUMN_ID, str(canvas_course_id))
            if ALL_CLASSES_CANVAS_ID_COLUMN:
                change_column_value_generic(int(ALL_COURSES_BOARD_ID), class_item_id, ALL_CLASSES_CANVAS_ID_COLUMN, str(canvas_course_id))
        else:
            create_subitem(plp_item_id, f"Enrollment in {class_name}: Failed - Could not create Canvas course.")
            return

    # Determine sections for enrollment
    m_series_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_M_SERIES_LABELS_COLUMN)
    ag_grad_val = get_column_value(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_CLASSES_AG_GRAD_COLUMN)
    m_series_text = m_series_val.get('text', "") if m_series_val else ""
    ag_grad_text = ag_grad_val.get('text', "") if ag_grad_val else ""
    
    sections = {"A-G" for s in ["AG"] if s in ag_grad_text} | {"Grad" for s in ["Grad"] if s in ag_grad_text} | {"M-Series" for s in ["M-series"] if s in m_series_text}
    if not sections: sections.add("All")
    
    enrollment_results = []
    for section_name in sections:
        section = create_section_if_not_exists(canvas_course_id, section_name)
        if section:
            result = enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)
            enrollment_results.append({'section': section_name, 'status': result})

    if enrollment_results:
        section_names = ", ".join([res['section'] for res in enrollment_results])
        all_statuses = {res['status'] for res in enrollment_results}
        final_status = "Failed" if "Failed" in all_statuses else "Success"
        create_subitem(plp_item_id, f"Enrolled in {class_name} (Sections: {section_names}): {final_status}")

# ==============================================================================
# SCRIPT-SPECIFIC HELPER FUNCTIONS
# ==============================================================================

def get_all_board_items(board_id):
    """Fetches all item IDs from a board, handling pagination."""
    all_item_ids = []
    cursor = None
    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        query = f"""
            query {{
                boards(ids: {board_id}) {{
                    items_page (limit: 100, {cursor_str}) {{
                        cursor
                        items {{ id }}
                    }}
                }}
            }}
        """
        result = execute_monday_graphql(query)
        if not result or 'data' not in result: break
        try:
            page_info = result['data']['boards'][0]['items_page']
            all_item_ids.extend([item['id'] for item in page_info['items']])
            cursor = page_info.get('cursor')
            if not cursor: break
            print(f"Fetched {len(all_item_ids)} items so far...")
        except (KeyError, IndexError):
            print("ERROR: Could not parse items from board response.")
            break
    return all_item_ids

def get_user_id(user_name):
    """Finds a user's ID by their full name."""
    query = f'query {{ users(kind: all) {{ id name }} }}'
    result = execute_monday_graphql(query)
    try:
        for user in result['data']['users']:
            if user['name'].lower() == user_name.lower():
                print(f"INFO: Found user ID for '{user_name}': {user['id']}")
                return user['id']
    except (KeyError, IndexError, TypeError):
        pass
    print(f"ERROR: Could not find user ID for '{user_name}'.")
    return None

def clear_subitems_by_creator(parent_item_id, creator_id_to_delete, dry_run=True):
    """Fetches all subitems and deletes those created by a specific user."""
    if not creator_id_to_delete:
        print("ERROR: No creator ID provided. Skipping deletion.")
        return
    
    query = f"""
        query {{
            items (ids: [{parent_item_id}]) {{
                subitems {{ id creator {{ id }} }}
            }}
        }}
    """
    result = execute_monday_graphql(query)
    subitems_to_delete = []
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            if subitem.get('creator') and str(subitem['creator'].get('id')) == str(creator_id_to_delete):
                subitems_to_delete.append(subitem['id'])
    except (KeyError, IndexError, TypeError):
        print(f"INFO: No subitems found or creator info missing for {parent_item_id}.")
        return

    if not subitems_to_delete:
        return

    print(f"INFO: Found {len(subitems_to_delete)} subitem(s) by creator {creator_id_to_delete} to delete for PLP item {parent_item_id}.")
    
    if dry_run:
        print("DRY RUN: Would delete the subitems listed above.")
        return

    for subitem_id in subitems_to_delete:
        print(f"DELETING subitem {subitem_id}...")
        delete_item(subitem_id)
        time.sleep(0.5)

def sync_single_plp_item(plp_item_id, dry_run=True):
    """Main logic to sync teachers and classes for one student."""
    print(f"\n--- Processing PLP Item: {plp_item_id} ---")
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details:
        print(f"WARNING: Could not get student details for PLP {plp_item_id}. Skipping.")
        return
    
    master_student_id = student_details['master_id']

    # --- Sync Teacher Assignments ---
    print("Syncing teacher assignments...")
    for trigger_col, mapping in MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.items():
        master_person_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), trigger_col)
        plp_target_mapping = next((t for t in mapping["targets"] if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
        
        if plp_target_mapping and master_person_val and master_person_val.get('value'):
            person_ids = get_people_ids_from_value(master_person_val['value'])
            if not person_ids: continue
            
            person_id = list(person_ids)[0]
            person_name = get_user_name(person_id)
            map_name = mapping.get("name", "Staff")

            print(f"INFO: Syncing '{map_name}' to '{person_name}'.")
            if not dry_run:
                update_people_column(plp_item_id, int(PLP_BOARD_ID), plp_target_mapping["target_column_id"], master_person_val['value'], plp_target_mapping["target_column_type"])
                create_subitem(plp_item_id, f"{map_name} set to {person_name}")
                time.sleep(1)

    # --- Sync Class Enrollments ---
    print("Syncing class enrollments...")
    course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
    all_class_ids = set()
    for col_id in course_column_ids:
        class_links = get_column_value(plp_item_id, int(PLP_BOARD_ID), col_id)
        if class_links and class_links.get('value'):
            all_class_ids.update(get_linked_ids_from_connect_column_value(class_links['value']))

    if not all_class_ids:
        print("INFO: No classes to sync.")
    else:
        for class_item_id in all_class_ids:
            print(f"INFO: Processing enrollment for class item {class_item_id}.")
            if not dry_run:
                manage_class_enrollment(plp_item_id, class_item_id, student_details)
                time.sleep(1)

# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================

if __name__ == '__main__':
    # ---! CONFIGURATION !---
    DRY_RUN = True
    TARGET_USER_NAME = "Sarah Bruce"
    # ---!  END CONFIG   !---

    print("======================================================")
    print("=== STARTING MONDAY.COM & CANVAS FULL SYNC SCRIPT ===")
    print("======================================================")
    if DRY_RUN:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!               DRY RUN MODE IS ON               !!!")
        print("!!!  No actual changes will be made to your data.  !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

    # --- Phase 1: Cleanup Subitems ---
    print(f"\n--- PHASE 1: DELETING SUBITEMS CREATED BY '{TARGET_USER_NAME}' ---")
    creator_id = get_user_id(TARGET_USER_NAME)
    if creator_id:
        plp_item_ids = get_all_board_items(PLP_BOARD_ID)
        print(f"Found {len(plp_item_ids)} total PLP items to check for subitem cleanup.")
        for i, item_id in enumerate(plp_item_ids):
            print(f"Checking PLP item {i+1}/{len(plp_item_ids)} (ID: {item_id})...")
            try:
                clear_subitems_by_creator(int(item_id), creator_id, dry_run=DRY_RUN)
            except Exception as e:
                print(f"FATAL ERROR during cleanup for item {item_id}: {e}")
            time.sleep(1)
    else:
        print("\nFATAL: Halting script because target user could not be found.")
        exit()

    # --- Phase 2: Sync Teachers and Classes ---
    print(f"\n--- PHASE 2: SYNCING TEACHERS AND CLASSES ---")
    # Re-use the list of items fetched in Phase 1
    if 'plp_item_ids' in locals() and plp_item_ids:
        for i, item_id in enumerate(plp_item_ids):
            try:
                sync_single_plp_item(int(item_id), dry_run=DRY_RUN)
            except Exception as e:
                print(f"FATAL ERROR during sync for item {item_id}: {e}")
            time.sleep(2) # Main delay between processing each student
    else:
        print("No items found on PLP board to process for sync.")

    print("\n======================================================")
    print("=== SCRIPT FINISHED                                ===")
    print("======================================================")
