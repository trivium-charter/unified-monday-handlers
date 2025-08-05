# ==============================================================================
# FINAL CONSOLIDATED APPLICATION (All Original Logic Restored and Bugs Fixed)
# ==============================================================================
import os
import json
import requests
from datetime import datetime
from flask import Flask, request, jsonify
from celery import Celery
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist

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

IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID")
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
SPED_TO_IEPAP_CONNECT_COLUMN_ID = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN_ID")

CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSES_TEACHER_COLUMN_ID = os.environ.get("CANVAS_COURSES_TEACHER_COLUMN_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")
CANVAS_BOARD_TITLE_COLUMN_ID = os.environ.get("CANVAS_BOARD_TITLE_COLUMN_ID")

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

# ==============================================================================
# MONDAY.COM UTILITIES
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def get_user_email(user_id):
    if user_id is None: return None
    query = f"query {{ users(ids: [{user_id}]) {{ email }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('users'):
        return result['data']['users'][0].get('email')
    return None
    
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

def find_item_by_person(board_id, person_column_id, person_id):
    """Finds the first item on a board assigned to a specific person."""
    # This query format is specifically for searching a Person column by user ID.
    # It requires the person_id to be inside a JSON-formatted string.
    query = f"""
        query {{
            items_page_by_column_values (
                board_id: {board_id},
                columns: [{{
                    column_id: "{person_column_id}",
                    column_values: ["{{\\"ids\\":[ {person_id} ]}}"]
                }}]
            ) {{
                items {{
                    id
                }}
            }}
        }}
    """
    result = execute_monday_graphql(query)
    # Check for a valid response and if any items were returned
    if result and result.get('data', {}).get('items_page_by_column_values', {}).get('items'):
        items = result['data']['items_page_by_column_values']['items']
        if items:
            return items[0]['id']
    return None

def update_item_name(item_id, board_id, new_name):
    graphql_value = json.dumps(json.dumps({"name": new_name}))
    mutation = f"mutation {{ change_multiple_column_values(board_id: {board_id}, item_id: {item_id}, column_values: {graphql_value}) {{ id }} }}"
    return execute_monday_graphql(mutation) is not None

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
    
    # --- START FIX ---
    # The API can return the value as a string or a dict.
    # This ensures it's always treated as a dict.
    if isinstance(value_data, str):
        try:
            value_data = json.loads(value_data)
        except json.JSONDecodeError:
            return set() # Return empty if the string is not valid JSON
    # --- END FIX ---
    
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

def get_all_staff_data(board_id, person_col_id, email_col_id, sis_id_col_id):
    """Fetches all items from the staff board and returns a list of staff details."""
    staff_data = []
    query = f"""
        query {{
            boards(ids: [{board_id}]) {{
                items_page {{
                    items {{
                        id
                        column_values(ids: ["{person_col_id}", "{email_col_id}", "{sis_id_col_id}"]) {{
                            id
                            value
                            text
                        }}
                    }}
                }}
            }}
        }}
    """
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('boards'):
        items = result['data']['boards'][0]['items_page']['items']
        for item in items:
            details = {'item_id': item['id']}
            for cv in item['column_values']:
                if cv['id'] == person_col_id:
                    person_ids = get_people_ids_from_value(cv.get('value'))
                    details['person_ids'] = person_ids
                elif cv['id'] == email_col_id:
                    details['email'] = cv.get('text')
                elif cv['id'] == sis_id_col_id:
                    details['sis_id'] = cv.get('text')
            staff_data.append(details)
    return staff_data

def create_monday_update(item_id, update_text):
    """Posts an update (a comment) to a Monday.com item."""
    # The text body must be a JSON-encoded string for the GraphQL mutation
    formatted_text = json.dumps(update_text)
    
    mutation = f"mutation {{ create_update (item_id: {item_id}, body: {formatted_text}) {{ id }} }}"
    return execute_monday_graphql(mutation)

# ==============================================================================
# CANVAS UTILITIES
# ==============================================================================
def initialize_canvas_api():
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY) if CANVAS_API_URL and CANVAS_API_KEY else None

def create_canvas_user(student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        account = canvas_api.get_account(1)
        user_payload = {'user': {'name': student_details['name'], 'terms_of_use': True}, 'pseudonym': {'unique_id': student_details['email'], 'sis_user_id': student_details['ssid'], 'login_id': student_details['email'], 'authentication_provider_id': '112'}, 'communication_channel': {'type': 'email', 'address': student_details['email'], 'skip_confirmation': True}}
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
    """
    Creates a Canvas course with robust, corrected retry logic for SIS ID conflicts.
    """
    canvas_api = initialize_canvas_api()
    if not all([canvas_api, CANVAS_SUBACCOUNT_ID, CANVAS_TEMPLATE_COURSE_ID]):
        print("ERROR: Missing Canvas Sub-Account or Template Course ID config.")
        return None
    try:
        account = canvas_api.get_account(CANVAS_SUBACCOUNT_ID)
    except ResourceDoesNotExist:
        print(f"ERROR: Canvas Sub-Account with ID '{CANVAS_SUBACCOUNT_ID}' not found.")
        return None

    base_sis_name = ''.join(e for e in course_name if e.isalnum()).replace(' ', '_').lower()
    base_sis_id = f"{base_sis_name}_{term_id}"

    max_attempts = 10
    for attempt in range(max_attempts):
        sis_id_to_try = base_sis_id if attempt == 0 else f"{base_sis_id}_{attempt}"
        course_data = {
            'name': course_name, 'course_code': course_name,
            'enrollment_term_id': f"sis_term_id:{term_id}", 'sis_course_id': sis_id_to_try,
            'source_course_id': CANVAS_TEMPLATE_COURSE_ID
        }
        try:
            print(f"INFO: [Attempt {attempt + 1}] Trying to create course '{course_name}' with SIS ID '{sis_id_to_try}'.")
            new_course = account.create_course(course=course_data)
            print(f"SUCCESS: Course '{course_name}' created with SIS ID '{sis_id_to_try}'.")
            return new_course
        except CanvasException as e:
            # THIS IS THE CRITICAL FIX: Check for the 400 status code and the specific message.
            if hasattr(e, 'status_code') and e.status_code == 400 and 'is already in use' in str(e).lower():
                print(f"WARNING: SIS ID '{sis_id_to_try}' is in use. Retrying with a new ID...")
                continue
            else:
                print(f"ERROR: A critical Canvas API error occurred for course '{course_name}': {e}")
                return None

    print(f"ERROR: Failed to create course '{course_name}' after {max_attempts} attempts. Aborting.")
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

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed: Canvas API not initialized"
    user = None
    try: user = canvas_api.get_user(student_details['email'], 'login_id')
    except ResourceDoesNotExist:
        if student_details.get('ssid'):
            try: user = canvas_api.get_user(student_details['ssid'], 'sis_user_id')
            except ResourceDoesNotExist: pass
    if not user: user = create_canvas_user(student_details)
    if user:
        if student_details.get('ssid') and hasattr(user, 'sis_user_id') and user.sis_user_id != student_details['ssid']:
            update_user_ssid(user, student_details['ssid'])
        return enroll_student_in_section(course_id, user.id, section_id)
    return "Failed: User not found/created"

def unenroll_student_from_course(course_id, student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return False
    user = None
    try: user = canvas_api.get_user(student_details.get('email'), 'login_id')
    except ResourceDoesNotExist:
        if student_details.get('ssid'):
            try: user = canvas_api.get_user(student_details['ssid'], 'sis_user_id')
            except ResourceDoesNotExist: pass
    if not user: return True
    try:
        course = canvas_api.get_course(course_id)
        for enrollment in course.get_enrollments(user_id=user.id):
            enrollment.deactivate(task='conclude')
        return True
    except CanvasException as e:
        print(f"ERROR: Canvas unenrollment failed: {e}")
        return False
def enroll_teacher_in_course(course_id, teacher_email, teacher_sis_id):
    """Enrolls a user as a Teacher, with a fallback to sis_user_id and detailed logging."""
    canvas_api = initialize_canvas_api()
    if not canvas_api:
        print("--- ENROLLMENT FAILED: Canvas API not initialized.")
        return "Failed: Canvas API not initialized"

    user_to_enroll = None
    print(f"\n--- Starting Enrollment Process for {teacher_email} in Course {course_id} ---")
    
    try:
        # First, try the direct lookup by login_id.
        print(f"--> Attempting to find user by login_id: {teacher_email}")
        user_to_enroll = canvas_api.get_user(teacher_email, 'login_id')
        print(f"    SUCCESS: Found user '{user_to_enroll.name}' (ID: {user_to_enroll.id}) by login_id.")
    except ResourceDoesNotExist:
        print(f"    INFO: User not found by login_id.")
        # If that fails, fall back to the reliable sis_user_id lookup.
        if teacher_sis_id:
            try:
                print(f"--> Attempting to find user by sis_user_id: {teacher_sis_id}")
                user_to_enroll = canvas_api.get_user(teacher_sis_id, 'sis_user_id')
                print(f"    SUCCESS: Found user '{user_to_enroll.name}' (ID: {user_to_enroll.id}) by sis_user_id.")
            except ResourceDoesNotExist:
                print(f"    INFO: User not found by sis_user_id.")
                pass  # User not found by SIS ID either
    
    if not user_to_enroll:
        print("--- ENROLLMENT FAILED: Could not find user in Canvas. ---")
        return f"Failed: User '{teacher_email}' not found by login_id or SIS ID '{teacher_sis_id}'."

    # --- Proceed with enrollment ---
    try:
        print(f"--> Attempting to get Course ID: {course_id}")
        course = canvas_api.get_course(course_id)
        print(f"    SUCCESS: Found course '{course.name}'.")
        
        print(f"--> Sending enrollment request for user {user_to_enroll.id} into course {course.id} with role 'TeacherEnrollment'.")
        enrollment = course.enroll_user(user_to_enroll, 'TeacherEnrollment', enrollment_state='active', notify=False)
        print(f"    SUCCESS: Enrollment API call successful.")
        print("--- ENROLLMENT COMPLETE ---")
        return "Success"
    except ResourceDoesNotExist:
        print(f"--- ENROLLMENT FAILED: Course with ID '{course_id}' not found in Canvas. ---")
        return f"Failed: Course with ID '{course_id}' not found in Canvas."
    except Conflict:
        print("--- ENROLLMENT FAILED: User is already enrolled. ---")
        return "Already Enrolled"
    except CanvasException as e:
        print(f"--- ENROLLMENT FAILED: A Canvas API error occurred: {e} ---")
        return f"Failed: {e}"
        
# ==============================================================================
# CELERY APP DEFINITION
# ==============================================================================
broker_use_ssl_config = {'ssl_cert_reqs': 'required'} if CELERY_BROKER_URL.startswith('rediss://') else {}
celery_app = Celery('tasks', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND, include=[__name__])
if broker_use_ssl_config:
    celery_app.conf.broker_use_ssl = broker_use_ssl_config
    celery_app.conf.redis_backend_use_ssl = broker_use_ssl_config
celery_app.conf.timezone = 'America/Los_Angeles'

# --- THE FIX ---
# Keep the connection alive by sending a heartbeat every 60 seconds (well below the 5-minute timeout).
celery_app.conf.broker_transport_options = {'health_check_interval': 60}
# As recommended by the logs, enable connection retries on startup for more resilience.
celery_app.conf.broker_connection_retry_on_startup = True
# ==============================================================================
# CELERY TASKS
# ==============================================================================
@celery_app.task
def sync_monday_titles_to_canvas():
    """
    Goes through ALL items on the Monday.com Canvas board, handling multiple pages,
    and pushes the title to the Canvas API, overwriting the course name in Canvas.
    """
    print("\n--- STARTING MONDAY.COM -> CANVAS TITLE SYNC (PAGINATED) ---")
    
    all_items = []
    cursor = None # Start with no cursor to get the first page

    while True:
        # This query is designed to fetch one page of items at a time
        query = f"""
            query ($cursor: String) {{
                boards(ids: [{CANVAS_BOARD_ID}]) {{
                    items_page (cursor: $cursor, limit: 100) {{
                        cursor
                        items {{
                            id
                            column_values(ids: ["{CANVAS_COURSE_ID_COLUMN_ID}", "{CANVAS_BOARD_TITLE_COLUMN_ID}"]) {{
                                id
                                text
                            }}
                        }}
                    }}
                }}
            }}
        """
        
        # For GraphQL queries with variables, we send them in a separate dict
        variables = {'cursor': cursor}
        
        # We need to post the query and variables together
        response = requests.post(
            MONDAY_API_URL,
            json={"query": query, "variables": variables},
            headers=MONDAY_HEADERS
        )
        result = response.json()

        if not (result and result.get('data', {}).get('boards')):
            print("ERROR: Could not fetch items from the Canvas Courses board.")
            return

        items_page = result['data']['boards'][0]['items_page']
        all_items.extend(items_page.get('items', []))
        cursor = items_page.get('cursor')

        # If the cursor is null, we have reached the last page
        if not cursor:
            break
            
    canvas_api = initialize_canvas_api()
    if not canvas_api:
        print("ERROR: Canvas API not initialized.")
        return

    print(f"Found {len(all_items)} total items to process...")

    for item in all_items:
        item_id = item['id']
        canvas_course_id = None
        new_course_title = None

        for cv in item['column_values']:
            if cv['id'] == CANVAS_COURSE_ID_COLUMN_ID:
                canvas_course_id = cv.get('text')
            elif cv['id'] == CANVAS_BOARD_TITLE_COLUMN_ID:
                new_course_title = cv.get('text')
        
        if not (canvas_course_id and new_course_title):
            print(f"  - SKIPPING Item {item_id}: Missing Canvas Course ID or Title.")
            continue
        
        try:
            course = canvas_api.get_course(canvas_course_id)
            print(f"  - UPDATING Course {canvas_course_id}: Setting name to '{new_course_title}'")
            course.update(course={'name': new_course_title})

        except Exception as e:
            print(f"  - FAILED for Item {item_id} (Canvas ID: {canvas_course_id}): {e}")

    print("--- SYNC COMPLETE ---")
@celery_app.task
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

def get_student_details_from_plp(plp_item_id):
    master_student_ids = get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_student_ids: return None
    master_student_id = list(master_student_ids)[0]
    student_name = get_item_name(master_student_id, int(MASTER_STUDENT_BOARD_ID))
    ssid = (get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_SSID_COLUMN) or {}).get('text', '')
    email = (get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_EMAIL_COLUMN) or {}).get('text', '')
    if not all([student_name, email]): return None
    return {'name': student_name, 'ssid': ssid, 'email': email}

def manage_class_enrollment(action, plp_item_id, class_item_id, student_details, subitem_cols=None):
    subitem_cols = subitem_cols or {}

    linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    if not linked_canvas_item_ids:
        all_courses_item_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
        print(f"INFO: Course '{all_courses_item_name}' is not linked to the Canvas Board. Skipping Canvas sync.")
        return
        
    canvas_item_id = list(linked_canvas_item_ids)[0]

    class_name = get_item_name(canvas_item_id, int(CANVAS_BOARD_ID))
        
    if not class_name:
        print(f"ERROR: Linked item {canvas_item_id} on Canvas Board {CANVAS_BOARD_ID} has no name. Cannot create course. Aborting.")
        return

    canvas_course_id = None
    course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    if course_id_val and course_id_val.get('text'):
        canvas_course_id = course_id_val['text']

    if not canvas_course_id and action == "enroll":
        print(f"INFO: No Canvas ID found on linked Monday item {canvas_item_id}. Attempting to create course for '{class_name}'.")
        new_course = create_canvas_course(class_name, CANVAS_TERM_ID)
        if new_course:
            canvas_course_id = new_course.id
            print(f"INFO: New course created with ID: {canvas_course_id}. Updating Monday.com item {canvas_item_id} on board {CANVAS_BOARD_ID}.")
            change_column_value_generic(int(CANVAS_BOARD_ID), canvas_item_id, CANVAS_COURSE_ID_COLUMN_ID, str(canvas_course_id))
            if ALL_CLASSES_CANVAS_ID_COLUMN:
                change_column_value_generic(int(ALL_COURSES_BOARD_ID), class_item_id, ALL_CLASSES_CANVAS_ID_COLUMN, str(canvas_course_id))
        else:
            print(f"ERROR: Failed to create Canvas course for '{class_name}'. Aborting enrollment.")
            return

    if not canvas_course_id:
        print(f"INFO: No Canvas Course ID available for '{class_name}' to perform action '{action}'. Skipping.")
        return

    if action == "enroll":
        m_series_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_M_SERIES_LABELS_COLUMN)
        ag_grad_val = get_column_value(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_CLASSES_AG_GRAD_COLUMN)
        m_series_text = (m_series_val.get('text') or "") if m_series_val else ""
        ag_grad_text = (ag_grad_val.get('text') or "") if ag_grad_val else ""
        
        sections = {"A-G" for s in ["AG"] if s in ag_grad_text} | {"Grad" for s in ["Grad"] if s in ag_grad_text} | {"M-Series" for s in ["M-series"] if s in m_series_text}
        if not sections: sections.add("All")
        
        for section_name in sections:
            section = create_section_if_not_exists(canvas_course_id, section_name)
            if section:
                result = enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)
                create_subitem(plp_item_id, f"Enrolled in {class_name} ({section_name}): {result}", subitem_cols)

    elif action == "unenroll":
        result = unenroll_student_from_course(canvas_course_id, student_details)
        create_subitem(plp_item_id, f"Unenrolled from {class_name}: {'Success' if result else 'Failed'}", subitem_cols)

@celery_app.task
def process_canvas_full_sync_from_status(event_data):
    if event_data.get('value', {}).get('label', {}).get('text', '') != PLP_CANVAS_SYNC_STATUS_VALUE: return
    plp_item_id = event_data.get('pulseId')
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return
    subitem_cols = {}
    first_rule = next((rule for rule in LOG_CONFIGS if str(rule.get("trigger_board_id")) == PLP_BOARD_ID and rule.get("log_type") == "ConnectBoardChange"), None)
    if first_rule and "params" in first_rule:
        params = first_rule["params"]
        if params.get("entry_type_column_id") and params.get("subitem_entry_type"):
            subitem_cols[params["entry_type_column_id"]] = {"labels": [str(params["subitem_entry_type"])]}
    course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
    all_class_ids = set()
    for col_id in course_column_ids:
        class_links = get_column_value(plp_item_id, int(PLP_BOARD_ID), col_id)
        if class_links and class_links.get('value'):
            all_class_ids.update(get_linked_ids_from_connect_column_value(class_links['value']))
    for class_item_id in all_class_ids:
        manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, subitem_cols)

@celery_app.task
def process_canvas_delta_sync_from_course_change(event_data):
    plp_item_id, user_id, trigger_column_id = event_data.get('pulseId'), event_data.get('userId'), event_data.get('columnId')
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return
    subitem_cols = {}
    rule = next((r for r in LOG_CONFIGS if r.get("trigger_column_id") == trigger_column_id), None)
    if rule and "params" in rule:
        params = rule["params"]
        if params.get("entry_type_column_id") and params.get("subitem_entry_type"):
            subitem_cols[params["entry_type_column_id"]] = {"labels": [str(params["subitem_entry_type"])]}
    current_ids, previous_ids = get_linked_ids_from_connect_column_value(event_data.get('value')), get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    added_ids, removed_ids = current_ids - previous_ids, previous_ids - current_ids
    category_name, date, changer = {v: k for k, v in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items()}.get(trigger_column_id, "Course"), datetime.now().strftime('%Y-%m-%d'), get_user_name(user_id) or "automation"
    
    for class_id in added_ids:
        is_canvas_course = get_linked_items_from_board_relation(class_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
        if is_canvas_course:
            manage_class_enrollment("enroll", plp_item_id, class_id, student_details, subitem_cols)
        else:
            class_name = get_item_name(class_id, int(ALL_COURSES_BOARD_ID))
            if class_name: create_subitem(plp_item_id, f"Added {category_name} course '{class_name}' on {date} by {changer}", subitem_cols)
            
    for class_id in removed_ids:
        is_canvas_course = get_linked_items_from_board_relation(class_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
        if is_canvas_course:
            manage_class_enrollment("unenroll", plp_item_id, class_id, student_details, subitem_cols)
        else:
            class_name = get_item_name(class_id, int(ALL_COURSES_BOARD_ID))
            if class_name: create_subitem(plp_item_id, f"Removed {category_name} course '{class_name}' on {date} by {changer}", subitem_cols)

@celery_app.task
def process_plp_course_sync_webhook(event_data):
    subitem_id, parent_item_id = event_data.get('pulseId'), event_data.get('parentItemId')
    current, previous = (get_linked_ids_from_connect_column_value(event_data.get(k)) for k in ['value', 'previousValue'])
    if not (current - previous) and not (previous - current): return
    dropdown_label = (get_column_value(subitem_id, int(event_data.get('boardId')), HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID) or {}).get('text')
    target_plp_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(dropdown_label)
    if not target_plp_col_id: return
    plp_linked_ids = get_linked_items_from_board_relation(parent_item_id, int(HS_ROSTER_BOARD_ID), HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
    if not plp_linked_ids: return
    plp_item_id = list(plp_linked_ids)[0]
    original_val = (get_column_value(plp_item_id, int(PLP_BOARD_ID), target_plp_col_id) or {}).get('value')
    for course_id in (current - previous): update_connect_board_column(plp_item_id, int(PLP_BOARD_ID), target_plp_col_id, course_id, "add")
    for course_id in (previous - current): update_connect_board_column(plp_item_id, int(PLP_BOARD_ID), target_plp_col_id, course_id, "remove")
    updated_val = (get_column_value(plp_item_id, int(PLP_BOARD_ID), target_plp_col_id) or {}).get('value')
    downstream_event = {'pulseId': plp_item_id, 'columnId': target_plp_col_id, 'value': updated_val, 'previousValue': original_val, 'userId': event_data.get('userId')}
    process_canvas_delta_sync_from_course_change.delay(downstream_event)

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    master_item_id, trigger_column_id, user_id = event_data.get('pulseId'), event_data.get('columnId'), event_data.get('userId')
    current_value_raw, previous_value_raw = event_data.get('value'), event_data.get('previousValue')
    mappings = MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.get(trigger_column_id)
    if not mappings: return
    for target in mappings["targets"]:
        linked_ids = get_linked_items_from_board_relation(master_item_id, int(MASTER_STUDENT_BOARD_ID), target["connect_column_id"])
        for linked_id in linked_ids:
            update_people_column(linked_id, int(target["board_id"]), target["target_column_id"], current_value_raw, target["target_column_type"])
    plp_target = next((t for t in mappings["targets"] if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
    if not plp_target: return
    plp_linked_ids = get_linked_items_from_board_relation(master_item_id, int(MASTER_STUDENT_BOARD_ID), plp_target["connect_column_id"])
    if not plp_linked_ids: return
    plp_item_id = list(plp_linked_ids)[0]
    col_name, changer, date = mappings.get("name", "Staff"), get_user_name(user_id) or "automation", datetime.now().strftime('%Y-%m-%d')
    current_ids, previous_ids = get_people_ids_from_value(current_value_raw), get_people_ids_from_value(previous_value_raw)
    for p_id in (current_ids - previous_ids):
        name = get_user_name(p_id)
        if name: create_subitem(plp_item_id, f"{col_name} changed to {name} on {date} by {changer}")
    for p_id in (previous_ids - current_ids):
        name = get_user_name(p_id)
        if name: create_subitem(plp_item_id, f"Removed {name} from {col_name} on {date} by {changer}")

@celery_app.task
def process_teacher_enrollment_webhook(event_data):
    """Processes a webhook to enroll a teacher in a Canvas course using a full staff list."""
    item_id = event_data.get('pulseId')
    board_id = event_data.get('boardId')

    canvas_course_id_val = get_column_value(item_id, board_id, CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = canvas_course_id_val.get('text') if canvas_course_id_val else None
    if not canvas_course_id:
        create_monday_update(item_id, "Enrollment Failed: Canvas Course ID is missing.")
        return

    current_ids = get_people_ids_from_value(event_data.get('value'))
    previous_ids = get_people_ids_from_value(event_data.get('previousValue'))
    added_teacher_ids = current_ids - previous_ids
    if not added_teacher_ids:
        return

    # Fetch all data from the staff board at once
    all_staff = get_all_staff_data(
        ALL_STAFF_BOARD_ID,
        ALL_STAFF_PERSON_COLUMN_ID,
        ALL_STAFF_EMAIL_COLUMN_ID,
        ALL_STAFF_SIS_ID_COLUMN_ID
    )

    if not all_staff:
        create_monday_update(item_id, "Enrollment Failed: Could not fetch data from the All Staff board.")
        return

    for teacher_id in added_teacher_ids:
        teacher_name = get_user_name(teacher_id) or f"User ID {teacher_id}"
        
        # Search for the teacher in the fetched data
        staff_info = next((s for s in all_staff if teacher_id in s.get('person_ids', [])), None)

        if not staff_info:
            create_monday_update(item_id, f"Enrollment for '{teacher_name}' Failed: Could not find user in the All Staff board.")
            continue

        teacher_email = staff_info.get('email')
        teacher_sis_id = staff_info.get('sis_id')

        if not teacher_email or not teacher_sis_id:
            create_monday_update(item_id, f"Enrollment for '{teacher_name}' Failed: Email or SIS ID is missing from the All Staff board.")
            continue

        result = enroll_teacher_in_course(canvas_course_id, teacher_email, teacher_sis_id)
        create_monday_update(item_id, f"Enrollment attempt for '{teacher_name}': {result}")
        
@celery_app.task
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
    if board_id == CANVAS_BOARD_ID and col_id == CANVAS_COURSES_TEACHER_COLUMN_ID:
        sync_monday_titles_to_canvas.delay() # Run our new sync function
        return jsonify({"message": "Monday -> Canvas title sync queued."}), 202
    
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
