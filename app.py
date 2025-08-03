# ==============================================================================
# CONSOLIDATED APPLICATION
# Original Files:
# - app_unified_webhook_handler.py
# - monday_tasks.py
# - celery_app.py
# - monday_utils.py
# - canvas_utils.py
# ==============================================================================

# ==============================================================================
# REQUIREMENTS (from requirements.txt)
# ==============================================================================
# Flask==2.3.2
# requests==2.31.0
# gunicorn==21.2.0
# celery[redis]==5.3.6
# gevent==23.9.1
# pytz==2024.1
# canvasapi==3.3.0
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
# --- General & API Keys ---
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"
CELERY_BROKER_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

# --- Monday.com Board and Column IDs ---
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
ALL_COURSES_NAME_COLUMN_ID = os.environ.get("ALL_COURSES_NAME_COLUMN_ID")
ALL_CLASSES_CANVAS_CONNECT_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_CONNECT_COLUMN")
ALL_CLASSES_CANVAS_ID_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_ID_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")

HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID")

IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID")
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
SPED_TO_IEPAP_CONNECT_COLUMN_ID = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN_ID")

CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSE_ID_COLUMN = os.environ.get("CANVAS_COURSE_ID_COLUMN")

# --- Canvas Specific Configuration ---
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

# --- JSON-based Configurations ---
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
# MONDAY.COM UTILITIES (from monday_utils.py)
# ==============================================================================
MONDAY_HEADERS = {
    "Authorization": MONDAY_API_KEY,
    "Content-Type": "application/json",
    "API-Version": "2023-10",
}

def execute_monday_graphql(query):
    """Executes a GraphQL query/mutation against the Monday.com API."""
    data = {"query": query}
    try:
        response = requests.post(MONDAY_API_URL, json=data, headers=MONDAY_HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error communicating with Monday.com API: {e}")
        if e.response is not None:
            print(f"Monday API Response Content: {e.response.text}")
        return None

def get_item_name(item_id, board_id):
    """Fetches the name of a Monday.com item given its ID and board ID."""
    query = f"""
    query {{
      boards (ids: {board_id}) {{
        items_page (query_params: {{ids: [{item_id}]}}) {{
          items {{ name }}
        }}
      }}
    }}
    """
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            return board['items_page']['items'][0].get('name')
    return None

def get_user_name(user_id):
    """Fetches a user's name from Monday.com given their user ID."""
    if user_id is None or user_id == -4:
        return None
    query = f"query {{ users (ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('users'):
        return result['data']['users'][0].get('name')
    return None

def get_column_value(item_id, board_id, column_id):
    """Fetches the column value and text for a given column."""
    query = f"""
    query {{
      boards (ids: {board_id}) {{
        items_page (query_params: {{ids: [{item_id}]}}) {{
          items {{ column_values (ids: ["{column_id}"]) {{ id value text }} }}
        }}
      }}
    }}
    """
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            item = board['items_page']['items'][0]
            for col_val in item['column_values']:
                if col_val['id'] == column_id:
                    parsed_value = None
                    if col_val.get('value'):
                        try:
                            parsed_value = json.loads(col_val['value'])
                        except json.JSONDecodeError:
                            parsed_value = col_val['value']
                    return {'value': parsed_value, 'text': col_val.get('text')}
    return None

def update_connect_board_column(item_id, board_id, connect_column_id, item_to_link_id, action="add"):
    """Adds or removes a link to an item in a Connect Boards column."""
    current_column_data = get_column_value(item_id, board_id, connect_column_id)
    current_linked_items = set()
    if current_column_data and current_column_data.get('value') and "linkedPulseIds" in current_column_data['value']:
        current_linked_items = {int(p_id['linkedPulseId']) for p_id in current_column_data['value']['linkedPulseIds']}

    target_item_id_int = int(item_to_link_id)
    if action == "add":
        if target_item_id_int in current_linked_items: return True
        updated_linked_items = current_linked_items | {target_item_id_int}
    elif action == "remove":
        if target_item_id_int not in current_linked_items: return True
        updated_linked_items = current_linked_items - {target_item_id_int}
    else:
        return False

    connect_value = {"linkedPulseIds": [{"linkedPulseId": lid} for lid in sorted(list(updated_linked_items))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    mutation = f"""
    mutation {{
      change_column_value (board_id: {board_id}, item_id: {item_id}, column_id: "{connect_column_id}", value: {graphql_value}) {{ id }}
    }}"""
    result = execute_monday_graphql(mutation)
    return bool(result and 'data' in result and result['data'].get('change_column_value'))

def get_linked_ids_from_connect_column_value(value_data):
    """Parses a "Connect boards" column value and returns a set of linked item IDs."""
    if not value_data: return set()
    parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data) if isinstance(value_data, str) else {}
    linked_ids = set()
    if "linkedPulseIds" in parsed_value:
        linked_ids.update(int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"] if "linkedPulseId" in item)
    elif "linkedItems" in parsed_value:
        linked_ids.update(int(item["id"]) for item in parsed_value["linkedItems"] if "id" in item)
    return linked_ids

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    """Fetches linked item IDs from a Connect Boards column."""
    column_data = get_column_value(item_id, board_id, connect_column_id)
    return get_linked_ids_from_connect_column_value(column_data['value']) if column_data and column_data.get('value') else set()

def update_item_name(item_id, board_id, new_name):
    """Updates the name of a Monday.com item."""
    column_values = json.dumps({"name": new_name})
    graphql_value = json.dumps(column_values)
    mutation = f"""
    mutation {{
      change_multiple_column_values(board_id: {board_id}, item_id: {item_id}, column_values: {graphql_value}) {{ id name }}
    }}"""
    result = execute_monday_graphql(mutation)
    return bool(result and 'data' in result and result['data'].get('change_multiple_column_values'))

def change_column_value_generic(board_id, item_id, column_id, value):
    """Updates a generic text or number column."""
    graphql_value = json.dumps(json.dumps(str(value)))
    mutation = f"""
    mutation {{
      change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}) {{ id }}
    }}"""
    result = execute_monday_graphql(mutation)
    return bool(result and 'data' in result and result['data'].get('change_column_value'))

def create_subitem(parent_item_id, subitem_name, column_values=None):
    """Creates a new subitem under a specified parent item."""
    values_for_api = {col_id: val if isinstance(val, dict) else str(val) for col_id, val in (column_values or {}).items()}
    column_values_json = json.dumps(values_for_api)
    mutation = f"""
    mutation {{
      create_subitem (parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {json.dumps(column_values_json)}) {{ id }}
    }}"""
    result = execute_monday_graphql(mutation)
    if result and 'data' in result and result['data'].get('create_subitem'):
        return result['data']['create_subitem'].get('id')
    return None

def create_item(board_id, item_name, column_values=None):
    """Creates a new main item on a specified board."""
    column_values_str = json.dumps(column_values or {})
    mutation = f"""
    mutation {{
      create_item (board_id: {board_id}, item_name: {json.dumps(item_name)}, column_values: {json.dumps(column_values_str)}) {{ id }}
    }}"""
    result = execute_monday_graphql(mutation)
    if result and 'data' in result and result['data'].get('create_item'):
        return result['data']['create_item'].get('id')
    return None

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    """Updates a People column on a Monday.com item."""
    parsed_new_value = new_people_value if isinstance(new_people_value, dict) else json.loads(new_people_value) if isinstance(new_people_value, str) else {}
    graphql_value = None
    persons_and_teams = parsed_new_value.get('personsAndTeams', [])

    if target_column_type == "person":
        person_id = persons_and_teams[0].get('id') if persons_and_teams else None
        graphql_value = json.dumps(json.dumps({"personId": person_id} if person_id else {}))
    elif target_column_type == "multiple-person":
        people_list = [{"id": p.get('id'), "kind": p.get('kind', 'person')} for p in persons_and_teams if 'id' in p]
        graphql_value = json.dumps(json.dumps({"personsAndTeams": people_list}))
    else:
        return False
    
    mutation = f"""
    mutation {{
      change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{people_column_id}", value: {graphql_value}) {{ id }}
    }}"""
    result = execute_monday_graphql(mutation)
    return bool(result and 'data' in result and result['data'].get('change_column_value'))


# ==============================================================================
# CANVAS UTILITIES (from canvas_utils.py)
# ==============================================================================
def initialize_canvas_api():
    """Initializes and returns a Canvas API object if configured."""
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY) if CANVAS_API_URL and CANVAS_API_KEY else None

def create_canvas_user(student_details):
    """Creates a new user in Canvas."""
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
        print(f"ERROR: API error during user creation: {e}")
        return None

def create_canvas_course(course_name, term_id):
    """Creates a new course in a specific sub-account using a template."""
    canvas_api = initialize_canvas_api()
    if not all([canvas_api, CANVAS_SUBACCOUNT_ID, CANVAS_TEMPLATE_COURSE_ID]):
        return None
    try:
        account = canvas_api.get_account(CANVAS_SUBACCOUNT_ID)
        sis_id_name = ''.join(e for e in course_name if e.isalnum()).replace(' ', '_').lower()
        sis_id = f"{sis_id_name}_{term_id}"
        course_data = {
            'name': course_name, 'course_code': course_name,
            'enrollment_term_id': f"sis_term_id:{term_id}",
            'sis_course_id': sis_id, 'source_course_id': CANVAS_TEMPLATE_COURSE_ID
        }
        return account.create_course(course=course_data)
    except Conflict:
        courses = account.get_courses(sis_course_id=sis_id)
        return next((c for c in courses if c.sis_course_id == sis_id), None)
    except CanvasException as e:
        print(f"ERROR: An unexpected API error occurred during course creation: {e}")
        return None

def create_section_if_not_exists(course_id, section_name):
    """Finds a section by name or creates it if it doesn't exist."""
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        course = canvas_api.get_course(course_id)
        existing_section = next((s for s in course.get_sections() if s.name.lower() == section_name.lower()), None)
        return existing_section or course.create_course_section(course_section={'name': section_name})
    except CanvasException as e:
        print(f"ERROR: API error finding/creating section '{section_name}': {e}")
        return None

def enroll_student_in_section(course_id, user_id, section_id):
    """Enrolls a student, making them active immediately."""
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        course = canvas_api.get_course(course_id)
        user = canvas_api.get_user(user_id)
        return course.enroll_user(user, 'StudentEnrollment', enrollment={'enrollment_state': 'active', 'course_section_id': section_id, 'notify': False})
    except CanvasException as e:
        return "Already Enrolled" if "already" in str(e).lower() else None

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    """Finds or creates a user, then enrolls them."""
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    user = None
    try:
        user = canvas_api.get_user(student_details['email'], 'login_id')
    except ResourceDoesNotExist:
        if student_details.get('ssid'):
            try:
                user = canvas_api.get_user(student_details['ssid'], 'sis_user_id')
            except ResourceDoesNotExist: pass
    if not user:
        user = create_canvas_user(student_details)
    return enroll_student_in_section(course_id, user.id, section_id) if user else None

def unenroll_student_from_course(course_id, student_details):
    """Deactivates active enrollments for a student."""
    canvas_api = initialize_canvas_api()
    if not canvas_api: return False
    user = None
    try:
        user = canvas_api.get_user(student_details.get('email'), 'login_id')
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
        print(f"ERROR: API error during un-enrollment: {e}")
        return False


# ==============================================================================
# CELERY APP DEFINITION (from celery_app.py)
# ==============================================================================
broker_use_ssl_config = {'ssl_cert_reqs': 'required'} if CELERY_BROKER_URL.startswith('rediss://') else {}

celery_app = Celery('tasks', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND, include=[__name__])
if broker_use_ssl_config:
    celery_app.conf.broker_use_ssl = broker_use_ssl_config
    celery_app.conf.redis_backend_use_ssl = broker_use_ssl_config
celery_app.conf.timezone = 'America/Los_Angeles'


# ==============================================================================
# CELERY TASKS (from monday_tasks.py)
# ==============================================================================
def get_student_details_from_plp(plp_item_id):
    """Helper to fetch student details from a PLP item."""
    master_student_ids = get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_student_ids: return None
    master_student_id = list(master_student_ids)[0]
    student_name = get_item_name(master_student_id, MASTER_STUDENT_BOARD_ID)
    ssid = (get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_SSID_COLUMN) or {}).get('text', '')
    email = (get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_EMAIL_COLUMN) or {}).get('text', '')
    return {'name': student_name, 'ssid': ssid, 'email': email} if all([student_name, ssid, email]) else None

def manage_class_enrollment(action, plp_item_id, class_item_id, student_details):
    """Manages the full enrollment/unenrollment logic for a single class."""
    class_name_val = get_column_value(class_item_id, ALL_COURSES_BOARD_ID, ALL_COURSES_NAME_COLUMN_ID) if ALL_COURSES_NAME_COLUMN_ID else None
    class_name = class_name_val.get('text') if class_name_val and class_name_val.get('text') else get_item_name(class_item_id, ALL_COURSES_BOARD_ID)
    if not class_name: return

    linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, ALL_COURSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)
    canvas_item_id = list(linked_canvas_item_ids)[0] if linked_canvas_item_ids else None
    canvas_course_id = ''
    if canvas_item_id:
        canvas_course_id_val = get_column_value(canvas_item_id, CANVAS_BOARD_ID, CANVAS_COURSE_ID_COLUMN)
        canvas_course_id = canvas_course_id_val.get('text', '') if canvas_course_id_val else ''

    if action == "enroll":
        if not canvas_course_id:
            new_course = create_canvas_course(class_name, CANVAS_TERM_ID)
            if not new_course: return
            canvas_course_id = new_course.id
            new_canvas_item_id = create_item(CANVAS_BOARD_ID, f"{class_name} - Canvas", {CANVAS_COURSE_ID_COLUMN: str(canvas_course_id)})
            if new_canvas_item_id:
                update_connect_board_column(class_item_id, ALL_COURSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN, new_canvas_item_id)
                if ALL_CLASSES_CANVAS_ID_COLUMN:
                    change_column_value_generic(ALL_COURSES_BOARD_ID, class_item_id, ALL_CLASSES_CANVAS_ID_COLUMN, str(canvas_course_id))

        m_series_text = (get_column_value(plp_item_id, PLP_BOARD_ID, PLP_M_SERIES_LABELS_COLUMN) or {}).get('text', '')
        ag_grad_text = (get_column_value(class_item_id, ALL_COURSES_BOARD_ID, ALL_CLASSES_AG_GRAD_COLUMN) or {}).get('text', '')
        sections = {"A-G" for s in ["AG"] if s in ag_grad_text} | {"Grad" for s in ["Grad"] if s in ag_grad_text} | {"M-Series" for s in ["M-Series"] if s in m_series_text}
        if not sections: sections.add("All")
            
        for section_name in sections:
            section = create_section_if_not_exists(canvas_course_id, section_name)
            if section:
                result = enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)
                create_subitem(plp_item_id, f"Enrolled in {class_name} ({section_name}): {result}")

    elif action == "unenroll" and canvas_course_id:
        result = unenroll_student_from_course(canvas_course_id, student_details)
        create_subitem(plp_item_id, f"Unenrolled from {class_name}: {'Success' if result else 'Failed'}")

@celery_app.task
def process_canvas_full_sync_from_status(event_data):
    if event_data.get('value', {}).get('label', {}).get('text', '') != PLP_CANVAS_SYNC_STATUS_VALUE:
        return
    plp_item_id = event_data.get('pulseId')
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return

    course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
    all_class_ids = set()
    for col_id in course_column_ids:
        class_links = get_column_value(plp_item_id, PLP_BOARD_ID, col_id)
        if class_links and class_links.get('value'):
            all_class_ids.update(get_linked_ids_from_connect_column_value(class_links['value']))
    
    for class_item_id in all_class_ids:
        manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details)

@celery_app.task
def process_canvas_delta_sync_from_course_change(event_data, log_configs):
    plp_item_id = event_data.get('pulseId')
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return

    current_ids = get_linked_ids_from_connect_column_value(event_data.get('value'))
    previous_ids = get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    added_ids = current_ids - previous_ids
    removed_ids = previous_ids - current_ids

    for class_id in added_ids: manage_class_enrollment("enroll", plp_item_id, class_id, student_details)
    for class_id in removed_ids: manage_class_enrollment("unenroll", plp_item_id, class_id, student_details)

@celery_app.task
def process_plp_course_sync_webhook(event_data):
    subitem_id, parent_item_id = event_data.get('pulseId'), event_data.get('parentItemId')
    added, removed = (get_linked_ids_from_connect_column_value(event_data.get(k)) for k in ['value', 'previousValue'])
    if not (added - removed) and not (removed - added): return

    dropdown_label = (get_column_value(subitem_id, event_data.get('boardId'), HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID) or {}).get('text')
    target_plp_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(dropdown_label)
    if not target_plp_col_id: return

    plp_linked_ids = get_linked_items_from_board_relation(parent_item_id, HS_ROSTER_BOARD_ID, HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
    if not plp_linked_ids: return
    plp_item_id = list(plp_linked_ids)[0]
    
    original_val = (get_column_value(plp_item_id, PLP_BOARD_ID, target_plp_col_id) or {}).get('value')
    for course_id in (added - removed): update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_col_id, course_id, "add")
    for course_id in (removed - added): update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_col_id, course_id, "remove")

    updated_val = (get_column_value(plp_item_id, PLP_BOARD_ID, target_plp_col_id) or {}).get('value')
    process_canvas_delta_sync_from_course_change.delay({
        'pulseId': plp_item_id, 'columnId': target_plp_col_id, 'userId': event_data.get('userId'),
        'value': updated_val, 'previousValue': original_val
    }, LOG_CONFIGS)

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    master_item_id, col_id, col_val = event_data.get('pulseId'), event_data.get('columnId'), event_data.get('value')
    mappings = MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.get(col_id)
    if not mappings: return

    for target in mappings["targets"]:
        linked_ids = get_linked_items_from_board_relation(master_item_id, MASTER_STUDENT_BOARD_ID, target["connect_column_id"])
        for linked_id in linked_ids:
            update_people_column(linked_id, target["board_id"], target["target_column_id"], col_val, target["target_column_type"])

@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    source_item_id, col_id, col_val = event_data.get('pulseId'), event_data.get('columnId'), event_data.get('value')
    config = SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.get(col_id)
    if not config: return

    linked_ids = get_linked_items_from_board_relation(source_item_id, SPED_STUDENTS_BOARD_ID, SPED_TO_IEPAP_CONNECT_COLUMN_ID)
    for linked_id in linked_ids:
        update_people_column(linked_id, IEP_AP_BOARD_ID, config["target_column_id"], col_val, config["target_column_type"])


# ==============================================================================
# FLASK WEB APP (from app_unified_webhook_handler.py)
# ==============================================================================
app = Flask(__name__)

@app.route('/monday-webhooks', methods=['POST'])
def monday_unified_webhooks():
    data = request.get_json()
    if 'challenge' in data:
        return jsonify({'challenge': data['challenge']})

    event = data.get('event', {})
    board_id = str(event.get('boardId'))
    col_id = event.get('columnId')
    parent_board_id = str(event.get('parentItemBoardId')) if event.get('parentItemBoardId') else None
    
    # Route to Canvas Sync Tasks
    if board_id == PLP_BOARD_ID:
        if col_id == PLP_CANVAS_SYNC_COLUMN_ID:
            process_canvas_full_sync_from_status.delay(event)
            return jsonify({"message": "Canvas Full Sync queued."}), 202
        elif col_id in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(','):
            process_canvas_delta_sync_from_course_change.delay(event, LOG_CONFIGS)
            return jsonify({"message": "Canvas Delta Sync queued."}), 202
    
    # Route to PLP Course Sync (HS Roster -> PLP)
    if parent_board_id == HS_ROSTER_BOARD_ID and col_id == HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID:
        process_plp_course_sync_webhook.delay(event)
        return jsonify({"message": "PLP Course Sync queued."}), 202

    # Route to People Sync Tasks
    if board_id == MASTER_STUDENT_BOARD_ID and col_id in MASTER_STUDENT_PEOPLE_COLUMNS:
        process_master_student_person_sync_webhook.delay(event)
        return jsonify({"message": "Master Student Person Sync queued."}), 202
    if board_id == SPED_STUDENTS_BOARD_ID and col_id in SPED_STUDENTS_PEOPLE_COLUMN_MAPPING:
        process_sped_students_person_sync_webhook.delay(event)
        return jsonify({"message": "SpEd Students Person Sync queued."}), 202

    return jsonify({"status": "ignored"}), 200

@app.route('/')
def home():
    return "Consolidated Webhook Handler is running!", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
