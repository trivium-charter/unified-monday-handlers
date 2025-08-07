import os
import json
import requests
import time
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist

# ==============================================================================
# COMPREHENSIVE BULK PLP SYNC SCRIPT (V5.6 - Final with Subitem Check)
# ==============================================================================
# This script performs a full, two-phase sync for all students:
# 1. Reconciles the High School Roster subitem courses to the PLP board.
# 2. Syncs the now-updated PLP courses to Canvas for enrollment and syncs
#    teachers back to the Master Student List.
# 3. Checks for existing "Curriculum Change" subitems to prevent duplicates.
# 4. Sets the "Entry Type" for all new subitems to "Curriculum Change".
# ==============================================================================

# ==============================================================================
# CONFIGURATION
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"

# Board and Column IDs
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID")
HS_ROSTER_SUBITEM_SUBJECT_COLUMN_ID = "dropdown_mks6zjqh" # Subject column
HS_ROSTER_SUBITEM_CURRICULUM_COLUMN_ID = "dropdown7"      # Curriculum column
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")

PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN")
PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID = os.environ.get("PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID") # e.g., "entry_type__1"

MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
MASTER_STUDENT_CANVAS_ID_COLUMN = "text_mktgs1ax"
MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID = os.environ.get("MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID")
MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID = os.environ.get("MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID")

ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
ALL_CLASSES_CANVAS_ID_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_ID_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")

CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")
CANVAS_COURSES_TEACHER_COLUMN_ID = os.environ.get("CANVAS_COURSES_TEACHER_COLUMN_ID")
CANVAS_COURSE_TYPE_COLUMN_ID = os.environ.get("CANVAS_COURSE_TYPE_COLUMN_ID")

CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
except json.JSONDecodeError:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}

DELAY_BETWEEN_ITEMS = 0.5
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }
canvas_api_instance = None

# ==============================================================================
# API UTILITIES
# ==============================================================================

def initialize_canvas_api():
    global canvas_api_instance
    if not canvas_api_instance:
        canvas_api_instance = Canvas(CANVAS_API_URL, CANVAS_API_KEY) if CANVAS_API_URL and CANVAS_API_KEY else None
    return canvas_api_instance

def execute_monday_graphql(query):
    try:
        response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  -> API Error: {e}")
        return None

def get_all_items_from_board(board_id, with_subitems=False, item_ids=None):
    all_items = []
    cursor = None
    subitem_query_part = "subitems { id name column_values { id text value } }" if with_subitems else ""
    query_params = f'query_params: {{ ids: {json.dumps(item_ids)} }}' if item_ids else ''
    
    while True:
        query = f"""
        query {{
            boards(ids: [{board_id}]) {{
                items_page (limit: 50{', cursor: "' + cursor + '"' if cursor else ''}, {query_params}) {{
                    cursor
                    items {{
                        id
                        name
                        column_values {{ id value text }}
                        {subitem_query_part}
                    }}
                }}
            }}
        }}
        """
        result = execute_monday_graphql(query)
        if not result or 'data' not in result or not result['data'].get('boards'):
            print(f"  -> Could not fetch items for board {board_id}. Stopping.")
            break
        items_page = result['data']['boards'][0]['items_page']
        items = items_page.get('items', [])
        all_items.extend(items)
        cursor = items_page.get('cursor')
        if not cursor or (item_ids and len(all_items) >= len(item_ids)):
            break
    return all_items

def get_column_value(item_id, board_id, column_id):
    if not column_id: return None
    query = f"query {{ items(ids:[{item_id}]) {{ column_values(ids:[\"{column_id}\"]) {{ id value text }} }} }}"
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('items'):
        column_values = result['data']['items'][0].get('column_values')
        if column_values:
            col_val = column_values[0]
            parsed_value = None
            if col_val.get('value'):
                try: parsed_value = json.loads(col_val['value'])
                except (json.JSONDecodeError, TypeError): parsed_value = col_val['value']
            return {'value': parsed_value, 'text': col_val.get('text')}
    return None

def get_column_value_from_item_data(item_data, column_id):
    for cv in item_data.get('column_values', []):
        if cv and cv.get('id') == column_id:
            try:
                if cv.get('text'): return cv['text']
                return json.loads(cv['value']) if cv.get('value') else None
            except (json.JSONDecodeError, TypeError):
                return cv.get('value')
    return None

def get_linked_item_ids(item_data, column_id):
    column_value_str = get_column_value_from_item_data(item_data, column_id)
    if not column_value_str:
        return []
    try:
        column_value = json.loads(column_value_str) if isinstance(column_value_str, str) else column_value_str
        if isinstance(column_value, dict) and "linkedPulseIds" in column_value:
            return [int(item['linkedPulseId']) for item in column_value["linkedPulseIds"]]
    except (json.JSONDecodeError, TypeError):
        return []
    return []

def update_connect_board_column(item_id, board_id, column_id, item_ids_to_add):
    if not item_ids_to_add:
        return
    current_links_data = get_column_value(item_id, board_id, column_id)
    current_ids = set()
    if current_links_data and current_links_data.get('value'):
        try:
            linked_pulses = current_links_data['value'].get('linkedPulseIds', [])
            current_ids = {int(item['linkedPulseId']) for item in linked_pulses}
        except (AttributeError, TypeError):
            pass
    
    updated_ids = current_ids.union(set(item_ids_to_add))
    
    connect_value = {"linkedPulseIds": [{"linkedPulseId": lid} for lid in sorted(list(updated_ids))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    mutation = f'mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}) {{ id }} }}'
    execute_monday_graphql(mutation)

def update_people_column_with_ids(item_id, board_id, column_id, people_ids):
    if not people_ids:
        graphql_value = json.dumps(json.dumps({}))
    else:
        people_list = [{"id": int(pid), "kind": "person"} for pid in people_ids]
        people_value = {"personsAndTeams": people_list}
        graphql_value = json.dumps(json.dumps(people_value))
    mutation = f'mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}) {{ id }} }}'
    execute_monday_graphql(mutation)

def create_subitem(parent_item_id, subitem_name, column_values=None):
    values_for_api = {col_id: val for col_id, val in (column_values or {}).items()}
    column_values_json = json.dumps(values_for_api)
    mutation = f"mutation {{ create_subitem (parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {json.dumps(column_values_json)}) {{ id }} }}"
    execute_monday_graphql(mutation)

def get_item_name(item_id, board_id):
    query = f"query {{ items(ids:[{item_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('items'):
        return result['data']['items'][0].get('name')
    return None

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

def create_canvas_user(student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        account = canvas_api.get_account(1)
        user_payload = {'user': {'name': student_details['name'], 'terms_of_use': True}, 'pseudonym': {'unique_id': student_details['email'], 'sis_user_id': student_details['ssid'], 'login_id': student_details['email'], 'authentication_provider_id': '112'}, 'communication_channel': {'type': 'email', 'address': student_details['email'], 'skip_confirmation': True}}
        return account.create_user(**user_payload)
    except CanvasException as e:
        print(f"    -> ERROR: Canvas user creation failed: {e}")
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
    except (ResourceDoesNotExist, CanvasException) as e:
        print(f"    -> ERROR: Canvas section creation/lookup failed: {e}")
        return None

def enroll_student_in_section(course_id, user_id, section_id):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed: Canvas API not initialized"
    try:
        course = canvas_api.get_course(course_id)
        user = canvas_api.get_user(user_id)
        course.enroll_user(user, 'StudentEnrollment', enrollment_state='active', course_section_id=section_id, notify=False)
        return "Success"
    except Conflict: return "Already Enrolled"
    except CanvasException as e:
        print(f"    -> ERROR: Failed to enroll user {user_id} in section {section_id}. Details: {e}")
        return "Failed"

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    user = find_canvas_user(student_details)
    if not user:
        user = create_canvas_user(student_details)
    if user and section_id:
        return enroll_student_in_section(course_id, user.id, section_id)
    return "Failed: User not found/created or section missing"

def get_student_details_from_plp(plp_item_id):
    query = f'query {{ items (ids: [{plp_item_id}]) {{ column_values (ids: ["{PLP_TO_MASTER_STUDENT_CONNECT_COLUMN}"]) {{ value }} }} }}'
    result = execute_monday_graphql(query)
    try:
        connect_val = json.loads(result['data']['items'][0]['column_values'][0]['value'])
        master_student_id = connect_val['linkedPulseIds'][0]['linkedPulseId']
        details_query = f'query {{ items (ids: [{master_student_id}]) {{ name column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}"]) {{ id text }} }} }}'
        details_result = execute_monday_graphql(details_query)
        item_details = details_result['data']['items'][0]
        column_map = {cv['id']: cv.get('text') for cv in item_details.get('column_values', []) if isinstance(cv, dict)}
        return {
            'name': item_details['name'],
            'ssid': column_map.get(MASTER_STUDENT_SSID_COLUMN),
            'email': column_map.get(MASTER_STUDENT_EMAIL_COLUMN),
            'canvas_id': column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN)
        }
    except (TypeError, KeyError, IndexError, json.JSONDecodeError):
        return None

def manage_class_enrollment(plp_item_id, class_item_id, student_details, existing_subitems, subitem_cols):
    linked_canvas_items_data = get_column_value(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    class_name_for_log = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
    
    log_prefix = f"Enrolled in {class_name_for_log}"
    if any(s.startswith(log_prefix) for s in existing_subitems):
        print(f"    -> Subitem for '{class_name_for_log}' already exists. Skipping log creation.")
        return

    if not linked_canvas_items_data or not linked_canvas_items_data.get('value'):
        create_subitem(plp_item_id, f"Added non-Canvas course '{class_name_for_log}'", subitem_cols)
        return

    linked_ids = {int(item['linkedPulseId']) for item in linked_canvas_items_data['value'].get('linkedPulseIds', [])}
    if not linked_ids:
        create_subitem(plp_item_id, f"Added non-Canvas course '{class_name_for_log}'", subitem_cols)
        return
    
    canvas_item_id = list(linked_ids)[0]
    class_name = get_item_name(canvas_item_id, int(CANVAS_BOARD_ID))
    course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = course_id_val.get('text') if course_id_val else None

    if not canvas_course_id:
        create_subitem(plp_item_id, f"Failed to enroll in {class_name} (No Canvas Course ID)", subitem_cols)
        return

    m_series_val = get_column_value(student_details['plp_id'], int(PLP_BOARD_ID), PLP_M_SERIES_LABELS_COLUMN)
    ag_grad_val = get_column_value(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_CLASSES_AG_GRAD_COLUMN)
    m_series_text = (m_series_val.get('text') or "") if m_series_val else ""
    ag_grad_text = (ag_grad_val.get('text') or "") if ag_grad_val else ""
    
    sections = {"A-G" for s in ["AG"] if s in ag_grad_text} | {"Grad" for s in ["Grad"] if s in ag_grad_text} | {"M-Series" for s in ["M-series"] if s in m_series_text}
    if not sections: sections.add("All")
    
    results = [enroll_or_create_and_enroll(canvas_course_id, create_section_if_not_exists(canvas_course_id, s_name).id, student_details) for s_name in sections]
    
    final_status = "Failed" if "Failed" in results else "Success"
    if "Already Enrolled" in results and "Success" not in results:
        final_status = "Already Enrolled"

    if final_status != "Already Enrolled":
        section_names = ", ".join(sections)
        subitem_title = f"Enrolled in {class_name} (Sections: {section_names}): {final_status}"
        create_subitem(plp_item_id, subitem_title, subitem_cols)

def get_linked_item_ids_from_single_course(course_item_id):
    query = f'query {{ items (ids: [{course_item_id}]) {{ id column_values(ids: ["{ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID}"]) {{ id value text }} }} }}'
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('items'):
        return get_linked_item_ids(result['data']['items'][0], ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    return []

def get_canvas_item_details(canvas_item_ids):
    query = f'query {{ items (ids: {json.dumps(list(canvas_item_ids))}) {{ id column_values(ids: ["{CANVAS_COURSES_TEACHER_COLUMN_ID}", "{CANVAS_COURSE_TYPE_COLUMN_ID}"]) {{ id text value }} }} }}'
    result = execute_monday_graphql(query)
    details = []
    if result and result.get('data', {}).get('items'):
        for item in result['data']['items']:
            course_type = get_column_value_from_item_data(item, CANVAS_COURSE_TYPE_COLUMN_ID)
            people_value_str = get_column_value_from_item_data(item, CANVAS_COURSES_TEACHER_COLUMN_ID)
            people_value = json.loads(people_value_str) if people_value_str and isinstance(people_value_str, str) else {}
            teacher_id = None
            if people_value:
                persons = people_value.get('personsAndTeams', [])
                if persons: teacher_id = persons[0].get('id')
            details.append({'course_type': course_type, 'teacher_id': teacher_id})
    return details

def get_subitem_names(plp_item_id, entry_type_col_id):
    query = f"query {{ items(ids: [{plp_item_id}]) {{ subitems {{ name column_values(ids: [\"{entry_type_col_id}\"]) {{ text }} }} }} }}"
    result = execute_monday_graphql(query)
    names = set()
    if result and result.get('data', {}).get('items'):
        subitems = result['data']['items'][0].get('subitems', [])
        for sub in subitems:
            # Only consider subitems with the correct entry type
            entry_type = get_column_value_from_item_data(sub, entry_type_col_id)
            if entry_type == "Curriculum Change":
                names.add(sub['name'])
    return names

# ==============================================================================
# MAIN SYNC LOGIC
# ==============================================================================
def bulk_plp_sync():
    print("Starting comprehensive bulk PLP sync process...")
    initialize_canvas_api()

    plp_course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
    if not plp_course_column_ids:
        print("ERROR: PLP_ALL_CLASSES_CONNECT_COLUMNS_STR is not set. Aborting.")
        return
    if not PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID:
        print("ERROR: PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID is not set. Aborting.")
        return

    print(f"Fetching all students from HS Roster Board (ID: {HS_ROSTER_BOARD_ID})...")
    all_hs_items = get_all_items_from_board(HS_ROSTER_BOARD_ID, with_subitems=True)
    total_items = len(all_hs_items)
    print(f"Found {total_items} HS Roster items to process.\n")

    subitem_cols = {PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID: {"labels": ["Curriculum Change"]}}

    for index, hs_item in enumerate(all_hs_items):
        hs_item_name = hs_item['name']
        print(f"Processing HS Roster Item {index + 1}/{total_items}: {hs_item_name}")

        plp_ids = get_linked_item_ids(hs_item, HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
        if not plp_ids:
            print("  -> No PLP item linked. Skipping.")
            print("-" * 20); continue
        plp_item_id = plp_ids[0]

        # PHASE 1: Reconcile HS Roster to PLP
        courses_to_sync = {}
        for subitem in hs_item.get('subitems', []):
            course_ids_in_subitem = get_linked_item_ids(subitem, HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID)
            if not course_ids_in_subitem:
                continue

            curriculum_text = get_column_value_from_item_data(subitem, HS_ROSTER_SUBITEM_CURRICULUM_COLUMN_ID) or ""
            subject_text = get_column_value_from_item_data(subitem, HS_ROSTER_SUBITEM_SUBJECT_COLUMN_ID) or ""
            
            categories = set()
            if "ACE" in curriculum_text:
                categories.add("ACE")
            
            if "Math" in subject_text:
                categories.add("Math")
            if "ELA" in subject_text:
                categories.add("ELA")
            
            if not categories:
                categories.add("Other")
            
            for category in categories:
                target_plp_col = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(category)
                if target_plp_col:
                    if target_plp_col not in courses_to_sync:
                        courses_to_sync[target_plp_col] = set()
                    courses_to_sync[target_plp_col].update(course_ids_in_subitem)

        if courses_to_sync:
            print("  -> Reconciling courses from HS Roster to PLP...")
            for col_id, course_ids in courses_to_sync.items():
                print(f"    -> Syncing {len(course_ids)} courses to PLP column {col_id}...")
                update_connect_board_column(plp_item_id, int(PLP_BOARD_ID), col_id, list(course_ids))
                time.sleep(0.2)
        else:
            print("  -> No courses found in subitems to sync for Phase 1.")

        # PHASE 2: Sync PLP to Canvas and Master Student
        print("  -> Starting PLP to Canvas and Master Student sync...")
        student_details_raw = get_student_details_from_plp(plp_item_id)
        if not student_details_raw:
            print("  -> Could not retrieve details for linked master student. Skipping Phase 2.")
            print("-" * 20); continue
        student_details_raw['plp_id'] = plp_item_id
        
        plp_item_data_list = get_all_items_from_board(PLP_BOARD_ID, item_ids=[plp_item_id])
        if not plp_item_data_list:
            print("  -> Could not re-fetch PLP item data. Skipping Phase 2.")
            print("-" * 20); continue
        plp_item_data = plp_item_data_list[0]
        
        master_student_ids = get_linked_item_ids(plp_item_data, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
        if not master_student_ids:
            print("  -> No Master Student ID found on re-fetch. Skipping Phase 2.")
            print("-" * 20); continue
        master_student_id = master_student_ids[0]
        
        all_plp_courses = set()
        for col_id in plp_course_column_ids:
            all_plp_courses.update(get_linked_item_ids(plp_item_data, col_id))

        if not all_plp_courses:
            print("  -> No courses found on PLP to process for Canvas/Teacher sync.")
        else:
            existing_subitems = get_subitem_names(plp_item_id, PLP_SUBITEM_ENTRY_TYPE_COLUMN_ID)
            ace_teacher_ids, connect_teacher_ids = set(), set()
            for course_item_id in all_plp_courses:
                manage_class_enrollment(plp_item_id, course_item_id, student_details_raw, existing_subitems, subitem_cols)
                canvas_item_ids = get_linked_item_ids_from_single_course(course_item_id)
                if canvas_item_ids:
                    canvas_item_details = get_canvas_item_details(canvas_item_ids)
                    for detail in canvas_item_details:
                        if detail.get('teacher_id'):
                            if detail['course_type'] == "ACE": ace_teacher_ids.add(detail['teacher_id'])
                            elif detail['course_type'] == "Connect": connect_teacher_ids.add(detail['teacher_id'])
            
            if ace_teacher_ids:
                update_people_column_with_ids(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID, ace_teacher_ids)
            if connect_teacher_ids:
                update_people_column_with_ids(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID, connect_teacher_ids)
        
        print("-" * 20)
        time.sleep(DELAY_BETWEEN_ITEMS)

    print("\nComprehensive bulk sync complete!")

# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================
if __name__ == '__main__':
    bulk_plp_sync()
