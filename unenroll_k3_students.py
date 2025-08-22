import os
import json
import requests
import time
import re
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, ResourceDoesNotExist, Conflict

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
# IMPORTANT: Set this to False only after you have reviewed the dry run output.
DRY_RUN = True

# --- Load configuration from environment variables ---
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"

PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
MASTER_STUDENT_GRADE_COLUMN_ID = os.environ.get("MASTER_STUDENT_GRADE_COLUMN_ID")
MASTER_STUDENT_CANVAS_ID_COLUMN = "text_mktgs1ax"
# This new variable is needed to find the TOR
MASTER_STUDENT_TOR_COLUMN_ID = os.environ.get("MASTER_STUDENT_TOR_COLUMN_ID") 

# Canvas Course ID for "ACE Study Hall"
ACE_STUDY_HALL_CANVAS_ID = 10128

# ==============================================================================
# 2. HELPER FUNCTIONS
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
    max_retries = 4; delay = 2
    for attempt in range(max_retries):
        try:
            response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS, timeout=30)
            if response.status_code == 429: print(f"WARNING: Rate limit hit. Waiting {delay} seconds..."); time.sleep(delay); delay *= 2; continue
            response.raise_for_status()
            json_response = response.json()
            if "errors" in json_response: print(f"ERROR: Monday GraphQL Error: {json_response['errors']}"); return None
            return json_response
        except requests.exceptions.RequestException as e:
            print(f"WARNING: Monday HTTP Request Error: {e}. Retrying...")
            if attempt < max_retries - 1: time.sleep(delay); delay *= 2
            else: print("ERROR: Final retry failed."); return None
    return None

def get_all_board_items(board_id):
    all_items = []
    cursor = None
    while True:
        cursor_str = f', cursor: "{cursor}"' if cursor else ""
        query = f'query {{ boards(ids: {board_id}) {{ items_page(limit: 100{cursor_str}) {{ cursor items {{ id name }} }} }} }}'
        result = execute_monday_graphql(query)
        if not result or 'data' not in result: break
        try:
            page_info = result['data']['boards'][0]['items_page']
            all_items.extend(page_info['items'])
            cursor = page_info.get('cursor')
            if not cursor: break
            print(f"  Fetched {len(all_items)} items from board {board_id}...")
            time.sleep(1)
        except (KeyError, IndexError):
            print(f"ERROR: Could not parse items from board {board_id}.")
            break
    return all_items

def get_student_details_from_plp(plp_item_id):
    query = f'query {{ items (ids: [{plp_item_id}]) {{ column_values (ids: ["{PLP_TO_MASTER_STUDENT_CONNECT_COLUMN}"]) {{ value }} }} }}'
    result = execute_monday_graphql(query)
    try:
        connect_value = json.loads(result['data']['items'][0]['column_values'][0]['value'])
        master_student_id = connect_value.get('linkedPulseIds', [{}])[0].get('linkedPulseId')
        if not master_student_id: return None

        details_query = f'query {{ items (ids: [{master_student_id}]) {{ id name column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}", "{MASTER_STUDENT_GRADE_COLUMN_ID}"]) {{ id text }} }} }}'
        details_result = execute_monday_graphql(details_query)
        item_details = details_result['data']['items'][0]
        
        column_map = {cv['id']: cv.get('text', '') for cv in item_details.get('column_values', [])}
        return {
            'master_id': item_details['id'],
            'name': item_details['name'],
            'email': column_map.get(MASTER_STUDENT_EMAIL_COLUMN, ''),
            'ssid': column_map.get(MASTER_STUDENT_SSID_COLUMN, ''),
            'canvas_id': column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN, ''),
            'grade_text': column_map.get(MASTER_STUDENT_GRADE_COLUMN_ID, '')
        }
    except (TypeError, KeyError, IndexError, json.JSONDecodeError):
        return None

def get_grade_level(grade_text):
    if not grade_text: return -1
    grade_text_upper = grade_text.upper()
    if "TK" in grade_text_upper: return -1
    if "K" in grade_text_upper: return 0
    match = re.search(r'\d+', grade_text)
    if match: return int(match.group(0))
    return -1

def get_user_name(user_id):
    if user_id is None: return None
    query = f"query {{ users(ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    try: return result['data']['users'][0].get('name')
    except (TypeError, KeyError, IndexError): return None

def get_people_ids_from_value(value_data):
    if not value_data: return set()
    if isinstance(value_data, str):
        try: value_data = json.loads(value_data)
        except json.JSONDecodeError: return set()
    persons_and_teams = value_data.get('personsAndTeams', [])
    return {person['id'] for person in persons_and_teams if 'id' in person and person.get('kind') == 'person'}

def get_roster_teacher_name(master_student_id):
    query = f'query {{ items(ids:[{master_student_id}]) {{ column_values(ids:["{MASTER_STUDENT_TOR_COLUMN_ID}"]) {{ value }} }} }}'
    result = execute_monday_graphql(query)
    try:
        tor_val_str = result['data']['items'][0]['column_values'][0]['value']
        if tor_val_str:
            tor_ids = get_people_ids_from_value(json.loads(tor_val_str))
            if tor_ids:
                tor_full_name = get_user_name(list(tor_ids)[0])
                if tor_full_name: return tor_full_name.split()[-1]
    except (TypeError, KeyError, IndexError, json.JSONDecodeError):
        pass
    return "Unassigned"

def initialize_canvas_api():
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY) if CANVAS_API_URL and CANVAS_API_KEY else None

def find_canvas_user(canvas_api, student_details):
    if not canvas_api: return None
    user = None
    if student_details.get('canvas_id'):
        try: user = canvas_api.get_user(student_details['canvas_id']);
        except ResourceDoesNotExist: pass
    if not user and student_details.get('email'):
        try: user = canvas_api.get_user(student_details['email'], 'login_id');
        except ResourceDoesNotExist: pass
    if not user and student_details.get('ssid'):
        try: user = canvas_api.get_user(student_details['ssid'], 'sis_user_id');
        except ResourceDoesNotExist: pass
    return user

def unenroll_student_from_course(course, user):
    try:
        enrollments = course.get_enrollments(user_id=user.id)
        unenrolled_count = 0
        for enrollment in enrollments:
            if enrollment.role == 'StudentEnrollment':
                enrollment.deactivate(task='conclude')
                unenrolled_count += 1
        return True, f"Success, concluded {unenrolled_count} enrollment(s)."
    except CanvasException as e:
        return False, f"API Error during unenrollment: {e}"

def create_section_if_not_exists(course, section_name):
    try:
        for section in course.get_sections():
            if section.name.lower() == section_name.lower():
                return section
        return course.create_course_section(course_section={'name': section_name})
    except CanvasException as e:
        print(f"    ERROR: Canvas section creation failed for '{section_name}': {e}")
        return None

def enroll_student_in_section(course, user, section):
    try:
        course.enroll_user(user, 'StudentEnrollment', enrollment={'course_section_id': section.id, 'notify': False})
        return True, f"Success, enrolled in section '{section.name}'."
    except Conflict:
        return True, f"Already enrolled in section '{section.name}'."
    except CanvasException as e:
        return False, f"API Error during enrollment: {e}"

# ==============================================================================
# 3. MAIN SCRIPT LOGIC
# ==============================================================================
if __name__ == '__main__':
    print("======================================================")
    print("=== ACE STUDY HALL CLEANUP & RE-SECTIONING SCRIPT  ===")
    print("======================================================")
    if DRY_RUN:
        print("\n" + "="*50)
        print("=== SCRIPT IS RUNNING IN DRY RUN MODE ===")
        print("="*50 + "\n")
    
    if not all([MONDAY_API_KEY, CANVAS_API_KEY, CANVAS_API_URL, PLP_BOARD_ID, MASTER_STUDENT_TOR_COLUMN_ID]):
        print("FATAL ERROR: Missing one or more required environment variables.")
        exit()

    canvas = initialize_canvas_api()
    if not canvas:
        print("FATAL ERROR: Could not initialize Canvas API.")
        exit()
        
    print("Step 1: Fetching all students from the PLP Board...")
    all_plp_students = get_all_board_items(PLP_BOARD_ID)
    print(f"Found {len(all_plp_students)} total students to check.")
    
    try:
        ace_course = canvas.get_course(ACE_STUDY_HALL_CANVAS_ID)
        print(f"Successfully connected to Canvas course: '{ace_course.name}'")
    except ResourceDoesNotExist:
        print(f"FATAL ERROR: Canvas course with ID {ACE_STUDY_HALL_CANVAS_ID} not found.")
        exit()

    print("\nStep 2: Processing all students...")
    for i, student_item in enumerate(all_plp_students, 1):
        plp_id = student_item['id']
        student_name = student_item['name']
        print(f"--- ({i}/{len(all_plp_students)}) Processing: {student_name} (PLP ID: {plp_id}) ---")
        
        details = get_student_details_from_plp(plp_id)
        if not details or not details.get('grade_text'):
            print("  -> SKIPPING: Could not retrieve student details or grade from Monday.com.")
            continue
            
        grade_level = get_grade_level(details['grade_text'])
        
        user = find_canvas_user(canvas, details)
        if not user:
            print("  -> SKIPPING: Student not found in Canvas.")
            continue
            
        # --- UNENROLLMENT LOGIC FOR K-5 ---
        if 0 <= grade_level <= 5:
            print(f"  -> ACTION (Unenroll): Student is in grade {details['grade_text']}.")
            if not DRY_RUN:
                success, message = unenroll_student_from_course(ace_course, user)
                print(f"     STATUS: {message}")
                time.sleep(1)
            else:
                print(f"     DRY RUN: Would unenroll {details['name']} ({details['email']}).")
        
        # --- RE-SECTIONING LOGIC FOR 6-12 ---
        elif 6 <= grade_level <= 12:
            print(f"  -> ACTION (Re-section): Student is in grade {details['grade_text']}.")
            
            target_section_name = get_roster_teacher_name(details['master_id'])
            print(f"     Target section is '{target_section_name}'.")

            if not DRY_RUN:
                # Find or create the target section
                target_section = create_section_if_not_exists(ace_course, target_section_name)
                if not target_section:
                    print("     ERROR: Failed to find or create target section. Skipping re-sectioning.")
                    continue

                # Check current enrollments
                current_enrollments = ace_course.get_enrollments(user_id=user.id)
                is_in_correct_section = False
                sections_to_unenroll_from = []

                for enrollment in current_enrollments:
                    if enrollment.course_section_id == target_section.id:
                        is_in_correct_section = True
                        print(f"     Student is already in the correct section.")
                    else:
                        sections_to_unenroll_from.append(enrollment)
                
                # Enroll in new section if needed
                if not is_in_correct_section:
                    success, message = enroll_student_in_section(ace_course, user, target_section)
                    print(f"     Enrollment Status: {message}")

                # Unenroll from old sections
                for old_enrollment in sections_to_unenroll_from:
                    try:
                        old_enrollment.deactivate(task='delete')
                        print(f"     Unenrolled from old section ID {old_enrollment.course_section_id}.")
                    except CanvasException as e:
                        print(f"     ERROR unenrolling from old section: {e}")
                
                time.sleep(1.5)
            else:
                print(f"     DRY RUN: Would ensure student is in section '{target_section_name}' and remove from others.")
        else:
            print(f"  -> SKIPPING: Student grade is '{details['grade_text']}', no action needed.")
            
    print("\n=====================================")
    print("=== SCRIPT FINISHED ===")
    print("=====================================\n")
