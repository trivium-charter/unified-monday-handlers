Python


import os
import json
import requests
import time
import re
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, ResourceDoesNotExist

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

# Canvas Course ID for "ACE Study Hall"
# This is pulled from the SPECIAL_COURSE_CANVAS_IDS dictionary in your nightly sync script
ACE_STUDY_HALL_CANVAS_ID = 10128

# ==============================================================================
# 2. HELPER FUNCTIONS (Adapted from your existing scripts)
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
    """Fetches all items from a board, handling pagination."""
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
            time.sleep(1) # Be nice to the API
        except (KeyError, IndexError):
            print(f"ERROR: Could not parse items from board {board_id}.")
            break
    return all_items

def get_student_details_from_plp(plp_item_id):
    """Fetches essential student details including their grade."""
    query = f'query {{ items (ids: [{plp_item_id}]) {{ column_values (ids: ["{PLP_TO_MASTER_STUDENT_CONNECT_COLUMN}"]) {{ value }} }} }}'
    result = execute_monday_graphql(query)
    try:
        connect_value = json.loads(result['data']['items'][0]['column_values'][0]['value'])
        master_student_id = connect_value.get('linkedPulseIds', [{}])[0].get('linkedPulseId')
        if not master_student_id: return None

        details_query = f'query {{ items (ids: [{master_student_id}]) {{ name column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}", "{MASTER_STUDENT_GRADE_COLUMN_ID}"]) {{ id text }} }} }}'
        details_result = execute_monday_graphql(details_query)
        item_details = details_result['data']['items'][0]
        
        column_map = {cv['id']: cv.get('text', '') for cv in item_details.get('column_values', [])}
        return {
            'name': item_details['name'],
            'email': column_map.get(MASTER_STUDENT_EMAIL_COLUMN, ''),
            'ssid': column_map.get(MASTER_STUDENT_SSID_COLUMN, ''),
            'canvas_id': column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN, ''),
            'grade_text': column_map.get(MASTER_STUDENT_GRADE_COLUMN_ID, '')
        }
    except (TypeError, KeyError, IndexError, json.JSONDecodeError):
        return None

def get_grade_level(grade_text):
    """Parses grade text (e.g., '1st Grade', 'K') into an integer."""
    if not grade_text: return -1  # Use -1 for unknown
    grade_text_upper = grade_text.upper()
    if "TK" in grade_text_upper: return -1 # Treat TK as pre-K
    if "K" in grade_text_upper: return 0
    match = re.search(r'\d+', grade_text)
    if match: return int(match.group(0))
    return -1

def initialize_canvas_api():
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY) if CANVAS_API_URL and CANVAS_API_KEY else None

def unenroll_student_from_course(canvas_api, course_id, student_details):
    """Finds a student in Canvas and concludes their enrollment in a course."""
    if not canvas_api: return False, "Canvas API not initialized"
    
    # Find user by various IDs
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

    if not user:
        return True, "User not found in Canvas, skipping." # Considered a success

    try:
        course = canvas_api.get_course(course_id)
        enrollments = course.get_enrollments(user_id=user.id)
        unenrolled_count = 0
        for enrollment in enrollments:
            if enrollment.role == 'StudentEnrollment':
                enrollment.deactivate(task='conclude')
                unenrolled_count += 1
        
        if unenrolled_count > 0:
            return True, f"Success, concluded {unenrolled_count} enrollment(s)."
        else:
            return True, "User was not enrolled in this course."

    except CanvasException as e:
        print(f"ERROR: Canvas unenrollment failed for user {user.id}: {e}")
        return False, f"API Error: {e}"

# ==============================================================================
# 3. MAIN SCRIPT LOGIC
# ==============================================================================
if __name__ == '__main__':
    print("======================================================")
    print("=== STARTING K-3 ACE STUDY HALL UNENROLLMENT SCRIPT ===")
    print("======================================================")
    if DRY_RUN:
        print("\n" + "="*50)
        print("=== SCRIPT IS RUNNING IN DRY RUN MODE ===")
        print("=== No actual changes will be made to Canvas. ===")
        print("="*50 + "\n")
    
    if not all([MONDAY_API_KEY, CANVAS_API_KEY, CANVAS_API_URL, PLP_BOARD_ID]):
        print("FATAL ERROR: Missing one or more required environment variables.")
        exit()

    canvas = initialize_canvas_api()
    if not canvas:
        print("FATAL ERROR: Could not initialize Canvas API.")
        exit()
        
    print("Step 1: Fetching all students from the PLP Board...")
    all_plp_students = get_all_board_items(PLP_BOARD_ID)
    print(f"Found {len(all_plp_students)} total students to check.")
    
    unenrolled_count = 0
    target_count = 0

    print("\nStep 2: Checking each student's grade and unenrolling if in K-3...")
    for i, student_item in enumerate(all_plp_students, 1):
        plp_id = student_item['id']
        student_name = student_item['name']
        print(f"--- ({i}/{len(all_plp_students)}) Processing: {student_name} (PLP ID: {plp_id}) ---")
        
        details = get_student_details_from_plp(plp_id)
        if not details or not details.get('grade_text'):
            print("  -> SKIPPING: Could not retrieve student details or grade from Monday.com.")
            continue
            
        grade_level = get_grade_level(details['grade_text'])
        
        if 0 <= grade_level <= 3:
            target_count += 1
            print(f"  -> TARGET FOUND: Student is in grade {details['grade_text']}. Preparing to unenroll from ACE Study Hall.")
            if not DRY_RUN:
                success, message = unenroll_student_from_course(canvas, ACE_STUDY_HALL_CANVAS_ID, details)
                print(f"     STATUS: {message}")
                if success and "Success" in message:
                    unenrolled_count += 1
                time.sleep(1) # Be nice to the Canvas API
            else:
                print(f"     DRY RUN: Would attempt to unenroll {details['name']} ({details['email']}).")
        else:
            print(f"  -> SKIPPING: Student grade is '{details['grade_text']}', not K-3.")
            
    print("\n======================================================")
    print("=== SCRIPT FINISHED ===")
    print(f"Total students checked: {len(all_plp_students)}")
    print(f"Total students identified in grades K-3: {target_count}")
    if not DRY_RUN:
        print(f"Total students successfully unenrolled: {unenrolled_count}")
    else:
        print("Dry run complete. No changes were made.")
    print("======================================================")
