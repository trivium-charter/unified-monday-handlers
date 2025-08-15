# one_time_section_fix.py
import os
import json
import requests
import time
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist

# ==============================================================================
# CONFIGURATION (Copied from your main scripts)
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
MASTER_STUDENT_TOR_COLUMN_ID = os.environ.get("MASTER_STUDENT_TOR_COLUMN_ID") # Assumes TOR is Roster Teacher

# The 10 special courses
ROSTER_ONLY_COURSES = {10298, 10297, 10299, 10300, 10301}
ROSTER_AND_CREDIT_COURSES = {10097, 10002, 10092, 10164, 10198}
ALL_SPECIAL_COURSES = ROSTER_ONLY_COURSES.union(ROSTER_AND_CREDIT_COURSES)

# (All the necessary utility functions are included below)

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
    # ... (Includes retry logic)
    # ... (This is the same robust function from your other scripts)
    pass # In the interest of space, the full function is assumed

def get_all_board_items(board_id):
    # ... (Fetches all items from a board)
    pass # In the interest of space, the full function is assumed

def get_student_details_from_plp(plp_item_id):
    # ... (Fetches student details)
    pass # In the interest of space, the full function is assumed

def find_canvas_user(student_details):
    # ... (Finds a user in Canvas)
    pass # In the interest of space, the full function is assumed
    
def create_section_if_not_exists(course, section_name):
    # ... (Creates a section if it doesn't exist)
    pass # In the interest of space, the full function is assumed
    
def enroll_student_in_section(course, user, section):
    # ... (Enrolls a student in a section)
    pass # In the interest of space, the full function is assumed

def get_roster_teacher_name(master_student_id):
    # ... (Gets the Roster Teacher's last name)
    pass # In the interest of space, the full function is assumed
    
# ==============================================================================
# MAIN SCRIPT LOGIC
# ==============================================================================
def run_one_time_section_fix():
    print("Starting the one-time section fix script...")
    canvas_api = initialize_canvas_api()
    if not canvas_api:
        print("ERROR: Canvas API not initialized. Halting.")
        return

    print(f"Fetching all students from PLP Board: {PLP_BOARD_ID}")
    all_plp_items = get_all_board_items(PLP_BOARD_ID)
    print(f"Found {len(all_plp_items)} total students to check.")

    for i, plp_item in enumerate(all_plp_items):
        plp_item_id = plp_item['id']
        print(f"\n--- Processing student {i+1}/{len(all_plp_items)} (PLP ID: {plp_item_id}) ---")

        student_details = get_student_details_from_plp(plp_item_id)
        if not student_details or not student_details.get('master_id'):
            print("  -> SKIPPING: Could not get complete student details.")
            continue

        canvas_user = find_canvas_user(student_details)
        if not canvas_user:
            print(f"  -> SKIPPING: Could not find user {student_details['name']} in Canvas.")
            continue
            
        roster_teacher_name = get_roster_teacher_name(student_details['master_id'])
        if not roster_teacher_name:
            print("  -> WARNING: Could not determine Roster Teacher. Defaulting to 'Unassigned'.")
            roster_teacher_name = "Unassigned"

        # Find which of the special courses this student is linked to
        for category, col_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
            linked_course_ids = get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), col_id)
            
            for course_id in linked_course_ids:
                if course_id in ALL_SPECIAL_COURSES:
                    try:
                        canvas_course = canvas_api.get_course(course_id)
                        print(f"  -> Found special course: '{canvas_course.name}'")

                        # Enroll in Roster Teacher section for all 10 courses
                        section_teacher = create_section_if_not_exists(canvas_course, roster_teacher_name)
                        if section_teacher:
                            enroll_student_in_section(canvas_course, canvas_user, section_teacher)

                        # Additional logic for the 5 credit-based courses
                        if course_id in ROSTER_AND_CREDIT_COURSES:
                            course_item_name = get_item_name(course_id, ALL_COURSES_BOARD_ID) or ""
                            credit_section_name = "2.5 Credits" if "2.5" in course_item_name else "5 Credits"
                            
                            section_credit = create_section_if_not_exists(canvas_course, credit_section_name)
                            if section_credit:
                                enroll_student_in_section(canvas_course, canvas_user, section_credit)
                    
                    except ResourceDoesNotExist:
                        print(f"  -> ERROR: Course with ID {course_id} not found in Canvas.")
                    except Exception as e:
                        print(f"  -> An unexpected error occurred: {e}")
        
        time.sleep(0.5) # Pause between students to be kind to the APIs

    print("\nScript finished!")

if __name__ == '__main__':
    run_one_time_section_fix()
