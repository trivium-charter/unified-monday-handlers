# canvas_utils.py

import os
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist

# --- Canvas API Configuration ---
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_ACCOUNT_ID = os.environ.get("CANVAS_ACCOUNT_ID", "1")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

# --- Core Utility Functions ---

def initialize_canvas_api():
    """Initializes and returns a Canvas API object."""
    if not CANVAS_API_URL or not CANVAS_API_KEY:
        print("ERROR: Canvas API URL or Key is not set in environment variables.")
        return None
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY)


def get_or_create_canvas_user(details):
    """
    The definitive method to find or create a Canvas user (student or teacher).
    It uses a robust, prioritized, and EFFICIENT search before creating a new user.
    """
    canvas = initialize_canvas_api()
    if not canvas: return None

    user_email = details.get('email')
    user_ssid = details.get('ssid')
    user_name = details.get('name')

    if not user_email or not user_name:
        print(f"CRITICAL: User details missing name or email. Cannot proceed. Details: {details}")
        return None
        
    try:
        # ### THE FIX: Use the efficient canvas.get_user() method ###
        
        # 1. Search by SIS ID (most reliable and fastest)
        if user_ssid:
            print(f"INFO: [Lookup 1/3] Searching by sis_user_id: {user_ssid}")
            try:
                user = canvas.get_user(user_ssid, 'sis_user_id')
                print(f"SUCCESS: Found user '{user.name}' by SIS ID.")
                return user
            except ResourceDoesNotExist:
                print("INFO: User not found by SIS ID.")
                pass # This is not an error, just means we continue.

        # 2. Search by Login ID (also very fast)
        print(f"INFO: [Lookup 2/3] Searching by login_id: {user_email}")
        try:
            user = canvas.get_user(user_email, 'login_id')
            print(f"SUCCESS: Found user '{user.name}' by Login ID.")
            return user
        except ResourceDoesNotExist:
            print("INFO: User not found by Login ID.")
            pass

        # 3. Last resort: General account search (This is the only one that uses the slower method)
        print(f"INFO: [Lookup 3/3] Performing account-wide search for email: {user_email}")
        account = canvas.get_account(CANVAS_ACCOUNT_ID)
        possible_users = account.get_users(search_term=user_email)
        # We must iterate here because search_term can return multiple results
        exact_match_users = [u for u in possible_users if hasattr(u, 'email') and u.email and u.email.lower() == user_email.lower()]
        if len(exact_match_users) == 1:
            print(f"SUCCESS: Found user '{exact_match_users[0].name}' via account-wide search.")
            return exact_match_users[0]
        
        # 4. If all searches fail, create the user
        print("INFO: All lookup methods failed. Creating new user.")
        sortable_name = f"{user_name.split(', ')[1]}, {user_name.split(', ')[0]}" if ', ' in user_name else user_name
        
        user_payload = {'name': user_name, 'sortable_name': sortable_name}
        pseudonym_payload = {'unique_id': user_email, 'sis_user_id': user_ssid, 'send_confirmation': False}
        
        new_user = account.create_user(pseudonym=pseudonym_payload, user=user_payload)
        new_user.edit(user={'terms_of_use': True})
        
        print(f"SUCCESS: Created new Canvas user '{new_user.name}' with ID: {new_user.id}")
        return new_user

    except Exception as e:
        print(f"CRITICAL: An exception occurred during get_or_create_canvas_user: {e}")
        return None

def create_course(course_name):
    """Creates a new, standard Canvas course for student enrollments."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        account = canvas.get_account(CANVAS_ACCOUNT_ID)
        sis_id = ''.join(e for e in course_name if e.isalnum()).lower()
        course_data = {'name': course_name, 'course_code': course_name, 'sis_course_id': sis_id}
        new_course = account.create_course(course=course_data)
        return {'id': new_course.id, 'name': new_course.name}
    except Conflict:
        print(f"INFO: Course with SIS ID '{sis_id}' may already exist. Searching...")
        account = canvas.get_account(CANVAS_ACCOUNT_ID)
        courses = list(account.get_courses(sis_course_id=sis_id))
        if courses:
            return {'id': courses[0].id, 'name': courses[0].name}
        return None
    except Exception as e:
        print(f"ERROR: API error during course creation: {e}")
        return None

def enroll_user_in_course(course_id, user_id, role):
    """Enrolls a user in a course with a specific role ('StudentEnrollment' or 'TeacherEnrollment')."""
    canvas = initialize_canvas_api()
    if not canvas: return False
    try:
        course = canvas.get_course(course_id)
        course.enroll_user(user_id, role, enrollment={'enrollment_state': 'active', 'notify': False})
        print(f"SUCCESS: Enrolled user {user_id} in course {course.id} as {role}.")
        return True
    except CanvasException as e:
        if "already" in str(e).lower():
            print(f"INFO: User {user_id} is already enrolled in course {course_id}.")
            return True
        print(f"ERROR: API error during enrollment: {e}")
        return False

def unenroll_user_from_course(course_id, user_id):
    """Unenrolls a user from a course by concluding their enrollment."""
    canvas = initialize_canvas_api()
    if not canvas: return False
    try:
        course = canvas.get_course(course_id)
        enrollments = course.get_enrollments(user_id=user_id, state=['active', 'invited'])
        if not enrollments:
            print(f"INFO: No active enrollments for user {user_id} in course {course_id}.")
            return True
        for enrollment in enrollments:
            print(f"INFO: Concluding enrollment {enrollment.id} for user {user_id}.")
            enrollment.deactivate(task='conclude')
        return True
    except Exception as e:
        print(f"ERROR: API error during un-enrollment for user {user_id}: {e}")
        return False

# ----- Legacy Functions for Teacher Webhook -----

def create_templated_course(course_name, term_id):
    """Creates a Course using a specific sub-account and template."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    if not CANVAS_SUBACCOUNT_ID or not CANVAS_TEMPLATE_COURSE_ID:
        print("ERROR: Missing CANVAS_SUBACCOUNT_ID or CANVAS_TEMPLATE_COURSE_ID.")
        return None
    try:
        account = canvas.get_account(CANVAS_SUBACCOUNT_ID)
        sis_id_name = ''.join(e for e in course_name if e.isalnum() or e in ['+', '-']).replace(' ', '_').lower()
        sis_id = f"{sis_id_name}_{term_id}"
        course_data = {'name': course_name, 'enrollment_term_id': f"sis_term_id:{term_id}", 'sis_course_id': sis_id, 'source_course_id': CANVAS_TEMPLATE_COURSE_ID }
        return account.create_course(course=course_data)
    except Conflict:
        print(f"INFO: Templated course with SIS ID '{sis_id}' may already exist.")
        account = canvas.get_account(CANVAS_SUBACCOUNT_ID)
        courses = list(account.get_courses(sis_course_id=sis_id))
        return courses[0] if courses else None
    except Exception as e:
        print(f"ERROR: API error during templated course creation: {e}")
        return None

def enroll_teacher(course_id, teacher_details):
    """Legacy function to find/create and enroll a teacher."""
    teacher_user = get_or_create_canvas_user(teacher_details)
    if teacher_user:
        return enroll_user_in_course(course_id, teacher_user.id, "TeacherEnrollment")
    return False

def unenroll_teacher(course_id, teacher_details):
    """Legacy function to find and unenroll a teacher."""
    canvas = initialize_canvas_api()
    if not canvas: return False
    try:
        user = canvas.get_user(teacher_details['email'], 'login_id')
        return unenroll_user_from_course(course_id, user.id)
    except ResourceDoesNotExist:
         print(f"INFO: Teacher '{teacher_details['email']}' not found in Canvas. Cannot unenroll.")
         return True
    except Exception as e:
         print(f"ERROR: Exception while finding teacher to unenroll: {e}")
         return False
