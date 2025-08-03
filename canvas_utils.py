import os
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceNotFound

# --- Canvas API Configuration (No Changes Here) ---
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_ACCOUNT_ID = os.environ.get("CANVAS_ACCOUNT_ID", "1")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")


# --- Core Utility Functions ---

def initialize_canvas_api():
    """Initializes and returns a Canvas API object. This pattern is used by all functions."""
    if not CANVAS_API_URL or not CANVAS_API_KEY:
        print("ERROR: Canvas API URL or Key is not set in environment variables.")
        return None
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY)


def get_or_create_canvas_user(details):
    """
    The definitive method to find or create a Canvas user (student or teacher).
    It uses a robust, prioritized 3-step search before creating a new user.

    Args:
        details (dict): Must contain 'name' and 'email'. 'ssid' is optional but recommended.
    
    Returns: A Canvas User object or None.
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
        account = canvas.get_account(CANVAS_ACCOUNT_ID)

        # 1. Search by SIS ID (most reliable)
        if user_ssid:
            print(f"INFO: [Lookup 1/3] Searching by sis_user_id: {user_ssid}")
            users = list(account.get_users(sis_user_id=user_ssid))
            if users:
                print(f"SUCCESS: Found user '{users[0].name}' by SIS ID.")
                return users[0]

        # 2. Search by Login ID (very reliable)
        print(f"INFO: [Lookup 2/3] Searching by login_id: {user_email}")
        users = list(account.get_users(sis_login_id=user_email))
        if users:
            print(f"SUCCESS: Found user '{users[0].name}' by Login ID.")
            return users[0]

        # 3. Last resort: General account search for the email
        print(f"INFO: [Lookup 3/3] Performing account-wide search for email: {user_email}")
        possible_users = account.get_users(search_term=user_email)
        exact_match_users = [u for u in possible_users if hasattr(u, 'email') and u.email and u.email.lower() == user_email.lower()]
        if len(exact_match_users) == 1:
            print(f"SUCCESS: Found user '{exact_match_users[0].name}' via account-wide search.")
            return exact_match_users[0]
            
        # 4. If all searches fail, create the user
        print("INFO: All lookup methods failed. Creating new user.")
        sortable_name = f"{user_name.split(', ')[1]}, {user_name.split(', ')[0]}" if ', ' in user_name else user_name
        
        user_payload = {'name': user_name, 'sortable_name': sortable_name}
        pseudonym_payload = {'unique_id': user_email, 'sis_user_id': user_ssid, 'send_confirmation': False}
        
        print(f"DEBUG: Creating user with payload: user={user_payload}, pseudonym={pseudonym_payload}")
        new_user = account.create_user(pseudonym=pseudonym_payload, user=user_payload)
        new_user.edit(user={'terms_of_use': True})
        
        print(f"SUCCESS: Created new Canvas user '{new_user.name}' with ID: {new_user.id}")
        return new_user

    except Exception as e:
        print(f"CRITICAL: An exception occurred during get_or_create_canvas_user: {e}")
        return None


def create_course(course_name):
    """
    Creates a new, standard Canvas course. Used for student enrollments.
    This does NOT use a template.
    """
    canvas = initialize_canvas_api()
    if not canvas: return None

    try:
        account = canvas.get_account(CANVAS_ACCOUNT_ID)
        sis_id = ''.join(e for e in course_name if e.isalnum()).lower()

        course_data = {'name': course_name, 'course_code': course_name, 'sis_course_id': sis_id}
        
        print(f"INFO: Attempting to create standard Canvas course '{course_name}'")
        new_course = account.create_course(course=course_data)
        # Return as a dictionary to match the expected format in monday_tasks
        return {'id': new_course.id, 'name': new_course.name}

    except Conflict:
        print(f"INFO: Course with SIS ID '{sis_id}' may already exist. Searching...")
        courses = list(account.get_courses(sis_course_id=sis_id))
        if courses:
            print(f"SUCCESS: Found existing course '{courses[0].name}' with ID {courses[0].id}.")
            return {'id': courses[0].id, 'name': courses[0].name}
        print(f"ERROR: Conflict creating course '{course_name}', but could not find existing one.")
        return None
    except Exception as e:
        print(f"ERROR: An API error occurred during course creation: {e}")
        return None


# --- Workflow-Specific Functions ---

def enroll_user(course_id, user_id, role):
    """
    Enrolls a user in a course with a specific role ('StudentEnrollment', 'TeacherEnrollment', etc.).
    This is the single function for all enrollment actions.
    """
    canvas = initialize_canvas_api()
    if not canvas: return False

    if role not in ['StudentEnrollment', 'TeacherEnrollment']:
        print(f"ERROR: Invalid enrollment role '{role}'.")
        return False
        
    try:
        course = canvas.get_course(course_id)
        user = canvas.get_user(user_id)
        
        enrollment_details = {'enrollment_state': 'active', 'notify': False}
        enrollment = course.enroll_user(user, role, enrollment=enrollment_details)
        
        print(f"SUCCESS: Enrolled user {user.name} ({user.id}) in course {course.name} ({course.id}) as {role}.")
        return True
    except CanvasException as e:
        if "already" in str(e).lower():
            print(f"INFO: User {user_id} is already enrolled in course {course_id}.")
            return True # Treat as success
        print(f"ERROR: API error during enrollment: {e}")
        return False


def unenroll_user(course_id, user_id):
    """
    Unenrolls a user from a course by concluding their enrollment.
    This is the single function for all un-enrollment actions.
    """
    canvas = initialize_canvas_api()
    if not canvas: return False
        
    try:
        course = canvas.get_course(course_id)
        enrollments = course.get_enrollments(user_id=user_id)
        
        active_enrollments = [e for e in enrollments if e.enrollment_state == 'active']
        if not active_enrollments:
            print(f"INFO: No active enrollments found for user {user_id} in course {course_id}.")
            return True

        for enrollment in active_enrollments:
            print(f"INFO: Concluding enrollment {enrollment.id} for user {user_id} in course {course_id}.")
            enrollment.deactivate(task='conclude')
        
        return True
    except Exception as e:
        print(f"ERROR: API error during un-enrollment for user {user_id}: {e}")
        return False

# ----- Functions below this line are for the legacy teacher webhook -----
# ----- They are kept for compatibility but should be updated eventually -----

def create_templated_course(course_name, term_id):
    """
    Creates a Course using a specific sub-account and template.
    This is used by the teacher enrollment webhook.
    """
    canvas = initialize_canvas_api()
    if not canvas: return None

    if not CANVAS_SUBACCOUNT_ID or not CANVAS_TEMPLATE_COURSE_ID:
        print("ERROR: Missing CANVAS_SUBACCOUNT_ID or CANVAS_TEMPLATE_COURSE_ID for templated course creation.")
        return None

    try:
        account = canvas.get_account(CANVAS_SUBACCOUNT_ID)
        sis_id_name = ''.join(e for e in course_name if e.isalnum() or e in ['+', '-']).replace(' ', '_').lower()
        sis_id = f"{sis_id_name}_{term_id}"
        course_data = { 'name': course_name, 'course_code': course_name, 'enrollment_term_id': f"sis_term_id:{term_id}", 'sis_course_id': sis_id, 'source_course_id': CANVAS_TEMPLATE_COURSE_ID }

        print(f"INFO: Creating templated course '{course_name}' in sub-account '{CANVAS_SUBACCOUNT_ID}'.")
        return account.create_course(course=course_data)
    except Conflict:
        print(f"INFO: Templated course with SIS ID '{sis_id}' may already exist. Searching...")
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
        return enroll_user(course_id, teacher_user.id, "TeacherEnrollment")
    return False


def unenroll_teacher(course_id, teacher_details):
    """Legacy function to find and unenroll a teacher."""
    teacher_user = get_or_create_canvas_user(teacher_details)
    if teacher_user:
        return unenroll_user(course_id, teacher_user.id)
    return False
