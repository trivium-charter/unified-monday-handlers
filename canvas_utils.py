#
# This is the complete and correct code for canvas_utils.py
#
import os
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceNotFound

# --- Canvas API Configuration ---
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_ACCOUNT_ID = os.environ.get("CANVAS_ACCOUNT_ID", "1")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

def initialize_canvas_api():
    """Initializes and returns a Canvas API object."""
    if not CANVAS_API_URL or not CANVAS_API_KEY:
        print("ERROR: Canvas API URL or Key is not set in environment variables.")
        return None
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY)

def get_or_create_canvas_user(details):
    """Finds or creates a Canvas user using a robust, prioritized search."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    user_email, user_ssid, user_name = details.get('email'), details.get('ssid'), details.get('name')
    if not user_email or not user_name:
        print(f"CRITICAL: User details missing name or email: {details}")
        return None
    try:
        account = canvas.get_account(CANVAS_ACCOUNT_ID)
        # 1. Search by SIS ID
        if user_ssid:
            print(f"INFO: [Lookup 1/3] Searching by sis_user_id: {user_ssid}")
            users = list(account.get_users(sis_user_id=user_ssid))
            if users:
                print(f"SUCCESS: Found user '{users[0].name}' by SIS ID.")
                return users[0]
        # 2. Search by Login ID
        print(f"INFO: [Lookup 2/3] Searching by login_id: {user_email}")
        users = list(account.get_users(sis_login_id=user_email))
        if users:
            print(f"SUCCESS: Found user '{users[0].name}' by Login ID.")
            return users[0]
        # 3. Create User
        print("INFO: All lookup methods failed. Creating new user.")
        sortable_name = f"{user_name.split(', ')[1]}, {user_name.split(', ')[0]}" if ', ' in user_name else user_name
        user_payload = {'name': user_name, 'sortable_name': sortable_name}
        pseudonym_payload = {'unique_id': user_email, 'sis_user_id': user_ssid, 'send_confirmation': False}
        new_user = account.create_user(pseudonym=pseudonym_payload, user=user_payload)
        new_user.edit(user={'terms_of_use': True})
        print(f"SUCCESS: Created new Canvas user '{new_user.name}' with ID: {new_user.id}")
        return new_user
    except Exception as e:
        print(f"CRITICAL: Exception in get_or_create_canvas_user: {e}")
        return None

def create_course(course_name):
    """Creates a new, standard Canvas course. Not from a template."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        account = canvas.get_account(CANVAS_ACCOUNT_ID)
        sis_id = ''.join(e for e in course_name if e.isalnum()).lower()
        course_data = {'name': course_name, 'course_code': course_name, 'sis_course_id': sis_id}
        print(f"INFO: Creating standard Canvas course '{course_name}'")
        new_course = account.create_course(course=course_data)
        return {'id': new_course.id, 'name': new_course.name}
    except Conflict:
        print(f"INFO: Course with SIS ID '{sis_id}' exists. Searching...")
        courses = list(account.get_courses(sis_course_id=sis_id))
        if courses:
            return {'id': courses[0].id, 'name': courses[0].name}
        return None
    except Exception as e:
        print(f"ERROR: API error during course creation: {e}")
        return None

def enroll_user(course_id, user_id, role):
    """Enrolls any user in a course with a specific role."""
    canvas = initialize_canvas_api()
    if not canvas: return False
    try:
        course = canvas.get_course(course_id)
        enrollment = course.enroll_user(user_id, role, enrollment={'enrollment_state': 'active', 'notify': False})
        print(f"SUCCESS: Enrolled user {user_id} in course {course_id} as {role}.")
        return True
    except CanvasException as e:
        if "already" in str(e).lower():
            print(f"INFO: User {user_id} is already enrolled in course {course_id}.")
            return True
        print(f"ERROR: API error during enrollment: {e}")
        return False

def unenroll_user(course_id, user_id):
    """Unenrolls any user from a course by concluding their enrollment."""
    canvas = initialize_canvas_api()
    if not canvas: return False
    try:
        course = canvas.get_course(course_id)
        enrollments = list(course.get_enrollments(user_id=user_id))
        active_enrollments = [e for e in enrollments if e.enrollment_state == 'active']
        if not active_enrollments:
            print(f"INFO: No active enrollments for user {user_id} in course {course_id}.")
            return True
        for enrollment in active_enrollments:
            enrollment.deactivate(task='conclude')
            print(f"SUCCESS: Concluded enrollment {enrollment.id} for user {user_id} in course {course_id}.")
        return True
    except Exception as e:
        print(f"ERROR: API error during un-enrollment: {e}")
        return False

# --- LEGACY TEACHER FUNCTIONS ---
def create_templated_course(course_name, term_id):
    """Creates a Course using a specific sub-account and template."""
    canvas = initialize_canvas_api()
    if not canvas or not CANVAS_SUBACCOUNT_ID or not CANVAS_TEMPLATE_COURSE_ID: return None
    try:
        account = canvas.get_account(CANVAS_SUBACCOUNT_ID)
        sis_id = f"{''.join(e for e in course_name if e.isalnum())}_{term_id}".lower()
        course_data = {'name': course_name, 'course_code': course_name, 'enrollment_term_id': f"sis_term_id:{term_id}", 'sis_course_id': sis_id, 'source_course_id': CANVAS_TEMPLATE_COURSE_ID}
        return account.create_course(course=course_data)
    except Conflict:
        account = canvas.get_account(CANVAS_SUBACCOUNT_ID)
        courses = list(account.get_courses(sis_course_id=sis_id))
        return courses[0] if courses else None
    except Exception as e:
        print(f"ERROR: API error during templated course creation: {e}")
        return None

def enroll_teacher(course_id, teacher_details):
    """Legacy function to find/create and enroll a teacher."""
    teacher_user = get_or_create_canvas_user(teacher_details)
    return enroll_user(course_id, teacher_user.id, "TeacherEnrollment") if teacher_user else False

def unenroll_teacher(course_id, teacher_details):
    """Legacy function to find and unenroll a teacher."""
    teacher_user = get_or_create_canvas_user(teacher_details)
    return unenroll_user(course_id, teacher_user.id) if teacher_user else False
