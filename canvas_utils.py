import os
import requests
from canvasapi import Canvas
from canvasapi.course import Course
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist
from canvasapi.enrollment import Enrollment

# --- Canvas API Configuration ---
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")

def initialize_canvas_api():
    """Initializes and returns a Canvas API object if configured."""
    if not CANVAS_API_URL or not CANVAS_API_KEY:
        print("ERROR: Canvas API URL or Key is not set.")
        return None
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY)

def create_canvas_user(student_details):
    """Creates a new user in Canvas, including an explicit communication channel."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        account = canvas.get_account(1)
        print(f"INFO: Attempting to create new Canvas user for email: {student_details['email']}")
        user_payload = {
            'user': {'name': student_details['name'], 'terms_of_use': True},
            'pseudonym': {
                'unique_id': student_details['email'],
                'sis_user_id': student_details['ssid'],
                'authentication_provider_id': '112'
            },
            'communication_channel': {
                'type': 'email',
                'address': student_details['email'],
                'skip_confirmation': True
            }
        }
        new_user = account.create_user(**user_payload)
        print(f"SUCCESS: Created new user '{student_details['name']}' with ID: {new_user.id}")
        return new_user
    except CanvasException as e:
        print(f"ERROR: API error during user creation: {e}")
        return None

def update_user_ssid(user, new_ssid):
    """Updates the SIS User ID for an existing Canvas user."""
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
    """Creates a new course in Canvas, handling conflicts."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    account = canvas.get_account(1)
    sis_id = f"{course_name.replace(' ', '_').lower()}_{term_id}"
    course_data = {
        'name': course_name, 'course_code': course_name,
        'enrollment_term_id': f"sis_term_id:{term_id}", 'sis_course_id': sis_id
    }
    try:
        return account.create_course(course=course_data)
    except Conflict:
        response = account._requester.request("GET", f"accounts/{account.id}/courses", params={'sis_course_id': sis_id})
        for course_data in response.json():
            if str(course_data.get("enrollment_term_id")) == str(term_id) and course_data.get("name") == course_name:
                return Course(account._requester, course_data)
    except CanvasException as e:
        print(f"ERROR: Unexpected API error during course creation for '{course_name}': {e}")
    return None

def create_section_if_not_exists(course_id, section_name):
    """Finds a section by name or creates it if it doesn't exist."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        course = canvas.get_course(course_id)
        for section in course.get_sections():
            if section.name.lower() == section_name.lower():
                return section
        return course.create_course_section(course_section={'name': section_name})
    except CanvasException as e:
        print(f"ERROR: API error finding/creating section '{section_name}': {e}")
    return None

def enroll_student_in_section(course_id, user_id, section_id):
    """Enrolls a student and verifies the enrollment."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        course, user = canvas.get_course(course_id), canvas.get_user(user_id)
        enrollment = course.enroll_user(user, "StudentEnrollment", enrollment={'course_section_id': section_id})
        return course.get_enrollment(enrollment.id)
    except CanvasException as e:
        if "already" in str(e).lower():
            return "Already Enrolled"
        print(f"ERROR: A Canvas API error occurred during enrollment: {e}")
    return None

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    """Finds or creates a user with robust logic, then enrolls them."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    user = None
    try:
        user = canvas.get_user(student_details['email'], 'login_id')
    except ResourceDoesNotExist:
        if student_details.get('ssid'):
            try:
                user = canvas.get_user(f"sis_user_id:{student_details['ssid']}")
            except ResourceDoesNotExist:
                pass
    if not user:
        user = create_canvas_user(student_details)
    if user:
        if hasattr(user, 'sis_user_id') and user.sis_user_id != student_details['ssid']:
            update_user_ssid(user, student_details['ssid'])
        return enroll_student_in_section(course_id, user.id, section_id)
    print(f"CRITICAL: User '{student_details['email']}' could not be found or created. Aborting enrollment.")
    return None

def unenroll_student_from_course(course_id, student_email):
    """Concludes a student's enrollment in a Canvas course."""
    canvas = initialize_canvas_api()
    if not canvas: return False
    try:
        user = canvas.get_user(student_email, 'login_id')
        course = canvas.get_course(course_id)
        if enrollments := course.get_enrollments(user_id=user.id):
            enrollments[0].deactivate(task='conclude')
        return True
    except CanvasException as e:
        if "not found" not in str(e):
            print(f"ERROR: API error during un-enrollment for '{student_email}': {e}")
        return True
