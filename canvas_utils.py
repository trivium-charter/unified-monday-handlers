import os
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException

# --- Canvas API Configuration ---
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")

def initialize_canvas_api():
    """Initializes and returns a Canvas API object if configured."""
    if not CANVAS_API_URL or not CANVAS_API_KEY:
        print("ERROR: CANVAS_UTILS - Canvas API URL or Key is not set.")
        return None
    try:
        return Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    except Exception as e:
        print(f"ERROR: CANVAS_UTILS - Failed to initialize Canvas API: {e}")
        return None

def create_canvas_user(student_details):
    """Creates a new user in Canvas with a Google SSO authentication provider."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    
    try:
        account = canvas.get_account(1)
        print(f"INFO: CANVAS_UTILS - Creating new Canvas user for email: {student_details['email']}")
        
        pseudonym_data = {
            'unique_id': student_details['email'],
            'sis_user_id': student_details['ssid'], 
            'login_id': student_details['email'], 
            'authentication_provider_id': '112'
        }

        # --- MODIFIED SECTION ---
        # Directly call the API to handle list-based responses
        response = account._requester.request(
            "POST",
            f"accounts/{account.id}/users",
            user={'name': student_details['name']},
            pseudonym=pseudonym_data
        )
        
        response_data = response.json()
        if isinstance(response_data, list) and response_data:
            user_attributes = response_data[0]
        else:
            user_attributes = response_data

        new_user = canvas.get_user(user_attributes['id'])
        # --- END MODIFIED SECTION ---
        
        print(f"SUCCESS: CANVAS_UTILS - Created new user '{new_user.name}' with ID: {new_user.id} and SIS ID: {student_details['ssid']}")
        return new_user
    except CanvasException as e:
        print(f"ERROR: CANVAS_UTILS - API error creating user '{student_details['email']}': {e}")
        return None

def update_user_ssid(user, new_ssid):
    """Updates the SIS User ID for an existing Canvas user."""
    try:
        logins = user.get_logins()
        if logins:
            login_to_update = logins[0]
            print(f"INFO: CANVAS_UTILS - Updating SSID for user '{user.name}' from '{login_to_update.sis_user_id}' to '{new_ssid}'.")
            login_to_update.edit(login={'sis_user_id': new_ssid})
            return True
        return False
    except CanvasException as e:
        print(f"ERROR: CANVAS_UTILS - API error updating SSID for user '{user.name}': {e}")
        return False

def create_canvas_course(course_name, term_id):
    """Creates a new course in Canvas."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        account = canvas.get_account(1)
        print(f"INFO: CANVAS_UTILS - Creating course '{course_name}' in term '{term_id}'.")
        course_data = {
            'name': course_name,
            'course_code': course_name,
            'enrollment_term_id': term_id,
            'sis_course_id': f"{course_name.replace(' ', '_').lower()}_{term_id}"
        }
        
        response = account._requester.request(
            "POST",
            f"accounts/{account.id}/courses",
            course=course_data
        )
        
        response_data = response.json()
        if isinstance(response_data, list) and response_data:
            course_attributes = response_data[0]
        else:
            course_attributes = response_data

        new_course = canvas.get_course(course_attributes['id'])

        print(f"SUCCESS: CANVAS_UTILS - Created Canvas course '{new_course.name}' with ID: {new_course.id}")
        return new_course
    except CanvasException as e:
        print(f"ERROR: CANVAS_UTILS - API error creating course '{course_name}': {e}")
        return None

def create_section_if_not_exists(course_id, section_name):
    """Finds a section by name or creates it if it doesn't exist."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        course = canvas.get_course(course_id)
        sections = course.get_sections()
        for section in sections:
            if section.name.lower() == section_name.lower():
                print(f"INFO: CANVAS_UTILS - Found existing section '{section.name}' (ID: {section.id}) in course {course_id}.")
                return section
        
        print(f"INFO: CANVAS_UTILS - Section '{section_name}' not found. Creating new section in course {course_id}.")
        
        response = course._requester.request(
            "POST",
            f"courses/{course.id}/sections",
            course_section={'name': section_name}
        )
        
        response_data = response.json()
        if isinstance(response_data, list) and response_data:
            section_attributes = response_data[0]
        else:
            section_attributes = response_data
            
        new_section = course.get_section(section_attributes['id'])

        print(f"SUCCESS: CANVAS_UTILS - Created section '{new_section.name}' (ID: {new_section.id}).")
        return new_section
    except CanvasException as e:
        print(f"ERROR: CANVAS_UTILS - API error finding/creating section '{section_name}': {e}")
        return None

def enroll_student_in_section(course_id, student_email, section_id):
    """Enrolls a student into a specific section of a Canvas course using their email."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        course = canvas.get_course(course_id)
        print(f"INFO: CANVAS_UTILS - Enrolling user '{student_email}' into course {course_id}, section {section_id}.")
        enrollment = course.enroll_user(
            user={'login_id': student_email},
            enrollment_type='Student',
            enrollment={'course_section_id': section_id}
        )
        print(f"SUCCESS: CANVAS_UTILS - Enrolled user '{student_email}' in section {section_id}. Enrollment ID: {enrollment.id}")
        return enrollment
    except CanvasException as e:
        if "already" in str(e).lower():
            print(f"INFO: CANVAS_UTILS - User '{student_email}' is already enrolled in course {course_id}. No action needed.")
            return "Already Enrolled"
        raise e

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    """Manager function to enroll a student, creating or updating the user as needed."""
    canvas = initialize_canvas_api()
    if not canvas: return None

    user = None
    try:
        user = canvas.get_user(student_details['email'], 'login_id')
        
        if user.sis_user_id != student_details['ssid']:
            print(f"INFO: CANVAS_UTILS - SSID mismatch for {student_details['email']}. Canvas: '{user.sis_user_id}', Monday: '{student_details['ssid']}'.")
            update_user_ssid(user, student_details['ssid'])
        
    except CanvasException as e:
        if "not found" in str(e).lower():
            print(f"INFO: CANVAS_UTILS - User '{student_details['email']}' not found. Attempting to create user.")
            user = create_canvas_user(student_details)
            if not user:
                print(f"ERROR: CANVAS_UTILS - Failed to create user. Enrollment aborted for section {section_id}.")
                return None
        else:
            print(f"ERROR: CANVAS_UTILS - API error while getting user '{student_details['email']}': {e}")
            return None

    try:
        return enroll_student_in_section(course_id, student_details['email'], section_id)
    except CanvasException as e:
        print(f"ERROR: CANVAS_UTILS - A final Canvas API error occurred during enrollment for '{student_details['email']}': {e}")
        return None


def unenroll_student_from_course(course_id, student_email):
    """Concludes (deactivates) a student's enrollment in a Canvas course."""
    canvas = initialize_canvas_api()
    if not canvas: return False
    try:
        course = canvas.get_course(course_id)
        user = canvas.get_user(student_email, 'login_id')
        enrollments = course.get_enrollments(user_id=user.id)
        if not enrollments:
            print(f"WARNING: CANVAS_UTILS - User {student_email} has no enrollment in course {course_id} to conclude.")
            return True
        
        enrollment_to_conclude = enrollments[0]
        print(f"INFO: CANVAS_UTILS - Concluding enrollment for user '{student_email}' in course {course_id}.")
        enrollment_to_conclude.deactivate(task='conclude')
        return True
    except CanvasException as e:
        if "not found" in str(e).lower():
            print(f"WARNING: CANVAS_UTILS - User with email '{student_email}' not found in Canvas. Cannot unenroll.")
            return True
        print(f"ERROR: CANVAS_UTILS - API error during un-enrollment for '{student_email}': {e}")
        return False
