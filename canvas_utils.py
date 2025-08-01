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

        response = account._requester.request(
            "POST",
            f"accounts/{account.id}/users",
            user={'name': student_details['name']},
            pseudonym=pseudonym_data
        )
        
        response_data = response.json()
        user_attributes = response_data[0] if isinstance(response_data, list) and response_data else response_data

        new_user = canvas.get_user(user_attributes['id'])
        
        print(f"SUCCESS: CANVAS_UTILS - Created and verified new user for '{student_details['name']}' with new ID: {new_user.id}")
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
    """
    Creates a new course in Canvas. If a course with the same SIS ID already exists,
    it verifies that course matches the name and term before using it.
    """
    canvas = initialize_canvas_api()
    if not canvas: return None
    
    account = canvas.get_account(1)
    sis_id = f"{course_name.replace(' ', '_').lower()}_{term_id}"
    
    course_data = {
        'name': course_name,
        'course_code': course_name,
        'enrollment_term_id': f"sis_term_id:{term_id}",
        'sis_course_id': sis_id
    }

    try:
        print(f"INFO: Attempting to create new course '{course_name}' with SIS ID '{sis_id}'.")
        new_course = account.create_course(course=course_data)
        print(f"SUCCESS: Successfully created new course '{new_course.name}' with ID: {new_course.id}")
        return new_course

    except Conflict:
        print(f"INFO: A course with SIS ID '{sis_id}' already exists. Verifying it meets requirements...")
        
        response = account._requester.request("GET", f"accounts/{account.id}/courses", params={'sis_course_id': sis_id})
        
        if not (courses_data := response.json()):
             print(f"CRITICAL: Canvas reported a conflict, but no course could be found with SIS ID '{sis_id}'.")
             return None

        for course_data in courses_data:
            if str(course_data.get("enrollment_term_id")) == str(term_id):
                existing_course = Course(account._requester, course_data)
                if existing_course.name == course_name:
                    print(f"INFO: Verified existing course '{existing_course.name}' (ID: {existing_course.id}) is correct.")
                    return existing_course
                else:
                    print(f"CRITICAL: Name mismatch. Found course '{existing_course.name}' but expected '{course_name}'. Aborting.")
                    return None
        
        print(f"CRITICAL: A course with SIS ID '{sis_id}' exists, but it is not in the correct term '{term_id}'. Aborting.")
        return None

    except CanvasException as e:
        print(f"ERROR: An unexpected API error occurred during course creation for '{course_name}': {e}")
        return None

def create_section_if_not_exists(course_id, section_name):
    """Finds a section by name or creates it if it doesn't exist."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    
    # --- MODIFIED LOGIC to handle Canvas API delays ---
    # Construct a course object locally without fetching it first.
    # The subsequent API calls will use the ID from this object.
    course = Course(canvas._requester, {'id': course_id})
    
    try:
        # Now, try to get the sections for this course
        sections = course.get_sections()
        for section in sections:
            if section.name.lower() == section_name.lower():
                return section
        
        new_section = course.create_course_section(course_section={'name': section_name})
        print(f"SUCCESS: CANVAS_UTILS - Created section '{new_section.name}' (ID: {new_section.id}).")
        return new_section
    except ResourceDoesNotExist:
        print(f"ERROR: CANVAS_UTILS - Course with ID '{course_id}' could not be found, even after creation. Aborting section creation.")
        return None
    except CanvasException as e:
        print(f"ERROR: CANVAS_UTILS - API error finding/creating section '{section_name}': {e}")
        return None

def enroll_student_in_section(course_id, user_id, section_id):
    """Enrolls a student and verifies the enrollment was successfully created."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        course, user = canvas.get_course(course_id), canvas.get_user(user_id)
        print(f"INFO: Enrolling user '{user.name}' into course {course_id}, section {section_id}.")
        
        provisional_enrollment = course.enroll_user(user, "StudentEnrollment", enrollment={'course_section_id': section_id})
        print(f"INFO: Enrollment reported success with provisional ID: {provisional_enrollment.id}. Verifying...")

        try:
            verified_enrollment = canvas.get_enrollment(provisional_enrollment.id)
            print(f"SUCCESS: Verified enrollment for user '{user.name}'. Enrollment ID: {verified_enrollment.id}")
            return verified_enrollment
        except CanvasException as e:
            if "404" in str(e):
                print(f"CRITICAL: Enrollment verification failed! API reported success, but enrollment {provisional_enrollment.id} was not found.")
                return None
            else:
                raise e
    except CanvasException as e:
        if "already" in str(e).lower():
            if enrollments := course.get_enrollments(user_id=user_id):
                print(f"INFO: User '{user_id}' is already enrolled.")
                return enrollments[0]
            return "Already Enrolled"
        raise e

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    """Manager function to enroll a student, creating or updating the user as needed."""
    canvas = initialize_canvas_api()
    if not canvas: return None

    user = None
    try:
        user = canvas.get_user(student_details['email'], 'login_id')
    except ResourceDoesNotExist:
        print(f"INFO: User '{student_details['email']}' not found. Attempting to create user.")
        user = create_canvas_user(student_details)
    except CanvasException as e:
        print(f"ERROR: API error while getting user '{student_details['email']}': {e}")
        return None

    if not user:
        print(f"ERROR: User object could not be retrieved or created. Aborting enrollment.")
        return None
    
    if hasattr(user, 'sis_user_id') and user.sis_user_id != student_details['ssid']:
        update_user_ssid(user, student_details['ssid'])

    try:
        return enroll_student_in_section(course_id, user.id, section_id)
    except CanvasException as e:
        print(f"ERROR: A final Canvas API error occurred during enrollment: {e}")
        return None

def unenroll_student_from_course(course_id, student_email):
    """Concludes (deactivates) a student's enrollment in a Canvas course."""
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
