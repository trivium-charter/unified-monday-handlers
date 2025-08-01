import os
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException
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
    """Searches for a course by SIS ID and Term ID, creating it only if it doesn't exist."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        account = canvas.get_account(1)
        sis_id = f"{course_name.replace(' ', '_').lower()}_{term_id}"
        
        # Step 1: Search for an existing course with the same SIS ID.
        print(f"INFO: Searching for existing course with SIS ID '{sis_id}' in term '{term_id}'...")
        existing_courses = list(account.get_courses(sis_course_id=sis_id))
        
        if existing_courses:
            for course in existing_courses:
                if str(course.enrollment_term_id) == str(term_id):
                    print(f"INFO: Found existing course '{course.name}' (ID: {course.id}) in the correct term.")
                    if course.name != course_name:
                        print(f"CRITICAL: Found course with matching SIS & Term, but name is different ('{course.name}'). Aborting.")
                        return None
                    return course
            
            print(f"CRITICAL: Course with SIS ID '{sis_id}' exists, but NOT in the requested term '{term_id}'. Aborting to prevent using an old course.")
            return None

        # Step 2: If no course was found, create a new one.
        print(f"INFO: No existing course found. Creating a new course named '{course_name}'.")
        new_course = account.create_course(course={'name': course_name, 'course_code': course_name, 'enrollment_term_id': term_id, 'sis_course_id': sis_id})
        
        print(f"SUCCESS: Successfully created and verified course '{new_course.name}' with ID: {new_course.id}")
        return new_course
        
    except CanvasException as e:
        print(f"ERROR: CANVAS_UTILS - API error during course search/creation for '{course_name}': {e}")
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
                return section
        
        new_section = course.create_course_section(course_section={'name': section_name})
        print(f"SUCCESS: CANVAS_UTILS - Created section '{new_section.name}' (ID: {new_section.id}).")
        return new_section
    except CanvasException as e:
        print(f"ERROR: CANVAS_UTILS - API error finding/creating section '{section_name}': {e}")
        return None

def enroll_student_in_section(course_id, user, section_id):
    """Enrolls a student and verifies the enrollment was successfully created."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        course = canvas.get_course(course_id)
        print(f"INFO: Enrolling user '{user.name}' into course {course_id}, section {section_id}.")
        
        provisional_enrollment = course.enroll_user(user, "StudentEnrollment", enrollment={'course_section_id': section_id})
        print(f"INFO: Enrollment reported success with provisional ID: {provisional_enrollment.id}. Verifying...")

        # --- VERIFICATION STEP ---
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
            # If already enrolled, let's return that enrollment object after finding it.
            enrollments = course.get_enrollments(user_id=user.id)
            if enrollments:
                print(f"INFO: User '{user.name}' is already enrolled.")
                return enrollments[0]
            return "Already Enrolled"
        raise e

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    """Manager function to enroll a student, creating or updating the user as needed."""
    canvas = initialize_canvas_api()
    if not canvas: return None

    try:
        user = canvas.get_user(student_details['email'], 'login_id')
    except CanvasException as e:
        if "not found" in str(e):
            print(f"INFO: User '{student_details['email']}' not found. Attempting to create user.")
            user = create_canvas_user(student_details)
        else:
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
        enrollments = course.get_enrollments(user_id=user.id)
        if not enrollments:
            return True
        
        enrollments[0].deactivate(task='conclude')
        return True
    except CanvasException as e:
        if "not found" in str(e):
            return True
        print(f"ERROR: API error during un-enrollment for '{student_email}': {e}")
        return False
