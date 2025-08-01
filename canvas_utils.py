import os
import requests
from canvasapi import Canvas
from canvasapi.course import Course
from canvasapi.exceptions import CanvasException
from canvasapi.enrollment import Enrollment

CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")

def initialize_canvas_api():
    if not CANVAS_API_URL or not CANVAS_API_KEY: return None
    try: return Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    except Exception as e: print(f"ERROR: CANVAS_UTILS - Failed to initialize Canvas API: {e}"); return None

def create_canvas_user(student_details):
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        account = canvas.get_account(1)
        pseudonym_data = {'unique_id': student_details['email'], 'sis_user_id': student_details['ssid'], 'login_id': student_details['email'], 'authentication_provider_id': '112'}
        response = account._requester.request("POST", f"accounts/{account.id}/users", user={'name': student_details['name']}, pseudonym=pseudonym_data)
        user_attributes = response.json()[0] if isinstance(response.json(), list) else response.json()
        new_user = canvas.get_user(user_attributes['id'])
        print(f"SUCCESS: CANVAS_UTILS - Created and verified new user for '{student_details['name']}' with new ID: {new_user.id}")
        return new_user
    except CanvasException as e: print(f"ERROR: CANVAS_UTILS - API error creating user '{student_details['email']}': {e}"); return None

def update_user_ssid(user, new_ssid):
    try:
        if logins := user.get_logins():
            logins[0].edit(login={'sis_user_id': new_ssid})
            return True
    except CanvasException as e: print(f"ERROR: CANVAS_UTILS - API error updating SSID for user '{user.name}': {e}"); return False

def create_canvas_course(course_name, term_id):
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        account = canvas.get_account(1)
        sis_id = f"{course_name.replace(' ', '_').lower()}_{term_id}"
        print(f"INFO: Searching for existing course with SIS ID '{sis_id}' in term '{term_id}'...")
        response = account._requester.request("GET", f"accounts/{account.id}/courses", params={'sis_course_id': sis_id})
        if courses_data := response.json():
            for course_data in courses_data:
                if str(course_data.get("enrollment_term_id")) == str(term_id):
                    course = Course(account._requester, course_data)
                    if course.name != course_name:
                        print(f"CRITICAL: Found course with matching SIS & Term, but name is different ('{course.name}'). Aborting.")
                        return None
                    print(f"INFO: Found existing course '{course.name}' (ID: {course.id}) in the correct term.")
                    return course
            print(f"CRITICAL: Course with SIS ID '{sis_id}' exists, but NOT in the requested term '{term_id}'. Aborting.")
            return None
        print(f"INFO: No existing course found. Creating a new course named '{course_name}'.")
        new_course = account.create_course(course={'name': course_name, 'course_code': course_name, 'enrollment_term_id': term_id, 'sis_course_id': sis_id})
        print(f"SUCCESS: Successfully created and verified course '{new_course.name}' with ID: {new_course.id}")
        return new_course
    except CanvasException as e: print(f"ERROR: API error during course search/creation for '{course_name}': {e}"); return None

def create_section_if_not_exists(course_id, section_name):
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        course = canvas.get_course(course_id)
        for section in course.get_sections():
            if section.name.lower() == section_name.lower(): return section
        new_section = course.create_course_section(course_section={'name': section_name})
        print(f"SUCCESS: CANVAS_UTILS - Created section '{new_section.name}' (ID: {new_section.id}).")
        return new_section
    except CanvasException as e: print(f"ERROR: CANVAS_UTILS - API error finding/creating section '{section_name}': {e}"); return None

def enroll_student_in_section(course_id, user_id, section_id):
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        course, user = canvas.get_course(course_id), canvas.get_user(user_id)
        print(f"INFO: Enrolling user '{user.name}' into course {course_id}, section {section_id}.")
        enrollment = course.enroll_user(user, "StudentEnrollment", enrollment={'course_section_id': section_id})
        print(f"INFO: Enrollment reported success with provisional ID: {enrollment.id}. Verifying...")
        try:
            verified = canvas.get_enrollment(enrollment.id)
            print(f"SUCCESS: Verified enrollment for user '{user.name}'. Enrollment ID: {verified.id}")
            return verified
        except CanvasException as e:
            if "404" in str(e): print(f"CRITICAL: Enrollment verification failed! API reported success, but enrollment {enrollment.id} was not found."); return None
            else: raise e
    except CanvasException as e:
        if "already" in str(e).lower():
            if enrollments := course.get_enrollments(user_id=user_id):
                print(f"INFO: User '{user_id}' is already enrolled.")
                return enrollments[0]
        raise e

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    canvas = initialize_canvas_api()
    if not canvas: return None
    try: user = canvas.get_user(student_details['email'], 'login_id')
    except CanvasException as e:
        if "not found" in str(e): user = create_canvas_user(student_details)
        else: print(f"ERROR: API error while getting user '{student_details['email']}': {e}"); return None
    if not user: print(f"ERROR: User object could not be retrieved/created. Aborting."); return None
    if hasattr(user, 'sis_user_id') and user.sis_user_id != student_details['ssid']: update_user_ssid(user, student_details['ssid'])
    try: return enroll_student_in_section(course_id, user.id, section_id)
    except CanvasException as e: print(f"ERROR: A final Canvas API error occurred during enrollment: {e}"); return None

def unenroll_student_from_course(course_id, student_email):
    canvas = initialize_canvas_api()
    if not canvas: return False
    try:
        user = canvas.get_user(student_email, 'login_id')
        course = canvas.get_course(course_id)
        if enrollments := course.get_enrollments(user_id=user.id):
            enrollments[0].deactivate(task='conclude')
        return True
    except CanvasException as e:
        if "not found" not in str(e): print(f"ERROR: API error during un-enrollment for '{student_email}': {e}"); return False
        return True
