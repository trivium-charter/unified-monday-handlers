import os
import requests
from canvasapi import Canvas
from canvasapi.course import Course
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist
from canvasapi.enrollment import Enrollment

# --- Canvas API Configuration ---
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_ACCOUNT_ID = os.environ.get("CANVAS_ACCOUNT_ID", "1") # Default to '1'

def initialize_canvas_api():
    """Initializes and returns a Canvas API object if configured."""
    if not CANVAS_API_URL or not CANVAS_API_KEY:
        print("ERROR: Canvas API URL or Key is not set.")
        return None
    return Canvas(CANVAS_API_URL, CANVAS_API_KEY)

def create_canvas_user(student_details):
    """Creates a new user in Canvas, including an explicit communication channel and login_id."""
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
                'login_id': student_details['email'],
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

# In canvas_utils.py

# In canvas_utils.py

# In canvas_utils.py

# In canvas_utils.py

def create_canvas_course(course_name, term_id):
    """Creates a new course in a specific sub-account and uses a template course."""
    canvas = initialize_canvas_api()
    if not canvas: return None

    # Get the new environment variables
    subaccount_id = os.environ.get("CANVAS_SUBACCOUNT_ID")
    template_course_id = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

    if not subaccount_id or not template_course_id:
        print("ERROR: Missing CANVAS_SUBACCOUNT_ID or CANVAS_TEMPLATE_COURSE_ID environment variables.")
        return None

    try:
        # Get the specific sub-account object
        account = canvas.get_account(subaccount_id)
    except ResourceDoesNotExist:
        print(f"ERROR: Canvas Sub-Account with ID '{subaccount_id}' not found.")
        return None

    # This creates a more unique SIS ID to prevent the duplicates you were seeing
    sis_id_name = ''.join(e for e in course_name if e.isalnum() or e in ['+', '-']).replace(' ', '_').lower()
    sis_id = f"{sis_id_name}_{term_id}"

    course_data = {
        'name': course_name,
        'course_code': course_name,
        'enrollment_term_id': f"sis_term_id:{term_id}",
        'sis_course_id': sis_id,
        'source_course_id': template_course_id  # This tells Canvas to use your template
    }

    try:
        print(f"INFO: Attempting to create Canvas course '{course_name}' in sub-account '{subaccount_id}' using template '{template_course_id}'.")
        # Create the course within the sub-account
        new_course = account.create_course(course=course_data)
        
        # The copy process happens in the background, so we return the new course object immediately
        return new_course

    except Conflict:
        print(f"INFO: A course with SIS ID '{sis_id}' may already exist. Searching for it.")
        try:
            courses = account.get_courses(sis_course_id=sis_id)
            for course in courses:
                if course.sis_course_id == sis_id:
                    print(f"SUCCESS: Found existing course '{course.name}' with ID {course.id}.")
                    return course
        except Exception as e:
            print(f"ERROR: A conflict occurred but could not find the course. Error: {e}")
            
    except CanvasException as e:
        print(f"ERROR: An unexpected API error occurred during course creation: {e}")

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
    """Enrolls a student, making them active immediately."""
    canvas = initialize_canvas_api()
    if not canvas: return None
    try:
        course = canvas.get_course(course_id)
        user = canvas.get_user(user_id)
        enrollment_type = 'StudentEnrollment'
        enrollment_details = {
            'enrollment_state': 'active',
            'course_section_id': section_id,
            'notify': False
        }
        enrollment = course.enroll_user(user, enrollment_type, enrollment=enrollment_details)
        return enrollment
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
        print(f"INFO: Searching for user by email: {student_details['email']}")
        user = canvas.get_user(student_details['email'], 'login_id')
        print(f"SUCCESS: Found user by email with ID: {user.id}")
    except ResourceDoesNotExist:
        print("INFO: User not found by email. Trying SIS ID...")
        if student_details.get('ssid'):
            try:
                print(f"INFO: Searching for user by sis_user_id: {student_details['ssid']}")
                user = canvas.get_user(student_details['ssid'], 'sis_user_id')
                print(f"SUCCESS: Found user by SIS ID with ID: {user.id}")
            except ResourceDoesNotExist:
                print("INFO: User not found by SIS ID.")
                pass
    if not user:
        user = create_canvas_user(student_details)
    if user:
        if hasattr(user, 'sis_user_id') and user.sis_user_id != student_details['ssid']:
            update_user_ssid(user, student_details['ssid'])
        return enroll_student_in_section(course_id, user.id, section_id)
    print(f"CRITICAL: User '{student_details['email']}' could not be found or created. Aborting enrollment.")
    return None

def unenroll_student_from_course(course_id, student_details):
    """Deactivates active enrollments or deletes pending invitations for a student."""
    canvas = initialize_canvas_api()
    if not canvas: return False
    
    user = None
    student_email = student_details.get('email')
    
    try:
        user = canvas.get_user(student_email, 'login_id')
    except ResourceDoesNotExist:
        if student_details.get('ssid'):
            try:
                user = canvas.get_user(student_details['ssid'], 'sis_user_id')
            except ResourceDoesNotExist:
                pass

    if not user:
        print(f"INFO: User '{student_email}' not found in Canvas. No action taken.")
        return True

    try:
        course = canvas.get_course(course_id)
        enrollments = course.get_enrollments(user_id=user.id)
        
        if not enrollments:
            print(f"INFO: No enrollments found for '{student_email}' in course {course_id}.")
            return True

        for enrollment in enrollments:
            print(f"INFO: Concluding enrollment for '{student_email}' (Enrollment ID: {enrollment.id}).")
            enrollment.deactivate(task='conclude')
        
        return True
    except CanvasException as e:
        print(f"ERROR: API error during un-enrollment for '{student_email}': {e}")
        return False
# In canvas_utils.py

# Add these two functions to the end of canvas_utils.py

def enroll_or_create_and_enroll_teacher(course_id, teacher_details):
    """
    Finds or creates a user (teacher) with robust 3-step logic, then enrolls them as a Teacher.
    This version includes a 3-step lookup to find 'ghost' users before attempting creation.
    """
    canvas = initialize_canvas_api()
    if not canvas: return None
    user = None
    teacher_email = teacher_details.get('email')
    
    if not teacher_email:
        print(f"CRITICAL: Teacher details are missing an email address. Cannot enroll.")
        return None

    # --- Robust 3-Step Lookup Logic ---
    try:
        # 1. Try to find user by their primary login ID (email)
        print(f"INFO: [Lookup 1/3] Searching for teacher by email (login_id): {teacher_email}")
        user = canvas.get_user(teacher_email, 'login_id')
        print(f"SUCCESS: Found teacher by login_id with ID: {user.id}")
    except ResourceDoesNotExist:
        print("INFO: Teacher not found by login_id.")
        try:
            # 2. If that fails, try to find them by their SIS ID
            print(f"INFO: [Lookup 2/3] Searching for teacher by sis_user_id: {teacher_email}")
            user = canvas.get_user(teacher_email, 'sis_user_id')
            print(f"SUCCESS: Found teacher by SIS ID with ID: {user.id}")
        except ResourceDoesNotExist:
            print("INFO: Teacher not found by SIS ID.")
            try:
                # 3. As a last resort, search the entire account for the user.
                print(f"INFO: [Lookup 3/3] Performing account-wide search for email: {teacher_email}")
                account = canvas.get_account(1) # Assumes the main account ID is 1
                possible_users = account.get_users(search_term=teacher_email)
                
                found_users = [u for u in possible_users if hasattr(u, 'login_id') and u.login_id.lower() == teacher_email.lower()]
                
                if len(found_users) == 1:
                    user = found_users[0]
                    print(f"SUCCESS: Found teacher via account-wide search with ID: {user.id}")
                else:
                    print(f"INFO: Account-wide search found {len(found_users)} matching users. Cannot proceed with this method.")
            except CanvasException as e:
                print(f"ERROR: An error occurred during account-wide user search: {e}")

    if not user:
        # Only create the user if ALL three lookups fail.
        print("INFO: All lookup methods failed. Proceeding to create a new user.")
        user = create_canvas_user({
            'name': teacher_details.get('name'), 
            'email': teacher_email, 
            'ssid': teacher_email # Use email as SIS ID for consistency
        })

    if user:
        try:
            course = canvas.get_course(course_id)
            enrollment = course.enroll_user(user, 'TeacherEnrollment', enrollment={'enrollment_state': 'active', 'notify': False})
            return enrollment
        except CanvasException as e:
            if "already" in str(e).lower():
                print(f"INFO: User {teacher_email} is already enrolled as a teacher in course {course_id}.")
                return "Already Enrolled as Teacher"
            print(f"ERROR: A Canvas API error occurred during teacher enrollment: {e}")
            return None
    
    print(f"CRITICAL: Teacher with email '{teacher_email}' could not be found or created. Aborting enrollment.")
    return None


def unenroll_teacher_from_course(course_id, teacher_details):
    """
    Finds an existing teacher's enrollment in a course and deactivates (unenrolls) it.
    """
    canvas = initialize_canvas_api()
    if not canvas: return None
    
    teacher_email = teacher_details.get('email')
    if not teacher_email:
        print(f"CRITICAL: Teacher details are missing an email address for unenrollment.")
        return None
    
    try:
        # User must exist to be unenrolled. Use the most reliable search method first.
        account = canvas.get_account(1)
        possible_users = account.get_users(search_term=teacher_email)
        found_users = [u for u in possible_users if hasattr(u, 'login_id') and u.login_id.lower() == teacher_email.lower()]
        
        if len(found_users) != 1:
            print(f"ERROR: Cannot unenroll. Found {len(found_users)} users for email {teacher_email}.")
            return None
        
        user = found_users[0]
        course = canvas.get_course(course_id)
        
        # Get all enrollments for this user in this course
        enrollments = course.get_enrollments(user_id=user.id)
        teacher_enrollment = next((e for e in enrollments if e.type == 'TeacherEnrollment'), None)

        if teacher_enrollment:
            print(f"INFO: Found teacher enrollment {teacher_enrollment.id} for user {user.id} in course {course_id}. Deactivating.")
            teacher_enrollment.deactivate() # Deactivate is the "unenroll" action
            return True
        else:
            print(f"WARNING: No active teacher enrollment found for user {user.id} in course {course_id} to unenroll.")
            return False
            
    except Exception as e:
        print(f"ERROR: Exception during teacher unenrollment: {e}")
        return False
def get_or_create_canvas_user(details):
    """
    Finds a user in Canvas using a prioritized search, or creates them if not found.
    This is the definitive method for retrieving a user object.

    Search Priority:
    1. SIS User ID (details['ssid']) - Most reliable and unique.
    2. Login ID (details['email']) - Also highly reliable.
    3. General Search Term (details['email']) - Broader search.

    If not found, it will create a new user with the provided details.

    Args:
        details (dict): A dictionary containing 'name', 'ssid', and 'email'.

    Returns:
        A Canvas User object if found or created, otherwise None.
    """
    try:
        account = canvas.get_account(CANVAS_ACCOUNT_ID)
        
        # --- Search Strategy 1: SIS User ID (Highest Priority) ---
        if details.get('ssid'):
            sis_id = f"sis_user_id:{details['ssid']}"
            print(f"DEBUG: Searching for Canvas user with {sis_id}")
            users = account.get_users(sis_user_id=details['ssid'])
            user_list = list(users)
            if user_list:
                print(f"SUCCESS: Found user '{user_list[0].name}' by SIS ID.")
                return user_list[0]

        # --- Search Strategy 2: Login ID (Second Priority) ---
        # Often the login ID is the email address.
        if details.get('email'):
            login_id = f"sis_login_id:{details['email']}"
            print(f"INFO: Search by SIS ID failed. Searching with {login_id}")
            users = account.get_users(sis_login_id=details['email'])
            user_list = list(users)
            if user_list:
                print(f"SUCCESS: Found user '{user_list[0].name}' by Login ID.")
                return user_list[0]

        # --- Search Strategy 3: General Search (Last Resort) ---
        if details.get('email'):
            email_search = f"email:{details['email']}"
            print(f"INFO: Search by Login ID failed. Searching with {email_search}")
            # This is a general search, less precise but can find users if login_id differs.
            users = account.get_users(search_term=details['email'])
            # We must filter the results to find an exact email match.
            for user in users:
                if hasattr(user, 'email') and user.email and user.email.lower() == details['email'].lower():
                    print(f"SUCCESS: Found user '{user.name}' by general email search.")
                    return user

        # --- If all searches fail, proceed to CREATE the user ---
        print(f"INFO: User not found with any search method. Proceeding to create user with details: {details}")
        
        # Prepare the data for the new user payload
        user_payload = {
            'name': details['name'],
            'sortable_name': f"{details['name'].split(',')[0].strip()}, {details['name'].split(',')[1].strip()}" if ',' in details['name'] else details['name'],
            'skip_registration': True,
            'terms_of_use': True # Automatically accept terms of use
        }
        pseudonym_payload = {
            'unique_id': details['email'], # This becomes the login_id
            'sis_user_id': details.get('ssid'),
            'send_confirmation': False
        }

        print(f"DEBUG: Creating user with payload: user={user_payload}, pseudonym={pseudonym_payload}")
        new_user = account.create_user(pseudonym=pseudonym_payload, user=user_payload)
        
        print(f"SUCCESS: Created new Canvas user '{new_user.name}' with ID: {new_user.id}")
        return new_user

    except Exception as e:
        print(f"CRITICAL: An error occurred during get_or_create_canvas_user: {e}")
        return None
