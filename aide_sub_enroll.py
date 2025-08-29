# enroll_users.py
#
# Description:
# This script enrolls two specified users into all Canvas courses for a given term
# with the "TA" (Teacher Assistant) role.
#
# It's designed to be run in a server environment where configuration is provided
# via environment variables for security.
#
# Required Python packages:
# requests
#
# To install dependencies:
# pip install requests
#
# Environment Variables to set before running:
# 1. CANVAS_API_URL: Your institution's Canvas URL (e.g., "https://triviumcharter.instructure.com")
# 2. CANVAS_API_KEY: Your Canvas API access token.
# 3. CANVAS_TERM_ID: The ID of the term you want to process courses from.
# 4. CANVAS_SUBACCOUNT_ID: The account ID to search for users within.
# 5. SEARCH_ROOT_ACCOUNT (optional): Set to "true" to search for courses across the entire institution.
# 6. DRY_RUN (optional): Set to "true" to list courses without enrolling. Defaults to true for safety.

import os
import requests
import sys

# --- Configuration ---
# Load configuration from environment variables for security.
CANVAS_DOMAIN = os.getenv("CANVAS_API_URL")
API_TOKEN = os.getenv("CANVAS_API_KEY")
TERM_ID = os.getenv("CANVAS_TERM_ID")
ACCOUNT_ID = os.getenv("CANVAS_SUBACCOUNT_ID")
SEARCH_ROOT = os.getenv("SEARCH_ROOT_ACCOUNT", "false").lower() == "true"
# Safety first: Default to dry run unless explicitly set to "false".
DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"


# --- Users to Enroll ---
# The email addresses of the users you want to add as TAs.
USERS_TO_ENROLL = [
    "aide@triviumcharter.org",
    "sub@triviumcharter.org",
]

# The role you want to assign.
ENROLLMENT_ROLE = "TaEnrollment" # This is the standard type for TA roles.

# --- Helper Functions ---

def validate_config():
    """Checks if all necessary environment variables are set."""
    if not all([CANVAS_DOMAIN, API_TOKEN, TERM_ID, ACCOUNT_ID]):
        print("Error: Missing one or more required environment variables:")
        print("- CANVAS_API_URL")
        print("- CANVAS_API_KEY")
        print("- CANVAS_TERM_ID")
        print("- CANVAS_SUBACCOUNT_ID")
        sys.exit(1) # Exit the script if configuration is incomplete.
    print("✅ Configuration validated successfully.")

def make_paginated_request(url, params=None):
    """
    Makes a GET request to the Canvas API and handles pagination to fetch all results.
    """
    results = []
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    
    while url:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            
            results.extend(response.json())
            
            # Canvas API uses the 'Link' header for pagination
            # We look for the 'next' page URL.
            url = response.links.get('next', {}).get('url')
                
        except requests.exceptions.RequestException as e:
            print(f"Error making API request to {url}: {e}")
            return None # Return None on error
            
    return results

def get_user_id(email):
    """
    Finds the Canvas user ID for a given email address.
    """
    print(f"Searching for user with email: {email}...")
    # User search is generally done at the account level specified
    url = f"{CANVAS_DOMAIN}/api/v1/accounts/{ACCOUNT_ID}/users"
    params = {"search_term": email}
    
    users = make_paginated_request(url, params)
    
    if not users:
        print(f"Could not find any user matching '{email}'.")
        return None
        
    # Find the exact match from the search results
    for user in users:
        # The login_id is often the email address
        if user.get('login_id') == email:
            print(f"Found user '{user.get('name')}' with ID: {user['id']}")
            return user['id']
            
    print(f"Search returned results, but no exact match found for '{email}'.")
    return None

def get_courses_in_term(term_id):
    """
    Retrieves a list of all courses for a specific term ID.
    """
    # Determine which account to search for courses in.
    course_search_account_id = "1" if SEARCH_ROOT else ACCOUNT_ID
    
    print(f"\nFetching all courses for Term ID: {term_id} within Account ID: {course_search_account_id}...")
    url = f"{CANVAS_DOMAIN}/api/v1/accounts/{course_search_account_id}/courses"
    params = {"enrollment_term_id": term_id, "per_page": 100, "include": ["term"]} # Include term info
    
    courses = make_paginated_request(url, params)
    
    if courses is not None:
        print(f"Found {len(courses)} courses.")
    return courses

def enroll_user_in_course(course_id, user_id, role_type):
    """
    Enrolls a user into a specific course with a given role.
    """
    url = f"{CANVAS_DOMAIN}/api/v1/courses/{course_id}/enrollments"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    payload = {
        "enrollment": {
            "user_id": user_id,
            "type": role_type,
            "enrollment_state": "active"
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        
        # Check for a 409 Conflict error, which can mean the user is already enrolled.
        if response.status_code == 409:
            print(f"  - User {user_id} is already enrolled in course {course_id}.")
            return False

        response.raise_for_status() # Raise an error for other bad statuses
        
        print(f"  - Successfully enrolled user {user_id} in course {course_id} as a {role_type}.")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"  - Failed to enroll user {user_id} in course {course_id}. Error: {e}")
        print(f"    Response body: {response.text}")
        return False

# --- Main Execution ---

def main():
    """Main function to orchestrate the enrollment process."""
    print("--- Starting Canvas TA Enrollment Script ---")
    if DRY_RUN:
        print("⚠️  Running in DRY RUN mode. No enrollments will be made.")
    validate_config()
    
    # Get all the courses for the specified term
    courses = get_courses_in_term(TERM_ID)
    if courses is None or not courses:
        print("No courses found for the specified term. Exiting.")
        sys.exit(1)
        
    # If in dry run mode, just list the courses and exit.
    if DRY_RUN:
        print("\n--- Courses Found (Dry Run) ---")
        for course in courses:
            term_name = course.get('term', {}).get('name', 'N/A')
            print(f"  - ID: {course['id']}, Name: \"{course.get('name', 'Unnamed Course')}\", Term: \"{term_name}\"")
        print("\n--- Dry Run Complete ---")
        print("Review the list above. If these are the correct courses, set the DRY_RUN environment variable to 'false' to perform enrollments.")
        sys.exit(0)

    # --- Proceed with Enrollment if not a dry run ---
    print("\n--- Step 1: Resolving User IDs ---")
    user_ids_to_enroll = []
    for email in USERS_TO_ENROLL:
        user_id = get_user_id(email)
        if user_id:
            user_ids_to_enroll.append(user_id)
        else:
            print(f"Halting script because user '{email}' could not be found.")
            sys.exit(1)
            
    print(f"\n--- Step 2: Processing {len(courses)} Courses for Enrollment ---")
    successful_enrollments = 0
    failed_enrollments = 0
    
    for course in courses:
        course_id = course['id']
        course_name = course.get('name', 'Unnamed Course')
        print(f"\nProcessing course: '{course_name}' (ID: {course_id})")
        
        for user_id in user_ids_to_enroll:
            if enroll_user_in_course(course_id, user_id, ENROLLMENT_ROLE):
                successful_enrollments += 1
            else:
                failed_enrollments += 1
                
    # Print summary
    print("\n--- Script Finished ---")
    print("Summary:")
    print(f"  - Successful new enrollments: {successful_enrollments}")
    print(f"  - Failed or pre-existing enrollments: {failed_enrollments}")
    print("------------------------")

if __name__ == "__main__":
    main()
