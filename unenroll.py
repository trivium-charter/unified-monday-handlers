# unenroll_users.py
#
# Description:
# This script "concludes" the enrollments for specified users in all courses
# within a given term. Concluding an enrollment makes it inactive but preserves
# a record, which is generally safer than deleting.
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
# 1. CANVAS_API_URL: Your institution's Canvas URL.
# 2. CANVAS_API_KEY: Your Canvas API access token.
# 3. CANVAS_TERM_ID: The ID of the term where the incorrect enrollments were made.
# 4. CANVAS_SUBACCOUNT_ID: The account ID to search within.
# 5. SEARCH_ROOT_ACCOUNT (optional): Set to "true" if the courses are across the entire institution.
# 6. DRY_RUN (optional): Set to "true" to list enrollments without removing them. Defaults to true for safety.

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
DRY_RUN = os.getenv("DRY_RUN", "false").lower() != "false"


# --- Users to Unenroll ---
# The email addresses of the users you want to remove.
USERS_TO_UNENROLL = [
    "aide@triviumcharter.org",
    "sub@triviumcharter.org",
]

# --- Helper Functions ---

def validate_config():
    """Checks if all necessary environment variables are set."""
    if not all([CANVAS_DOMAIN, API_TOKEN, TERM_ID, ACCOUNT_ID]):
        print("Error: Missing one or more required environment variables.")
        sys.exit(1)
    print("✅ Configuration validated successfully.")

def make_paginated_request(url, params=None):
    """Makes a GET request to the Canvas API and handles pagination."""
    results = []
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    while url:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            results.extend(response.json())
            url = response.links.get('next', {}).get('url')
        except requests.exceptions.RequestException as e:
            print(f"Error making API request to {url}: {e}")
            return None
    return results

def get_user_id(email):
    """Finds the Canvas user ID for a given email address."""
    print(f"Searching for user with email: {email}...")
    url = f"{CANVAS_DOMAIN}/api/v1/accounts/{ACCOUNT_ID}/users"
    params = {"search_term": email}
    users = make_paginated_request(url, params)
    if users:
        for user in users:
            if user.get('login_id') == email:
                print(f"Found user '{user.get('name')}' with ID: {user['id']}")
                return user['id']
    print(f"Could not find any user matching '{email}'.")
    return None

def get_courses_in_term(term_id):
    """Retrieves a list of all courses for a specific term ID."""
    course_search_account_id = "1" if SEARCH_ROOT else ACCOUNT_ID
    print(f"\nFetching all courses for Term ID: {term_id} within Account ID: {course_search_account_id}...")
    url = f"{CANVAS_DOMAIN}/api/v1/accounts/{course_search_account_id}/courses"
    params = {"enrollment_term_id": term_id, "per_page": 100}
    courses = make_paginated_request(url, params)
    if courses is not None:
        print(f"Found {len(courses)} courses in the term.")
    return courses

def get_enrollments_for_user_in_course(course_id, user_id):
    """Finds enrollments for a specific user in a specific course."""
    url = f"{CANVAS_DOMAIN}/api/v1/courses/{course_id}/enrollments"
    params = {"user_id": user_id}
    return make_paginated_request(url, params)

def conclude_enrollment(course_id, enrollment_id):
    """Concludes a specific enrollment, making it inactive."""
    url = f"{CANVAS_DOMAIN}/api/v1/courses/{course_id}/enrollments/{enrollment_id}"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    params = {"task": "conclude"}
    try:
        response = requests.delete(url, headers=headers, params=params)
        response.raise_for_status()
        print(f"  - ✅ Successfully concluded enrollment ID {enrollment_id}.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"  - ❌ Failed to conclude enrollment ID {enrollment_id}. Error: {e}")
        print(f"    Response body: {response.text}")
        return False

# --- Main Execution ---

def main():
    """Main function to orchestrate the unenrollment process."""
    print("--- Starting Canvas Unenrollment Script ---")
    if DRY_RUN:
        print("⚠️  Running in DRY RUN mode. No enrollments will be removed.")
    validate_config()

    # 1. Get User IDs
    print("\n--- Step 1: Resolving User IDs ---")
    user_ids_to_unenroll = {} # Using a dict to map ID back to email for logging
    for email in USERS_TO_UNENROLL:
        user_id = get_user_id(email)
        if not user_id:
            print(f"Halting script because user '{email}' could not be found.")
            sys.exit(1)
        user_ids_to_unenroll[user_id] = email

    # 2. Get Courses
    courses = get_courses_in_term(TERM_ID)
    if not courses:
        print("No courses found for the term or an error occurred. Exiting.")
        sys.exit(1)

    # 3. Find and process enrollments
    print(f"\n--- Step 2: Scanning {len(courses)} Courses for Enrollments to Remove ---")
    enrollments_to_remove = []
    for course in courses:
        course_id = course['id']
        course_name = course.get('name', 'Unnamed Course')
        print(f"\nScanning course: '{course_name}' (ID: {course_id})")
        for user_id in user_ids_to_unenroll:
            enrollments = get_enrollments_for_user_in_course(course_id, user_id)
            if enrollments:
                for enrollment in enrollments:
                    # Only target active enrollments
                    if enrollment.get('enrollment_state') == 'active':
                        enrollments_to_remove.append(enrollment)
                        print(f"  - Found active enrollment for {user_ids_to_unenroll[user_id]} (Enrollment ID: {enrollment['id']})")

    if not enrollments_to_remove:
        print("\nNo active enrollments found for the specified users in this term. Nothing to do.")
        sys.exit(0)

    # 4. If Dry Run, print and exit. Otherwise, conclude.
    if DRY_RUN:
        print(f"\n--- Dry Run Complete: Found {len(enrollments_to_remove)} enrollments to remove ---")
        print("To remove these enrollments, set the DRY_RUN environment variable to 'false'.")
        sys.exit(0)
    
    print(f"\n--- Step 3: Concluding {len(enrollments_to_remove)} Enrollments ---")
    success_count = 0
    fail_count = 0
    for enrollment in enrollments_to_remove:
        if conclude_enrollment(enrollment['course_id'], enrollment['id']):
            success_count += 1
        else:
            fail_count += 1

    print("\n--- Unenrollment Script Finished ---")
    print("Summary:")
    print(f"  - Successfully concluded enrollments: {success_count}")
    print(f"  - Failed to conclude enrollments: {fail_count}")
    print("------------------------------------")

if __name__ == "__main__":
    main()
