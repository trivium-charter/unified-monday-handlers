# accept_invitations.py
#
# Description:
# This script finds and accepts all pending course invitations for a list of specified users.
# This is useful when a Canvas account's settings force API-created enrollments
# to become "invited" instead of "active".
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
# 3. CANVAS_SUBACCOUNT_ID: The account ID where the users are located.
# 4. DRY_RUN (optional): Set to "true" to list invitations without accepting them. Defaults to true for safety.

import os
import requests
import sys

# --- Configuration ---
CANVAS_DOMAIN = os.getenv("CANVAS_API_URL")
API_TOKEN = os.getenv("CANVAS_API_KEY")
ACCOUNT_ID = os.getenv("CANVAS_SUBACCOUNT_ID")
# Safety first: Default to dry run unless explicitly set to "false".
DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"

# --- Users to process ---
USERS_TO_PROCESS = [
    "aide@triviumcharter.org",
    "sub@triviumcharter.org",
]

# --- Helper Functions ---

def validate_config():
    """Checks if all necessary environment variables are set."""
    if not all([CANVAS_DOMAIN, API_TOKEN, ACCOUNT_ID]):
        print("Error: Missing one or more required environment variables:")
        print("- CANVAS_API_URL")
        print("- CANVAS_API_KEY")
        print("- CANVAS_SUBACCOUNT_ID")
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

def get_pending_invitations(user_id):
    """Gets all enrollments for a user that are in the 'invited' state."""
    print(f"\nFetching pending invitations for user ID: {user_id}...")
    url = f"{CANVAS_DOMAIN}/api/v1/users/{user_id}/enrollments"
    params = {"state[]": "invited", "per_page": 100}
    invitations = make_paginated_request(url, params)
    if invitations is not None:
        print(f"Found {len(invitations)} pending invitations.")
    return invitations

def accept_invitation(course_id, enrollment_id):
    """Accepts a course invitation for a specific enrollment."""
    url = f"{CANVAS_DOMAIN}/api/v1/courses/{course_id}/enrollments/{enrollment_id}/accept"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        print(f"  - ✅ Successfully accepted invitation in course {course_id}.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"  - ❌ Failed to accept invitation in course {course_id}. Error: {e}")
        return False

# --- Main Execution ---

def main():
    """Main function to find and accept invitations."""
    print("--- Starting Script to Accept Course Invitations ---")
    if DRY_RUN:
        print("⚠️  Running in DRY RUN mode. No invitations will be accepted.")
    validate_config()

    # 1. Get User IDs
    user_ids = []
    for email in USERS_TO_PROCESS:
        user_id = get_user_id(email)
        if user_id:
            user_ids.append(user_id)
        else:
            print(f"Cannot proceed without finding user '{email}'.")
            sys.exit(1)

    # 2. Find all pending invitations for all users
    all_invitations = []
    for user_id in user_ids:
        invites = get_pending_invitations(user_id)
        if invites:
            all_invitations.extend(invites)

    if not all_invitations:
        print("\nNo pending invitations found for any of the specified users. Exiting.")
        sys.exit(0)

    # 3. If Dry Run, print and exit.
    if DRY_RUN:
        print("\n--- Invitations Found (Dry Run) ---")
        for inv in all_invitations:
            print(f"  - User: {inv['user']['name']}, Course ID: {inv['course_id']}, Role: {inv['role']}")
        print("\n--- Dry Run Complete ---")
        print(f"Found a total of {len(all_invitations)} invitations to be accepted.")
        print("To accept them, set the DRY_RUN environment variable to 'false'.")
        sys.exit(0)

    # 4. If not a dry run, accept them.
    print(f"\n--- Accepting {len(all_invitations)} Invitations ---")
    success_count = 0
    fail_count = 0
    for inv in all_invitations:
        if accept_invitation(inv['course_id'], inv['id']):
            success_count += 1
        else:
            fail_count += 1

    print("\n--- Script Finished ---")
    print("Summary:")
    print(f"  - Successfully accepted invitations: {success_count}")
    print(f"  - Failed to accept: {fail_count}")
    print("--------------------------")

if __name__ == "__main__":
    main()
