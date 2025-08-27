import os
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException

# ==============================================================================
# ## CONFIGURATION
# 1. Fill in your Canvas URL and your NEWEST API Key.
# 2. Find a safe, non-critical course (like a sandbox or test course)
#    and paste its ID below. You can get the ID from the course URL
#    (e.g., in "yourschool.instructure.com/courses/12345", the ID is 12345).
# 3. Use a fake email address for the test student.
# ==============================================================================
CANVAS_API_URL = "https://yourschool.instructure.com"
CANVAS_API_KEY = "PASTE_YOUR_NEWEST_API_KEY_HERE"

TEST_COURSE_ID = 10128  # <-- Use your test course's ID
TEST_STUDENT_EMAIL = "test-student-67890@example.com" # A fake email is fine
# ==============================================================================


def test_api_key():
    """
    Tests if the API key has the correct permissions to enroll and accept.
    """
    print("--- Starting API Key Test ---")
    
    try:
        canvas = Canvas(CANVAS_API_URL, CANVAS_API_KEY)
        account = canvas.get_account(1)
        print("✅ Successfully connected to Canvas.")
    except Exception as e:
        print(f"❌ FAILED: Could not connect to Canvas. Check your URL and API Key. Details: {e}")
        return

    test_user = None
    enrollment = None
    
    try:
        # Step 1: Find the test course
        print(f"1. Finding test course {TEST_COURSE_ID}...")
        course = canvas.get_course(TEST_COURSE_ID)
        print(f"   -> Found course: '{course.name}'")

        # Step 2: Create a temporary test user
        print(f"2. Creating temporary user '{TEST_STUDENT_EMAIL}'...")
        user_payload = {
            'user': {'name': 'API Test Student', 'terms_of_use': True},
            'pseudonym': {'unique_id': TEST_STUDENT_EMAIL}
        }
        test_user = account.create_user(**user_payload)
        print(f"   -> Created user with ID: {test_user.id}")

        # Step 3: Enroll the user, which creates the invitation
        print(f"3. Enrolling user in course (creating invitation)...")
        enrollment = course.enroll_user(test_user, 'StudentEnrollment')
        print(f"   -> Enrollment created with state: '{enrollment.enrollment_state}'")

        # Step 4: The critical test - try to accept the invitation
        print("4. ATTEMPTING TO ACCEPT THE INVITATION...")
        if enrollment.enrollment_state == 'invited':
            enrollment.accept()
            print("\n✅ SUCCESS! Invitation accepted. The API key has the correct permissions.")
        else:
            print("\n✅ SUCCESS! Enrollment was immediately active. The API key works.")

    except CanvasException as e:
        print(f"\n❌ FAILED: The API call was blocked by Canvas.")
        print(f"   -> Error message: {e}")
        print("   -> This means the 'Scopes' on your Developer Key are incorrect OR you are using an old API token.")
    
    finally:
        # Step 5: Clean up the test user and enrollment
        print("\n5. Cleaning up...")
        if enrollment:
            try:
                print(f"   -> Deleting enrollment for test user...")
                enrollment.deactivate(task='delete')
            except CanvasException:
                pass 
        if test_user:
            try:
                print(f"   -> Deleting test user...")
                # *** THIS IS THE CORRECTED LINE ***
                account.delete_user(test_user)
            except CanvasException as e:
                print(f"      -> Could not delete test user. You may need to do this manually. Error: {e}")
        
        print("--- Test complete ---")

if __name__ == '__main__':
    test_api_key()
