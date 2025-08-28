import os
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException

# ==============================================================================
# ## CONFIGURATION
# 1. Fill in your Canvas URL and your NEWEST API Key.
# 2. Add your test course's ID.
# 3. Use a NEW, UNIQUE fake email address for the test student.
# ==============================================================================
CANVAS_API_URL = "https://triviumcharter.instructure.com"
CANVAS_API_KEY = "11194~3kYCrnakTrxch3AFxYtxm3ZZQFTAFnQfWzycWFxDFM9L9FRR3rFFZFyGEk6F9uGY"

TEST_COURSE_ID = 10128  # <-- Use your test course's ID
TEST_STUDENT_EMAIL = "test-student-0828@example.com" # <-- Use a new, unique email
# ==============================================================================


def test_api_key():
    """
    Tests if the API key has the correct permissions to enroll and accept.
    The cleanup step is DISABLED so you can verify the result in Canvas.
    """
    print("--- Starting API Key Test (Cleanup Disabled) ---")
    
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
        print(f"1. Finding test course {TEST_COURSE_ID}...")
        course = canvas.get_course(TEST_COURSE_ID)
        print(f"   -> Found course: '{course.name}'")

        print(f"2. Creating temporary user '{TEST_STUDENT_EMAIL}'...")
        user_payload = {
            'user': {'name': 'API Test Student', 'terms_of_use': True},
            'pseudonym': {'unique_id': TEST_STUDENT_EMAIL}
        }
        test_user = account.create_user(**user_payload)
        print(f"   -> Created user with ID: {test_user.id}")

        print(f"3. Enrolling user in course (creating invitation)...")
        enrollment = course.enroll_user(test_user, 'StudentEnrollment')
        print(f"   -> Enrollment created with state: '{enrollment.enrollment_state}'")

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
        # The cleanup step has been intentionally disabled for manual verification.
        print("\n5. SKIPPING CLEANUP STEP.")
        print("--- Test complete. You can now check for the user in your Canvas course. ---")


if __name__ == '__main__':
    test_api_key()
