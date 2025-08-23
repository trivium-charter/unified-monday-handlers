import os
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, ResourceDoesNotExist

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# 1. Your Canvas API URL and Key (loaded from environment variables)
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")

# 2. The specific Canvas Course ID for the ACE Study Hall
ACE_STUDY_HALL_COURSE_ID = 10128

# 3. SAFETY SWITCH: Set to False to perform the actual unenrollments.
#    When True, it will only list the students it would remove.
DRY_RUN = True

# ==============================================================================
# SCRIPT LOGIC
# ==============================================================================

def reset_study_hall_enrollments():
    """Finds a course and removes ALL student enrollments from ALL sections within it."""

    if not all([CANVAS_API_URL, CANVAS_API_KEY]):
        print("ERROR: CANVAS_API_URL and CANVAS_API_KEY must be set in your environment variables.")
        return

    print("--- Starting Full Study Hall Reset Script ---")
    if DRY_RUN:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!               DRY RUN MODE IS ON               !!!")
        print("!!! No students will actually be removed from Canvas.!!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

    try:
        # Initialize the Canvas API
        canvas = Canvas(CANVAS_API_URL, CANVAS_API_KEY)
        print(f"INFO: Successfully connected to Canvas at {CANVAS_API_URL}")

        # 1. Find the course
        print(f"INFO: Looking for course with ID: {ACE_STUDY_HALL_COURSE_ID}")
        course = canvas.get_course(ACE_STUDY_HALL_COURSE_ID)
        print(f"SUCCESS: Found course -> '{course.name}'")

        # 2. Get all student enrollments in the entire course
        print("INFO: Fetching all student enrollments from all sections...")
        enrollments = course.get_enrollments(type=['StudentEnrollment'])
        
        student_enrollments = list(enrollments)
        if not student_enrollments:
            print("INFO: No student enrollments found in this course. Nothing to do.")
            return
            
        print(f"INFO: Found {len(student_enrollments)} total students to remove from this course.")

        # 3. Loop through and unenroll each student
        for enrollment in student_enrollments:
            try:
                user = canvas.get_user(enrollment.user_id)
                print(f"  -> Processing student: {user.name} (User ID: {user.id})")
                
                if not DRY_RUN:
                    print(f"     ...REMOVING enrollment...")
                    enrollment.deactivate(task='conclude')
                    print(f"     SUCCESS: Removed {user.name}.")
                else:
                    print(f"     DRY RUN: Would remove {user.name}.")

            except ResourceDoesNotExist:
                print(f"  -> WARNING: Could not find user for enrollment ID {enrollment.id}. Skipping.")
            except CanvasException as e:
                print(f"  -> ERROR: Failed to process enrollment for user {enrollment.user_id}. Details: {e}")

    except ResourceDoesNotExist:
        print(f"FATAL ERROR: Could not find a course with ID '{ACE_STUDY_HALL_COURSE_ID}'. Please check the ID.")
    except CanvasException as e:
        print(f"FATAL ERROR: A Canvas API error occurred: {e}")
    except Exception as e:
        print(f"AN UNEXPECTED ERROR OCCURRED: {e}")

    print("\n--- Cleanup Script Finished ---")


if __name__ == '__main__':
    reset_study_hall_enrollments()
