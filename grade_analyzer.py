import os
import requests
import json
import numpy as np
from datetime import datetime, timedelta

from canvasapi import Canvas

# --- Configuration from Environment Variables ---
# Assumes you have these set in your environment (e.g., DigitalOcean App Platform)

# Canvas API Configuration
CANVAS_URL = os.getenv('CANVAS_API_URL', 'https://triviumcharter.instructure.com/')
CANVAS_KEY = os.getenv('CANVAS_API_KEY')

# Monday.com API Configuration
MONDAY_API_KEY = os.getenv('MONDAY_API_KEY')
MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_HEADERS = {"Authorization": MONDAY_API_KEY, "API-Version": "2023-10"}

# Monday.com Board and Column IDs
CANVAS_BOARD_ID = int(os.getenv('CANVAS_BOARD_ID', 0)) # Board with the list of courses
CANVAS_COURSE_ID_COLUMN_ID = os.getenv('CANVAS_COURSE_ID_COLUMN_ID') # Column containing the Canvas Course ID

# --- Constants ---
# Add any variations of test/sample student names here
STUDENTS_TO_IGNORE = ["Test Student", "Sample Student", "Student, Test", "Student, Sample", "zz_Student"]
# Submissions graded in less than this time will be considered auto-graded
MIN_HUMAN_GRADING_TIME_SECONDS = 60

# --- Initialize API Clients ---
canvas = Canvas(CANVAS_URL, CANVAS_KEY)

# --- Helper Functions ---

def get_courses_from_monday(board_id, course_id_column):
    """Fetches all items (courses) from the specified Monday.com board."""
    query = f'''
        query {{
            boards(ids: {board_id}) {{
                items_page {{
                    items {{
                        id
                        name
                        column_values(ids: ["{course_id_column}"]) {{
                            value
                        }}
                    }}
                }}
            }}
        }}
    '''
    try:
        response = requests.post(MONDAY_API_URL, json={'query': query}, headers=MONDAY_HEADERS)
        response.raise_for_status()
        data = response.json()
        items = data['data']['boards'][0]['items_page']['items']
        
        courses = []
        for item in items:
            # The course ID is stored as a JSON string, e.g., '"12345"'
            course_id_str = item['column_values'][0]['value']
            if course_id_str and course_id_str != 'null':
                # Remove quotes and convert to int
                course_id = int(json.loads(course_id_str))
                courses.append({'monday_item_id': item['id'], 'canvas_course_id': course_id})
        return courses
    except Exception as e:
        print(f"Error fetching courses from Monday.com: {e}")
        return []

def update_monday_board(board_id, item_id, column_values):
    """Updates columns for a specific item on a Monday.com board."""
    # Monday API requires column values to be a JSON encoded string
    column_values_str = json.dumps(column_values)
    
    query = f'''
        mutation {{
            change_multiple_column_values(
                board_id: {board_id},
                item_id: {item_id},
                column_values: {column_values_str}
            ) {{
                id
            }}
        }}
    '''
    try:
        response = requests.post(MONDAY_API_URL, json={'query': query}, headers=MONDAY_HEADERS)
        response.raise_for_status()
        print(f"Successfully updated Monday.com item {item_id}.")
    except Exception as e:
        print(f"Error updating Monday.com item {item_id}: {e}\nResponse: {response.text}")


def get_canvas_stats(course_id):
    """Gathers all required grading statistics for a single Canvas course."""
    print(f"\nProcessing Canvas course ID: {course_id}...")
    stats = {}
    
    try:
        course = canvas.get_course(course_id)
        stats['text65__1'] = course.name
        print(f"  Analyzing course: '{course.name}'")
    except Exception as e:
        print(f"  ERROR: Could not fetch course. Skipping. Reason: {e}")
        return None

    # 1. Get enrollments and filter out test/inactive students
    enrollments = course.get_enrollments(type=['StudentEnrollment'])
    active_students = [e for e in enrollments if e.user['name'] not in STUDENTS_TO_IGNORE and e.enrollment_state == 'active']
    student_ids_to_include = {s.user_id for s in active_students}
    stats['numeric3__1'] = len(active_students)
    print(f"  Found {len(active_students)} active, non-test students.")

    if not active_students:
        print("  WARNING: No active students found. Ending analysis for this course.")
        return stats

    # 2. Get class grades for active students
    class_grades = [e.grades.get('current_score') for e in active_students if e.grades.get('current_score') is not None]
    if class_grades:
        stats['median_class_grade2__1'] = round(np.median(class_grades), 2)
        stats['mean_class_grade__1'] = round(np.mean(class_grades), 2)

    # 3. Get assignments and submissions
    assignments = course.get_assignments()
    assignment_groups = {group.id: group.name for group in course.get_assignment_groups()}
    
    published_assignments = [a for a in assignments if a.published and not a.omit_from_final_grade]
    stats['total_assignments3__1'] = len(published_assignments)
    print(f"  Found {len(published_assignments)} published assignments.")
    
    
    # Initialize lists for statistics
    grading_times_days = []
    last_graded_date = None
    graded_submission_count = 0
    waiting_for_grading_count = 0
    past_due_ungraded_count = 0
    
    all_scores = []
    category_scores = {'Learn': [], 'Practice': [], 'SYK': []}
    
    print(f"  Gathering submissions for each assignment...")
    
    # Correctly loop through each assignment to get its submissions
    for assignment in published_assignments:
        submissions = assignment.get_submissions()
        for sub in submissions:
            # Filter for submissions from active, non-test students
            if sub.user_id not in student_ids_to_include:
                continue

            # Check for waiting submissions
            if sub.workflow_state == 'submitted':
                waiting_for_grading_count += 1
                
            # Check for past-due ungraded (0 score)
            due_at = assignment.due_at
            if due_at and datetime.fromisoformat(due_at.replace('Z', '')) < datetime.utcnow() and sub.score is None:
                past_due_ungraded_count += 1

            # Process graded submissions for more detailed stats
            if sub.workflow_state == 'graded' and sub.score is not None and sub.submitted_at and sub.graded_at:
                all_scores.append(sub.score)
                
                # Find assignment category and map it to our keys
                group_id = assignment.assignment_group_id
                if group_id and group_id in assignment_groups:
                    category_name = assignment_groups[group_id]
                    if category_name == 'Learn':
                        category_scores['Learn'].append(sub.score)
                    elif category_name == 'Practice':
                        category_scores['Practice'].append(sub.score)
                    elif category_name in ['SYK', 'Show You Know']:
                        category_scores['SYK'].append(sub.score)

                # Calculate human grading time
                submitted_at = datetime.fromisoformat(sub.submitted_at.replace('Z', ''))
                graded_at = datetime.fromisoformat(sub.graded_at.replace('Z', ''))
                
                if last_graded_date is None or graded_at > last_graded_date:
                    last_graded_date = graded_at

                time_diff = graded_at - submitted_at
                if time_diff.total_seconds() > MIN_HUMAN_GRADING_TIME_SECONDS:
                    grading_times_days.append(time_diff.total_seconds() / (60 * 60 * 24))
                    graded_submission_count += 1

    # 4. Calculate and assign final stats
    stats['graded_submissions6__1'] = graded_submission_count
    stats['numeric6__1'] = waiting_for_grading_count
    stats['numbers5__1'] = past_due_ungraded_count
    
    if last_graded_date:
        stats['date_last_graded8__1'] = last_graded_date.strftime('%Y-%m-%d')

    if all_scores:
        stats['median_assignment_score8__1'] = round(np.median(all_scores), 2)
        stats['mean_assignment_score__1'] = round(np.mean(all_scores), 2)

    if grading_times_days:
        stats['numeric__1'] = round(np.median(grading_times_days), 2)
        stats['numeric0__1'] = round(np.mean(grading_times_days), 2)

    if category_scores['Learn']:
        stats['numbers6__1'] = round(np.median(category_scores['Learn']), 2)
        stats['numbers784__1'] = round(np.mean(category_scores['Learn']), 2)
    if category_scores['Practice']:
        stats['numbers86__1'] = round(np.median(category_scores['Practice']), 2)
        stats['numbers88__1'] = round(np.mean(category_scores['Practice']), 2)
    if category_scores['SYK']:
        stats['numbers2__1'] = round(np.median(category_scores['SYK']), 2)
        stats['numbers20__1'] = round(np.mean(category_scores['SYK']), 2)
        
    print(f"  - STATS SUMMARY: Mean Grade={stats.get('mean_class_grade__1', 'N/A')}, Waiting={stats.get('numeric6__1', 0)}, Mean Grading Time={stats.get('numeric0__1', 'N/A')} days")
    print(f"  Processing complete for course '{course.name}'.")
    return stats

# --- Main Execution ---

def main():
    """Main function to run the grade analysis."""
    print("--- Starting Canvas Grade Analyzer ---")
    if not all([CANVAS_KEY, MONDAY_API_KEY, CANVAS_BOARD_ID, CANVAS_COURSE_ID_COLUMN_ID]):
        print("FATAL: Missing one or more required environment variables.")
        return

    courses_to_process = get_courses_from_monday(CANVAS_BOARD_ID, CANVAS_COURSE_ID_COLUMN_ID)
    
    if not courses_to_process:
        print("No courses found on the Monday.com board to process.")
        return

    print(f"Found {len(courses_to_process)} courses to analyze from board ID {CANVAS_BOARD_ID}.")

    for course_info in courses_to_process:
        stats = get_canvas_stats(course_info['canvas_course_id'])
        
        if stats:
            update_payload = {k: str(v) for k, v in stats.items() if v is not None}
            if update_payload:
                update_monday_board(CANVAS_BOARD_ID, course_info['monday_item_id'], update_payload)

    print("\n--- Analysis complete. ---")

if __name__ == "__main__":
    main()

