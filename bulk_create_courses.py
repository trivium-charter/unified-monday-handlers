import os
import json
import requests
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException

# ==============================================================================
# SCRIPT CONFIGURATION (Pulls from your existing environment variables)
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"

CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")

CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2024-01" }

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================
def execute_monday_graphql(query, variables=None):
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    try:
        response = requests.post(MONDAY_API_URL, json=payload, headers=MONDAY_HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"FATAL: Monday.com API Error: {e}")
        return None

def change_column_value_generic(board_id, item_id, column_id, value):
    query = f'mutation($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {{ change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {{ id }} }}'
    variables = {"boardId": int(board_id), "itemId": int(item_id), "columnId": column_id, "value": json.dumps(str(value))}
    print(f"  EXECUTING MONDAY UPDATE for item {item_id}...")
    result = execute_monday_graphql(query, variables)
    if result and 'errors' not in result:
        print(f"  MONDAY UPDATE Succeeded for item {item_id}.")
    else:
        print(f"  MONDAY UPDATE Failed for item {item_id}. Response: {result}")


def get_all_items_from_board(board_id, column_ids):
    """Fetches all items from a board with pagination, including specified column values."""
    all_items = []
    cursor = None
    limit = 100
    column_ids_str = " ".join(f'"{c}"' for c in column_ids)
    
    while True:
        query = f"""query ($boardId: ID!, $limit: Int, $cursor: String) {{
            boards(ids: [$boardId]) {{
                items_page(limit: $limit, cursor: $cursor) {{
                    cursor
                    items {{
                        id
                        name
                        column_values(ids: [{column_ids_str}]) {{
                            id
                            text
                        }}
                    }}
                }}
            }}
        }}"""
        variables = {"boardId": int(board_id), "limit": limit, "cursor": cursor}
        
        result = execute_monday_graphql(query, variables)
        if not result or 'data' not in result or not result['data'].get('boards'):
            print("Error fetching board data or board is empty.")
            break
        
        page_info = result['data']['boards'][0]['items_page']
        items_on_page = page_info.get('items', [])
        all_items.extend(items_on_page)
        
        cursor = page_info.get('cursor')
        if not cursor:
            break
    return all_items


def create_canvas_course(course_name, term_id):
    """
    Creates a Canvas course with robust, corrected retry logic for SIS ID conflicts.
    """
    canvas_api = Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    account = canvas_api.get_account(CANVAS_SUBACCOUNT_ID)

    base_sis_name = ''.join(e for e in course_name if e.isalnum()).replace(' ', '_').lower()
    base_sis_id = f"{base_sis_name}_{term_id}"
    
    max_attempts = 10
    for attempt in range(max_attempts):
        sis_id_to_try = base_sis_id if attempt == 0 else f"{base_sis_id}_{attempt}"
        course_data = {
            'name': course_name, 'course_code': course_name,
            'enrollment_term_id': f"sis_term_id:{term_id}", 'sis_course_id': sis_id_to_try,
            'source_course_id': CANVAS_TEMPLATE_COURSE_ID
        }
        try:
            print(f"  [Attempt {attempt + 1}] Trying to create '{course_name}' with SIS ID '{sis_id_to_try}'...")
            new_course = account.create_course(course=course_data)
            print(f"  SUCCESS: Course created with SIS ID '{sis_id_to_try}'. Canvas ID: {new_course.id}")
            return new_course
        except CanvasException as e:
            if hasattr(e, 'status_code') and e.status_code == 400 and 'is already in use' in str(e).lower():
                print(f"  WARNING: SIS ID '{sis_id_to_try}' is in use. Retrying...")
                continue
            else:
                print(f"  FATAL ERROR for course '{course_name}': {e}. Aborting this course.")
                return None
    
    print(f"  ERROR: Failed to create course '{course_name}' after {max_attempts} attempts. Giving up.")
    return None

def main():
    """Main function to run the bulk creation process."""
    print("--- CORRECTED SCRIPT INITIATED ---")
    print(f"Starting bulk Canvas course creation process by reading the 'Canvas Courses' board ({CANVAS_BOARD_ID}).")
    
    all_canvas_board_items = get_all_items_from_board(CANVAS_BOARD_ID, [CANVAS_COURSE_ID_COLUMN_ID])
    print(f"Found {len(all_canvas_board_items)} total items on the 'Canvas Courses' board.")
    
    courses_to_create = []
    for item in all_canvas_board_items:
        course_id_col = next((c for c in item['column_values'] if c['id'] == CANVAS_COURSE_ID_COLUMN_ID), None)
        
        # Check if the column is missing or its text value is empty/null
        if not course_id_col or not course_id_col.get('text'):
            courses_to_create.append({
                "monday_item_id": item['id'],
                "title": item['name']
            })
            
    if not courses_to_create:
        print("\nAll items on the 'Canvas Courses' board already have a Course ID. Nothing to do. Exiting.")
        return

    print(f"\nFound {len(courses_to_create)} courses that need to be created in Canvas. Starting process...")
    
    success_count = 0
    fail_count = 0
    for course_data in courses_to_create:
        print(f"\nProcessing '{course_data['title']}' (Monday Item ID: {course_data['monday_item_id']})")
        new_course = create_canvas_course(course_data['title'], CANVAS_TERM_ID)
        
        if new_course:
            change_column_value_generic(
                CANVAS_BOARD_ID, 
                course_data['monday_item_id'], 
                CANVAS_COURSE_ID_COLUMN_ID, 
                new_course.id
            )
            success_count += 1
        else:
            print(f"  SKIPPING UPDATE: Course creation failed for '{course_data['title']}'.")
            fail_count += 1
            
    print(f"\n--- Bulk Creation Complete ---")
    print(f"Successfully created and updated: {success_count}")
    print(f"Failed to create: {fail_count}")

if __name__ == "__main__":
    main()
