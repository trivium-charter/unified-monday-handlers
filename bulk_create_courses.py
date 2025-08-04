import os
import json
import requests
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist

# ==============================================================================
# SCRIPT CONFIGURATION (Pulls from your existing environment variables)
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
CANVAS_API_KEY = os.environ.get("CANVAS_API_KEY")
CANVAS_API_URL = os.environ.get("CANVAS_API_URL")
MONDAY_API_URL = "https://api.monday.com/v2"

ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
ALL_CLASSES_CANVAS_ID_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_ID_COLUMN")

CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")

CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
CANVAS_SUBACCOUNT_ID = os.environ.get("CANVAS_SUBACCOUNT_ID")
CANVAS_TEMPLATE_COURSE_ID = os.environ.get("CANVAS_TEMPLATE_COURSE_ID")

MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2024-01" }

# ==============================================================================
# UTILITY FUNCTIONS (Copied from app.py)
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

def get_item_name(item_id, board_id):
    query = f"query($boardId: [ID!], $itemId: [ID!]) {{ boards(ids: $boardId) {{ items_page(query_params: {{ids: $itemId}}) {{ items {{ name }} }} }} }}"
    variables = {"boardId": int(board_id), "itemId": int(item_id)}
    result = execute_monday_graphql(query, variables)
    try:
        return result['data']['boards'][0]['items_page']['items'][0]['name']
    except (TypeError, KeyError, IndexError):
        return None

def change_column_value_generic(board_id, item_id, column_id, value):
    query = f'mutation($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {{ change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {{ id }} }}'
    variables = {"boardId": int(board_id), "itemId": int(item_id), "columnId": column_id, "value": json.dumps(str(value))}
    return execute_monday_graphql(query, variables)

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
                            value
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
            # THIS IS THE CRITICAL FIX: Check for the 400 status code and the specific message.
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
    print("Starting bulk Canvas course creation process...")
    print(f"Fetching all items from 'All Courses' board ({ALL_COURSES_BOARD_ID})...")
    
    required_cols = [ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID]
    all_courses_items = get_all_items_from_board(ALL_COURSES_BOARD_ID, required_cols)
    print(f"Found {len(all_courses_items)} total items on the 'All Courses' board.")
    
    courses_to_create = []
    for item in all_courses_items:
        connect_canvas_col = next((c for c in item['column_values'] if c['id'] == ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID), None)
        
        if not connect_canvas_col or not connect_canvas_col.get('value'):
            continue # Skip if not linked to Canvas board at all
            
        try:
            linked_items = json.loads(connect_canvas_col['value']).get('linkedPulseIds', [])
            if not linked_items:
                continue
            
            canvas_item_id = linked_items[0]['linkedPulseId']
            
            # Now we need to check the 'Course ID' column on THIS item
            query = f'query($itemId: [ID!]) {{ items(ids: $itemId) {{ column_values(ids: ["{CANVAS_COURSE_ID_COLUMN_ID}"]) {{ text }} }} }}'
            variables = {"itemId": canvas_item_id}
            result = execute_monday_graphql(query, variables)
            canvas_course_id_text = result['data']['items'][0]['column_values'][0].get('text')
            
            if not canvas_course_id_text:
                course_title = get_item_name(canvas_item_id, CANVAS_BOARD_ID)
                if not course_title:
                   print(f"WARNING: Skipping item {item['id']} because its linked Canvas item {canvas_item_id} has no name.")
                   continue
                
                courses_to_create.append({
                    "all_courses_item_id": item['id'],
                    "canvas_item_id": canvas_item_id,
                    "title": course_title
                })
                
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"WARNING: Skipping item {item['id']} due to malformed data: {e}")
            continue
            
    if not courses_to_create:
        print("\nAll found Canvas courses already have a Course ID. Nothing to do. Exiting.")
        return

    print(f"\nFound {len(courses_to_create)} courses that need to be created in Canvas. Starting process...")
    
    success_count = 0
    fail_count = 0
    for course_data in courses_to_create:
        print(f"\nProcessing '{course_data['title']}' (Monday Canvas Item ID: {course_data['canvas_item_id']})")
        new_course = create_canvas_course(course_data['title'], CANVAS_TERM_ID)
        
        if new_course:
            print(f"  UPDATING MONDAY: Setting Canvas ID '{new_course.id}' on item '{course_data['canvas_item_id']}'...")
            change_column_value_generic(CANVAS_BOARD_ID, course_data['canvas_item_id'], CANVAS_COURSE_ID_COLUMN_ID, new_course.id)
            if ALL_CLASSES_CANVAS_ID_COLUMN:
                change_column_value_generic(ALL_COURSES_BOARD_ID, course_data['all_courses_item_id'], ALL_CLASSES_CANVAS_ID_COLUMN, new_course.id)
            success_count += 1
        else:
            print(f"  SKIPPING UPDATE: Course creation failed for '{course_data['title']}'.")
            fail_count += 1
            
    print(f"\n--- Bulk Creation Complete ---")
    print(f"Successfully created and updated: {success_count}")
    print(f"Failed to create: {fail_count}")

if __name__ == "__main__":
    main()
