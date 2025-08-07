import os
import json
import requests
import time

# ==============================================================================
# BULK SYNC SCRIPT FOR PLP COURSE TO MASTER STUDENT TEACHER ASSIGNMENTS (V2)
# ==============================================================================
# This script reads all course assignments from a student's PLP, checks a
# status column on the linked Canvas Board item to determine the course type
# (e.g., "ACE" or "Connect"), and syncs the assigned teacher to the
# corresponding People column on the Master Student List item.
# ==============================================================================

# ==============================================================================
# CONFIGURATION
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"

# Board and Column IDs from environment variables
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSES_TEACHER_COLUMN_ID = os.environ.get("CANVAS_COURSES_TEACHER_COLUMN_ID")

# New variables for the specific sync mappings
# ================== START MODIFICATION ==================
# Using the existing environment variable as requested
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
# =================== END MODIFICATION ===================
MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID = os.environ.get("MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID")
MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID = os.environ.get("MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID")
CANVAS_COURSE_TYPE_COLUMN_ID = os.environ.get("CANVAS_COURSE_TYPE_COLUMN_ID") # Should be status__1

DELAY_BETWEEN_ITEMS = 0.5 # A slightly longer delay to be safe with nested API calls

# ==============================================================================
# MONDAY.COM API UTILITIES
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
    """Executes a GraphQL query against the Monday.com API."""
    try:
        response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  -> API Error: {e}")
        return None

def get_all_items_from_board(board_id):
    """Fetches all items and their column values from a specified board."""
    all_items = []
    cursor = None
    while True:
        query = f"""
        query {{
            boards(ids: [{board_id}]) {{
                items_page (limit: 100{', cursor: "' + cursor + '"' if cursor else ''}) {{
                    cursor
                    items {{
                        id
                        name
                        column_values {{
                            id
                            value
                        }}
                    }}
                }}
            }}
        }}
        """
        result = execute_monday_graphql(query)
        if not result or 'data' not in result or not result['data']['boards']:
            print(f"  -> Could not fetch items for board {board_id}. Stopping.")
            break
        items_page = result['data']['boards'][0]['items_page']
        items = items_page.get('items', [])
        all_items.extend(items)
        cursor = items_page.get('cursor')
        if not cursor:
            break
    return all_items

def get_column_value_from_item_data(item_data, column_id):
    """Extracts a single column value from pre-fetched item data."""
    for cv in item_data.get('column_values', []):
        if cv['id'] == column_id:
            try:
                # Use .get('text') for status columns, otherwise parse value
                if cv.get('text'):
                    return cv['text']
                return json.loads(cv['value']) if cv.get('value') else None
            except (json.JSONDecodeError, TypeError):
                return cv.get('value')
    return None

def get_linked_item_ids(item_data, column_id):
    """Gets linked item IDs from a connect_boards column value."""
    column_value = get_column_value_from_item_data(item_data, column_id)
    if not isinstance(column_value, dict) or "linkedPulseIds" not in column_value:
        return []
    return [item['linkedPulseId'] for item in column_value["linkedPulseIds"]]

def update_people_column_with_ids(item_id, board_id, column_id, people_ids):
    """Updates a People column on an item with a set of user IDs."""
    if not people_ids:
        graphql_value = json.dumps(json.dumps({})) # Clear the column
    else:
        people_list = [{"id": int(pid), "kind": "person"} for pid in people_ids]
        people_value = {"personsAndTeams": people_list}
        graphql_value = json.dumps(json.dumps(people_value))

    mutation = f"""
    mutation {{
        change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}) {{
            id
        }}
    }}
    """
    execute_monday_graphql(mutation)

# ==============================================================================
# MAIN SYNC LOGIC
# ==============================================================================
def bulk_sync_plp_courses():
    """Main function to orchestrate the bulk sync from PLP to Master Student List."""
    print("Starting bulk PLP course sync process...")

    # Validate that all necessary environment variables are set
    required_vars = [
        PLP_BOARD_ID, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN, MASTER_STUDENT_BOARD_ID,
        ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID, CANVAS_BOARD_ID, CANVAS_COURSES_TEACHER_COLUMN_ID,
        PLP_ALL_CLASSES_CONNECT_COLUMNS_STR, MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID,
        MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID, CANVAS_COURSE_TYPE_COLUMN_ID
    ]
    if not all(required_vars):
        print("ERROR: One or more required environment variables are missing. Aborting.")
        return

    plp_course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
    if not plp_course_column_ids:
        print("ERROR: PLP_ALL_CLASSES_CONNECT_COLUMNS_STR is not set. Aborting.")
        return

    print(f"Fetching all students from PLP Board (ID: {PLP_BOARD_ID})...")
    all_plp_items = get_all_items_from_board(PLP_BOARD_ID)
    
    if not all_plp_items:
        print("No items found on the PLP board. Nothing to sync.")
        return

    total_items = len(all_plp_items)
    print(f"Found {total_items} PLP items to process.\n")

    for index, plp_item in enumerate(all_plp_items):
        plp_item_id = plp_item['id']
        plp_item_name = plp_item['name']
        print(f"Processing PLP {index + 1}/{total_items}: {plp_item_name} (ID: {plp_item_id})")

        master_student_ids = get_linked_item_ids(plp_item, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
        if not master_student_ids:
            print("  -> No Master Student item linked. Skipping.")
            print("-" * 20)
            continue
        
        master_student_id = master_student_ids[0]

        # 1. Gather all course IDs from all specified columns on the PLP item
        all_course_ids = set()
        for col_id in plp_course_column_ids:
            all_course_ids.update(get_linked_item_ids(plp_item, col_id))

        if not all_course_ids:
            print("  -> No courses found for this student. Skipping.")
            print("-" * 20)
            continue
        
        # 2. Get the linked Canvas item IDs for all found courses
        canvas_links_query = f'query {{ items (ids: {json.dumps(list(all_course_ids))}) {{ id column_values(ids: ["{ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID}"]) {{ value }} }} }}'
        canvas_links_result = execute_monday_graphql(canvas_links_query)
        
        canvas_item_ids = set()
        if canvas_links_result and canvas_links_result.get('data', {}).get('items'):
            for course_item in canvas_links_result['data']['items']:
                canvas_item_ids.update(get_linked_item_ids(course_item, ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID))

        if not canvas_item_ids:
            print("  -> No linked Canvas items found for these courses. Skipping.")
            print("-" * 20)
            continue
            
        # 3. Get the type and teacher for each Canvas item
        teachers_query = f'query {{ items (ids: {json.dumps(list(canvas_item_ids))}) {{ id column_values(ids: ["{CANVAS_COURSES_TEACHER_COLUMN_ID}", "{CANVAS_COURSE_TYPE_COLUMN_ID}"]) {{ id text value }} }} }}'
        teachers_result = execute_monday_graphql(teachers_query)

        ace_teacher_ids = set()
        connect_teacher_ids = set()

        if teachers_result and teachers_result.get('data', {}).get('items'):
            for canvas_item in teachers_result['data']['items']:
                course_type = get_column_value_from_item_data(canvas_item, CANVAS_COURSE_TYPE_COLUMN_ID)
                people_value = get_column_value_from_item_data(canvas_item, CANVAS_COURSES_TEACHER_COLUMN_ID)
                
                if people_value:
                    persons = people_value.get('personsAndTeams', [])
                    teacher_id = persons[0]['id'] if persons else None
                    if teacher_id:
                        if course_type == "ACE":
                            ace_teacher_ids.add(teacher_id)
                        elif course_type == "Connect":
                            connect_teacher_ids.add(teacher_id)
        
        # 4. Update the Master Student record with the categorized teachers
        print(f"  -> Found {len(ace_teacher_ids)} ACE teachers and {len(connect_teacher_ids)} Connect teachers.")
        
        if ace_teacher_ids:
            print(f"    -> Syncing ACE teachers to column {MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID}...")
            update_people_column_with_ids(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_ACE_PEOPLE_COLUMN_ID, ace_teacher_ids)
        
        if connect_teacher_ids:
            print(f"    -> Syncing Connect teachers to column {MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID}...")
            update_people_column_with_ids(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_CONNECT_PEOPLE_COLUMN_ID, connect_teacher_ids)

        print("-" * 20)
        time.sleep(DELAY_BETWEEN_ITEMS)

    print("\nBulk PLP course sync complete!")

# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================
if __name__ == '__main__':
    bulk_sync_plp_courses()
