import os
import json
import requests
import time

# ==============================================================================
# BULK SYNC SCRIPT FOR MASTER STUDENT LIST TEACHER ASSIGNMENTS
# ==============================================================================
# This script is designed for a one-time, manual run to sync all teacher
# assignments from the Master Student List board to all linked boards as
# defined in your configuration. It includes a delay to prevent API rate-limiting.
# ==============================================================================

# ==============================================================================
# CONFIGURATION
# Load environment variables. Ensure these are set in your terminal before running.
# ==============================================================================
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"

# IDs from your main application's configuration
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")

try:
    # This mapping is the most critical part. It tells the script which columns to sync.
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
except json.JSONDecodeError:
    print("ERROR: Could not parse MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS. Please check your environment variable.")
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}

# Delay between processing each student item to avoid hitting the API rate limit.
# 0.25 seconds is a safe starting point.
DELAY_BETWEEN_ITEMS = 0.25

# ==============================================================================
# MONDAY.COM API UTILITIES (Copied from app.py for standalone use)
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
        # Construct the query with pagination support (using a cursor)
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
                            text
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
            break # No more pages
            
    return all_items

def get_linked_items_from_board_relation(item_data, connect_column_id):
    """Fetches linked item IDs from a 'Connect Boards' column using pre-fetched item data."""
    column_data = get_column_value_from_item_data(item_data, connect_column_id)
    if not column_data or not column_data.get('value'):
        return set()
        
    value_data = column_data['value']
    parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data) if isinstance(value_data, str) else {}
    if "linkedPulseIds" in parsed_value:
        return {int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"] if "linkedPulseId" in item}
    return set()

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    """Updates a People column on a specific item."""
    parsed_new_value = new_people_value if isinstance(new_people_value, dict) else json.loads(new_people_value) if isinstance(new_people_value, str) else {}
    persons_and_teams = parsed_new_value.get('personsAndTeams', [])
    
    if target_column_type == "person":
        person_id = persons_and_teams[0].get('id') if persons_and_teams else None
        graphql_value = json.dumps(json.dumps({"personId": person_id} if person_id else {}))
    elif target_column_type == "multiple-person":
        people_list = [{"id": p.get('id'), "kind": "person"} for p in persons_and_teams if 'id' in p]
        graphql_value = json.dumps(json.dumps({"personsAndTeams": people_list}))
    else:
        return False
        
    mutation = f"""
    mutation {{
        change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: "{people_column_id}", value: {graphql_value}) {{
            id
        }}
    }}
    """
    return execute_monday_graphql(mutation) is not None

# Helper to get a column value from already fetched item data
def get_column_value_from_item_data(item_data, column_id):
    for cv in item_data.get('column_values', []):
        if cv['id'] == column_id:
            parsed_value = None
            if cv.get('value'):
                try: parsed_value = json.loads(cv['value'])
                except json.JSONDecodeError: parsed_value = cv['value']
            return {'value': parsed_value, 'text': cv.get('text')}
    return None

# ==============================================================================
# MAIN SYNC LOGIC
# ==============================================================================
def bulk_sync_teachers():
    """Main function to orchestrate the bulk sync process."""
    print("Starting bulk teacher sync process...")
    
    if not MONDAY_API_KEY or not MASTER_STUDENT_BOARD_ID or not MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS:
        print("ERROR: Missing one or more required environment variables. Aborting.")
        return

    print(f"Fetching all students from Master Student Board (ID: {MASTER_STUDENT_BOARD_ID})...")
    all_students = get_all_items_from_board(MASTER_STUDENT_BOARD_ID)
    
    if not all_students:
        print("No students found on the board. Nothing to sync.")
        return

    total_students = len(all_students)
    print(f"Found {total_students} students to process.\n")

    for index, student_item in enumerate(all_students):
        student_id = student_item['id']
        student_name = student_item['name']
        print(f"Processing student {index + 1}/{total_students}: {student_name} (ID: {student_id})")

        # Iterate through each configured 'People' column that needs syncing
        for source_col_id, mappings in MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.items():
            people_col_data = get_column_value_from_item_data(student_item, source_col_id)

            # Skip if the source 'People' column is empty
            if not people_col_data or not people_col_data.get('value'):
                continue

            people_value_raw = people_col_data['value']
            
            # Iterate through the target boards/columns defined in the mapping
            for target in mappings.get("targets", []):
                target_board_id = target.get("board_id")
                target_col_id = target.get("target_column_id")
                connect_col_id = target.get("connect_column_id")
                target_col_type = target.get("target_column_type")

                if not all([target_board_id, target_col_id, connect_col_id, target_col_type]):
                    continue

                # Find all items linked to this student on the target board
                linked_item_ids = get_linked_items_from_board_relation(student_item, connect_col_id)
                
                if not linked_item_ids:
                    continue
                
                print(f"  -> Found {len(linked_item_ids)} linked item(s) on board {target_board_id} to update.")
                
                for linked_item_id in linked_item_ids:
                    print(f"    -> Syncing teacher to item {linked_item_id} in column {target_col_id}...")
                    update_people_column(
                        linked_item_id,
                        target_board_id,
                        target_col_id,
                        people_value_raw,
                        target_col_type
                    )
                    # Pause to respect API rate limits
                    time.sleep(DELAY_BETWEEN_ITEMS)

        print("-" * 20)

    print("\nBulk sync complete!")

# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================
if __name__ == '__main__':
    bulk_sync_teachers()
