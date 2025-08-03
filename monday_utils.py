#
# This is the complete and correct code for monday_utils.py
#
import os
import requests
import json

MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"
HEADERS = {
    "Authorization": MONDAY_API_KEY,
    "Content-Type": "application/json",
    "API-Version": "2023-10",
}

# These must be set for the get_canvas_api_id function to work
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")


def execute_monday_graphql(query):
    """Executes a GraphQL query/mutation against the Monday.com API."""
    data = {"query": query}
    try:
        response = requests.post(MONDAY_API_URL, json=data, headers=HEADERS)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error communicating with Monday.com API: {e}")
        if e.response is not None:
            print(f"Monday API Response Content: {e.response.text}")
        return None

def get_item_name(item_id, board_id):
    """Fetches the name of a Monday.com item given its ID and board ID."""
    query = f"""
    query {{
      boards (ids: {board_id}) {{
        items_page (query_params: {{ids: [{item_id}]}}) {{
          items {{ name }}
        }}
      }}
    }}"""
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('boards'):
        if result['data']['boards'][0]['items_page']['items']:
            return result['data']['boards'][0]['items_page']['items'][0].get('name')
    return None

def get_user_name(user_id):
    """Fetches a user's name from Monday.com given their user ID."""
    if not user_id or user_id == -4: return None
    query = f"query {{ users (ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('users'):
        return result['data']['users'][0].get('name')
    return None

def get_user_details(user_id):
    """Fetches the name and email for a specific monday.com user ID."""
    query = f'query {{ users (ids: [{user_id}]) {{ id name email }} }}'
    results = execute_monday_graphql(query)
    if results and 'data' in results and 'users' in results['data'] and results['data']['users']:
        return results['data']['users'][0]
    return None
        
def get_column_value(item_id, board_id, column_id):
    """Fetches the column value and text for a given column."""
    query = f"""
    query {{
      boards (ids: {board_id}) {{
        items_page (query_params: {{ids: [{item_id}]}}) {{
          items {{
            column_values (ids: ["{column_id}"]) {{ id value text }}
          }}
        }}
      }}
    }}"""
    result = execute_monday_graphql(query)
    if not result or 'errors' in result:
        print(f"Failed to get column value for item {item_id}, column {column_id}. Result: {result}")
        return None
    if result.get('data', {}).get('boards', [{}])[0].get('items_page', {}).get('items', [{}]):
        item = result['data']['boards'][0]['items_page']['items'][0]
        for col_val in item.get('column_values', []):
            if col_val.get('id') == column_id:
                raw_value = col_val.get('value')
                text_value = col_val.get('text')
                parsed_value = json.loads(raw_value) if raw_value and raw_value.startswith('{') else raw_value
                return {'value': parsed_value, 'text': text_value}
    return None

def update_connect_board_column(item_id, board_id, connect_column_id, item_to_link_id, action="add"):
    """Adds or removes a link to an item in a Connect Boards column."""
    current_ids = get_linked_items_from_board_relation(item_id, board_id, connect_column_id)
    target_id = int(item_to_link_id)

    if action == "add":
        if target_id in current_ids: return True # Already there
        updated_ids = current_ids | {target_id}
    elif action == "remove":
        if target_id not in current_ids: return True # Already gone
        updated_ids = current_ids - {target_id}
    else:
        return False

    connect_value = {"linkedPulseIds": [{"linkedPulseId": lid} for lid in sorted(list(updated_ids))]}
    value_str = json.dumps(json.dumps(connect_value))
    mutation = f'mutation {{ change_column_value (board_id: {board_id}, item_id: {item_id}, column_id: "{connect_column_id}", value: {value_str}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('change_column_value')

def get_linked_ids_from_connect_column_value(value_data):
    """Parses data from a "Connect boards" column and returns a set of linked item IDs."""
    if not value_data: return set()
    if isinstance(value_data, str):
        try: value_data = json.loads(value_data)
        except json.JSONDecodeError: return set()
    
    linked_ids = set()
    if isinstance(value_data, dict):
        key = "linkedPulseIds" if "linkedPulseIds" in value_data else "linkedItems"
        id_key = "linkedPulseId" if key == "linkedPulseIds" else "id"
        for item in value_data.get(key, []):
            if isinstance(item, dict) and id_key in item:
                linked_ids.add(int(item[id_key]))
    return linked_ids

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    """Fetches the linked item IDs from a specific Connect Boards column for a given item."""
    column_data = get_column_value(item_id, board_id, connect_column_id)
    return get_linked_ids_from_connect_column_value(column_data.get('value')) if column_data else set()

def change_column_value_generic(board_id, item_id, column_id, value):
    """Updates a generic text or number column on a Monday.com item."""
    value_str = json.dumps(json.dumps(str(value)))
    mutation = f'mutation {{ change_column_value (board_id: {int(board_id)}, item_id: {int(item_id)}, column_id: "{column_id}", value: {value_str}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('change_column_value')

def create_subitem(parent_item_id, subitem_name, column_values=None):
    """Creates a new subitem under a specified parent item."""
    values_str = json.dumps(json.dumps(column_values)) if column_values else "{}"
    mutation = f'mutation {{ create_subitem (parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {values_str}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    if result and 'data' in result and 'create_subitem' in result['data']:
        return result['data']['create_subitem'].get('id')
    return None

def create_update(item_id, update_text):
    """Creates an update on a Monday.com item."""
    mutation = f'mutation {{ create_update (item_id: {item_id}, body: {json.dumps(update_text)}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and 'create_update' in result['data']

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    """Updates a People column on a Monday.com item."""
    if isinstance(new_people_value, str):
        try: new_people_value = json.loads(new_people_value)
        except json.JSONDecodeError: new_people_value = {}
    
    people_list = [{"id": p['id']} for p in new_people_value.get('personsAndTeams', []) if 'id' in p]
    value_str = json.dumps(json.dumps({"personsAndTeams": people_list}))
    mutation = f'mutation {{ change_column_value (board_id: {int(board_id)}, item_id: {int(item_id)}, column_id: "{people_column_id}", value: {value_str}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and 'change_column_value' in result['data']

def find_subitem_by_category_and_linked_course(parent_item_id, category_col_id, category_name, connect_col_id, linked_course_id):
    """Finds a subitem under a parent that matches a category and contains a specific linked item."""
    query = f"""
    query {{
      items (ids: [{parent_item_id}]) {{
        subitems {{
          id
          column_values (ids: ["{category_col_id}", "{connect_col_id}"]) {{
            id text value
          }}
        }}
      }}
    }}"""
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data']['items']:
        for subitem in result['data']['items'][0].get('subitems', []):
            cat_match = any(c['id'] == category_col_id and c.get('text', '').lower() == category_name.lower() for c in subitem.get('column_values', []))
            course_match = any(c['id'] == connect_col_id and linked_course_id in get_linked_ids_from_connect_column_value(c.get('value')) for c in subitem.get('column_values', []))
            if cat_match and course_match:
                return int(subitem['id'])
    return None

def get_canvas_api_id_from_all_courses_item(all_courses_item_id):
    """Performs a 'double-hop' lookup to get the numeric Canvas API ID."""
    if not all([ALL_COURSES_BOARD_ID, CANVAS_BOARD_ID, ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID, CANVAS_COURSE_ID_COLUMN_ID]):
        print("ERROR: Missing one or more config variables for get_canvas_api_id_from_all_courses_item.")
        return None

    linked_canvas_ids = get_linked_items_from_board_relation(
        all_courses_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID
    )
    if not linked_canvas_ids:
        print(f"WARN: No linked Canvas item found for All Courses item {all_courses_item_id}")
        return None
    
    canvas_item_id = int(list(linked_canvas_ids)[0])
    api_id_value = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    
    if api_id_value and api_id_value.get('text'):
        return api_id_value['text']
    else:
        print(f"WARN: Could not retrieve Canvas API ID from Canvas item {canvas_item_id}")
        return None
