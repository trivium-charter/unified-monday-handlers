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

# --- This function uses these global variables for convenience ---
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")


def execute_monday_graphql(query):
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
    query = f"query {{ items(ids:[{item_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data']['items']:
        return result['data']['items'][0].get('name')
    return None

def get_user_name(user_id):
    if user_id is None: return None
    query = f"query {{ users (ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data']['users']:
        return result['data']['users'][0].get('name')
    return None

def get_user_details(user_id):
    query = f"query {{ users (ids: [{user_id}]) {{ id name email }} }}"
    results = execute_monday_graphql(query)
    if results and 'data' in results and 'users' in results['data'] and results['data']['users']:
        return results['data']['users'][0]
    return None
        
def get_column_value(item_id, board_id, column_id):
    query = f'query {{ boards(ids: {board_id}) {{ items_page(query_params:{{ids:[{item_id}]}}) {{ items {{ column_values (ids:["{column_id}"]) {{ id value text }} }} }} }} }}'
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data']['boards'][0]['items_page']['items']:
        for col_val in result['data']['boards'][0]['items_page']['items'][0]['column_values']:
            if col_val['id'] == column_id:
                return {'value': json.loads(col_val.get('value') or '{}'), 'text': col_val.get('text')}
    return None

def update_connect_board_column(item_id, board_id, connect_column_id, item_to_link_id, action="add"):
    current_linked_items = get_linked_items_from_board_relation(item_id, board_id, connect_column_id)
    if action == "add":
        updated_linked_items = current_linked_items | {int(item_to_link_id)}
    elif action == "remove":
        updated_linked_items = current_linked_items - {int(item_to_link_id)}
    else: return False
    connect_value = {"linkedPulseIds": [{"linkedPulseId": lid} for lid in sorted(list(updated_linked_items))]}
    mutation = f'mutation {{ change_column_value(board_id:{board_id}, item_id:{item_id}, column_id:"{connect_column_id}", value:{json.dumps(json.dumps(connect_value))}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return True if result and 'data' in result and 'change_column_value' in result['data'] else False

def get_linked_ids_from_connect_column_value(value_data):
    if not value_data: return set()
    if isinstance(value_data, str):
        try: value_data = json.loads(value_data)
        except json.JSONDecodeError: return set()
    if 'linkedPulseIds' in value_data:
        return {int(item['linkedPulseId']) for item in value_data['linkedPulseIds']}
    if 'linkedItems' in value_data:
        return {int(item['id']) for item in value_data['linkedItems']}
    return set()

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    column_data = get_column_value(item_id, board_id, connect_column_id)
    if column_data and 'value' in column_data:
        return get_linked_ids_from_connect_column_value(column_data['value'])
    return set()

def change_column_value_generic(board_id, item_id, column_id, value):
    mutation = f'mutation {{ change_column_value(board_id:{board_id}, item_id:{item_id}, column_id:"{column_id}", value:{json.dumps(str(value))}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return True if result and 'data' in result and 'change_column_value' in result['data'] else False

def create_subitem(parent_item_id, subitem_name, column_values=None):
    # THIS IS THE ORIGINAL, ROBUST VERSION OF THIS FUNCTION
    column_values_str = json.dumps(column_values) if column_values else "{}"
    mutation = f"""mutation {{ create_subitem(parent_item_id:{parent_item_id}, item_name:{json.dumps(subitem_name)}, column_values: {json.dumps(column_values_str)}) {{ id }} }}"""
    result = execute_monday_graphql(mutation)
    if result and 'data' in result and 'create_subitem' in result['data']:
        return result['data']['create_subitem'].get('id')
    return None
        
def create_update(item_id, update_text):
    mutation = f'mutation {{ create_update(item_id: {item_id}, body: {json.dumps(update_text)}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return True if result and 'data' in result and 'create_update' in result['data'] else False

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    if isinstance(new_people_value, str):
        try: new_people_value = json.loads(new_people_value)
        except json.JSONDecodeError: new_people_value = {}
    graphql_value = {"personsAndTeams": [{"id": p['id'], "kind": p.get('kind', 'person')} for p in new_people_value.get('personsAndTeams', [])]}
    mutation = f'mutation {{ change_column_value(board_id:{board_id}, item_id:{item_id}, column_id:"{people_column_id}", value:{json.dumps(json.dumps(graphql_value))}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return True if result and 'data' in result and 'change_column_value' in result['data'] else False

def find_subitem_by_category_and_linked_course(parent_item_id, category_col_id, category_name, connect_col_id, linked_course_id):
    query = f'query {{ items(ids:[{parent_item_id}]) {{ subitems {{ id column_values(ids:["{category_col_id}","{connect_col_id}"]) {{ id text value }} }} }} }}'
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data']['items']:
        for subitem in result['data']['items'][0].get('subitems', []):
            cat_match, course_match = False, False
            for col_val in subitem.get('column_values', []):
                if col_val['id'] == category_col_id and col_val.get('text', '').lower() == category_name.lower(): cat_match = True
                if col_val['id'] == connect_col_id and linked_course_id in get_linked_ids_from_connect_column_value(col_val.get('value')): course_match = True
            if cat_match and course_match: return int(subitem['id'])
    return None

# --- NEW FUNCTION ADDED HERE ---
def get_canvas_api_id_from_all_courses_item(all_courses_item_id):
    if not all([ALL_COURSES_BOARD_ID, CANVAS_BOARD_ID, ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID, CANVAS_COURSE_ID_COLUMN_ID]):
        print("ERROR: Missing one or more config variables for get_canvas_api_id_from_all_courses_item.")
        return None
    linked_canvas_ids = get_linked_items_from_board_relation(all_courses_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    if not linked_canvas_ids:
        return None
    canvas_item_id = int(list(linked_canvas_ids)[0])
    api_id_value = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    if api_id_value and api_id_value.get('text'):
        return api_id_value['text']
    return None
