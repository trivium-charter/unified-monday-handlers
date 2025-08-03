#
# This is the complete and correct code for monday_utils.py
#
import os
import requests
import json

MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"
HEADERS = {"Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10"}
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")

def execute_monday_graphql(query):
    try:
        response = requests.post(MONDAY_API_URL, json={"query": query}, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Monday API Error: {e}")
        return None

def get_item_name(item_id, board_id):
    result = execute_monday_graphql(f"query {{ items(ids:[{item_id}]) {{ name }} }}")
    return result['data']['items'][0].get('name') if result and result.get('data', {}).get('items') else None

def get_user_name(user_id):
    if not user_id: return None
    result = execute_monday_graphql(f"query {{ users (ids: [{user_id}]) {{ name }} }}")
    return result['data']['users'][0].get('name') if result and result.get('data', {}).get('users') else None

def get_user_details(user_id):
    result = execute_monday_graphql(f"query {{ users (ids: [{user_id}]) {{ id name email }} }}")
    return result['data']['users'][0] if result and result.get('data', {}).get('users') else None

def get_column_value(item_id, board_id, column_id):
    query = f'query {{ boards(ids: {board_id}) {{ items_page(query_params:{{ids:[{item_id}]}}) {{ items {{ column_values (ids:["{column_id}"]) {{ id value text }} }} }} }} }}'
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('boards', [{}])[0].get('items_page', {}).get('items'):
        for col_val in result['data']['boards'][0]['items_page']['items'][0].get('column_values', []):
            if col_val['id'] == column_id:
                raw_value = col_val.get('value')
                return {'value': json.loads(raw_value) if raw_value else None, 'text': col_val.get('text')}
    return {}

def get_linked_ids_from_connect_column_value(value_data):
    if not value_data: return set()
    if isinstance(value_data, str):
        try: value_data = json.loads(value_data)
        except json.JSONDecodeError: return set()
    if isinstance(value_data, dict):
        key = "linkedPulseIds" if "linkedPulseIds" in value_data else "linkedItems"
        id_key = "linkedPulseId" if key == "linkedPulseIds" else "id"
        return {int(item[id_key]) for item in value_data.get(key, []) if item and id_key in item}
    return set()

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    col_data = get_column_value(item_id, board_id, connect_column_id)
    return get_linked_ids_from_connect_column_value(col_data.get('value'))

def change_column_value_generic(board_id, item_id, column_id, value):
    mutation = f'mutation {{ change_column_value(board_id:{board_id}, item_id:{item_id}, column_id:"{column_id}", value:{json.dumps(str(value))}) {{ id }} }}'
    return execute_monday_graphql(mutation) is not None

def update_connect_board_column(item_id, board_id, connect_column_id, item_to_link, action):
    current_ids = get_linked_items_from_board_relation(item_id, board_id, connect_column_id)
    if action == "add": updated_ids = current_ids | {int(item_to_link)}
    else: updated_ids = current_ids - {int(item_to_link)}
    value = {"linkedPulseIds": [{"linkedPulseId": lid} for lid in sorted(list(updated_ids))]}
    mutation = f'mutation {{ change_column_value(board_id:{board_id}, item_id:{item_id}, column_id:"{connect_column_id}", value:{json.dumps(json.dumps(value))}) {{ id }} }}'
    return execute_monday_graphql(mutation) is not None

def create_subitem(parent_item_id, subitem_name, column_values=None):
    cols_str = json.dumps(json.dumps(column_values)) if column_values else json.dumps("{}")
    mutation = f"mutation {{ create_subitem(parent_item_id:{parent_item_id}, item_name:{json.dumps(subitem_name)}, column_values: {cols_str}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result['data']['create_subitem'].get('id') if result and result.get('data', {}).get('create_subitem') else None

def create_update(item_id, update_text):
    mutation = f'mutation {{ create_update(item_id: {item_id}, body: {json.dumps(update_text)}) {{ id }} }}'
    return execute_monday_graphql(mutation) is not None

def update_people_column(item_id, board_id, col_id, new_val, col_type):
    if isinstance(new_val, str):
        try: new_val = json.loads(new_val)
        except json.JSONDecodeError: new_val = {}
    value = {"personsAndTeams": [{"id": p['id']} for p in new_val.get('personsAndTeams', [])]}
    mutation = f'mutation {{ change_column_value(board_id:{board_id}, item_id:{item_id}, column_id:"{col_id}", value:{json.dumps(json.dumps(value))}) {{ id }} }}'
    return execute_monday_graphql(mutation) is not None

def find_subitem_by_category_and_linked_course(p_id, cat_col, cat_name, conn_col, course_id):
    query = f'query {{ items(ids:[{p_id}]) {{ subitems {{ id column_values(ids:["{cat_col}","{conn_col}"]) {{ id text value }} }} }} }}'
    result = execute_monday_graphql(query)
    if result and result.get('data', {}).get('items', [{}])[0].get('subitems'):
        for sub in result['data']['items'][0]['subitems']:
            cat_match = any(c['id'] == cat_col and c.get('text', '').lower() == cat_name.lower() for c in sub.get('column_values', []))
            course_match = any(c['id'] == conn_col and course_id in get_linked_ids_from_connect_column_value(c.get('value')) for c in sub.get('column_values', []))
            if cat_match and course_match: return int(sub['id'])
    return None

def get_canvas_api_id_from_all_courses_item(all_courses_item_id):
    if not all([ALL_COURSES_BOARD_ID, CANVAS_BOARD_ID, ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID, CANVAS_COURSE_ID_COLUMN_ID]): return None
    linked_ids = get_linked_items_from_board_relation(all_courses_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    if not linked_ids: return None
    canvas_item_id = list(linked_ids)[0]
    api_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    return api_id_val.get('text') if api_id_val else None
