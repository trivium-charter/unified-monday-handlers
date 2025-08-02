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
    query = f"query {{ boards (ids: {board_id}) {{ items_page (query_params: {{ids: [{item_id}]}}) {{ items {{ name }} }} }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            return board['items_page']['items'][0].get('name')
    return None

def get_user_name(user_id):
    """Fetches a user's name from Monday.com given their user ID."""
    if user_id is None or user_id == -4:
        return None
    query = f"query {{ users (ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('users'):
        return result['data']['users'][0].get('name')
    return None

def get_column_value(item_id, board_id, column_id):
    """Fetches the column value and text for a given column."""
    query = f"query {{ boards(ids: {board_id}) {{ items_page(query_params: {{ids: [{item_id}]}}) {{ items {{ column_values(ids: [\"{column_id}\"]) {{ id value text }} }} }} }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            item = board['items_page']['items'][0]
            for col_val in item['column_values']:
                if col_val['id'] == column_id:
                    raw_value = col_val.get('value')
                    parsed_value = json.loads(raw_value) if raw_value and raw_value != 'null' else None
                    return {'value': parsed_value, 'text': col_val.get('text')}
    return None

def get_linked_ids_from_connect_column_value(value_data):
    """Parses a "Connect boards" column value and returns a set of linked item IDs."""
    if not value_data: return set()
    if isinstance(value_data, str):
        try: value_data = json.loads(value_data)
        except json.JSONDecodeError: return set()
    if not isinstance(value_data, dict): return set()
    if "linkedPulseIds" in value_data:
        return {int(item['linkedPulseId']) for item in value_data.get("linkedPulseIds", []) if 'linkedPulseId' in item}
    return set()

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    """Fetches the linked item IDs from a specific Connect Boards column."""
    column_data = get_column_value(item_id, board_id, connect_column_id)
    if column_data and column_data.get('value') is not None:
        return get_linked_ids_from_connect_column_value(column_data['value'])
    return set()

def update_connect_board_column(item_id, board_id, connect_column_id, item_to_link_id, action="add"):
    """Adds or removes a link to an item in a Connect Boards column."""
    current_linked_items = get_linked_items_from_board_relation(item_id, board_id, connect_column_id)
    target_item_id_int = int(item_to_link_id)
    if action == "add":
        if target_item_id_int in current_linked_items: return True
        updated_linked_items = current_linked_items | {target_item_id_int}
    elif action == "remove":
        if target_item_id_int not in current_linked_items: return True
        updated_linked_items = current_linked_items - {target_item_id_int}
    else: return False
    connect_value = {"linkedPulseIds": [{"linkedPulseId": lid} for lid in sorted(list(updated_linked_items))]}
    graphql_value = json.dumps(json.dumps(connect_value))
    mutation = f"mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: \"{connect_column_id}\", value: {graphql_value}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('change_column_value') is not None

def update_item_name(item_id, board_id, new_name):
    """Updates the name of a Monday.com item."""
    column_values = json.dumps({"name": new_name})
    graphql_value = json.dumps(column_values)
    mutation = f"mutation {{ change_multiple_column_values(board_id: {board_id}, item_id: {item_id}, column_values: {graphql_value}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('change_multiple_column_values')

def change_column_value_generic(board_id, item_id, column_id, value):
    """Updates a generic text or number column on a Monday.com item."""
    graphql_value = json.dumps(json.dumps(str(value)))
    mutation = f"mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: \"{column_id}\", value: {graphql_value}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('change_column_value')

def create_subitem(parent_item_id, subitem_name, column_values=None):
    """Creates a subitem and returns its ID and board ID."""
    values_for_api = json.dumps(json.dumps(column_values or {}))
    mutation = f"mutation {{ create_subitem(parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {values_for_api}) {{ id board {{ id }} }} }}"
    result = execute_monday_graphql(mutation)
    if result and 'data' in result and (subitem := result['data'].get('create_subitem')):
        if subitem.get('id') and subitem.get('board', {}).get('id'):
            return {'id': subitem.get('id'), 'board_id': subitem['board']['id']}
    return None

def create_update(item_id, update_text):
    """Creates an update on a Monday.com item."""
    mutation = f"mutation {{ create_update(item_id: {item_id}, body: {json.dumps(update_text)}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('create_update')

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    """Updates a People column on a Monday.com item."""
    parsed_new_value = json.loads(new_people_value) if isinstance(new_people_value, str) else new_people_value
    value_to_set = {}
    if target_column_type == "person":
        person_id = (parsed_new_value.get('personsAndTeams') or [{}])[0].get('id')
        if person_id: value_to_set = {"personId": person_id}
    elif target_column_type == "multiple-person":
        people_list = [{"id": p.get('id'), "kind": p.get('kind', 'person')} for p in parsed_new_value.get('personsAndTeams', []) if 'id' in p]
        value_to_set = {"personsAndTeams": people_list}
    graphql_value = json.dumps(json.dumps(value_to_set))
    mutation = f"mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: \"{people_column_id}\", value: {graphql_value}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('change_column_value')

def update_long_text_column(board_id, item_id, column_id, text_value):
    """Updates a Long Text column on a Monday.com item."""
    column_value = {"text": str(text_value)}
    graphql_value = json.dumps(json.dumps(column_value))
    mutation = f"mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: \"{column_id}\", value: {graphql_value}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('change_column_value')
