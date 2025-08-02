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
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
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
          items {{
            name
          }}
        }}
      }}
    }}
    """
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
    query = f"""
    query {{
      users (ids: [{user_id}]) {{
        name
      }}
    }}
    """
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('users'):
        return result['data']['users'][0].get('name')
    return None

def get_column_value(item_id, board_id, column_id):
    """Fetches the column value and text for a given column."""
    query = f"""
    query {{
      boards (ids: {board_id}) {{
        items_page (query_params: {{ids: [{item_id}]}}) {{
          items {{
            column_values (ids: ["{column_id}"]) {{
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
    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            item = board['items_page']['items'][0]
            for col_val in item['column_values']:
                if col_val['id'] == column_id:
                    raw_value = col_val.get('value')
                    parsed_value = json.loads(raw_value) if raw_value else None
                    return {'value': parsed_value, 'text': col_val.get('text')}
    return None

def get_linked_ids_from_connect_column_value(value_data):
    """Parses a "Connect boards" column value and returns a set of linked item IDs."""
    if not value_data:
        return set()
    linked_ids = set()
    if "linkedPulseIds" in value_data:
        for item_dict in value_data["linkedPulseIds"]:
            if isinstance(item_dict, dict) and "linkedPulseId" in item_dict:
                linked_ids.add(int(item_dict["linkedPulseId"]))
    return linked_ids

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    """Fetches the linked item IDs from a specific Connect Boards column."""
    column_data = get_column_value(item_id, board_id, connect_column_id)
    if column_data and column_data.get('value') is not None:
        return get_linked_ids_from_connect_column_value(column_data['value'])
    return set()

def change_column_value_generic(board_id, item_id, column_id, value):
    """Updates a generic text or number column on a Monday.com item."""
    graphql_value_string_literal = json.dumps(json.dumps(str(value)))
    mutation = f"""
    mutation {{
      change_column_value (
        board_id: {board_id},
        item_id: {item_id},
        column_id: "{column_id}",
        value: {graphql_value_string_literal}
      ) {{
        id
      }}
    }}
    """
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('change_column_value')

def create_subitem(parent_item_id, subitem_name, column_values=None):
    """Creates a subitem and returns its ID and board ID."""
    values_for_api = json.dumps(column_values or {})
    mutation = f"""
    mutation {{
      create_subitem (
        parent_item_id: {parent_item_id},
        item_name: {json.dumps(subitem_name)},
        column_values: {json.dumps(values_for_api)}
      ) {{
        id
        board {{ id }}
      }}
    }}
    """
    result = execute_monday_graphql(mutation)
    if result and 'data' in result and (subitem := result['data'].get('create_subitem')):
        return {'id': subitem.get('id'), 'board_id': subitem.get('board', {}).get('id')}
    return None

def update_long_text_column(board_id, item_id, column_id, text_value):
    """Updates a Long Text column on a Monday.com item."""
    column_value = {"text": str(text_value)}
    graphql_value_string_literal = json.dumps(json.dumps(column_value))
    mutation = f"""
    mutation {{
      change_column_value (
        board_id: {board_id},
        item_id: {item_id},
        column_id: "{column_id}",
        value: {graphql_value_string_literal}
      ) {{
        id
      }}
    }}
    """
    result = execute_monday_graphql(mutation)
    if result and 'data' in result and result['data'].get('change_column_value'):
        print(f"Successfully updated long text column '{column_id}' for item {item_id}.")
        return True
    else:
        print(f"ERROR: Failed to update long text column '{column_id}' for item {item_id}. Result: {result}")
        return False

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    """Updates a People column on a Monday.com item."""
    parsed_new_value = json.loads(new_people_value) if isinstance(new_people_value, str) else new_people_value
    value_to_set = {}
    if target_column_type == "person":
        person_id = (parsed_new_value.get('personsAndTeams') or [{}])[0].get('id')
        if person_id:
            value_to_set = {"personId": person_id}
    elif target_column_type == "multiple-person":
        people_list = [{"id": p.get('id'), "kind": p.get('kind', 'person')} for p in parsed_new_value.get('personsAndTeams', []) if 'id' in p]
        value_to_set = {"personsAndTeams": people_list}
    graphql_value_string_literal = json.dumps(json.dumps(value_to_set))
    mutation = f"""
    mutation {{
      change_column_value (
        board_id: {board_id},
        item_id: {item_id},
        column_id: "{people_column_id}",
        value: {graphql_value_string_literal}
      ) {{
        id
      }}
    }}
    """
    result = execute_monday_graphql(mutation)
    return result and 'data' in result and result['data'].get('change_column_value')
