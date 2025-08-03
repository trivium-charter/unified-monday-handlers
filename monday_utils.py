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
    if not result or 'errors' in result or not result.get('data', {}).get('boards'):
        return None
    
    board = result['data']['boards'][0]
    if board.get('items_page') and board['items_page'].get('items'):
        return board['items_page']['items'][0].get('name')
    return None

def get_user_name(user_id):
    """Fetches a user's name from Monday.com given their user ID."""
    if user_id is None:
        return None

    query = f"query {{ users (ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)

    if result and 'data' in result and result.get('data',{}).get('users'):
        return result['data']['users'][0].get('name')
    return None

def get_user_details(user_id):
    """Fetches the name and email for a specific monday.com user ID."""
    query = f"query {{ users (ids: [{user_id}]) {{ id name email }} }}"
    results = execute_monday_graphql(query)
        
    if results and 'data' in results and results.get('data',{}).get('users'):
        return results['data']['users'][0]
    else:
        print(f"WARNING: Could not find user details for ID: {user_id}. Response: {results}")
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

    if not result or 'errors' in result or not result.get('data', {}).get('boards'):
        return None

    board = result['data']['boards'][0]
    if board.get('items_page') and board['items_page'].get('items'):
        col_values = board['items_page']['items'][0].get('column_values')
        if col_values:
            col_val = col_values[0]
            raw_value_json_str = col_val.get('value')
            text_value = col_val.get('text')
            parsed_value_obj = None
            if raw_value_json_str:
                try: parsed_value_obj = json.loads(raw_value_json_str)
                except json.JSONDecodeError: parsed_value_obj = raw_value_json_str
            return {'value': parsed_value_obj, 'text': text_value}
    return None

def get_linked_ids_from_connect_column_value(value_data):
    """Parses a "Connect boards" column value and returns a set of linked item IDs."""
    if not value_data: return set()
    if isinstance(value_data, str):
        try: value_data = json.loads(value_data)
        except json.JSONDecodeError: return set()
    
    if not isinstance(value_data, dict): return set()

    linked_ids = set()
    pulse_ids = value_data.get("linkedPulseIds") or value_data.get("linkedItems")
    if pulse_ids and isinstance(pulse_ids, list):
        for item_dict in pulse_ids:
            if isinstance(item_dict, dict):
                item_id = item_dict.get("linkedPulseId") or item_dict.get("id")
                if item_id: linked_ids.add(int(item_id))
    return linked_ids

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    """Fetches the linked item IDs from a specific Connect Boards column for a given item."""
    column_data = get_column_value(item_id, board_id, connect_column_id)
    if column_data and column_data.get('value') is not None:
        return get_linked_ids_from_connect_column_value(column_data['value'])
    return set()

def change_column_value_generic(board_id, item_id, column_id, value):
    """Updates a generic column on a Monday.com item. Handles simple strings/numbers."""
    # This double-dumps is required by the Monday.com API for this mutation
    graphql_value_string_literal = json.dumps(json.dumps(str(value)))
    mutation = f"""
    mutation {{
      change_column_value (
        board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value_string_literal}
      ) {{ id }}
    }}"""
    result = execute_monday_graphql(mutation)
    return bool(result and 'data' in result and result['data'].get('change_column_value'))

def create_subitem(parent_item_id, subitem_name, column_values=None):
    """Creates a new subitem under a specified parent item."""
    column_values_str = json.dumps(column_values) if column_values else "{}"
    mutation = f"""
    mutation {{
      create_subitem (
        parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {json.dumps(column_values_str)}
      ) {{ id }}
    }}"""
    result = execute_monday_graphql(mutation)
    if result and 'data' in result and result['data'].get('create_subitem'):
        return result['data']['create_subitem'].get('id')
    return None

def create_update(item_id, update_text):
    """Creates an update on a Monday.com item."""
    mutation = f'mutation {{ create_update (item_id: {item_id}, body: {json.dumps(update_text)}) {{ id }} }}'
    result = execute_monday_graphql(mutation)
    return bool(result and 'data' in result and result['data'].get('create_update'))

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
    if result and 'data' in result and result['data'].get('items'):
        parent_item = result['data']['items'][0]
        if parent_item.get('subitems'):
            for subitem in parent_item['subitems']:
                category_match = False
                course_match = False
                for col_val in subitem['column_values']:
                    if col_val['id'] == category_col_id and col_val.get('text', '').lower() == category_name.lower():
                        category_match = True
                    if col_val['id'] == connect_col_id:
                        if linked_course_id in get_linked_ids_from_connect_column_value(col_val.get('value')):
                            course_match = True
                if category_match and course_match:
                    return int(subitem['id'])
    return None
