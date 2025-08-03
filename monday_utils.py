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
    print(f"DEBUG: Attempting to fetch name for item '{item_id}' on board '{board_id}' with query:\n{query}")
    result = execute_monday_graphql(query)

    if result is None:
        print(f"DEBUG: execute_monday_graphql returned None for item {item_id} name. API call likely failed.")
        return None
    elif 'errors' in result:
        print(f"DEBUG: Monday API returned errors when fetching name for item '{item_id}': {result['errors']}")
        return None
    elif result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            item = board['items_page']['items'][0]
            print(f"DEBUG: Fetched item {item_id} name: '{item.get('name')}'")
            return item.get('name')
    print(f"DEBUG: Could not fetch name for item {item_id}. Result: {result}")
    return None

def get_user_name(user_id):
    """Fetches a user's name from Monday.com given their user ID."""
    if user_id is None or user_id == -4: # -4 often indicates an automation
        return None

    query = f"""
    query {{
      users (ids: [{user_id}]) {{
        name
      }}
    }}
    """
    print(f"DEBUG: Attempting to fetch name for user '{user_id}' with query:\n{query}")
    result = execute_monday_graphql(query)

    if result is None:
        print(f"DEBUG: execute_monday_graphql returned None for user {user_id} name. API call likely failed.")
        return None
    elif 'errors' in result:
        print(f"DEBUG: Monday API returned errors when fetching name for user '{user_id}': {result['errors']}")
        return None
    elif result and 'data' in result and result['data'].get('users'):
        user = result['data']['users'][0]
        print(f"DEBUG: Fetched user {user_id} name: '{user.get('name')}'")
        return user.get('name')
    print(f"DEBUG: Could not fetch name for user {user_id}. Result: {result}")
    return None

# In monday_utils.py

# Add this function to the end of monday_utils.py

def get_user_details(user_id):
    """
    Fetches the name and email for a specific monday.com user ID.
    """
    try:
        query = f'''
        query {{
            users (ids: [{user_id}]) {{
                id
                name
                email
            }}
        }}
        '''
        print(f"DEBUG: Attempting to fetch details for user '{user_id}'")
        # ** THE FIX: Changed 'execute_query' to 'execute_monday_graphql' **
        results = execute_monday_graphql(query)
        
        if results and 'data' in results and 'users' in results['data'] and results['data']['users']:
            user_data = results['data']['users'][0]
            print(f"DEBUG: Fetched user {user_id} details: Name='{user_data.get('name')}', Email='{user_data.get('email')}'")
            return {
                'id': user_data.get('id'),
                'name': user_data.get('name'),
                'email': user_data.get('email')
            }
        else:
            print(f"WARNING: Could not find user details for ID: {user_id}. Response: {results}")
            return None
    except Exception as e:
        print(f"ERROR: Exception while fetching user details for ID {user_id}: {e}")
        return None
        
def get_column_value(item_id, board_id, column_id):
    """
    Fetches the column value and text for a given column.
    Returns a dictionary {'value': parsed_value_obj, 'text': text_string} or None.
    """
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
    print(f"DEBUG: Attempting to fetch column '{column_id}' for item '{item_id}' on board '{board_id}' with query:\n{query}")
    result = execute_monday_graphql(query)

    if result is None or 'errors' in result:
        print(f"DEBUG: Failed to get column value for item {item_id}, column {column_id}. Result: {result}")
        return None

    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            item = board['items_page']['items'][0]
            for col_val in item['column_values']:
                if col_val['id'] == column_id:
                    raw_value_json_str = col_val.get('value')
                    text_value = col_val.get('text')

                    parsed_value_obj = None
                    if raw_value_json_str:
                        try:
                            parsed_value_obj = json.loads(raw_value_json_str)
                        except json.JSONDecodeError:
                            print(f"WARNING: Raw value for column '{column_id}' is not valid JSON: {raw_value_json_str}")
                            parsed_value_obj = raw_value_json_str

                    print(f"DEBUG: Fetched column '{column_id}'. Raw value (parsed): {parsed_value_obj}, Text: '{text_value}'")
                    return {'value': parsed_value_obj, 'text': text_value}
    print(f"DEBUG: Column ID '{column_id}' not found for item {item_id} on board {board_id}.")
    return None

def update_connect_board_column(item_id, board_id, connect_column_id, item_to_link_id, action="add"):
    """
    Adds or removes a link to an item in a Connect Boards column.
    action can be "add" or "remove".
    """
    current_column_data = get_column_value(item_id, board_id, connect_column_id)
    current_linked_items = set()
    if current_column_data and current_column_data['value'] and "linkedPulseIds" in current_column_data['value']:
        current_linked_items = {int(p_id['linkedPulseId']) for p_id in current_column_data['value']['linkedPulseIds']}

    target_item_id_int = int(item_to_link_id)

    updated_linked_ids_list = []

    if action == "add":
        if target_item_id_int in current_linked_items:
            print(f"DEBUG: Item {target_item_id_int} already linked in column {connect_column_id}. No action needed.")
            return True
        updated_linked_items = current_linked_items | {target_item_id_int}
        updated_linked_ids_list = [{"linkedPulseId": lid} for lid in sorted(list(updated_linked_items))]
    elif action == "remove":
        if target_item_id_int not in current_linked_items:
            print(f"DEBUG: Item {target_item_id_int} not linked in column {connect_column_id}. No action needed.")
            return True
        updated_linked_items = current_linked_items - {target_item_id_int}
        updated_linked_ids_list = [{"linkedPulseId": lid} for lid in sorted(list(updated_linked_items))]
    else:
        print(f"ERROR: Invalid action '{action}' for update_connect_board_column. Must be 'add' or 'remove'.")
        return False

    connect_value_dict = {"linkedPulseIds": updated_linked_ids_list}
    graphql_value_string_literal = json.dumps(json.dumps(connect_value_dict))

    mutation = f"""
    mutation {{
      change_column_value (
        board_id: {board_id},
        item_id: {item_id},
        column_id: "{connect_column_id}",
        value: {graphql_value_string_literal}
      ) {{
        id
      }}
    }}
    """
    print(f"DEBUG: Attempting to {action} link item {target_item_id_int} in column {connect_column_id} for item {item_id} on board {board_id} with mutation:\n{mutation}")
    print(f"DEBUG: Full Mutation Query: \n{mutation}")
    result = execute_monday_graphql(mutation)

    if result and 'data' in result and result['data'].get('change_column_value'):
        print(f"Successfully {action}ed link for item {target_item_id_int} in column {connect_column_id} for item {item_id}.")
        return True
    else:
        print(f"Failed to {action} link for item {target_item_id_int} in column {connect_column_id} for item {item_id}. Result: {result}")
        if result and 'errors' in result:
            print(f"Monday API Errors: {result['errors']}")
        return False

def get_linked_ids_from_connect_column_value(value_data):
    """
    Parses the value data (which can be a dict from webhook or a JSON string from API)
    from a "Connect boards" column and returns a set of linked item IDs.
    """
    if not value_data:
        return set()

    if isinstance(value_data, str):
        try:
            parsed_value = json.loads(value_data)
        except json.JSONDecodeError:
            print(f"WARNING: Connect board column string value is not valid JSON: {value_data}")
            return set()
    elif isinstance(value_data, dict):
        parsed_value = value_data
    else:
        print(f"WARNING: Unexpected type for connect board column value: {type(value_data)}")
        return set()

    linked_ids = set()
    if "linkedPulseIds" in parsed_value:
        for item_dict in parsed_value["linkedPulseIds"]:
            if isinstance(item_dict, dict) and "linkedPulseId" in item_dict:
                linked_ids.add(int(item_dict["linkedPulseId"]))
    elif "linkedItems" in parsed_value:
        for item_dict in parsed_value["linkedItems"]:
            if isinstance(item_dict, dict) and "id" in item_dict:
                linked_ids.add(int(item_dict["id"]))

    print(f"DEBUG: Parsed linked IDs from value data: {linked_ids}")
    return linked_ids

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    """
    Fetches the linked item IDs from a specific Connect Boards column for a given item.
    It combines get_column_value and get_linked_ids_from_connect_column_value.
    Returns a set of linked item IDs.
    """
    print(f"DEBUG: monday_utils: Calling get_column_value for item {item_id} on board {board_id}, column {connect_column_id}")
    column_data = get_column_value(item_id, board_id, connect_column_id)
    if column_data and column_data.get('value') is not None:
        print(f"DEBUG: monday_utils: Calling get_linked_ids_from_connect_column_value with data: {column_data['value']}")
        return get_linked_ids_from_connect_column_value(column_data['value'])
    print(f"DEBUG: monday_utils: No linked items found or column data missing for item {item_id}, column {connect_column_id}.")
    return set()

def update_item_name(item_id, board_id, new_name):
    """Updates the name of a Monday.com item."""
    column_values_json_dict = {"name": new_name}
    inner_json_string = json.dumps(column_values_json_dict, separators=(',', ':'))
    graphql_column_values_string_literal = json.dumps(inner_json_string)

    mutation = f"""
    mutation {{
      change_multiple_column_values (
        board_id: {board_id},
        item_id: {item_id},
        column_values: {graphql_column_values_string_literal}
      ) {{
        id
        name
      }}
    }}
    """
    print(f"DEBUG: Attempting to update item {item_id} name to: '{new_name}'")
    result = execute_monday_graphql(mutation)

    if result and 'data' in result and result['data'].get('change_multiple_column_values'):
        print(f"Successfully updated item {item_id} name to '{new_name}'.")
        return True
    else:
        print(f"Failed to update item {item_id} name. Result: {result}")
        if result and 'errors' in result:
            print(f"Monday API Errors: {result['errors']}")
        return False

def change_column_value_generic(board_id, item_id, column_id, value):
    """
    Updates a generic text or number column on a Monday.com item.
    This is for simple string or number values.
    """
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
    print(f"DEBUG: monday_utils: Attempting to update column '{column_id}' for item {item_id} on board {board_id} with value: '{value}'")
    print(f"DEBUG: monday_utils: Constructed GraphQL value: {graphql_value_string_literal}")
    print(f"DEBUG: monday_utils: Full Mutation Query:\n{mutation}")

    result = execute_monday_graphql(mutation)

    if result and 'data' in result and result['data'].get('change_column_value'):
        print(f"Successfully updated column '{column_id}' for item {item_id} on board {board_id}.")
        return True
    else:
        print(f"ERROR: monday_utils: Failed to update column '{column_id}' for item {item_id} on board {board_id}. Result: {result}")
        if result and 'errors' in result:
            print(f"Monday API Errors: {result['errors']}")
        return False

def create_subitem(parent_item_id, subitem_name, column_values=None):
    """
    Creates a new subitem under a specified parent item.
    """
    values_for_monday_api = {}
    if column_values:
        for col_id, value in column_values.items():
            if isinstance(value, dict):
                values_for_monday_api[col_id] = value
            else:
                values_for_monday_api[col_id] = str(value)

    column_values_json_string_for_graphql = json.dumps(values_for_monday_api)
    
    print(f"DEBUG: monday_utils: Subitem column_values sent to GraphQL (inner JSON): {column_values_json_string_for_graphql}")

    mutation = f"""
    mutation {{
      create_subitem (
        parent_item_id: {parent_item_id},
        item_name: {json.dumps(subitem_name)},
        column_values: {json.dumps(column_values_json_string_for_graphql)}
      ) {{
        id
        name
        board {{
          id
        }}
      }}
    }}
    """
    print(f"DEBUG: monday_utils: Attempting to create subitem '{subitem_name}' under parent {parent_item_id} with mutation:\n{mutation}")
    result = execute_monday_graphql(mutation)

    if result and 'data' in result and result['data'].get('create_subitem'):
        new_subitem_id = result['data']['create_subitem'].get('id')
        print(f"Successfully created subitem '{subitem_name}' (ID: {new_subitem_id}) under item {parent_item_id}.")
        return new_subitem_id
    else:
        print(f"ERROR: monday_utils: Failed to create subitem '{subitem_name}' under item {parent_item_id}. Result: {result}")
        if result and 'errors' in result:
            print(f"Monday API Errors: {result['errors']}")
        return None

# In monday_utils.py

def create_item(board_id, item_name, column_values=None):
    """
    Creates a new main item on a specified board.
    """
    # Prepare column values for the GraphQL mutation
    column_values_str = json.dumps(column_values) if column_values else "{}"

    mutation = f"""
    mutation {{
      create_item (
        board_id: {board_id},
        item_name: {json.dumps(item_name)},
        column_values: {json.dumps(column_values_str)}
      ) {{
        id
      }}
    }}
    """
    print(f"DEBUG: monday_utils: Attempting to create item '{item_name}' on board {board_id}")
    result = execute_monday_graphql(mutation)

    if result and 'data' in result and result['data'].get('create_item'):
        new_item_id = result['data']['create_item'].get('id')
        print(f"Successfully created item '{item_name}' (ID: {new_item_id}) on board {board_id}.")
        return new_item_id
    else:
        print(f"ERROR: monday_utils: Failed to create item '{item_name}' on board {board_id}. Result: {result}")
        if result and 'errors' in result:
            print(f"Monday API Errors: {result['errors']}")
        return None
        
def create_update(item_id, update_text):
    """
    Creates an update on a Monday.com item.
    """
    mutation = f"""
    mutation {{
      create_update (
        item_id: {item_id},
        body: {json.dumps(update_text)}
      ) {{
        id
      }}
    }}
    """
    print(f"DEBUG: monday_utils: Attempting to create update for item {item_id} with text: '{update_text}'")
    result = execute_monday_graphql(mutation)

    if result and 'data' in result and result['data'].get('create_update'):
        print(f"Successfully created update for item {item_id}.")
        return True
    else:
        print(f"ERROR: monday_utils: Failed to create update for item {item_id}. Result: {result}")
        if result and 'errors' in result:
            print(f"Monday API Errors: {result['errors']}")
        return False

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    """
    Updates a People column on a Monday.com item.
    """
    graphql_value_string_literal = None

    if isinstance(new_people_value, str):
        try:
            parsed_new_value = json.loads(new_people_value)
        except json.JSONDecodeError:
            print(f"WARNING: monday_utils: Raw people value is not valid JSON: {new_people_value}. Treating as empty.")
            parsed_new_value = {}
    elif isinstance(new_people_value, dict):
        parsed_new_value = new_people_value
    else:
        print(f"ERROR: monday_utils: Unexpected type for new_people_value: {type(new_people_value)}. Cannot process.")
        return False

    if target_column_type == "person":
        if parsed_new_value and parsed_new_value.get('personsAndTeams') and len(parsed_new_value['personsAndTeams']) > 0:
            person_id = parsed_new_value['personsAndTeams'][0].get('id')
            if person_id is not None:
                graphql_value_string_literal = json.dumps(json.dumps({"personId": person_id}))
            else:
                graphql_value_string_literal = json.dumps(json.dumps({}))
        else:
            graphql_value_string_literal = json.dumps(json.dumps({}))
    elif target_column_type == "multiple-person":
        if parsed_new_value and parsed_new_value.get('personsAndTeams') is not None:
            people_list = []
            for p in parsed_new_value['personsAndTeams']:
                if 'id' in p:
                    people_list.append({"id": p.get('id'), "kind": p.get('kind', 'person')})
            graphql_value_string_literal = json.dumps(json.dumps({"personsAndTeams": people_list}))
        else:
            graphql_value_string_literal = json.dumps(json.dumps({"personsAndTeams": []}))
    else:
        print(f"WARNING: monday_utils: Unknown target_column_type '{target_column_type}'. Cannot update column {people_column_id}.")
        return False

    if graphql_value_string_literal is None:
        print(f"ERROR: monday_utils: Failed to construct GraphQL value for column {people_column_id} of type {target_column_type}.")
        return False

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
    print(f"DEBUG: monday_utils: Attempting to update people column '{people_column_id}' (type: {target_column_type}) for item {item_id} on board {board_id} with raw input value: {new_people_value}")
    print(f"DEBUG: monday_utils: Constructed GraphQL value: {graphql_value_string_literal}")
    print(f"DEBUG: monday_utils: Full Mutation query:\n{mutation}")
    
    result = execute_monday_graphql(mutation) 

    if result and 'data' in result and result['data'].get('change_column_value'):
        print(f"Successfully updated people column '{people_column_id}' for item {item_id} on board {board_id}.")
        return True
    else:
        print(f"ERROR: monday_utils: Failed to update people column '{people_column_id}' for item {item_id} on board {board_id}. Result: {result}")
        if result and 'errors' in result:
            print(f"Monday API Errors: {result['errors']}")
        return False

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    """
    Fetches the linked item IDs from a specific Connect Boards column for a given item.
    It combines get_column_value and get_linked_ids_from_connect_column_value.
    Returns a set of linked item IDs.
    """
    print(f"DEBUG: monday_utils: Calling get_column_value for item {item_id} on board {board_id}, column {connect_column_id}")
    column_data = get_column_value(item_id, board_id, connect_column_id)
    if column_data and column_data.get('value') is not None:
        print(f"DEBUG: monday_utils: Calling get_linked_ids_from_connect_column_value with data: {column_data['value']}")
        return get_linked_ids_from_connect_column_value(column_data['value'])
    print(f"DEBUG: monday_utils: No linked items found or column data missing for item {item_id}, column {connect_column_id}.")
    return set()
