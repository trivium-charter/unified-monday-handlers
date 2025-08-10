import os
import requests
import json

# --- CONFIGURATION ---
# Please get these values from your existing script or environment variables
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"
ALL_STAFF_BOARD_ID = os.environ.get("ALL_STAFF_BOARD_ID")

# --- IDs TO TEST ---
# 1. Get this from your Monday.com board by opening Kirsten Perkins' item.
#    The item ID is the number in the URL.
STAFF_ITEM_ID_TO_TEST = 4344164043 # <-- REPLACE WITH THE ACTUAL ITEM ID FOR KIRSTEN PERKINS

# 2. This is the column ID you provided.
PERSON_COLUMN_ID_TO_TEST = "multiple_person_mkrkvkek"
# --- END CONFIGURATION ---


def execute_monday_graphql(query):
    """Basic function to execute a GraphQL query."""
    headers = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }
    try:
        response = requests.post(MONDAY_API_URL, json={"query": query}, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        return None

def debug_get_column_value():
    """Fetches and prints the raw value of a single column for a single item."""
    query = f"""
    query {{
        items (ids: [{STAFF_ITEM_ID_TO_TEST}]) {{
            name
            column_values (ids: ["{PERSON_COLUMN_ID_TO_TEST}"]) {{
                id
                text
                value
                type
            }}
        }}
    }}
    """
    
    print("--- Sending Query to Monday.com ---")
    print(query)
    
    result = execute_monday_graphql(query)
    
    print("\n--- RAW RESPONSE FROM MONDAY.COM ---")
    if result:
        print(json.dumps(result, indent=2))
    else:
        print("No result returned.")

if __name__ == '__main__':
    debug_get_column_value()
