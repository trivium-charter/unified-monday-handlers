#!/usr/bin/env python3
# ==============================================================================
# ONE-TIME SUBITEM CLEANUP SCRIPT
# ==============================================================================

import os
import json
import requests
import mysql.connector

# --- CONFIGURATION (Copy the relevant variables from your main script) ---
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = os.environ.get("DB_PORT", 3306)
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")

# --- MONDAY.COM UTILITIES ---
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
    try:
        response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Monday.com API Error: {e}")
        return None

def get_all_board_items(board_id):
    all_items = []
    cursor = None
    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        query = f'query {{ boards(ids: {board_id}) {{ items_page(limit: 100, {cursor_str}) {{ cursor items {{ id name }} }} }} }}'
        result = execute_monday_graphql(query)
        if not result or 'data' not in result: break
        try:
            page_info = result['data']['boards'][0]['items_page']
            all_items.extend(page_info['items'])
            cursor = page_info.get('cursor')
            if not cursor: break
            print(f"  Fetched {len(all_items)} items...")
        except (KeyError, IndexError):
            break
    return all_items

def get_user_id(user_name):
    query = f"query {{ users(kind: all) {{ id name }} }}"
    result = execute_monday_graphql(query)
    try:
        for user in result['data']['users']:
            if user['name'].lower() == user_name.lower():
                return user['id']
    except (KeyError, IndexError, TypeError):
        pass
    return None

def delete_item(item_id):
    mutation = f"mutation {{ delete_item (item_id: {item_id}) {{ id }} }}"
    return execute_monday_graphql(mutation)

def clear_subitems_by_creator(parent_item_id, creator_id_to_delete):
    query = f'query {{ items (ids: [{parent_item_id}]) {{ subitems {{ id creator {{ id }} }} }} }}'
    result = execute_monday_graphql(query)
    subitems_to_delete = []
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            if subitem.get('creator') and str(subitem['creator'].get('id')) == str(creator_id_to_delete):
                subitems_to_delete.append(subitem['id'])
    except (KeyError, IndexError, TypeError):
        return
    if not subitems_to_delete:
        return
    print(f"INFO: Found {len(subitems_to_delete)} subitem(s) by creator {creator_id_to_delete} to delete for item {parent_item_id}.")
    for subitem_id in subitems_to_delete:
        print(f"DELETING subitem {subitem_id}...")
        delete_item(subitem_id)

# --- SCRIPT EXECUTION ---
if __name__ == '__main__':
    TARGET_USER_NAME = "Sarah Bruce"
    
    print("======================================================")
    print("=== STARTING TARGETED SUBITEM CLEANUP SCRIPT     ===")
    print("======================================================")
    
    db = None
    cursor = None
    try:
        print("INFO: Connecting to the database...")
        ssl_opts = {'ssl_ca': 'ca.pem', 'ssl_verify_cert': True}
        db = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME, port=int(DB_PORT), **ssl_opts
        )
        cursor = db.cursor()

        print("INFO: Fetching list of already processed students (safe list)...")
        cursor.execute("SELECT student_id FROM processed_students")
        safe_student_ids = {row[0] for row in cursor.fetchall()}
        print(f"INFO: Found {len(safe_student_ids)} students on the safe list. They will be skipped.")

        print("INFO: Finding creator ID...")
        creator_id = get_user_id(TARGET_USER_NAME)
        if not creator_id:
            raise Exception(f"Could not find target user '{TARGET_USER_NAME}'.")

        print("INFO: Fetching all PLP board items...")
        all_plp_items = get_all_board_items(PLP_BOARD_ID)

        items_to_clean = [item for item in all_plp_items if int(item['id']) not in safe_student_ids]
        
        print(f"INFO: Found {len(items_to_clean)} students whose subitems will be cleaned.")

        for i, item in enumerate(items_to_clean, 1):
            item_id = int(item['id'])
            print(f"\n--- Cleaning item {i}/{len(items_to_clean)} (ID: {item_id}) ---")
            clear_subitems_by_creator(item_id, creator_id)

    except Exception as e:
        print(f"A critical error occurred: {e}")
    finally:
        if cursor: cursor.close()
        if db and db.is_connected():
            db.close()
            print("\nINFO: Database connection closed.")

    print("\n======================================================")
    print("=== CLEANUP SCRIPT FINISHED                        ===")
    print("======================================================")
