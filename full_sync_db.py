#!/usr/bin/env python3
import os
import json
import requests
import time
import sys
import signal
import traceback
from datetime import datetime
import pymysql
from pymysql.err import IntegrityError

# ==============================================================================
# DATABASE CONFIGURATION
# ==============================================================================
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "monday_sync")
DB_PORT = int(os.environ.get("DB_PORT", 3306))

# ==============================================================================
# GRACEFUL SHUTDOWN HANDLER
# ==============================================================================
shutdown_requested = False

def signal_handler(signum, frame):
    global shutdown_requested
    print(f"\nReceived signal {signum}. Initiating graceful shutdown...")
    shutdown_requested = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ==============================================================================
# DATABASE SETUP AND MANAGEMENT
# ==============================================================================

def get_db_connection():
    """Create a new database connection with retry logic."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            return pymysql.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                port=DB_PORT,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10
            )
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"Database connection attempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff

def init_database():
    """Initialize database tables if they don't exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Create sync progress table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_progress (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    plp_item_id INT UNIQUE NOT NULL,
                    student_name VARCHAR(255),
                    status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
                    subitems_deleted BOOLEAN DEFAULT FALSE,
                    sync_completed BOOLEAN DEFAULT FALSE,
                    error_message TEXT,
                    started_at TIMESTAMP NULL,
                    completed_at TIMESTAMP NULL,
                    retry_count INT DEFAULT 0,
                    INDEX idx_status (status),
                    INDEX idx_plp_item (plp_item_id)
                )
            """)
            
            # Create deleted subitems tracking table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS deleted_subitems (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    plp_item_id INT NOT NULL,
                    subitem_id INT NOT NULL,
                    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    deleted_by VARCHAR(255),
                    UNIQUE KEY unique_subitem (plp_item_id, subitem_id),
                    INDEX idx_plp_item (plp_item_id)
                )
            """)
            
            # Create sync run metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_runs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP NULL,
                    total_items INT DEFAULT 0,
                    processed_items INT DEFAULT 0,
                    failed_items INT DEFAULT 0,
                    status ENUM('running', 'completed', 'failed', 'interrupted') DEFAULT 'running'
                )
            """)
            
            conn.commit()
            print("Database tables initialized successfully")
    finally:
        conn.close()

def get_or_create_sync_run():
    """Get current sync run or create a new one."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Check for existing incomplete run
            cursor.execute("""
                SELECT id FROM sync_runs 
                WHERE status = 'running' 
                ORDER BY started_at DESC LIMIT 1
            """)
            result = cursor.fetchone()
            
            if result:
                run_id = result['id']
                print(f"Resuming sync run ID: {run_id}")
            else:
                cursor.execute("""
                    INSERT INTO sync_runs (status) VALUES ('running')
                """)
                conn.commit()
                run_id = cursor.lastrowid
                print(f"Started new sync run ID: {run_id}")
            
            return run_id
    finally:
        conn.close()

def populate_items_to_process(plp_board_id, batch_size=100):
    """Populate database with items to process, fetching in batches."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            print("Fetching items from Monday.com...")
            cursor_str = None
            total_added = 0
            
            while True:
                if shutdown_requested:
                    print("Shutdown requested during item population")
                    break
                    
                # Fetch batch from Monday.com
                items_batch = fetch_board_items_batch(plp_board_id, cursor_str, batch_size)
                if not items_batch['items']:
                    break
                
                # Insert items into database
                for item in items_batch['items']:
                    try:
                        cursor.execute("""
                            INSERT IGNORE INTO sync_progress 
                            (plp_item_id, student_name, status) 
                            VALUES (%s, %s, 'pending')
                        """, (item['id'], item.get('name', '')))
                        if cursor.rowcount > 0:
                            total_added += 1
                    except IntegrityError:
                        pass  # Item already exists, skip
                
                conn.commit()
                print(f"Added {total_added} items to process queue...")
                
                cursor_str = items_batch.get('cursor')
                if not cursor_str:
                    break
                    
            print(f"Total items queued for processing: {total_added}")
            return total_added
    finally:
        conn.close()

def fetch_board_items_batch(board_id, cursor=None, limit=100):
    """Fetch a batch of items from Monday.com board."""
    cursor_str = f'cursor: "{cursor}"' if cursor else ""
    query = f"""
        query {{
            boards(ids: {board_id}) {{
                items_page (limit: {limit}, {cursor_str}) {{
                    cursor
                    items {{ id name }}
                }}
            }}
        }}
    """
    
    result = execute_monday_graphql(query)
    if not result or 'data' not in result:
        return {'items': [], 'cursor': None}
    
    try:
        page_info = result['data']['boards'][0]['items_page']
        return {
            'items': page_info.get('items', []),
            'cursor': page_info.get('cursor')
        }
    except (KeyError, IndexError):
        return {'items': [], 'cursor': None}

def get_next_item_to_process():
    """Get the next item to process from the database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # First, reset any stuck 'processing' items (older than 10 minutes)
            cursor.execute("""
                UPDATE sync_progress 
                SET status = 'failed', 
                    error_message = 'Processing timeout - reset for retry'
                WHERE status = 'processing' 
                AND started_at < DATE_SUB(NOW(), INTERVAL 10 MINUTE)
            """)
            conn.commit()
            
            # Get next pending item or retry failed items (max 3 retries)
            cursor.execute("""
                SELECT plp_item_id, student_name 
                FROM sync_progress 
                WHERE status IN ('pending', 'failed') 
                AND retry_count < 3
                ORDER BY 
                    CASE WHEN status = 'pending' THEN 0 ELSE 1 END,
                    plp_item_id
                LIMIT 1
                FOR UPDATE
            """)
            
            item = cursor.fetchone()
            if item:
                # Mark as processing
                cursor.execute("""
                    UPDATE sync_progress 
                    SET status = 'processing', 
                        started_at = NOW(),
                        retry_count = retry_count + 1
                    WHERE plp_item_id = %s
                """, (item['plp_item_id'],))
                conn.commit()
                
            return item
    finally:
        conn.close()

def mark_item_completed(plp_item_id, subitems_deleted=True):
    """Mark an item as successfully processed."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE sync_progress 
                SET status = 'completed', 
                    completed_at = NOW(),
                    subitems_deleted = %s,
                    sync_completed = TRUE,
                    error_message = NULL
                WHERE plp_item_id = %s
            """, (subitems_deleted, plp_item_id))
            conn.commit()
    finally:
        conn.close()

def mark_item_failed(plp_item_id, error_message):
    """Mark an item as failed with error message."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE sync_progress 
                SET status = 'failed', 
                    error_message = %s,
                    completed_at = NOW()
                WHERE plp_item_id = %s
            """, (str(error_message)[:1000], plp_item_id))
            conn.commit()
    finally:
        conn.close()

def track_deleted_subitem(plp_item_id, subitem_id, deleted_by):
    """Track a deleted subitem to avoid re-deletion."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT IGNORE INTO deleted_subitems 
                (plp_item_id, subitem_id, deleted_by) 
                VALUES (%s, %s, %s)
            """, (plp_item_id, subitem_id, deleted_by))
            conn.commit()
    finally:
        conn.close()

def was_subitem_already_deleted(plp_item_id, subitem_id):
    """Check if a subitem was already deleted."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 1 FROM deleted_subitems 
                WHERE plp_item_id = %s AND subitem_id = %s
            """, (plp_item_id, subitem_id))
            return cursor.fetchone() is not None
    finally:
        conn.close()

def get_progress_stats():
    """Get current progress statistics."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
                FROM sync_progress
            """)
            return cursor.fetchone()
    finally:
        conn.close()

# ==============================================================================
# MODIFIED CLEAR SUBITEMS FUNCTION
# ==============================================================================

def clear_subitems_by_creator_with_tracking(parent_item_id, creator_id_to_delete, creator_name, dry_run=True):
    """Fetches all subitems and deletes those created by a specific user, tracking in DB."""
    if not creator_id_to_delete:
        print("ERROR: No creator ID provided. Skipping deletion.")
        return
    
    query = f"""
        query {{
            items (ids: [{parent_item_id}]) {{
                subitems {{ id creator {{ id }} }}
            }}
        }}
    """
    result = execute_monday_graphql(query)
    subitems_to_delete = []
    
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            subitem_id = subitem['id']
            
            # Check if already deleted
            if was_subitem_already_deleted(parent_item_id, subitem_id):
                continue
                
            if subitem.get('creator') and str(subitem['creator'].get('id')) == str(creator_id_to_delete):
                subitems_to_delete.append(subitem_id)
    except (KeyError, IndexError, TypeError):
        return

    if not subitems_to_delete:
        return

    print(f"INFO: Found {len(subitems_to_delete)} new subitem(s) to delete for PLP item {parent_item_id}.")
    
    if dry_run:
        print("DRY RUN: Would delete the subitems listed above.")
        return

    for subitem_id in subitems_to_delete:
        try:
            print(f"DELETING subitem {subitem_id}...")
            delete_item(subitem_id)
            track_deleted_subitem(parent_item_id, subitem_id, creator_name)
            time.sleep(0.5)
        except Exception as e:
            print(f"ERROR deleting subitem {subitem_id}: {e}")

# ==============================================================================
# MAIN PROCESSING LOOP
# ==============================================================================

def main():
    # Configuration
    DRY_RUN = os.environ.get("DRY_RUN", "False").lower() == "true"
    TARGET_USER_NAME = os.environ.get("TARGET_USER_NAME", "Sarah Bruce")
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10))
    
    print("======================================================")
    print("=== STARTING MONDAY.COM & CANVAS FULL SYNC SCRIPT ===")
    print("======================================================")
    
    if DRY_RUN:
        print("\n!!! DRY RUN MODE IS ON !!!\n")
    
    # Initialize database
    init_database()
    
    # Get or create sync run
    run_id = get_or_create_sync_run()
    
    # Get creator ID
    creator_id = get_user_id(TARGET_USER_NAME)
    if not creator_id:
        print("\nFATAL: Target user not found.")
        return
    
    # Populate items to process (only if needed)
    stats = get_progress_stats()
    if stats['total'] == 0:
        print("No items in queue. Fetching from Monday.com...")
        populate_items_to_process(PLP_BOARD_ID)
    else:
        print(f"Resuming with {stats['pending']} pending, {stats['failed']} failed items")
    
    # Main processing loop
    processed_count = 0
    while not shutdown_requested:
        # Get next item
        item = get_next_item_to_process()
        if not item:
            print("No more items to process!")
            break
        
        plp_item_id = item['plp_item_id']
        student_name = item['student_name']
        
        print(f"\n===== Processing: {student_name} (ID: {plp_item_id}) =====")
        
        try:
            # Phase 1: Delete subitems
            print(f"Phase 1: Deleting subitems...")
            clear_subitems_by_creator_with_tracking(
                plp_item_id, creator_id, TARGET_USER_NAME, dry_run=DRY_RUN
            )
            
            # Phase 2: Sync data
            print(f"Phase 2: Syncing data...")
            sync_single_plp_item(plp_item_id, dry_run=DRY_RUN)
            
            # Mark as completed
            if not DRY_RUN:
                mark_item_completed(plp_item_id)
            
            processed_count += 1
            
            # Print progress every 10 items
            if processed_count % 10 == 0:
                stats = get_progress_stats()
                print(f"\n--- PROGRESS: {stats['completed']}/{stats['total']} completed ---")
            
            # Rate limiting
            if not DRY_RUN:
                time.sleep(2)
                
        except Exception as e:
            error_msg = f"Error processing {plp_item_id}: {str(e)}"
            print(f"ERROR: {error_msg}")
            traceback.print_exc()
            
            if not DRY_RUN:
                mark_item_failed(plp_item_id, error_msg)
            
            # Continue with next item
            continue
    
    # Final stats
    final_stats = get_progress_stats()
    print("\n======================================================")
    print(f"=== SYNC {'INTERRUPTED' if shutdown_requested else 'COMPLETED'} ===")
    print(f"=== Total: {final_stats['total']} ===")
    print(f"=== Completed: {final_stats['completed']} ===")
    print(f"=== Failed: {final_stats['failed']} ===")
    print("======================================================")
    
    # Update sync run status
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            status = 'interrupted' if shutdown_requested else 'completed'
            cursor.execute("""
                UPDATE sync_runs 
                SET status = %s,
                    completed_at = NOW(),
                    processed_items = %s,
                    failed_items = %s
                WHERE id = %s
            """, (status, final_stats['completed'], final_stats['failed'], run_id))
            conn.commit()
    finally:
        conn.close()

  # ==============================================================================
# MONDAY.COM UTILITIES (Copied from app.py)
# ==============================================================================
MONDAY_HEADERS = { "Authorization": MONDAY_API_KEY, "Content-Type": "application/json", "API-Version": "2023-10" }

def execute_monday_graphql(query):
    try:
        response = requests.post(MONDAY_API_URL, json={"query": query}, headers=MONDAY_HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Monday.com API Error: {e}")
        return None

def get_item_name(item_id, board_id):
    query = f"query {{ boards(ids: {board_id}) {{ items_page(query_params: {{ids: [{item_id}]}}) {{ items {{ name }} }} }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('boards'):
        board = result['data']['boards'][0]
        if board.get('items_page') and board['items_page'].get('items'):
            return board['items_page']['items'][0].get('name')
    return None

def get_user_name(user_id):
    if user_id is None or user_id == -4: return None
    query = f"query {{ users(ids: [{user_id}]) {{ name }} }}"
    result = execute_monday_graphql(query)
    if result and 'data' in result and result['data'].get('users'):
        return result['data']['users'][0].get('name')
    return None

def get_column_value(item_id, board_id, column_id): # board_id is no longer used but kept for compatibility
    if not item_id or not column_id:
        return None
    # This is the simplified query, proven to work by the debug script.
    query = f"""
    query {{
        items (ids: [{item_id}]) {{
            column_values (ids: ["{column_id}"]) {{
                id
                text
                value
                type
            }}
        }}
    }}
    """
    result = execute_monday_graphql(query)
    
    if result and result.get('data', {}).get('items'):
        try:
            # The path to the data is now simpler, matching the new query.
            column_list = result['data']['items'][0].get('column_values', [])
            if not column_list:
                # This happens if the column ID exists on the board but not on this specific item.
                return None
            
            col_val = column_list[0]
            parsed_value = col_val.get('value')
            if isinstance(parsed_value, str):
                try:
                    # The value from the API is a string containing JSON, so we parse it.
                    parsed_value = json.loads(parsed_value)
                except json.JSONDecodeError:
                    # If parsing fails, we leave it as the raw string.
                    pass
            
            return {'value': parsed_value, 'text': col_val.get('text')}
        except (IndexError, KeyError):
            # This handles cases where the item exists but something is wrong with the column data structure.
            return None
    return None
def delete_item(item_id):
    """Deletes an item or subitem."""
    mutation = f"mutation {{ delete_item (item_id: {item_id}) {{ id }} }}"
    return execute_monday_graphql(mutation)
    
def change_column_value_generic(board_id, item_id, column_id, value):
    graphql_value = json.dumps(str(value))
    mutation = f"""
        mutation {{
            change_column_value(
                board_id: {board_id}, item_id: {item_id}, column_id: "{column_id}", value: {graphql_value}
            ) {{ id }}
        }} """
    return execute_monday_graphql(mutation) is not None

def get_people_ids_from_value(value_data):
    if not value_data: return set()
    if isinstance(value_data, str):
        try:
            value_data = json.loads(value_data)
        except json.JSONDecodeError:
            return set()
    persons_and_teams = value_data.get('personsAndTeams', [])
    return {person['id'] for person in persons_and_teams if 'id' in person}

def get_linked_ids_from_connect_column_value(value_data):
    if not value_data: return set()
    parsed_value = value_data if isinstance(value_data, dict) else json.loads(value_data) if isinstance(value_data, str) else {}
    if "linkedPulseIds" in parsed_value:
        return {int(item["linkedPulseId"]) for item in parsed_value["linkedPulseIds"] if "linkedPulseId" in item}
    return set()

def get_linked_items_from_board_relation(item_id, board_id, connect_column_id):
    column_data = get_column_value(item_id, board_id, connect_column_id)
    return get_linked_ids_from_connect_column_value(column_data.get('value')) if column_data else set()

def create_subitem(parent_item_id, subitem_name, column_values=None):
    values_for_api = {col_id: val for col_id, val in (column_values or {}).items()}
    column_values_json = json.dumps(values_for_api)
    mutation = f"mutation {{ create_subitem (parent_item_id: {parent_item_id}, item_name: {json.dumps(subitem_name)}, column_values: {json.dumps(column_values_json)}) {{ id }} }}"
    result = execute_monday_graphql(mutation)
    return result['data']['create_subitem'].get('id') if result and 'data' in result and result['data'].get('create_subitem') else None

def update_people_column(item_id, board_id, people_column_id, new_people_value, target_column_type):
    # First, get the new person's ID from the value we are adding
    new_persons_and_teams = new_people_value.get('personsAndTeams', [])
    if not new_persons_and_teams:
        return False
    new_person_id = new_persons_and_teams[0].get('id')
    if not new_person_id:
        return False

    # Get the list of people already in the column
    current_col_val = get_column_value(item_id, board_id, people_column_id)
    current_people_ids = set()
    if current_col_val and current_col_val.get('value'):
        current_people_ids = get_people_ids_from_value(current_col_val['value'])

    # Add the new person to the set of existing people
    current_people_ids.add(new_person_id)
    
    # Prepare the final value for the API
    updated_people_list = [{"id": int(pid), "kind": "person"} for pid in current_people_ids]
    
    # Based on the column type, format the final value correctly
    if target_column_type == "person":
        # A "person" column can only hold one person
        final_value = {"personId": int(new_person_id)}
    elif target_column_type == "multiple-person":
        # A "multiple-person" column can hold a list
        final_value = {"personsAndTeams": updated_people_list}
    else:
        return False

    graphql_value = json.dumps(json.dumps(final_value))
    mutation = f"mutation {{ change_column_value(board_id: {board_id}, item_id: {item_id}, column_id: \"{people_column_id}\", value: {graphql_value}) {{ id }} }}"
    
    return execute_monday_graphql(mutation) is not None

# ==============================================================================
# CANVAS UTILITIES (Copied from app.py)
# ==============================================================================
# In a real-world scenario, you would import these from a shared library
# to avoid code duplication. For this standalone script, they are copied directly.
from canvasapi import Canvas
from canvasapi.exceptions import CanvasException, Conflict, ResourceDoesNotExist

def initialize_canvas_api():
    if CANVAS_API_URL and CANVAS_API_KEY:
        return Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    return None

def find_canvas_user(student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None

    # Search by explicit Canvas ID first
    if student_details.get('canvas_id'):
        try:
            return canvas_api.get_user(student_details['canvas_id'])
        except (ResourceDoesNotExist, ValueError):
            pass

    # Then by email login
    if student_details.get('email'):
        try:
            return canvas_api.get_user(student_details['email'], 'login_id')
        except ResourceDoesNotExist:
            pass

    # Then by SIS ID
    if student_details.get('ssid'):
        try:
            return canvas_api.get_user(student_details['ssid'], 'sis_user_id')
        except ResourceDoesNotExist:
            pass
            
    # Fallback to broad search by email
    if student_details.get('email'):
        try:
            search_results = canvas_api.get_account(1).get_users(search_term=student_details['email'])
            users = [u for u in search_results]
            if len(users) == 1:
                return users[0]
        except (ResourceDoesNotExist, CanvasException):
             pass

    return None

def create_canvas_user(student_details):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        account = canvas_api.get_account(1)
        user_payload = {
            'user': {'name': student_details['name'], 'terms_of_use': True},
            'pseudonym': {
                'unique_id': student_details['email'],
                'sis_user_id': student_details['ssid'],
                'send_confirmation': False
            },
            'communication_channel': {
                'type': 'email',
                'address': student_details['email'],
                'skip_confirmation': True
            }
        }
        new_user = account.create_user(**user_payload)
        return new_user
    except CanvasException as e:
        print(f"ERROR: Canvas user creation failed: {e}")
        return None

def update_user_ssid(user, new_ssid):
    try:
        # --- THIS BLOCK IS THE SECOND FIX ---
        # It checks for both new and old versions of the canvasapi library method
        if hasattr(user, 'list_logins'):
            logins = user.list_logins()
        else:
            logins = user.get_logins()

        if logins:
            login_to_update = logins[0]
            login_to_update.edit(login={'sis_user_id': new_ssid})
            return True
            
    except CanvasException as e:
        print(f"ERROR: API error updating SSID for user '{user.name}': {e}")
    return False

def create_canvas_course(course_name, term_id):
    canvas_api = initialize_canvas_api()
    if not all([canvas_api, CANVAS_SUBACCOUNT_ID]):
        print("ERROR: Missing Canvas Sub-Account ID config.")
        return None
    try:
        account = canvas_api.get_account(CANVAS_SUBACCOUNT_ID)
    except ResourceDoesNotExist:
        print(f"ERROR: Canvas Sub-Account with ID '{CANVAS_SUBACCOUNT_ID}' not found.")
        return None

    course_data = {
        'name': course_name,
        'course_code': course_name,
        'enrollment_term_id': term_id,
        'is_template': False
    }

    if CANVAS_TEMPLATE_COURSE_ID:
        course_data['source_course_id'] = CANVAS_TEMPLATE_COURSE_ID

    try:
        print(f"INFO: Trying to create course '{course_name}'.")
        new_course = account.create_course(course=course_data)
        print(f"SUCCESS: Course '{course_name}' created with ID {new_course.id}.")
        return new_course
    except CanvasException as e:
        print(f"ERROR: A critical Canvas API error occurred for course '{course_name}': {e}")
        return None

def create_section_if_not_exists(course_id, section_name):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return None
    try:
        course = canvas_api.get_course(course_id)
        for section in course.get_sections():
            if section.name.lower() == section_name.lower():
                return section
        return course.create_course_section(course_section={'name': section_name})
    except CanvasException as e:
        print(f"ERROR: Canvas section creation/check failed: {e}")
        return None

def enroll_student_in_section(course_id, user_id, section_id):
    canvas_api = initialize_canvas_api()
    if not canvas_api: return "Failed: Canvas API not initialized"
    try:
        course = canvas_api.get_course(course_id)
        user = canvas_api.get_user(user_id)
        enrollment = course.enroll_user(user, 'StudentEnrollment', enrollment={'course_section_id': section_id, 'notify': False})
        return "Success"
    except Conflict: return "Already Enrolled"
    except CanvasException as e:
        print(f"ERROR: Failed to enroll user {user_id} in section {section_id}. Details: {e}")
        return "Failed"

def enroll_or_create_and_enroll(course_id, section_id, student_details):
    user = find_canvas_user(student_details)
    if not user:
        print(f"INFO: Canvas user not found for {student_details['email']}. Creating new user.")
        user = create_canvas_user(student_details)

    if user:
        if student_details.get('ssid') and hasattr(user, 'sis_user_id') and user.sis_user_id != student_details['ssid']:
            update_user_ssid(user, student_details['ssid'])
        return enroll_student_in_section(course_id, user.id, section_id)

    return "Failed: User not found/created"

# ==============================================================================
# CORE LOGIC FUNCTIONS (Adapted from app.py)
# ==============================================================================

def get_student_details_from_plp(plp_item_id):
    query = f"""
    query {{
        items (ids: [{plp_item_id}]) {{
            column_values (ids: ["{PLP_TO_MASTER_STUDENT_CONNECT_COLUMN}"]) {{
                value
            }}
        }}
    }}
    """
    result = execute_monday_graphql(query)
    try:
        connect_column_value = json.loads(result['data']['items'][0]['column_values'][0]['value'])
        linked_ids = [item['linkedPulseId'] for item in connect_column_value.get('linkedPulseIds', [])]
        if not linked_ids:
            return None
        master_student_id = linked_ids[0]

        details_query = f"""
        query {{
            items (ids: [{master_student_id}]) {{
                id
                name
                column_values(ids: ["{MASTER_STUDENT_SSID_COLUMN}", "{MASTER_STUDENT_EMAIL_COLUMN}", "{MASTER_STUDENT_CANVAS_ID_COLUMN}"]) {{
                    id
                    text
                }}
            }}
        }}
        """
        details_result = execute_monday_graphql(details_query)
        item_details = details_result['data']['items'][0]
        student_name = item_details['name']
        
        column_map = {cv['id']: cv.get('text') for cv in item_details.get('column_values', []) if isinstance(cv, dict)}
        
        ssid = column_map.get(MASTER_STUDENT_SSID_COLUMN, '')
        email = column_map.get(MASTER_STUDENT_EMAIL_COLUMN, '')
        canvas_id = column_map.get(MASTER_STUDENT_CANVAS_ID_COLUMN, '')

        if not all([student_name, email]):
            return None

        return {'name': student_name, 'ssid': ssid, 'email': email, 'canvas_id': canvas_id, 'master_id': item_details['id']}
    except (TypeError, KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not parse student details from Monday.com response for PLP {plp_item_id}: {e}")
        return None

def manage_class_enrollment(action, plp_item_id, class_item_id, student_details, category_name, subitem_cols=None):
    subitem_cols = subitem_cols or {}

    linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
    all_courses_item_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"

    if not linked_canvas_item_ids:
        if action == "enroll":
            create_subitem(plp_item_id, f"Added {category_name} '{all_courses_item_name}'", subitem_cols)
        elif action == "unenroll":
            create_subitem(plp_item_id, f"Removed {category_name} '{all_courses_item_name}'", subitem_cols)
        return

    canvas_item_id = list(linked_canvas_item_ids)[0]
    class_name = get_item_name(canvas_item_id, int(CANVAS_BOARD_ID))
    if not class_name:
        print(f"ERROR: Linked item {canvas_item_id} on Canvas Board {CANVAS_BOARD_ID} has no name. Aborting.")
        return

    course_id_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = course_id_val.get('text') if course_id_val else None

    if not canvas_course_id and action == "enroll":
        print(f"INFO: No Canvas ID found on Monday item {canvas_item_id}. Attempting to create course for '{class_name}'.")
        new_course = create_canvas_course(class_name, CANVAS_TERM_ID)
        if new_course:
            canvas_course_id = new_course.id
            change_column_value_generic(int(CANVAS_BOARD_ID), canvas_item_id, CANVAS_COURSE_ID_COLUMN_ID, str(canvas_course_id))
            if ALL_CLASSES_CANVAS_ID_COLUMN:
                change_column_value_generic(int(ALL_COURSES_BOARD_ID), class_item_id, ALL_CLASSES_CANVAS_ID_COLUMN, str(canvas_course_id))
        else:
            create_subitem(plp_item_id, f"Added {category_name} '{class_name}': Failed - Could not create Canvas course.", subitem_cols)
            return

    if not canvas_course_id:
        print(f"INFO: No Canvas Course ID available for '{class_name}' to perform action '{action}'. Skipping.")
        return

    if action == "enroll":
        m_series_val = get_column_value(plp_item_id, int(PLP_BOARD_ID), PLP_M_SERIES_LABELS_COLUMN)
        ag_grad_val = get_column_value(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_CLASSES_AG_GRAD_COLUMN)
        m_series_text = (m_series_val.get('text') or "") if m_series_val else ""
        ag_grad_text = (ag_grad_val.get('text') or "") if ag_grad_val else ""
        
        sections = {"A-G" for s in ["AG"] if s in ag_grad_text} | {"Grad" for s in ["Grad"] if s in ag_grad_text} | {"M-Series" for s in ["M-series"] if s in m_series_text}
        if not sections: sections.add("All")
        
        enrollment_results = []
        for section_name in sections:
            section = create_section_if_not_exists(canvas_course_id, section_name)
            if section:
                # --- THIS LINE IS THE FIX ---
                result = enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)
                enrollment_results.append({'section': section_name, 'status': result})

        if enrollment_results:
            section_names = ", ".join([res['section'] for res in enrollment_results])
            all_statuses = {res['status'] for res in enrollment_results}
            final_status = "Failed" if "Failed" in all_statuses else "Success"
            subitem_title = f"Added {category_name} '{class_name}' (Sections: {section_names}): {final_status}"
            create_subitem(plp_item_id, subitem_title, subitem_cols)

    elif action == "unenroll":
        result = unenroll_student_from_course(canvas_course_id, student_details)
        create_subitem(plp_item_id, f"Removed {category_name} '{class_name}': {'Success' if result else 'Failed'}", subitem_cols)

# ==============================================================================
# SCRIPT-SPECIFIC HELPER FUNCTIONS
# ==============================================================================

def get_all_board_items(board_id):
    """Fetches all item objects from a board, handling pagination."""
    all_items = [] # Changed variable name for clarity
    cursor = None
    while True:
        cursor_str = f'cursor: "{cursor}"' if cursor else ""
        # The query needs to fetch both id and name for other functions
        query = f"""
            query {{
                boards(ids: {board_id}) {{
                    items_page (limit: 100, {cursor_str}) {{
                        cursor
                        items {{ id name }}
                    }}
                }}
            }}
        """
        result = execute_monday_graphql(query)
        if not result or 'data' not in result: break
        try:
            page_info = result['data']['boards'][0]['items_page']
            
            # --- THIS LINE IS THE FIX ---
            # It now extends the list with the full item objects, not just their IDs
            all_items.extend(page_info['items'])
            
            cursor = page_info.get('cursor')
            if not cursor: break
            print(f"Fetched {len(all_items)} items so far...")
        except (KeyError, IndexError):
            print("ERROR: Could not parse items from board response.")
            break
    return all_items

def get_user_id(user_name):
    """Finds a user's ID by their full name."""
    query = f'query {{ users(kind: all) {{ id name }} }}'
    result = execute_monday_graphql(query)
    try:
        for user in result['data']['users']:
            if user['name'].lower() == user_name.lower():
                print(f"INFO: Found user ID for '{user_name}': {user['id']}")
                return user['id']
    except (KeyError, IndexError, TypeError):
        pass
    print(f"ERROR: Could not find user ID for '{user_name}'.")
    return None

def clear_subitems_by_creator(parent_item_id, creator_id_to_delete, dry_run=True):
    """Fetches all subitems and deletes those created by a specific user."""
    if not creator_id_to_delete:
        print("ERROR: No creator ID provided. Skipping deletion.")
        return
    
    query = f"""
        query {{
            items (ids: [{parent_item_id}]) {{
                subitems {{ id creator {{ id }} }}
            }}
        }}
    """
    result = execute_monday_graphql(query)
    subitems_to_delete = []
    try:
        subitems = result['data']['items'][0]['subitems']
        for subitem in subitems:
            if subitem.get('creator') and str(subitem['creator'].get('id')) == str(creator_id_to_delete):
                subitems_to_delete.append(subitem['id'])
    except (KeyError, IndexError, TypeError):
        print(f"INFO: No subitems found or creator info missing for {parent_item_id}.")
        return

    if not subitems_to_delete:
        return

    print(f"INFO: Found {len(subitems_to_delete)} subitem(s) by creator {creator_id_to_delete} to delete for PLP item {parent_item_id}.")
    
    if dry_run:
        print("DRY RUN: Would delete the subitems listed above.")
        return

    for subitem_id in subitems_to_delete:
        print(f"DELETING subitem {subitem_id}...")
        delete_item(subitem_id)
        time.sleep(0.5)

def sync_single_plp_item(plp_item_id, dry_run=True):
    """
    Final, corrected version that handles non-Canvas courses properly during
    the ACE/Connect teacher sync.
    """
    print(f"\n--- Processing PLP Item: {plp_item_id} ---")
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details:
        print(f"WARNING: Could not get student details for PLP {plp_item_id}. Skipping.")
        return

    master_student_id = student_details.get('master_id')
    if not master_student_id:
        print(f"ERROR: Could not find Master Student ID for PLP {plp_item_id}. Skipping.")
        return

    ENTRY_TYPE_COLUMN_ID = "your_entry_type_column_id"
    staff_change_values = {ENTRY_TYPE_COLUMN_ID: {"labels": ["Staff Change"]}}
    curriculum_change_values = {ENTRY_TYPE_COLUMN_ID: {"labels": ["Curriculum Change"]}}

    if not dry_run:
        print("Syncing teacher assignments from Master Student board to PLP...")
        for trigger_col, mapping in MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.items():
            master_person_val = get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), trigger_col)
            plp_target_mapping = next((t for t in mapping["targets"] if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
            if plp_target_mapping and master_person_val and master_person_val.get('value'):
                update_people_column(plp_item_id, int(PLP_BOARD_ID), plp_target_mapping["target_column_id"], master_person_val['value'], plp_target_mapping["target_column_type"])
                person_ids = get_people_ids_from_value(master_person_val['value'])
                for person_id in person_ids:
                    person_name = get_user_name(person_id)
                    if person_name:
                        log_message = f"{mapping.get('name', 'Staff')} set to {person_name}"
                        create_subitem(plp_item_id, log_message, column_values=staff_change_values)
                time.sleep(1)

    print("Syncing class enrollments and ACE/Connect teachers...")
    class_id_to_category_map = {}
    for category, column_id in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items():
        linked_class_ids = get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), column_id)
        for class_id in linked_class_ids:
            class_id_to_category_map[class_id] = category

    all_class_ids = class_id_to_category_map.keys()
    if not all_class_ids:
        print("INFO: No classes to sync.")
        return

    CANVAS_BOARD_CLASS_TYPE_COLUMN_ID = "status__1"
    ACE_TEACHER_COLUMN_ID_ON_MASTER = "multiple_person_mks1wrfv"
    CONNECT_TEACHER_COLUMN_ID_ON_MASTER = "multiple_person_mks11jeg"

    for class_item_id in all_class_ids:
        class_name = get_item_name(class_item_id, int(ALL_COURSES_BOARD_ID)) or f"Item {class_item_id}"
        print(f"Processing class: '{class_name}'")

        category_name = class_id_to_category_map.get(class_item_id, "Course")
        if not dry_run:
            manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, category_name, subitem_cols=curriculum_change_values)

        linked_canvas_item_ids = get_linked_items_from_board_relation(class_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
        
        if linked_canvas_item_ids:
            canvas_item_id = list(linked_canvas_item_ids)[0]
            class_type_val = get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_BOARD_CLASS_TYPE_COLUMN_ID)
            
            # --- THIS BLOCK IS THE FIRST FIX ---
            # It now safely handles cases where the class_type_val is None
            class_type_text = ""
            if class_type_val and class_type_val.get('text'):
                class_type_text = class_type_val.get('text', '').lower()
            
            target_master_col_id = None
            if 'ace' in class_type_text: target_master_col_id = ACE_TEACHER_COLUMN_ID_ON_MASTER
            elif 'connect' in class_type_text: target_master_col_id = CONNECT_TEACHER_COLUMN_ID_ON_MASTER
            
            if target_master_col_id:
                teacher_person_value = get_teacher_person_value_from_canvas_board(canvas_item_id)
                if teacher_person_value:
                    if not dry_run:
                        update_people_column(master_student_id, int(MASTER_STUDENT_BOARD_ID), target_master_col_id, teacher_person_value, "multiple-person")
                else:
                    print(f"WARNING: Could not find a linked teacher on the Canvas Board for course '{class_name}'.")
        
        if not dry_run:
            time.sleep(1)
def get_teacher_person_value_from_canvas_board(canvas_item_id):
    """DEBUG version to find the teacher's 'Person' value."""
    print(f"\n--- DEBUG: Inside get_teacher_person_value_from_canvas_board ---")
    print(f"DEBUG: Starting with canvas_item_id: {canvas_item_id}")

    # --- Step 1: Find the linked staff item ---
    print(f"DEBUG: Looking on board [CANVAS_BOARD_ID: {CANVAS_BOARD_ID}] for item {canvas_item_id} using column [CANVAS_TO_STAFF_CONNECT_COLUMN_ID: {CANVAS_TO_STAFF_CONNECT_COLUMN_ID}]")
    linked_staff_ids = get_linked_items_from_board_relation(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_TO_STAFF_CONNECT_COLUMN_ID)
    print(f"DEBUG: Found linked_staff_ids: {linked_staff_ids}")
    
    if not linked_staff_ids:
        print(f"DEBUG: No staff IDs found. Returning None.")
        print(f"--- END DEBUG ---")
        return None

    staff_item_id = list(linked_staff_ids)[0]
    print(f"DEBUG: Will use staff_item_id: {staff_item_id}")

    # --- Step 2: Get the 'Person' column value for that staff member ---
    print(f"DEBUG: Looking on board [ALL_STAFF_BOARD_ID: {ALL_STAFF_BOARD_ID}] for item {staff_item_id} using column [ALL_STAFF_PERSON_COLUMN_ID: {ALL_STAFF_PERSON_COLUMN_ID}]")
    person_col_val = get_column_value(staff_item_id, int(ALL_STAFF_BOARD_ID), ALL_STAFF_PERSON_COLUMN_ID)
    print(f"DEBUG: Got person_col_val: {person_col_val}")
    
    if not person_col_val:
        print(f"DEBUG: No person column value found. Returning None.")
        print(f"--- END DEBUG ---")
        return None
        
    final_value = person_col_val.get('value')
    print(f"DEBUG: Final value to be returned: {final_value}")
    print(f"--- END DEBUG ---")
    return final_value

    if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
