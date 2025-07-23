import os
import json
from datetime import datetime
from celery_app import celery_app
import monday_utils as monday # Import your shared utility functions

# --- Global Configuration (Environment Variables for Tasks) ---
# These are loaded when the worker starts, or when the task is defined
# Make sure these are also available to your Celery worker processes

# Environment variables for process_general_webhook
MONDAY_LOGGING_CONFIGS_STR = os.environ.get("MONDAY_LOGGING_CONFIGS", "[]")
try:
    MONDAY_LOGGING_CONFIGS = json.loads(MONDAY_LOGGING_CONFIGS_STR)
except json.JSONDecodeError as e:
    print(f"ERROR: MONDAY_TASKS: MONDAY_LOGGING_CONFIGS environment variable is not valid JSON: {e}. Defaulting to empty list.")
    MONDAY_LOGGING_CONFIGS = []
print(f"DEBUG: MONDAY_TASKS: LOG_CONFIGS loaded (type: {type(MONDAY_LOGGING_CONFIGS)}, len: {len(MONDAY_LOGGING_CONFIGS)}): {MONDAY_LOGGING_CONFIGS}")


# Environment variables for process_plp_course_sync_webhook
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID", "")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID", "")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID", "")
HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID", "")
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID", "")
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID", "")
PLP_CATEGORY_TO_CONNECT_COLUMN_MAP_STR = os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}")
try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(PLP_CATEGORY_TO_CONNECT_COLUMN_MAP_STR)
except json.JSONDecodeError as e:
    print(f"ERROR: MONDAY_TASKS: PLP_CATEGORY_TO_CONNECT_COLUMN_MAP environment variable is not valid JSON: {e}. Defaulting to empty map.")
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
print(f"DEBUG: MONDAY_TASKS: HS_ROSTER_BOARD_ID: '{HS_ROSTER_BOARD_ID}'")
print(f"DEBUG: MONDAY_TASKS: PLP_CATEGORY_TO_CONNECT_COLUMN_MAP loaded (type: {type(PLP_CATEGORY_TO_CONNECT_COLUMN_MAP)}, keys: {list(PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.keys())}): {PLP_CATEGORY_TO_CONNECT_COLUMN_MAP}")


# Environment variables for Master Student Person Sync
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID", "")
MASTER_STUDENT_PEOPLE_COLUMNS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}")
try:
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(MASTER_STUDENT_PEOPLE_COLUMNS_STR)
except json.JSONDecodeError as e:
    print(f"ERROR: MONDAY_TASKS: MASTER_STUDENT_PEOPLE_COLUMNS environment variable is not valid JSON: {e}. Defaulting to empty map.")
    MASTER_STUDENT_PEOPLE_COLUMNS = {}
COLUMN_MAPPINGS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}")
try:
    COLUMN_MAPPINGS = json.loads(COLUMN_MAPPINGS_STR)
except json.JSONDecodeError as e:
    print(f"ERROR: MONDAY_TASKS: MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS environment variable is not valid JSON: {e}. Defaulting to empty map.")
    COLUMN_MAPPINGS = {}
print(f"DEBUG: MONDAY_TASKS: MASTER_STUDENT_LIST_BOARD_ID: '{MASTER_STUDENT_LIST_BOARD_ID}'")
print(f"DEBUG: MONDAY_TASKS: MASTER_STUDENT_PEOPLE_COLUMNS loaded (type: {type(MASTER_STUDENT_PEOPLE_COLUMNS)}, keys: {list(MASTER_STUDENT_PEOPLE_COLUMNS.keys())}): {MASTER_STUDENT_PEOPLE_COLUMNS}")
print(f"DEBUG: MONDAY_TASKS: COLUMN_MAPPINGS loaded (type: {type(COLUMN_MAPPINGS)}, keys: {list(COLUMN_MAPPINGS.keys())}): {COLUMN_MAPPINGS}")


# Environment variables for SpEd Students Person Sync
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID", "")
IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID", "")
SPED_TO_IEPAP_CONNECT_COLUMN_ID = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN_ID", "")
SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR = os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}")
try:
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR)
except json.JSONDecodeError as e:
    print(f"ERROR: MONDAY_TASKS: SPED_STUDENTS_PEOPLE_COLUMN_MAPPING environment variable is not valid JSON: {e}. Defaulting to empty map.")
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}
print(f"DEBUG: MONDAY_TASKS: SPED_STUDENTS_BOARD_ID: '{SPED_STUDENTS_BOARD_ID}'")
print(f"DEBUG: MONDAY_TASKS: SPED_STUDENTS_PEOPLE_COLUMN_MAPPING loaded (type: {type(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING)}, keys: {list(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.keys())}): {SPED_STUDENTS_PEOPLE_COLUMN_MAPPING}")


# Environment variables for app-monday-subitem-logger-main (part of general logger rules)
# Note: These are specific parameters passed within the config_rule to process_general_webhook,
# but also used for environment variable checks within this module.
MONDAY_MAIN_BOARD_ID_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_MAIN_BOARD_ID", "")
MONDAY_CONNECT_BOARD_COLUMN_ID_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_CONNECT_BOARD_COLUMN_ID", "")
LINKED_BOARD_ID_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_LINKED_BOARD_ID", "")
MONDAY_SUBJECT_PREFIX_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_SUBJECT_PREFIX", "")
MONDAY_ENTRY_TYPE_COLUMN_ID_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_ENTRY_TYPE_COLUMN_ID", "")
print(f"DEBUG: MONDAY_TASKS: Subitem Logger MONDAY_MAIN_BOARD_ID: '{MONDAY_MAIN_BOARD_ID_FOR_SUBITEM_LOGGER}'")


# --- Helper for Name Formatting ---
def format_name_last_first(name_str):
    """
    Reformats a 'First Last' string to 'Last, First'.
    Handles multiple first names or middle initials.
    """
    if not name_str or not isinstance(name_str, str):
        return name_str # Return as is if not a valid string

    parts = name_str.strip().split()
    if len(parts) >= 2:
        last_name = parts[-1]
        first_names = " ".join(parts[:-1])
        return f"{last_name}, {first_names}"
    else:
        return name_str # Return original if not enough parts to reformat

# --- Celery Tasks ---

@celery_app.task
def process_general_webhook(event_data, config_rule):
    """
    Celery task to handle general webhook processing in the background.
    """
    # Extract data from event_data
    webhook_board_id = event_data.get('boardId')
    item_id_from_webhook = event_data.get('pulseId')
    trigger_column_id_from_webhook = event_data.get('columnId')
    event_user_id = event_data.get('userId')
    current_column_value = event_data.get('value')
    previous_column_value = event_data.get('previousValue')
    webhook_type = event_data.get('type')
    parent_item_id_from_webhook = event_data.get('parentItemId')
    parent_board_id_from_webhook = event_data.get('parentItemBoardId')

    # Extract data from config_rule
    log_type = config_rule.get("log_type")
    params = config_rule.get("params", {})
    configured_trigger_board_id = config_rule.get("trigger_board_id")
    configured_trigger_col_id = config_rule.get("trigger_column_id")

    print(f"DEBUG: MONDAY_TASKS: process_general_webhook - Task received for log_type: '{log_type}', board: '{webhook_board_id}', item: '{item_id_from_webhook}'")

    # Re-validate the board ID in the task (important if multiple rules dispatch to this task)
    try:
        if configured_trigger_board_id and int(webhook_board_id) != int(configured_trigger_board_id):
            print(f"WARNING: MONDAY_TASKS: process_general_webhook - Task received for unmatched board ID. Expected {configured_trigger_board_id}, got {webhook_board_id}. Skipping.")
            return False
    except (ValueError, TypeError):
        print(f"ERROR: MONDAY_TASKS: process_general_webhook - Invalid 'trigger_board_id' in config rule or webhook payload. Skipping.")
        return False

    success = False # Default to False, change to True upon successful action

    # --- Implementations for specific log_types ---

    if log_type == "CopyToItemName" and webhook_type in ["create_item", "create_pulse"]:
        source_column_id = params.get("source_column_id")
        if not source_column_id:
            print(f"ERROR: MONDAY_TASKS: CopyToItemName - Missing 'source_column_id' in params.")
            return False

        source_column_data = monday.get_column_value(item_id_from_webhook, webhook_board_id, source_column_id)
        source_text = None
        if source_column_data and source_column_data.get('text'):
            source_text = source_column_data['text']

        if not source_text or not source_text.strip():
            print(f"INFO: MONDAY_TASKS: CopyToItemName - Source column '{source_column_id}' for item {item_id_from_webhook} is empty. Skipping name update.")
            success = True # Consider this a success if no action needed
        else:
            print(f"INFO: MONDAY_TASKS: CopyToItemName - Attempting to update item {item_id_from_webhook} name on board {webhook_board_id} to '{source_text}'.")
            success = monday.update_item_name(item_id_from_webhook, webhook_board_id, source_text)

    elif webhook_type == "update_column_value" and trigger_column_id_from_webhook == configured_trigger_col_id:
        if log_type == "NameReformat":
            target_text_column_id = params.get("target_text_column_id")
            
            item_name = monday.get_item_name(item_id_from_webhook, webhook_board_id)
            if item_name:
                # --- APPLY LAST, FIRST FORMATTING ---
                new_name = format_name_last_first(item_name) # Call the new helper function
                print(f"DEBUG: MONDAY_TASKS: NameReformat - Original name: '{item_name}', Reformatted name: '{new_name}'")
                
                if target_text_column_id:
                    print(f"INFO: MONDAY_TASKS: NameReformat - Attempting to update column '{target_text_column_id}' on item {item_id_from_webhook} with '{new_name}'.")
                    success = monday.change_column_value_generic(
                        board_id=webhook_board_id,
                        item_id=item_id_from_webhook,
                        column_id=target_text_column_id,
                        value=new_name # Pass the reformatted name
                    )
                else:
                    # If target_text_column_id is NOT provided, it defaults to updating the item's main name
                    print(f"INFO: MONDAY_TASKS: NameReformat - Attempting to update item name on {item_id_from_webhook} to '{new_name}'.")
                    success = monday.update_item_name(item_id_from_webhook, webhook_board_id, new_name)
            else:
                print(f"INFO: MONDAY_TASKS: NameReformat - Could not retrieve item name for {item_id_from_webhook}. Skipping NameReformat.")
                success = True

        elif log_type == "ConnectBoardChange":
            print(f"DEBUG: MONDAY_TASKS: ConnectBoardChange: Entered ConnectBoardChange logic block.")
            print(f"DEBUG: MONDAY_TASKS: ConnectBoardChange: params received: {params}")

            main_item_id = item_id_from_webhook # The item that triggered the webhook (parent for subitem creation)
            connected_board_id = params.get('linked_board_id') # The board being linked to
            subitem_name_prefix = params.get('subitem_name_prefix') # e.g., "Math", "ELA", "ACE", "Other/Elective"
            subitem_entry_type = params.get('subitem_entry_type') # e.g., "Curriculum Change"
            entry_type_column_id_from_params = params.get('entry_type_column_id') # Column for "Curriculum Change" on subitem board
            
            print(f"DEBUG: MONDAY_TASKS: ConnectBoardChange: subitem_name_prefix from params: '{subitem_name_prefix}'")
            print(f"DEBUG: MONDAY_TASKS: ConnectBoardChange: subitem_entry_type from params: '{subitem_entry_type}'")

            if not all([connected_board_id, subitem_name_prefix, subitem_entry_type, entry_type_column_id_from_params]):
                print(f"ERROR: MONDAY_TASKS: ConnectBoardChange: Missing required parameters for subitem creation from params: {params}")
                if not connected_board_id: print("  - linked_board_id is missing")
                if not subitem_name_prefix: print("  - subitem_name_prefix is missing")
                if not subitem_entry_type: print("  - subitem_entry_type is missing")
                if not entry_type_column_id_from_params: print("  - entry_type_column_id is missing")
                return False

            current_linked_ids = monday.get_linked_ids_from_connect_column_value(current_column_value)
            previous_linked_ids = monday.get_linked_ids_from_connect_column_value(previous_column_value)

            added_links = current_linked_ids - previous_linked_ids
            removed_links = previous_linked_ids - current_linked_ids

            print(f"INFO: MONDAY_TASKS: ConnectBoardChange (Subitem Creation) detected for item {main_item_id}. Added: {added_links}, Removed: {removed_links}")

            overall_op_successful = True
            current_date = datetime.now().strftime('%Y-%m-%d')
            changer_user_name = monday.get_user_name(event_user_id)
            user_log_text = ""
            if changer_user_name:
                user_log_text = f" by {changer_user_name}"
            elif event_user_id == -4:
                user_log_text = " by automation"

            subject_prefix_text = ""
            if subitem_name_prefix:
                subject_prefix_text = f"{subitem_name_prefix} "
            print(f"DEBUG: MONDAY_TASKS: ConnectBoardChange: Constructed subject_prefix_text: '{subject_prefix_text}'")

            additional_subitem_columns = {}
            if entry_type_column_id_from_params:
                additional_subitem_columns[entry_type_column_id_from_params] = {"labels": [str(subitem_entry_type)]}
                print(f"DEBUG: MONDAY_TASKS: ConnectBoardChange: Added entry type column value: {additional_subitem_columns}")
            else:
                 print(f"WARNING: MONDAY_TASKS: ConnectBoardChange: entry_type_column_id_from_params is not set. Subitem entry type will not be set.")


            # --- Create subitems for Added Items ---
            for item_id_linked in added_links:
                linked_item_name = monday.get_item_name(item_id_linked, connected_board_id)
                if linked_item_name:
                    subitem_name = f"Added {subject_prefix_text}'{linked_item_name}' on {current_date}{user_log_text}"
                    print(f"DEBUG: MONDAY_TASKS: ConnectBoardChange: Proposed subitem name (Added): '{subitem_name}'")
                    if not monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns):
                        overall_op_successful = False
                        print(f"ERROR: MONDAY_TASKS: ConnectBoardChange: Failed to create subitem for added link {item_id_linked}.")
                else:
                    print(f"WARNING: MONDAY_TASKS: ConnectBoardChange: Could not retrieve name for added item ID: {item_id_linked}. Skipping subitem creation.")
                    overall_op_successful = False

            # --- Create subitems for Removed Items ---
            for item_id_linked in removed_links:
                linked_item_name = monday.get_item_name(item_id_linked, connected_board_id)
                if linked_item_name:
                    subitem_name = f"Removed {subject_prefix_text}'{linked_item_name}' on {current_date}{user_log_text}"
                    print(f"DEBUG: MONDAY_TASKS: ConnectBoardChange: Proposed subitem name (Removed): '{subitem_name}'")
                    if not monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns):
                        overall_op_successful = False
                        print(f"ERROR: MONDAY_TASKS: ConnectBoardChange: Failed to create subitem for removed link {item_id_linked}.")
                else:
                    print(f"WARNING: MONDAY_TASKS: ConnectBoardChange: Could not retrieve name for removed item ID: {item_id_linked}. Skipping subitem creation.")
                    overall_op_successful = False
            
            success = overall_op_successful

        elif log_type == "StatusChange":
            current_status_data = current_column_value
            previous_status_data = previous_column_value

            current_status_label = current_status_data.get('label') if isinstance(current_status_data, dict) else str(current_status_data)
            previous_status_label = previous_status_data.get('label') if isinstance(previous_status_data, dict) else str(previous_status_data)

            user_name = monday.get_user_name(event_user_id)
            update_text = ""
            if user_name:
                update_text = f"Status changed from '{previous_status_label}' to '{current_status_label}' by {user_name}."
            else:
                update_text = f"Status changed from '{previous_status_label}' to '{current_status_label}'."

            print(f"INFO: MONDAY_TASKS: StatusChange - Status change detected for item {item_id_from_webhook}: {update_text}")
            success = monday.create_update(item_id_from_webhook, update_text)

        elif log_type == "SubitemConnectRelay":
            print("INFO: MONDAY_TASKS: SubitemConnectRelay - task executed (logic needs full implementation if used).")
            success = True # Placeholder if not fully implemented

        else:
            print(f"INFO: MONDAY_TASKS: Webhook type '{webhook_type}' or column ID '{trigger_column_id_from_webhook}' did not match rule '{log_type}'. Ignoring in task.")
            success = True # Task successfully processed (nothing to do)

    # Note: This return value is from the `elif` block.
    # The `process_general_webhook` should always return True unless a critical unhandled error occurs.
    # If the rule didn't match, or if it skipped due to a warning, it should still be considered "processed".
    return success


@celery_app.task
def process_plp_course_sync_webhook(event_data):
    """
    Celery task to handle the PLP Course Sync logic in the background.
    This is the *original working logic* from plp_course_sync_app.py.
    """
    subitem_id = event_data.get('pulseId')
    subitem_board_id = event_data.get('boardId')
    parent_item_id = event_data.get('parentItemId')
    parent_board_id_from_webhook = event_data.get('parentItemBoardId')
    trigger_column_id = event_data.get('columnId')
    current_value = event_data.get('value')
    previous_value = event_data.get('previousValue')

    print(f"DEBUG: MONDAY_TASKS: process_plp_course_sync_webhook - Entering for subitem {subitem_id} on board {subitem_board_id}.")
    print(f"DEBUG: MONDAY_TASKS: process_plp_course_sync_webhook - Parent item ID: {parent_item_id} on parent board ID: {parent_board_id_from_webhook}")
    print(f"DEBUG: MONDAY_TASKS: process_plp_course_sync_webhook - Trigger column: {trigger_column_id}")

    added_all_courses_ids = set()
    removed_all_courses_ids = set()

    # 1. Validate required environment variables
    if not all([HS_ROSTER_BOARD_ID, HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID,
                HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID, HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID,
                ALL_COURSES_BOARD_ID, PLP_BOARD_ID, PLP_CATEGORY_TO_CONNECT_COLUMN_MAP]):
        print("ERROR: MONDAY_TASKS: process_plp_course_sync_webhook - Missing one or more required environment variables. Check DO App Platform config.")
        return False

    # Validate that this webhook is for a subitem belonging to the configured HS Roster main board.
    try:
        if (not parent_board_id_from_webhook or
            int(parent_board_id_from_webhook) != int(HS_ROSTER_BOARD_ID) or
            trigger_column_id != HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID):

            print(f"INFO: MONDAY_TASKS: process_plp_course_sync_webhook - Webhook received. Parent item board ID ({parent_board_id_from_webhook}) does not match configured HS Roster board ({HS_ROSTER_BOARD_ID}) OR trigger column ({trigger_column_id}) does not match configured connect column ({HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}). Ignoring.")
            return True
    except ValueError:
        print(f"ERROR: MONDAY_TASKS: process_plp_course_sync_webhook - HS_ROSTER_BOARD_ID '{HS_ROSTER_BOARD_ID}' or parentItemBoardId '{parent_board_id_from_webhook}' is not a valid integer. Ignoring task for safety.")
        return False

    if not parent_item_id:
        print(f"ERROR: MONDAY_TASKS: process_plp_course_sync_webhook - Parent item ID not found in webhook payload for subitem {subitem_id}. This component expects subitem webhooks. Cannot process.")
        return False

    # 2. Determine which All Courses items were added or removed from the subitem's connect column.
    current_all_courses_ids = monday.get_linked_ids_from_connect_column_value(current_value)
    previous_all_courses_ids = monday.get_linked_ids_from_connect_column_value(previous_value)

    if isinstance(current_all_courses_ids, set) and isinstance(previous_all_courses_ids, set):
        added_all_courses_ids = current_all_courses_ids - previous_all_courses_ids
        removed_all_courses_ids = previous_all_courses_ids - current_all_courses_ids
    else:
        print(f"ERROR: MONDAY_TASKS: process_plp_course_sync_webhook - Expected sets for linked IDs, but received types {type(current_all_courses_ids)} and {type(previous_all_courses_ids)}. Cannot proceed with set operations.")
        return False

    if not added_all_courses_ids and not removed_all_courses_ids:
        print("DEBUG: MONDAY_TASKS: process_plp_course_sync_webhook - No effective changes detected in subitem's All Courses connect column. Exiting handler.")
        return True

    operation_successful = True

    # 3. Get the subitem's dropdown value (e.g., "Math", "ELA") to categorize the course.
    subitem_dropdown_data = monday.get_column_value(subitem_id, subitem_board_id, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID)
    subitem_dropdown_label = subitem_dropdown_data.get('text') if subitem_dropdown_data else None

    if not subitem_dropdown_label:
        print(f"WARNING: MONDAY_TASKS: process_plp_course_sync_webhook - Subitem {subitem_id} dropdown column '{HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID}' is empty or invalid. Cannot determine PLP category. Skipping sync for this subitem.")
        return True

    target_plp_connect_column_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(subitem_dropdown_label)
    if not target_plp_connect_column_id:
        print(f"WARNING: MONDAY_TASKS: process_plp_course_sync_webhook - Subitem dropdown label '{subitem_dropdown_label}' not found in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP. Ensure map is correct. Skipping sync for this subitem.")
        return True

    print(f"DEBUG: MONDAY_TASKS: process_plp_course_sync_webhook - Subitem dropdown label: '{subitem_dropdown_label}', mapped to PLP connect column: '{target_plp_connect_column_id}'")

    # 4. Get the PLP item ID linked to the HS Roster main item (parent of the subitem).
    plp_link_data = monday.get_column_value(parent_item_id, parent_board_id_from_webhook, HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID)
    plp_linked_ids = monday.get_linked_ids_from_connect_column_value(plp_link_data.get('value')) if plp_link_data else set()

    if not plp_linked_ids:
        print(f"INFO: MONDAY_TASKS: process_plp_course_sync_webhook - No PLP item found linked to HS Roster main item {parent_item_id} via column '{HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID}'. Cannot perform PLP sync for this subitem. Skipping.")
        return True

    plp_item_id = list(plp_linked_ids)[0]
    print(f"DEBUG: MONDAY_TASKS: process_plp_course_sync_webhook - Found PLP Item ID {plp_item_id} linked to HS Roster main item {parent_item_id}.")

    # 5. Update the relevant PLP column (on the PLP board) with the All Courses item ID(s).
    for all_courses_item_id in added_all_courses_ids:
        print(f"INFO: MONDAY_TASKS: process_plp_course_sync_webhook - Attempting to ADD All Courses item {all_courses_item_id} to PLP item {plp_item_id} in column {target_plp_connect_column_id} on PLP board {PLP_BOARD_ID}.")
        if not monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id, all_courses_item_id, action="add"):
            operation_successful = False

    for all_courses_item_id in removed_all_courses_ids:
        print(f"INFO: MONDAY_TASKS: process_plp_course_sync_webhook - Attempting to REMOVE All Courses item {all_courses_item_id} from PLP item {plp_item_id} in column {target_plp_connect_column_id} on PLP board {PLP_BOARD_ID}.")
        if not monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id, all_courses_item_id, action="remove"):
            operation_successful = False

    return operation_successful


@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    """
    Celery task to handle the syncing of People column changes from Master Student List
    to linked items on other boards.
    """
    master_item_id = event_data.get('pulseId')
    master_board_id = event_data.get('boardId')
    trigger_column_id = event_data.get('columnId')
    current_column_value_raw = event_data.get('value')

    print(f"DEBUG: MONDAY_TASKS: process_master_student_person_sync_webhook - Entering for item {master_item_id} on board {master_board_id}.")
    print(f"DEBUG: MONDAY_TASKS: process_master_student_person_sync_webhook - Trigger column ID: {trigger_column_id}")
    print(f"DEBUG: MONDAY_TASKS: process_master_student_person_sync_webhook - Current column value (raw): {current_column_value_raw}")

    # 1. Validate if the webhook is from the Master Student List board and a relevant People column
    try:
        if not MASTER_STUDENT_LIST_BOARD_ID or int(master_board_id) != int(MASTER_STUDENT_LIST_BOARD_ID):
            print(f"INFO: MONDAY_TASKS: process_master_student_person_sync_webhook - Webhook for board ID {master_board_id} received, but configured for Master Student List board ({MASTER_STUDENT_LIST_BOARD_ID}). Ignoring.")
            return True
    except ValueError:
        print(f"ERROR: MONDAY_TASKS: process_master_student_person_sync_webhook - MASTER_STUDENT_LIST_BOARD_ID '{MASTER_STUDENT_LIST_BOARD_ID}' is not a valid integer. Ignoring webhook for safety.")
        return False

    if trigger_column_id not in MASTER_STUDENT_PEOPLE_COLUMNS:
        print(f"INFO: MONDAY_TASKS: process_master_student_person_sync_webhook - Trigger column '{trigger_column_id}' is not one of the configured Master Student People columns. Ignoring.")
        return True

    operation_successful = True

    # 2. Iterate through all target boards configured for this people column
    mappings_for_this_column = COLUMN_MAPPINGS.get(trigger_column_id)
    if not mappings_for_this_column:
        print(f"ERROR: MONDAY_TASKS: process_master_student_person_sync_webhook - No mappings found for trigger column '{trigger_column_id}' in COLUMN_MAPPINGS. Please check configuration.")
        return False

    for target_config in mappings_for_this_column["targets"]:
        target_board_id = target_config["board_id"]
        master_connect_column_id = target_config["connect_column_id"]
        target_people_column_id = target_config["target_column_id"]
        target_column_type = target_config["target_column_type"]

        print(f"DEBUG: MONDAY_TASKS: process_master_student_person_sync_webhook - Processing target board {target_board_id} for people column '{MASTER_STUDENT_PEOPLE_COLUMNS.get(trigger_column_id, 'Unknown')}'.")
        print(f"DEBUG: MONDAY_TASKS: process_master_student_person_sync_webhook -   Master Connect Column: {master_connect_column_id}")
        print(f"DEBUG: MONDAY_TASKS: process_master_student_person_sync_webhook -   Target People Column: {target_people_column_id} (Type: {target_column_type})")

        # 3. Get the linked item IDs on the target board via the Master Student List's connect column
        linked_item_ids_on_target_board = monday.get_linked_items_from_board_relation(
            item_id=master_item_id,
            board_id=master_board_id, # Use the actual master_board_id
            connect_column_id=master_connect_column_id
        )

        if not linked_item_ids_on_target_board:
            print(f"INFO: MONDAY_TASKS: process_master_student_person_sync_webhook - No items found linked to Master Student item {master_item_id} on board {target_board_id} via column {master_connect_column_id}. Skipping sync for this board.")
            continue

        # 4. For each linked item, update its corresponding People column
        for linked_target_item_id in linked_item_ids_on_target_board:
            print(f"INFO: MONDAY_TASKS: process_master_student_person_sync_webhook - Attempting to update people column '{target_people_column_id}' on item {linked_target_item_id} (board {target_board_id}) with new value: {current_column_value_raw} and type: {target_column_type}.")
            success = monday.update_people_column(
                item_id=linked_target_item_id,
                board_id=target_board_id,
                people_column_id=target_people_column_id,
                new_people_value=current_column_value_raw,
                target_column_type=target_column_type
            )
            if not success:
                operation_successful = False
                print(f"ERROR: MONDAY_TASKS: process_master_student_person_sync_webhook - Failed to update people column for linked item {linked_target_item_id} on board {target_board_id}.")

    return operation_successful


@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    """
    Celery task to handle the syncing of People column changes from SpEd Students board
    to linked items on the IEP and AP board.
    """
    source_item_id = event_data.get('pulseId')
    source_board_id = event_data.get('boardId')
    trigger_column_id = event_data.get('columnId')
    current_column_value_raw = event_data.get('value')

    print(f"DEBUG: MONDAY_TASKS: process_sped_students_person_sync_webhook - Entering for item {source_item_id} on board {source_board_id}.")
    print(f"DEBUG: MONDAY_TASKS: process_sped_students_person_sync_webhook - Trigger column ID: {trigger_column_id}")
    print(f"DEBUG: MONDAY_TASKS: process_sped_students_person_sync_webhook - Current column value (raw): {current_column_value_raw}")

    # 1. Validate if the webhook is from the SpEd Students board and a relevant People column
    try:
        if not SPED_STUDENTS_BOARD_ID or int(source_board_id) != int(SPED_STUDENTS_BOARD_ID):
            print(f"INFO: MONDAY_TASKS: process_sped_students_person_sync_webhook - Webhook for board ID {source_board_id} received, but configured for SpEd Students board ({SPED_STUDENTS_BOARD_ID}). Ignoring.")
            return True
    except ValueError:
        print(f"ERROR: MONDAY_TASKS: process_sped_students_person_sync_webhook - SPED_STUDENTS_BOARD_ID '{SPED_STUDENTS_BOARD_ID}' is not a valid integer. Ignoring webhook for safety.")
        return False

    if trigger_column_id not in SPED_STUDENTS_PEOPLE_COLUMN_MAPPING:
        print(f"INFO: MONDAY_TASKS: process_sped_students_person_sync_webhook - Trigger column '{trigger_column_id}' is not one of the configured SpEd Students People columns for sync. Ignoring.")
        return True

    operation_successful = True

    # Get the mapping for the triggered column
    column_sync_config = SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.get(trigger_column_id)
    if not column_sync_config:
        print(f"ERROR: MONDAY_TASKS: process_sped_students_person_sync_webhook - No sync configuration found for trigger column '{trigger_column_id}'. This should not happen if the above check passed.")
        return False

    target_people_column_id = column_sync_config["target_column_id"]
    target_column_type = column_sync_config["target_column_type"]

    # 2. Get the linked item IDs on the IEP and AP board via the SpEd Students board's connect column
    linked_iep_ap_item_ids = monday.get_linked_items_from_board_relation(
        item_id=source_item_id,
        board_id=source_board_id, # Use the actual source_board_id
        connect_column_id=SPED_TO_IEPAP_CONNECT_COLUMN_ID
    )

    if not linked_iep_ap_item_ids:
        print(f"INFO: MONDAY_TASKS: process_sped_students_person_sync_webhook - No IEP and AP items found linked to SpEd Students item {source_item_id} on board {IEP_AP_BOARD_ID} via column {SPED_TO_IEPAP_CONNECT_COLUMN_ID}. Skipping sync for this item.")
        return True

    # 3. For each linked IEP and AP item, update its corresponding People column
    for linked_iep_ap_item_id in linked_iep_ap_item_ids:
        print(f"INFO: MONDAY_TASKS: process_sped_students_person_sync_webhook - Attempting to update people column '{target_people_column_id}' on item {linked_iep_ap_item_id} (board {IEP_AP_BOARD_ID}) with new value: {current_column_value_raw} and type: {target_column_type}.")
        success = monday.update_people_column(
            item_id=linked_iep_ap_item_id,
            board_id=IEP_AP_BOARD_ID,
            people_column_id=target_people_column_id,
            new_people_value=current_column_value_raw,
            target_column_type=target_column_type
        )
        if not success:
            operation_successful = False
            print(f"ERROR: MONDAY_TASKS: process_sped_students_person_sync_webhook - Failed to update people column for linked item {linked_iep_ap_item_id} on board {IEP_AP_BOARD_ID}.")

    return operation_successful
