import os
import json
from datetime import datetime # Needed for dynamic subitem names
from celery_app import celery_app
import monday_utils as monday # Import your shared utility functions

# --- Global Configuration (Environment Variables for Tasks) ---
# These are loaded when the worker starts, or when the task is defined
# Make sure these are also available to your Celery worker processes

# Environment variables for process_general_webhook
MONDAY_LOGGING_CONFIGS_STR = os.environ.get("MONDAY_LOGGING_CONFIGS", "[]")
try:
    MONDAY_LOGGING_CONFIGS = json.loads(MONDAY_LOGGING_CONFIGS_STR)
except json.JSONDecodeError:
    print("ERROR: MONDAY_LOGGING_CONFIGS environment variable is not valid JSON. Defaulting to empty list.")
    MONDAY_LOGGING_CONFIGS = []

# Environment variables for process_plp_course_sync_webhook
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID")
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_CATEGORY_TO_CONNECT_COLUMN_MAP_STR = os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}")
try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(PLP_CATEGORY_TO_CONNECT_COLUMN_MAP_STR)
except json.JSONDecodeError:
    print("ERROR: PLP_CATEGORY_TO_CONNECT_COLUMN_MAP environment variable is not valid JSON. Defaulting to empty map.")
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}

# Environment variables for Master Student Person Sync
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID")
# Mappings for other boards (loaded from environment variable, similar to PLP_CATEGORY_TO_CONNECT_COLUMN_MAP)
COLUMN_MAPPINGS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}") # Assuming you'll set this env var
try:
    COLUMN_MAPPINGS = json.loads(COLUMN_MAPPINGS_STR)
except json.JSONDecodeError:
    print("ERROR: MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS environment variable is not valid JSON. Defaulting to empty map.")
    COLUMN_MAPPINGS = {}
# Master Student People Column mapping (for validation)
MASTER_STUDENT_PEOPLE_COLUMNS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}")
try:
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(MASTER_STUDENT_PEOPLE_COLUMNS_STR)
except json.JSONDecodeError:
    print("ERROR: MASTER_STUDENT_PEOPLE_COLUMNS environment variable is not valid JSON. Defaulting to empty map.")
    MASTER_STUDENT_PEOPLE_COLUMNS = {}

# Environment variables for SpEd Students Person Sync
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID")
SPED_TO_IEPAP_CONNECT_COLUMN_ID = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN_ID")
SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR = os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}")
try:
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR)
except json.JSONDecodeError:
    print("ERROR: SPED_STUDENTS_PEOPLE_COLUMN_MAPPING environment variable is not valid JSON. Defaulting to empty map.")
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}

# Environment variables for app-monday-subitem-logger-main (assuming this is part of general logger rules)
MONDAY_MAIN_BOARD_ID = os.environ.get("MONDAY_MAIN_BOARD_ID")
MONDAY_CONNECT_BOARD_COLUMN_ID = os.environ.get("MONDAY_CONNECT_BOARD_COLUMN_ID")
LINKED_BOARD_ID = os.environ.get("MONDAY_LINKED_BOARD_ID")
ORIGINAL_PEOPLE_COLUMN_ID = os.environ.get("MONDAY_ORIGINAL_PEOPLE_COLUMN_ID") # Not directly used in task, but configured.
TARGET_COLUMN_ID = os.environ.get("MONDAY_TARGET_COLUMN_ID") # Not directly used in task, but configured.
MONDAY_SUBJECT_PREFIX = os.environ.get("MONDAY_SUBJECT_PREFIX", "")
MONDAY_ENTRY_TYPE_COLUMN_ID = os.environ.get("MONDAY_ENTRY_TYPE_COLUMN_ID", "")


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

    print(f"DEBUG: Celery general task received for log_type: '{log_type}', board: '{webhook_board_id}', item: '{item_id_from_webhook}'")

    # Re-validate the board ID in the task (important if multiple rules dispatch to this task)
    try:
        if configured_trigger_board_id and int(webhook_board_id) != int(configured_trigger_board_id):
            print(f"WARNING: Task received for unmatched board ID. Expected {configured_trigger_board_id}, got {webhook_board_id}. Skipping.")
            return False
    except (ValueError, TypeError):
        print(f"ERROR: Invalid 'trigger_board_id' in config rule or webhook payload. Skipping.")
        return False

    success = False # Default to False, change to True upon successful action

    # --- Implementations for specific log_types (from original app.py logic) ---

    if log_type == "CopyToItemName" and webhook_type in ["create_item", "create_pulse"]:
        source_column_id = params.get("source_column_id")
        if not source_column_id:
            print(f"ERROR: Missing 'source_column_id' in params for CopyToItemName log_type.")
            return False

        source_column_data = monday.get_column_value(item_id_from_webhook, webhook_board_id, source_column_id)
        source_text = None
        if source_column_data and source_column_data.get('text'):
            source_text = source_column_data['text']

        if not source_text or not source_text.strip():
            print(f"INFO: Source column '{source_column_id}' for item {item_id_from_webhook} is empty. Skipping name update.")
            success = True # Consider this a success if no action needed
        else:
            print(f"INFO: Attempting to update item {item_id_from_webhook} name on board {webhook_board_id} to '{source_text}'.")
            success = monday.update_item_name(item_id_from_webhook, webhook_board_id, source_text)

    elif webhook_type == "update_column_value" and trigger_column_id_from_webhook == configured_trigger_col_id:
        if log_type == "NameReformat":
            # Original NameReformat logic (adjust/complete based on your exact original rules)
            target_text_column_id = params.get("target_text_column_id") # If reformatting to a different column
            # Assume original intent was to reformat the item name itself unless target_text_column_id is given
            
            item_name = monday.get_item_name(item_id_from_webhook, webhook_board_id)
            if item_name:
                # Example: Simple uppercase reformat. Adapt this to your actual reformatting logic.
                new_name = item_name.upper()
                
                if target_text_column_id:
                    # If writing to a separate text column
                    column_values_to_update = {target_text_column_id: new_name}
                    # monday.change_multiple_column_values(webhook_board_id, item_id_from_webhook, column_values_to_update) # Example, if this utility exists
                    print(f"INFO: NameReformat - Would update column '{target_text_column_id}' on item {item_id_from_webhook} with '{new_name}'.")
                    # For a simple text column, `update_item_name` might not be the right utility,
                    # you'd need a more generic `change_multiple_column_values` or similar.
                    # As a placeholder, for a text column, you could use `update_item_name` if `target_text_column_id` is actually the "name" column.
                    # For safety, let's just log for now if `target_text_column_id` is set and not the main name.
                    print("WARNING: NameReformat logic for specific target text column might need a dedicated `change_column_value` utility.")
                    success = True # Assuming success if the logic runs without error.
                else:
                    # If reformatting the item's primary name
                    success = monday.update_item_name(item_id_from_webhook, webhook_board_id, new_name)
            else:
                print(f"INFO: Could not retrieve item name for {item_id_from_webhook}. Skipping NameReformat.")
                success = True # Considered successful if no action needed

        elif log_type == "ConnectBoardChange":
            # This logic comes from your `app-monday-subitem-logger-main.py`
            # This specifically creates subitems or logs updates based on connect board changes.
            main_item_id = item_id_from_webhook # The item that triggered the webhook (parent for subitem creation)
            main_board_id = webhook_board_id # The board the main item is on

            linked_board_id = params.get('linked_board_id') # The board being linked to
            subject_prefix = params.get('monday_subject_prefix', "") # e.g., "Math" or "Science"
            entry_type_column_id = params.get('monday_entry_type_column_id', "") # Column for "Curriculum Change"
            
            if not all([linked_board_id]): # Only basic check here, broader done in Flask
                print(f"ERROR: Missing required parameters for ConnectBoardChange (subitem creation): {params}")
                return False

            current_linked_ids = monday.get_linked_ids_from_connect_column_value(current_column_value)
            previous_linked_ids = monday.get_linked_ids_from_connect_column_value(previous_column_value)

            added_links = current_linked_ids - previous_linked_ids
            removed_links = previous_linked_ids - current_linked_ids

            print(f"INFO: ConnectBoardChange (Subitem Creation) detected for item {main_item_id}. Added: {added_links}, Removed: {removed_links}")

            operation_successful_local = True
            current_date = datetime.now().strftime('%Y-%m-%d')
            changer_user_name = monday.get_user_name(event_user_id)
            user_log_text = ""
            if changer_user_name:
                user_log_text = f" by {changer_user_name}"
            elif event_user_id == -4:
                user_log_text = " by automation"

            subject_prefix_text = f"{subject_prefix} " if subject_prefix else ""

            additional_subitem_columns = {}
            if entry_type_column_id:
                additional_subitem_columns[entry_type_column_id] = {"labels": ["Curriculum Change"]} # Format for Monday dropdown/status

            # Create subitems for Added Items
            for item_id_linked in added_links:
                linked_item_name = monday.get_item_name(item_id_linked, linked_board_id)
                if linked_item_name:
                    subitem_name = f"Added {subject_prefix_text}'{linked_item_name}' on {current_date}{user_log_text}"
                    # You might need to update subitem_column_values to also link to the original linked_item_id
                    # if your subitem board has a connect column for that.
                    # For example, if subitem has a 'connect_original_item' column:
                    # additional_subitem_columns['connect_original_item'] = {"linkedPulseIds": [{"linkedPulseId": item_id_linked}]}
                    
                    if not monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns):
                        operation_successful_local = False
                        print(f"ERROR: Failed to create subitem for added link {item_id_linked}.")
                else:
                    print(f"WARNING: Could not retrieve name for added item ID: {item_id_linked}. Skipping subitem creation.")
                    operation_successful_local = False

            # Create subitems for Removed Items
            for item_id_linked in removed_links:
                linked_item_name = monday.get_item_name(item_id_linked, linked_board_id)
                if linked_item_name:
                    subitem_name = f"Removed {subject_prefix_text}'{linked_item_name}' on {current_date}{user_log_text}"
                    # Similar to added, pass any relevant columns
                    if not monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns):
                        operation_successful_local = False
                        print(f"ERROR: Failed to create subitem for removed link {item_id_linked}.")
                else:
                    print(f"WARNING: Could not retrieve name for removed item ID: {item_id_linked}. Skipping subitem creation.")
                    operation_successful_local = False
            
            success = operation_successful_local


        elif log_type == "StatusChange":
            # Original StatusChange logic
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

            print(f"INFO: Status change detected for item {item_id_from_webhook}: {update_text}")
            success = monday.create_update(item_id_from_webhook, update_text)


        elif log_type == "SubitemConnectRelay":
            # This was a placeholder. If this is different from PLP sync and truly a "general" relay,
            # its logic would be here. If it was part of the PLP sync intent, it's covered by `process_plp_course_sync_webhook`.
            print("INFO: SubitemConnectRelay task executed (logic needs implementation if general purpose).")
            success = True # Placeholder if not fully implemented

        else:
            print(f"WARNING: Unknown log_type '{log_type}' for update_column_value. Skipping.")
            success = False

    else:
        print(f"INFO: Webhook type '{webhook_type}' or column ID '{trigger_column_id_from_webhook}' did not match rule '{log_type}'. Ignoring in task.")
        return True # Task successfully processed (nothing to do)

    return success

# --- PLP Course Sync Task (Original handle_hs_roster_course_sync logic) ---
@celery_app.task
def process_plp_course_sync_webhook(event_data):
    """
    Celery task to handle the PLP Course Sync logic in the background.
    This is the *original working logic* from plp_course_sync_app.py.
    """
    subitem_id = event_data.get('pulseId') # This is the subitem ID (from subitem board)
    subitem_board_id = event_data.get('boardId') # This is the subitem board ID (from webhook)
    parent_item_id = event_data.get('parentItemId') # Parent of the subitem (HS Roster main item)
    parent_item_board_id_from_webhook = event_data.get('parentItemBoardId') # The main board ID of the parent item
    trigger_column_id = event_data.get('columnId') # The ID of the column that triggered the webhook
    current_value = event_data.get('value') # Current value of the triggered column
    previous_value = event_data.get('previousValue') # Previous value of the triggered column

    print(f"DEBUG: Entering process_plp_course_sync_webhook for subitem {subitem_id} on board {subitem_board_id}.")
    print(f"DEBUG: Parent item ID: {parent_item_id} on parent board ID: {parent_item_board_id_from_webhook}")
    print(f"DEBUG: Trigger column: {trigger_column_id}")

    added_all_courses_ids = set()
    removed_all_courses_ids = set()

    # 1. Validate required environment variables
    if not all([HS_ROSTER_BOARD_ID, HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID,
                HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID, HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID,
                ALL_COURSES_BOARD_ID, PLP_BOARD_ID, PLP_CATEGORY_TO_CONNECT_COLUMN_MAP]):
        print("ERROR: Missing one or more required environment variables for PLP Course Sync. Check DO App Platform config.")
        return False # Task failed

    # Validate that this webhook is for a subitem belonging to the configured HS Roster main board.
    try:
        if (not parent_item_board_id_from_webhook or
            int(parent_item_board_id_from_webhook) != int(HS_ROSTER_BOARD_ID) or
            trigger_column_id != HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID):

            print(f"INFO: Webhook received. Parent item board ID ({parent_item_board_id_from_webhook}) does not match configured HS Roster board ({HS_ROSTER_BOARD_ID}) OR trigger column ({trigger_column_id}) does not match configured connect column ({HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}). Ignoring.")
            return True
    except ValueError:
        print(f"ERROR: HS_ROSTER_BOARD_ID '{HS_ROSTER_BOARD_ID}' or parentItemBoardId '{parent_item_board_id_from_webhook}' is not a valid integer. Ignoring task for safety.")
        return False

    if not parent_item_id:
        print(f"ERROR: Parent item ID not found in webhook payload for subitem {subitem_id}. This component expects subitem webhooks. Cannot process.")
        return False

    # 2. Determine which All Courses items were added or removed from the subitem's connect column.
    current_all_courses_ids = monday.get_linked_ids_from_connect_column_value(current_value)
    previous_all_courses_ids = monday.get_linked_ids_from_connect_column_value(previous_value)

    if isinstance(current_all_courses_ids, set) and isinstance(previous_all_courses_ids, set):
        added_all_courses_ids = current_all_courses_ids - previous_all_courses_ids
        removed_all_courses_ids = previous_all_courses_ids - current_all_courses_ids
    else:
        print(f"ERROR: Expected sets for linked IDs, but received types {type(current_all_courses_ids)} and {type(previous_all_courses_ids)}. Cannot proceed with set operations.")
        return False

    if not added_all_courses_ids and not removed_all_courses_ids:
        print("DEBUG: No effective changes detected in subitem's All Courses connect column. Exiting handler.")
        return True

    operation_successful = True # Track overall success of this handling

    # 3. Get the subitem's dropdown value (e.g., "Math", "ELA") to categorize the course.
    subitem_dropdown_data = monday.get_column_value(subitem_id, subitem_board_id, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID)
    subitem_dropdown_label = subitem_dropdown_data.get('text') if subitem_dropdown_data else None

    if not subitem_dropdown_label:
        print(f"WARNING: Subitem {subitem_id} dropdown column '{HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID}' is empty or invalid. Cannot determine PLP category. Skipping sync for this subitem.")
        return True # Continue processing, but this subitem won't be synced

    # Map the dropdown label to the specific PLP connect column ID.
    target_plp_connect_column_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(subitem_dropdown_label)
    if not target_plp_connect_column_id:
        print(f"WARNING: Subitem dropdown label '{subitem_dropdown_label}' not found in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP. Ensure map is correct. Skipping sync for this subitem.")
        return True # Continue, as this is a config issue, not a code error

    print(f"DEBUG: Subitem dropdown label: '{subitem_dropdown_label}', mapped to PLP connect column: '{target_plp_connect_column_id}'")

    # 4. Get the PLP item ID linked to the HS Roster main item (parent of the subitem).
    plp_link_data = monday.get_column_value(parent_item_id, parent_item_board_id_from_webhook, HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID)
    plp_linked_ids = monday.get_linked_ids_from_connect_column_value(plp_link_data.get('value')) if plp_link_data else set()

    if not plp_linked_ids:
        print(f"INFO: No PLP item found linked to HS Roster main item {parent_item_id} via column '{HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID}'. Cannot perform PLP sync for this subitem. Skipping.")
        return True # Not an error, just no PLP item to link to

    # Assuming one PLP item linked to HS Roster item for simplicity based on typical Monday.com setups.
    # If multiple PLP items can be linked, this logic needs to iterate over `plp_linked_ids`.
    plp_item_id = list(plp_linked_ids)[0]
    print(f"DEBUG: Found PLP Item ID {plp_item_id} linked to HS Roster main item {parent_item_id}.")

    # 5. Update the relevant PLP column (on the PLP board) with the All Courses item ID(s).
    for all_courses_item_id in added_all_courses_ids:
        print(f"INFO: Attempting to ADD All Courses item {all_courses_item_id} to PLP item {plp_item_id} in column {target_plp_connect_column_id} on PLP board {PLP_BOARD_ID}.")
        if not monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id, all_courses_item_id, action="add"):
            operation_successful = False

    for all_courses_item_id in removed_all_courses_ids:
        print(f"INFO: Attempting to REMOVE All Courses item {all_courses_item_id} from PLP item {plp_item_id} in column {target_plp_connect_column_id} on PLP board {PLP_BOARD_ID}.")
        if not monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id, all_courses_item_id, action="remove"):
            operation_successful = False

    return operation_successful


# --- Master Student Person Sync Task (Original app_master_student_person_sync.py logic) ---
@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    """
    Celery task to handle the syncing of People column changes from Master Student List
    to linked items on other boards.
    """
    master_item_id = event_data.get('pulseId')
    master_board_id = event_data.get('boardId')
    trigger_column_id = event_data.get('columnId')
    current_column_value_raw = event_data.get('value') # This is the raw value from the webhook

    print(f"DEBUG: Entering process_master_student_person_sync_webhook for item {master_item_id} on board {master_board_id}.")
    print(f"DEBUG: Trigger column ID: {trigger_column_id}")
    print(f"DEBUG: Current column value (raw): {current_column_value_raw}")

    # 1. Validate if the webhook is from the Master Student List board and a relevant People column
    try:
        if not MASTER_STUDENT_LIST_BOARD_ID or master_board_id != int(MASTER_STUDENT_LIST_BOARD_ID):
            print(f"INFO: Webhook for board ID {master_board_id} received, but configured for Master Student List board ({MASTER_STUDENT_LIST_BOARD_ID}). Ignoring.")
            return True # Not an error, just not relevant to this specific handler
    except ValueError:
        print(f"ERROR: MASTER_STUDENT_LIST_BOARD_ID '{MASTER_STUDENT_LIST_BOARD_ID}' is not a valid integer. Ignoring webhook for safety.")
        return False

    if trigger_column_id not in MASTER_STUDENT_PEOPLE_COLUMNS:
        print(f"INFO: Trigger column '{trigger_column_id}' is not one of the configured Master Student People columns. Ignoring.")
        return True # Not an error, just not relevant

    operation_successful = True

    # 2. Iterate through all target boards configured for this people column
    mappings_for_this_column = COLUMN_MAPPINGS.get(trigger_column_id)
    if not mappings_for_this_column:
        print(f"ERROR: No mappings found for trigger column '{trigger_column_id}' in COLUMN_MAPPINGS. Please check configuration.")
        return False

    for target_config in mappings_for_this_column["targets"]:
        target_board_id = target_config["board_id"]
        master_connect_column_id = target_config["connect_column_id"]
        target_people_column_id = target_config["target_column_id"]
        target_column_type = target_config["target_column_type"] # Retrieve the type field

        print(f"DEBUG: Processing target board {target_board_id} for people column '{MASTER_STUDENT_PEOPLE_COLUMNS[trigger_column_id]}'.")
        print(f"DEBUG:   Master Connect Column: {master_connect_column_id}")
        print(f"DEBUG:   Target People Column: {target_people_column_id} (Type: {target_column_type})")

        # 3. Get the linked item IDs on the target board via the Master Student List's connect column
        linked_item_ids_on_target_board = monday.get_linked_items_from_board_relation(
            item_id=master_item_id,
            board_id=master_board_id, # Use the actual master_board_id
            connect_column_id=master_connect_column_id
        )

        if not linked_item_ids_on_target_board:
            print(f"INFO: No items found linked to Master Student item {master_item_id} on board {target_board_id} via column {master_connect_column_id}. Skipping sync for this board.")
            continue # Move to the next target board

        # 4. For each linked item, update its corresponding People column
        for linked_target_item_id in linked_item_ids_on_target_board:
            print(f"INFO: Attempting to update people column '{target_people_column_id}' on item {linked_target_item_id} (board {target_board_id}) with new value: {current_column_value_raw} and type: {target_column_type}.")
            success = monday.update_people_column(
                item_id=linked_target_item_id,
                board_id=target_board_id,
                people_column_id=target_people_column_id,
                new_people_value=current_column_value_raw, # Pass the raw value directly
                target_column_type=target_column_type # Pass the type to the helper
            )
            if not success:
                operation_successful = False
                print(f"ERROR: Failed to update people column for linked item {linked_target_item_id} on board {target_board_id}.")

    return operation_successful


# --- SpEd Students Person Sync Task (Original app_sped_students_person_sync.py logic) ---
@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    """
    Celery task to handle the syncing of People column changes from SpEd Students board
    to linked items on the IEP and AP board.
    """
    source_item_id = event_data.get('pulseId')
    source_board_id = event_data.get('boardId')
    trigger_column_id = event_data.get('columnId')
    current_column_value_raw = event_data.get('value') # This is the raw value from the webhook

    print(f"DEBUG: Entering process_sped_students_person_sync_webhook for item {source_item_id} on board {source_board_id}.")
    print(f"DEBUG: Trigger column ID: {trigger_column_id}")
    print(f"DEBUG: Current column value (raw): {current_column_value_raw}")

    # 1. Validate if the webhook is from the SpEd Students board and a relevant People column
    try:
        if not SPED_STUDENTS_BOARD_ID or source_board_id != int(SPED_STUDENTS_BOARD_ID):
            print(f"INFO: Webhook for board ID {source_board_id} received, but configured for SpEd Students board ({SPED_STUDENTS_BOARD_ID}). Ignoring.")
            return True # Not an error, just not relevant to this specific handler
    except ValueError:
        print(f"ERROR: SPED_STUDENTS_BOARD_ID '{SPED_STUDENTS_BOARD_ID}' is not a valid integer. Ignoring webhook for safety.")
        return False

    if trigger_column_id not in SPED_STUDENTS_PEOPLE_COLUMN_MAPPING:
        print(f"INFO: Trigger column '{trigger_column_id}' is not one of the configured SpEd Students People columns for sync. Ignoring.")
        return True # Not an error, just not relevant

    operation_successful = True

    # Get the mapping for the triggered column
    column_sync_config = SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.get(trigger_column_id)
    if not column_sync_config:
        print(f"ERROR: No sync configuration found for trigger column '{trigger_column_id}'. This should not happen if the above check passed.")
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
        print(f"INFO: No IEP and AP items found linked to SpEd Students item {source_item_id} on board {IEP_AP_BOARD_ID} via column {SPED_TO_IEPAP_CONNECT_COLUMN_ID}. Skipping sync for this item.")
        return True # Not an error, just no linked item to update

    # 3. For each linked IEP and AP item, update its corresponding People column
    for linked_iep_ap_item_id in linked_iep_ap_item_ids:
        print(f"INFO: Updating people column '{target_people_column_id}' on IEP/AP item {linked_iep_ap_item_id} (board {IEP_AP_BOARD_ID}) with new value: {current_column_value_raw} and type: {target_column_type}.")
        success = monday.update_people_column(
            item_id=linked_iep_ap_item_id,
            board_id=IEP_AP_BOARD_ID,
            people_column_id=target_people_column_id,
            new_people_value=current_column_value_raw, # Pass the raw value directly
            target_column_type=target_column_type # Pass the type to the helper
        )
        if not success:
            operation_successful = False
            print(f"ERROR: Failed to update people column for linked IEP/AP item {linked_iep_ap_item_id} on board {IEP_AP_BOARD_ID}.")

    return operation_successful
