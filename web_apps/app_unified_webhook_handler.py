# web_apps/app_unified_webhook_handler.py

import os
import json
from flask import Flask, request, jsonify

from celery_app import celery_app
# Import ALL your tasks from monday_tasks.py
from monday_tasks import (
    process_general_webhook,
    process_plp_course_sync_webhook,
    process_master_student_person_sync_webhook,
    process_sped_students_person_sync_webhook
)

# --- GLOBAL FLASK APP INSTANCE ---
app = Flask(__name__)

# --- Global Configuration (Environment Variables) ---
# All environment variables needed for pre-dispatch validation should be loaded here
# All these should be set at the App-level in DigitalOcean

MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
print(f"DEBUG: APP_UNIFIED_HANDLER: MONDAY_API_KEY loaded: {'SET' if MONDAY_API_KEY else 'NOT SET'}")

# Configs for General Logger (from app_general_logger.py)
MONDAY_LOGGING_CONFIGS_STR = os.environ.get("MONDAY_LOGGING_CONFIGS", "[]")
try:
    LOG_CONFIGS = json.loads(MONDAY_LOGGING_CONFIGS_STR)
except json.JSONDecodeError as e:
    print(f"ERROR: APP_UNIFIED_HANDLER: MONDAY_LOGGING_CONFIGS environment variable is not valid JSON: {e}. Defaulting to empty list.")
    LOG_CONFIGS = []
print(f"DEBUG: APP_UNIFIED_HANDLER: LOG_CONFIGS loaded (type: {type(LOG_CONFIGS)}, len: {len(LOG_CONFIGS)}): {LOG_CONFIGS}")


# Configs for PLP Course Sync (from app_plp_course_sync.py)
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID", "")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID", "")
print(f"DEBUG: APP_UNIFIED_HANDLER: HS_ROSTER_BOARD_ID: '{HS_ROSTER_BOARD_ID}'")
print(f"DEBUG: APP_UNIFIED_HANDLER: HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID: '{HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}'")


# Configs for Master Student Person Sync (from app_master_student_person_sync.py)
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID", "")
MASTER_STUDENT_PEOPLE_COLUMNS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}")
try:
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(MASTER_STUDENT_PEOPLE_COLUMNS_STR)
except json.JSONDecodeError as e:
    print(f"ERROR: APP_UNIFIED_HANDLER: MASTER_STUDENT_PEOPLE_COLUMNS environment variable is not valid JSON: {e}. Defaulting to empty map.")
    MASTER_STUDENT_PEOPLE_COLUMNS = {}
print(f"DEBUG: APP_UNIFIED_HANDLER: MASTER_STUDENT_LIST_BOARD_ID: '{MASTER_STUDENT_LIST_BOARD_ID}'")
print(f"DEBUG: APP_UNIFIED_HANDLER: MASTER_STUDENT_PEOPLE_COLUMNS loaded (type: {type(MASTER_STUDENT_PEOPLE_COLUMNS)}, keys: {list(MASTER_STUDENT_PEOPLE_COLUMNS.keys())}): {MASTER_STUDENT_PEOPLE_COLUMNS}")


# Configs for SpEd Students Person Sync (from app_sped_students_person_sync.py)
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID", "")
SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR = os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}")
try:
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR)
except json.JSONDecodeError as e:
    print(f"ERROR: APP_UNIFIED_HANDLER: SPED_STUDENTS_PEOPLE_COLUMN_MAPPING environment variable is not valid JSON: {e}. Defaulting to empty map.")
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}
print(f"DEBUG: APP_UNIFIED_HANDLER: SPED_STUDENTS_BOARD_ID: '{SPED_STUDENTS_BOARD_ID}'")
print(f"DEBUG: APP_UNIFIED_HANDLER: SPED_STUDENTS_PEOPLE_COLUMN_MAPPING loaded (type: {type(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING)}, keys: {list(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.keys())}): {SPED_STUDENTS_PEOPLE_COLUMN_MAPPING}")


# Configs for App-Monday-Subitem-Logger-Main (part of general logger rules)
# Note: These are specific parameters passed within the config_rule to process_general_webhook,
# but the app-level dispatcher needs them for its pre-check/routing decisions.
MONDAY_MAIN_BOARD_ID_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_MAIN_BOARD_ID", "")
MONDAY_CONNECT_BOARD_COLUMN_ID_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_CONNECT_BOARD_COLUMN_ID", "")
LINKED_BOARD_ID_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_LINKED_BOARD_ID", "")
MONDAY_SUBJECT_PREFIX_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_SUBJECT_PREFIX", "")
MONDAY_ENTRY_TYPE_COLUMN_ID_FOR_SUBITEM_LOGGER = os.environ.get("MONDAY_ENTRY_TYPE_COLUMN_ID", "")
print(f"DEBUG: APP_UNIFIED_HANDLER: Subitem Logger MONDAY_MAIN_BOARD_ID: '{MONDAY_MAIN_BOARD_ID_FOR_SUBITEM_LOGGER}'")
print(f"DEBUG: APP_UNIFIED_HANDLER: Subitem Logger MONDAY_CONNECT_BOARD_COLUMN_ID: '{MONDAY_CONNECT_BOARD_COLUMN_ID_FOR_SUBITEM_LOGGER}'")


# --- Main Unified Webhook Endpoint ---
@app.route('/monday-webhooks', methods=['POST']) # All webhooks will now point to this single endpoint
def monday_unified_webhooks():
    if request.method == 'POST':
        data = request.get_json()
        print(f"Received unified webhook payload: {data}")

        if 'challenge' in data:
            print("Responding to Monday.com webhook challenge.")
            return jsonify({'challenge': data['challenge']})

        event = data.get('event', {})
        webhook_board_id = event.get('boardId')
        webhook_type = event.get('type')
        trigger_column_id = event.get('columnId') # Column that triggered the event
        parent_item_board_id = event.get('parentItemBoardId') # For subitem webhooks

        # Basic API Key check
        if not MONDAY_API_KEY:
            print("ERROR: APP_UNIFIED_HANDLER: MONDAY_API_KEY environment variable is not set. Cannot dispatch task.")
            return jsonify({"status": "error", "message": "Server configuration incomplete (API Key missing)."}), 500

        # --- DISPATCHING LOGIC ---
        # Order of checks matters: put more specific handlers first or use unique identifiers.

        # 1. PLP Course Sync Check (Specific to HS Roster subitems' All Courses column)
        # Triggered by update_column_value event on a SUBITEM within a specific parent board (HS_ROSTER_BOARD_ID)
        # and a specific connect column (HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID).
        if webhook_type == "update_column_value" and parent_item_board_id: # Only subitem webhooks have parentItemBoardId
            try:
                if (HS_ROSTER_BOARD_ID and int(parent_item_board_id) == int(HS_ROSTER_BOARD_ID) and
                    HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID and trigger_column_id == HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID):
                    print("INFO: APP_UNIFIED_HANDLER: Dispatching to PLP Course Sync task.")
                    process_plp_course_sync_webhook.delay(event)
                    return jsonify({"status": "success", "message": "PLP Course Sync task queued."}), 202
            except ValueError:
                print(f"ERROR: APP_UNIFIED_HANDLER: Invalid board ID in PLP config ({HS_ROSTER_BOARD_ID}) or webhook ({parent_item_board_id}).")
                # Do not return here, let it fall through to other checks or ignored if not critical.


        # 2. Master Student Person Sync Check (Specific to Master Student List board People columns)
        # Triggered by update_column_value event on the MASTER_STUDENT_LIST_BOARD_ID for mapped People columns.
        if webhook_type == "update_column_value":
            try:
                if (MASTER_STUDENT_LIST_BOARD_ID and int(webhook_board_id) == int(MASTER_STUDENT_LIST_BOARD_ID) and
                    trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS): # Checks if trigger_column_id is one of the mapped people columns
                    print("INFO: APP_UNIFIED_HANDLER: Dispatching to Master Student Person Sync task.")
                    process_master_student_person_sync_webhook.delay(event)
                    return jsonify({"status": "success", "message": "Master Student Person Sync task queued."}), 202
            except ValueError:
                print(f"ERROR: APP_UNIFIED_HANDLER: Invalid board ID in Master Student config ({MASTER_STUDENT_LIST_BOARD_ID}) or webhook ({webhook_board_id}).")
                # Do not return here, let it fall through to other checks or ignored if not critical.


        # 3. SpEd Students Person Sync Check (Specific to SpEd Students board People columns)
        # Triggered by update_column_value event on the SPED_STUDENTS_BOARD_ID for mapped People columns.
        if webhook_type == "update_column_value":
            try:
                if (SPED_STUDENTS_BOARD_ID and int(webhook_board_id) == int(SPED_STUDENTS_BOARD_ID) and
                    trigger_column_id in SPED_STUDENTS_PEOPLE_COLUMN_MAPPING): # Checks if trigger_column_id is one of the mapped people columns
                    print("INFO: APP_UNIFIED_HANDLER: Dispatching to SpEd Students Person Sync task.")
                    process_sped_students_person_sync_webhook.delay(event)
                    return jsonify({"status": "success", "message": "SpEd Students Person Sync task queued."}), 202
            except ValueError:
                print(f"ERROR: APP_UNIFIED_HANDLER: Invalid board ID in SpEd Students config ({SPED_STUDENTS_BOARD_ID}) or webhook ({webhook_board_id}).")
                # Do not return here, let it fall through to other checks or ignored if not critical.


        # 4. General Logger/Subitem Logger Check (Uses LOG_CONFIGS rules for dispatch)
        # This is the most generic, often falling through from the others.
        for config_rule in LOG_CONFIGS:
            log_type = config_rule.get("log_type")
            configured_trigger_board_id = config_rule.get("trigger_board_id")
            configured_trigger_col_id = config_rule.get("trigger_column_id")
            rule_params = config_rule.get("params", {}) # Capture params from config_rule

            # Validate board ID from config rule
            if configured_trigger_board_id:
                try:
                    if int(webhook_board_id) != int(configured_trigger_board_id):
                        continue # Rule doesn't apply to this board
                except ValueError:
                    print(f"WARNING: APP_UNIFIED_HANDLER: Invalid 'trigger_board_id' in config rule: {config_rule}. Skipping rule.")
                    continue
            else:
                print(f"WARNING: APP_UNIFIED_HANDLER: 'trigger_board_id' missing in config rule: {config_rule}. Skipping rule for board matching.")
                continue

            # Check for CopyToItemName on creation events
            if log_type == "CopyToItemName" and webhook_type in ["create_item", "create_pulse"]:
                print(f"INFO: APP_UNIFIED_HANDLER: Dispatching to General Logger (CopyToItemName).")
                process_general_webhook.delay(event, config_rule)
                return jsonify({"status": "success", "message": "General Logger (CopyToItemName) task queued."}), 202

            # Check for other update_column_value events
            elif webhook_type == "update_column_value":
                if configured_trigger_col_id and trigger_column_id != configured_trigger_col_id:
                    continue # Rule doesn't apply to this column

                # If we reach here, it's a match for general processing
                # This includes ConnectBoardChange (Subitem Logger) if configured in LOG_CONFIGS
                print(f"INFO: APP_UNIFIED_HANDLER: Dispatching to General Logger ({log_type}).")
                process_general_webhook.delay(event, config_rule)
                return jsonify({"status": "success", "message": f"General Logger ({log_type}) task queued."}), 202

        # If no specific dispatch rule matched after all checks
        print(f"INFO: APP_UNIFIED_HANDLER: No specific dispatch rule matched for webhook on board {webhook_board_id}, type {webhook_type}, column {trigger_column_id}. Ignoring payload.")
        return jsonify({"status": "ignored", "message": "No matching dispatch rule found."}), 200

    print("ERROR: APP_UNIFIED_HANDLER: Invalid request method. Only POST is allowed for webhooks.")
    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    """Simple home route for health check."""
    return "Monday.com Unified Webhook Handler is running!", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
