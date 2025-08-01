import os
import json
from flask import Flask, request, jsonify

from celery_app import celery_app
# --- MODIFIED: Ensure all tasks are imported ---
from monday_tasks import (
    process_general_webhook,
    process_master_student_person_sync_webhook,
    process_sped_students_person_sync_webhook,
    process_plp_course_sync_webhook,
    process_canvas_sync_webhook
)

# --- GLOBAL FLASK APP INSTANCE ---
app = Flask(__name__)

# --- Global Configuration (Environment Variables) ---
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")

# --- Configs for Canvas Sync ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID", "8993025745")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "board_relation_mkqnbtaf,board_relation_mkqnxyjd,board_relation_mkqn34pg,board_relation_mkr54dtg")
PLP_CANVAS_SYNC_STATUS_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_STATUS_COLUMN_ID", "color_mktdzdxj")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE", "Sync")

# --- Configs for Original Automations ---
LOG_CONFIGS_STR = os.environ.get("MONDAY_LOGGING_CONFIGS", "[]")
try:
    LOG_CONFIGS = json.loads(LOG_CONFIGS_STR)
except json.JSONDecodeError:
    LOG_CONFIGS = []

MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID", "")
MASTER_STUDENT_PEOPLE_COLUMNS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}")
try:
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(MASTER_STUDENT_PEOPLE_COLUMNS_STR)
except json.JSONDecodeError:
    MASTER_STUDENT_PEOPLE_COLUMNS = {}

# --- Main Unified Webhook Endpoint ---
@app.route('/monday-webhooks', methods=['POST'])
def monday_unified_webhooks():
    if request.method == 'POST':
        data = request.get_json()
        if 'challenge' in data:
            return jsonify({'challenge': data['challenge']})

        event = data.get('event', {})
        webhook_board_id = event.get('boardId')
        trigger_column_id = event.get('columnId')
        
        if not MONDAY_API_KEY:
            print("ERROR: MONDAY_API_KEY is not set.")
            return jsonify({"status": "error", "message": "Server configuration incomplete."}), 500

        # --- DISPATCHING LOGIC ---
        
        # 1. Canvas Sync Check (Top Priority)
        plp_connect_cols = [col.strip() for col in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',')]
        try:
            is_plp_board = PLP_BOARD_ID and int(webhook_board_id) == int(PLP_BOARD_ID)
            is_connect_trigger = trigger_column_id in plp_connect_cols
            
            is_status_trigger = False
            if trigger_column_id == PLP_CANVAS_SYNC_STATUS_COLUMN_ID:
                status_label = event.get('value', {}).get('label', {}).get('text', '')
                if status_label == PLP_CANVAS_SYNC_STATUS_VALUE:
                    is_status_trigger = True

            if is_plp_board and (is_connect_trigger or is_status_trigger):
                print("INFO: Dispatching to Canvas Sync task.")
                process_canvas_sync_webhook.delay(event)
                return jsonify({"status": "success", "message": "Canvas Sync task queued."}), 202
        except (ValueError, TypeError) as e:
            print(f"ERROR: Invalid board/column ID in Canvas config. Error: {e}")

        # 2. Master Student Person Sync Check
        try:
            if (MASTER_STUDENT_LIST_BOARD_ID and int(webhook_board_id) == int(MASTER_STUDENT_LIST_BOARD_ID) and
                trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS):
                print("INFO: Dispatching to Master Student Person Sync task.")
                process_master_student_person_sync_webhook.delay(event)
                return jsonify({"status": "success", "message": "Master Student Person Sync task queued."}), 202
        except (ValueError, TypeError):
            pass # Ignore and fall through

        # 3. General Logger Check (Includes Subitem Creation)
        for config_rule in LOG_CONFIGS:
            if str(config_rule.get("trigger_board_id")) == str(webhook_board_id):
                # Check if the trigger column matches (or if any column triggers)
                if config_rule.get("trigger_column_id") is None or str(config_rule.get("trigger_column_id")) == str(trigger_column_id):
                    print(f"INFO: Dispatching to General Logger ({config_rule.get('log_type')}).")
                    process_general_webhook.delay(event, config_rule)
                    return jsonify({"status": "success", "message": "General Logger task queued."}), 202

        # (You can add dispatchers for other specific tasks like SpEd sync here if needed)

        print(f"INFO: No matching dispatch rule found for webhook.")
        return jsonify({"status": "ignored", "message": "No matching dispatch rule found."}), 200

    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    return "Monday.com Unified Webhook Handler is running!", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
