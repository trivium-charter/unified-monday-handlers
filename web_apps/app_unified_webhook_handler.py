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
    process_sped_students_person_sync_webhook,
    process_canvas_sync_webhook  # Assuming this task will be added
)

# --- GLOBAL FLASK APP INSTANCE ---
app = Flask(__name__)

# --- Global Configuration (Environment Variables) ---
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")

# Configs for General Logger
LOG_CONFIGS_STR = os.environ.get("MONDAY_LOGGING_CONFIGS", "[]")
try:
    LOG_CONFIGS = json.loads(LOG_CONFIGS_STR)
except json.JSONDecodeError:
    LOG_CONFIGS = []

# Configs for PLP Course Sync
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")

# Configs for Master Student Person Sync
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID")
MASTER_STUDENT_PEOPLE_COLUMNS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}")
try:
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(MASTER_STUDENT_PEOPLE_COLUMNS_STR)
except json.JSONDecodeError:
    MASTER_STUDENT_PEOPLE_COLUMNS = {}

# Configs for SpEd Students Person Sync
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR = os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}")
try:
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR)
except json.JSONDecodeError:
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}

# --- Main Unified Webhook Endpoint ---
@app.route('/monday-webhooks', methods=['POST'])
def monday_unified_webhooks():
    if request.method == 'POST':
        data = request.get_json()
        if 'challenge' in data:
            return jsonify({'challenge': data['challenge']})

        event = data.get('event', {})
        webhook_board_id = str(event.get('boardId'))
        webhook_type = event.get('type')
        trigger_column_id = event.get('columnId')
        parent_item_board_id = str(event.get('parentItemBoardId')) if event.get('parentItemBoardId') else None

        # --- DISPATCHING LOGIC ---

        # 1. Canvas Sync Check
        # This is a simplified example; your actual logic might be in LOG_CONFIGS
        if webhook_type == "update_column_value" and trigger_column_id == os.environ.get("PLP_CANVAS_SYNC_COLUMN_ID"):
             print("INFO: Dispatching to Canvas Sync task.")
             process_canvas_sync_webhook.delay(event)
             return jsonify({"status": "success", "message": "Canvas Sync task queued."}), 202

        # 2. PLP Course Sync Check
        if (webhook_type == "update_column_value" and parent_item_board_id == HS_ROSTER_BOARD_ID and
                trigger_column_id == HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID):
            print("INFO: Dispatching to PLP Course Sync task.")
            process_plp_course_sync_webhook.delay(event)
            return jsonify({"status": "success", "message": "PLP Course Sync task queued."}), 202

        # 3. Master Student Person Sync Check
        if (webhook_type == "update_column_value" and webhook_board_id == MASTER_STUDENT_LIST_BOARD_ID and
                trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS):
            print("INFO: Dispatching to Master Student Person Sync task.")
            process_master_student_person_sync_webhook.delay(event)
            return jsonify({"status": "success", "message": "Master Student Person Sync task queued."}), 202

        # 4. SpEd Students Person Sync Check
        if (webhook_type == "update_column_value" and webhook_board_id == SPED_STUDENTS_BOARD_ID and
                trigger_column_id in SPED_STUDENTS_PEOPLE_COLUMN_MAPPING):
            print("INFO: Dispatching to SpEd Students Person Sync task.")
            process_sped_students_person_sync_webhook.delay(event)
            return jsonify({"status": "success", "message": "SpEd Students Person Sync task queued."}), 202

        # 5. General Logger Check
        for config_rule in LOG_CONFIGS:
            if (str(config_rule.get("trigger_board_id")) == webhook_board_id and
               (not config_rule.get("trigger_column_id") or config_rule.get("trigger_column_id") == trigger_column_id)):
                print(f"INFO: Dispatching to General Logger ({config_rule.get('log_type')}).")
                process_general_webhook.delay(event, config_rule)
                return jsonify({"status": "success", "message": "General Logger task queued."}), 202

        return jsonify({"status": "ignored", "message": "No matching dispatch rule found."}), 200

    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    return "Monday.com Unified Webhook Handler is running!", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
