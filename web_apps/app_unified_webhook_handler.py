import os
import json
from flask import Flask, request, jsonify
from celery_app import celery_app
from monday_tasks import (
    process_general_webhook,
    process_plp_course_sync_webhook,
    process_master_student_person_sync_webhook,
    process_sped_students_person_sync_webhook,
    process_canvas_full_sync_from_status,
    process_canvas_delta_sync_from_course_change
)

app = Flask(__name__)

# --- Load Environment Variables for Dispatching ---
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID", "")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID", "")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID", "")
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID", "")
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID", "")

try:
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}"))
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}"))
    LOG_CONFIGS = json.loads(os.environ.get("MONDAY_LOGGING_CONFIGS", "[]"))
except json.JSONDecodeError:
    MASTER_STUDENT_PEOPLE_COLUMNS = {}
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}
    LOG_CONFIGS = []

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

        # 1. Unified Canvas Sync Check (routes to different tasks based on column)
        PLP_TRIGGER_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
        PLP_TRIGGER_COLUMN_IDS = [c.strip() for c in PLP_TRIGGER_COLUMNS_STR.split(',') if c.strip()]
        PLP_CANVAS_SYNC_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_COLUMN_ID")

        if (webhook_type == "update_column_value" and
            PLP_BOARD_ID and webhook_board_id == PLP_BOARD_ID and
            trigger_column_id in PLP_TRIGGER_COLUMN_IDS):

            if trigger_column_id == PLP_CANVAS_SYNC_COLUMN_ID:
                print("INFO: Dispatching to Canvas FULL Sync task from status change.")
                process_canvas_full_sync_from_status.delay(event)
                return jsonify({"status": "success", "message": "Canvas Full Sync task queued."}), 202
            else:
                print("INFO: Dispatching to Canvas DELTA Sync task from course change.")
                process_canvas_delta_sync_from_course_change.delay(event, LOG_CONFIGS)
                return jsonify({"status": "success", "message": "Canvas Delta Sync task queued."}), 202

        # 2. PLP Course Sync Check (HS Roster to PLP)
        if (webhook_type == "update_column_value" and parent_item_board_id and
            HS_ROSTER_BOARD_ID and parent_item_board_id == HS_ROSTER_BOARD_ID and
            trigger_column_id == HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID):
            print("INFO: Dispatching to PLP Course Sync task.")
            process_plp_course_sync_webhook.delay(event)
            return jsonify({"status": "success", "message": "PLP Course Sync task queued."}), 202

        # 3. Master Student Person Sync Check
        if (webhook_type == "update_column_value" and MASTER_STUDENT_BOARD_ID and
            webhook_board_id == MASTER_STUDENT_BOARD_ID and
            trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS):
            print("INFO: Dispatching to Master Student Person Sync task.")
            process_master_student_person_sync_webhook.delay(event)
            return jsonify({"status": "success", "message": "Master Student Person Sync task queued."}), 202

        # 4. SpEd Students Person Sync Check
        if (webhook_type == "update_column_value" and SPED_STUDENTS_BOARD_ID and
            webhook_board_id == SPED_STUDENTS_BOARD_ID and
            trigger_column_id in SPED_STUDENTS_PEOPLE_COLUMN_MAPPING):
            print("INFO: Dispatching to SpEd Students Person Sync task.")
            process_sped_students_person_sync_webhook.delay(event)
            return jsonify({"status": "success", "message": "SpEd Students Person Sync task queued."}), 202

        # 5. General Logger Check
        for config_rule in LOG_CONFIGS:
            if str(config_rule.get("trigger_board_id")) == webhook_board_id:
                if (webhook_type in ["create_item", "create_pulse"] and config_rule.get("log_type") == "CopyToItemName") or \
                   (webhook_type == "update_column_value" and (not config_rule.get("trigger_column_id") or config_rule.get("trigger_column_id") == trigger_column_id)):
                    print(f"INFO: Dispatching to General Logger ({config_rule.get('log_type')}).")
                    process_general_webhook.delay(event, config_rule)
                    return jsonify({"status": "success", "message": f"General Logger ({config_rule.get('log_type')}) task queued."}), 202
        
        return jsonify({"status": "ignored", "message": "No matching dispatch rule found."}), 200

    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    return "Monday.com Unified Webhook Handler is running!", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
