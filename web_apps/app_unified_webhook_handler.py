import os
import json
from flask import Flask, request, jsonify
from celery_app import celery_app
from monday_tasks import (
    process_general_webhook,
    process_master_student_person_sync_webhook,
    process_sped_students_person_sync_webhook,
    process_canvas_sync_webhook
)

app = Flask(__name__)

# --- Load Environment Configurations ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_CANVAS_SYNC_STATUS_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_STATUS_COLUMN_ID")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE", "Sync")
LOG_CONFIGS = json.loads(os.environ.get("MONDAY_LOGGING_CONFIGS", "[]"))
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID")
MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}"))
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
SPED_STUDENTS_PEOPLE_COLUMN = json.loads(os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN", "{}"))

@app.route('/monday-webhooks', methods=['POST'])
def monday_unified_webhooks():
    if 'challenge' in request.get_json():
        return jsonify({'challenge': request.get_json()['challenge']})

    event = request.get_json().get('event', {})
    webhook_board_id = str(event.get('boardId'))
    trigger_column_id = str(event.get('columnId'))
    
    task_queued = False

    # --- DISPATCHING LOGIC ---
    plp_connect_cols = [col.strip() for col in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',')]
    
    is_canvas_connect_trigger = (str(webhook_board_id) == str(PLP_BOARD_ID) and trigger_column_id in plp_connect_cols)
    is_canvas_status_trigger = (str(webhook_board_id) == str(PLP_BOARD_ID) and 
                                trigger_column_id == str(PLP_CANVAS_SYNC_STATUS_COLUMN_ID) and 
                                event.get('value', {}).get('label', {}).get('text', '') == PLP_CANVAS_SYNC_STATUS_VALUE)
    
    if is_canvas_connect_trigger or is_canvas_status_trigger:
        print("INFO: Dispatching to Canvas Sync task.")
        process_canvas_sync_webhook.delay(event)
        task_queued = True

    if webhook_board_id == str(MASTER_STUDENT_LIST_BOARD_ID) and trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS:
        print("INFO: Dispatching to Master Student Person Sync & Subitem task.")
        process_master_student_person_sync_webhook.delay(event)
        task_queued = True

    if webhook_board_id == str(SPED_STUDENTS_BOARD_ID) and trigger_column_id in SPED_STUDENTS_PEOPLE_COLUMN:
        print("INFO: Dispatching to SPED Students Person Sync task.")
        process_sped_students_person_sync_webhook.delay(event)
        task_queued = True

    for config_rule in LOG_CONFIGS:
        if str(config_rule.get("trigger_board_id")) == webhook_board_id and str(config_rule.get("trigger_column_id")) == trigger_column_id:
            if is_canvas_connect_trigger:
                print("INFO: Skipping General Logger for Canvas-related event.")
                continue
            if webhook_board_id == str(MASTER_STUDENT_LIST_BOARD_ID) and trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS:
                continue
            print(f"INFO: Dispatching to General Logger ({config_rule.get('log_type')}).")
            process_general_webhook.delay(event, config_rule)
            task_queued = True

    if task_queued:
        return jsonify({"status": "success", "message": "Task(s) queued."}), 202
    else:
        print(f"INFO: No matching rule for webhook on board {webhook_board_id}, column {trigger_column_id}.")
        return jsonify({"status": "ignored", "message": "No matching rule found."}), 200

@app.route('/')
def home():
    return "Monday.com Unified Webhook Handler is running!", 200
