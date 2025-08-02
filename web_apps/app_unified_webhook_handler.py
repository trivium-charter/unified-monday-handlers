import os
import json
from flask import Flask, request, jsonify
from celery_app import celery_app
from monday_tasks import (
    process_general_webhook,
    process_master_student_person_sync_webhook,
    process_sped_students_person_sync_webhook,
    process_canvas_sync_webhook,
    process_hs_roster_linking_webhook,
    cleanup_hs_roster_links
)

app = Flask(__name__)

# --- Load Environment Configurations ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
HS_COURSE_ROSTER_BOARD_ID = os.environ.get("HS_COURSE_ROSTER_BOARD_ID", "8792275301")
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
    parent_item_board_id = str(event.get('parentItemBoardId')) if event.get('parentItemBoardId') else None
    
    task_queued = False

    # --- DISPATCHING LOGIC ---
    plp_connect_cols = [col.strip() for col in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',')]
    
    if (str(webhook_board_id) == str(PLP_BOARD_ID) and trigger_column_id in plp_connect_cols) or \
       (str(webhook_board_id) == str(PLP_BOARD_ID) and trigger_column_id == str(PLP_CANVAS_SYNC_STATUS_COLUMN_ID)):
        process_canvas_sync_webhook.delay(event)
        task_queued = True

    if parent_item_board_id == str(HS_COURSE_ROSTER_BOARD_ID) and trigger_column_id == "board_relation_mkr0bwsf":
        print("INFO: Dispatching to HS Roster Linking task.")
        process_hs_roster_linking_webhook.delay(event)
        task_queued = True

    if webhook_board_id == str(MASTER_STUDENT_LIST_BOARD_ID) and trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS:
        process_master_student_person_sync_webhook.delay(event)
        task_queued = True

    if webhook_board_id == str(SPED_STUDENTS_BOARD_ID) and trigger_column_id in SPED_STUDENTS_PEOPLE_COLUMN:
        process_sped_students_person_sync_webhook.delay(event)
        task_queued = True

    for config_rule in LOG_CONFIGS:
        if str(config_rule.get("trigger_board_id")) == webhook_board_id and str(config_rule.get("trigger_column_id")) == trigger_column_id:
            if webhook_board_id == str(MASTER_STUDENT_LIST_BOARD_ID) and trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS:
                continue
            process_general_webhook.delay(event, config_rule)
            task_queued = True

    if task_queued:
        return jsonify({"status": "success", "message": "Task(s) queued."}), 202
    else:
        print(f"INFO: No matching rule for webhook on board {webhook_board_id}, column {trigger_column_id}.")
        return jsonify({"status": "ignored", "message": "No matching rule found."}), 200

@app.route('/cleanup-roster-links', methods=['GET', 'POST'])
def trigger_cleanup():
    """Manual trigger for the HS Roster cleanup task."""
    print("INFO: Manual trigger received for HS Roster cleanup.")
    cleanup_hs_roster_links.delay()
    return "HS Course Roster cleanup task has been queued.", 202

@app.route('/')
def home():
    return "Monday.com Unified Webhook Handler is running!", 200
