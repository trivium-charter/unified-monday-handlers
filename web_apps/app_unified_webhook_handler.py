import os
import json
from flask import Flask, request, jsonify

from celery_app import celery_app
# Correctly import all tasks, including the new dedicated subitem logger
from monday_tasks import (
    process_general_webhook,
    process_master_student_person_sync_webhook,
    process_sped_students_person_sync_webhook,
    process_canvas_sync_webhook,
    process_people_subitem_logging
)

app = Flask(__name__)

# --- Load Environment Configurations ---
# ... (Full list of environment variables as provided in the previous turn) ...

@app.route('/monday-webhooks', methods=['POST'])
def monday_unified_webhooks():
    if 'challenge' in request.get_json():
        return jsonify({'challenge': request.get_json()['challenge']})

    event = request.get_json().get('event', {})
    webhook_board_id = str(event.get('boardId'))
    trigger_column_id = str(event.get('columnId'))
    
    task_queued = False

    # --- DISPATCHING LOGIC ---

    # 1. Canvas Sync Check
    # ... (this section is unchanged) ...

    # 2. Master Student Automation Check (Triggers BOTH sync and subitem tasks)
    if webhook_board_id == str(MASTER_STUDENT_LIST_BOARD_ID) and trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS:
        print("INFO: Dispatching to Master Student Person Sync task.")
        process_master_student_person_sync_webhook.delay(event)
        
        print("INFO: Dispatching to People Subitem Logging task.")
        process_people_subitem_logging.delay(event)
        
        task_queued = True

    # 3. SPED Students Person Sync Check
    # ... (this section is unchanged) ...

    # 4. General Logger Check (for any other subitem rules)
    for config_rule in LOG_CONFIGS:
        if str(config_rule.get("trigger_board_id")) == webhook_board_id and str(config_rule.get("trigger_column_id")) == trigger_column_id:
            # This check prevents the generic logger from running for the same event handled above
            if webhook_board_id == str(MASTER_STUDENT_LIST_BOARD_ID) and trigger_column_id in MASTER_STUDENT_PEOPLE_COLUMNS:
                continue
            
            print(f"INFO: Dispatching to General Logger ({config_rule.get('log_type')}).")
            process_general_webhook.delay(event, config_rule)
            task_queued = True

    if task_queued:
        return jsonify({"status": "success", "message": "Task(s) queued."}), 202
    else:
        print(f"INFO: No matching dispatch rule found for webhook on board {webhook_board_id} for column {trigger_column_id}.")
        return jsonify({"status": "ignored", "message": "No matching dispatch rule found."}), 200

@app.route('/')
def home():
    return "Monday.com Unified Webhook Handler is running!", 200
