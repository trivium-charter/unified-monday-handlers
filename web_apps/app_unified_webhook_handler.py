# app_unified_webhook_handler.py

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
    process_canvas_delta_sync_from_course_change,
    create_course_shell_from_monday # ### RENAMED: Import the new, clearer task name
)

app = Flask(__name__)

# --- Load Environment Variables for Dispatching ---
# ... (this section is unchanged)
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID", "")

# ... (rest of file is the same until the dispatching logic)

@app.route('/monday-webhooks', methods=['POST'])
def monday_unified_webhooks():
    # ... (this part is unchanged)
    
        # --- FINAL DISPATCHING LOGIC ---

        # ### ADDED FOR DIRECT CANVAS COURSE CREATION ###
        # This rule is designed for the "All Courses" board. It triggers the simple workflow
        # to create a course shell if one doesn't exist. This rule should be placed first
        # to ensure it's evaluated before more generic rules.
        if (webhook_type == "update_column_value" and
            ALL_COURSES_BOARD_ID and webhook_board_id == ALL_COURSES_BOARD_ID):
            print("INFO: Dispatching to 'create_course_shell_from_monday' workflow.")
            create_course_shell_from_monday.delay(event) # Call the new task
            return jsonify({"status": "success", "message": "Course Shell Creation workflow queued."}), 202

        # 1. Unified Canvas Sync Check (routes to different tasks based on column)
        # ... (rest of your dispatching rules are unchanged)
