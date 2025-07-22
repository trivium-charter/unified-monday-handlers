# web_apps/app_master_student_person_sync.py
import os
import json
from flask import Flask, request, jsonify

from celery_app import celery_app
from monday_tasks import process_master_student_person_sync_webhook # Task lives in monday_tasks.py

app = Flask(__name__)

# --- Global Configuration (Environment Variables) ---
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID")
MASTER_STUDENT_PEOPLE_COLUMNS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}")
try:
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(MASTER_STUDENT_PEOPLE_COLUMNS_STR)
except json.JSONDecodeError:
    print("ERROR: MASTER_STUDENT_PEOPLE_COLUMNS environment variable is not valid JSON. Defaulting to empty map.")
    MASTER_STUDENT_PEOPLE_COLUMNS = {}

@app.route('/monday-master-student-person-sync', methods=['POST'])
def monday_master_student_person_sync():
    if request.method == 'POST':
        data = request.get_json()
        print(f"Received webhook payload for Master Student Person Sync: {data}")

        if 'challenge' in data:
            print("Responding to Monday.com webhook challenge.")
            return jsonify({'challenge': data['challenge']})

        event = data.get('event', {})
        webhook_type = event.get('type')

        if webhook_type == "update_column_value":
            if not all([MONDAY_API_KEY, MASTER_STUDENT_LIST_BOARD_ID]):
                print("ERROR: Missing essential environment variables for Master Student Person Sync pre-check. Cannot queue task.")
                return jsonify({"status": "error", "message": "Server configuration incomplete."}), 500

            master_board_id = event.get('boardId')
            trigger_column_id = event.get('columnId')
            try:
                if (not master_board_id or int(master_board_id) != int(MASTER_STUDENT_LIST_BOARD_ID) or
                    trigger_column_id not in MASTER_STUDENT_PEOPLE_COLUMNS):
                    print(f"INFO: Webhook received. Board ID ({master_board_id}) or trigger column ({trigger_column_id}) not relevant to Master Student Sync. Ignoring.")
                    return jsonify({"status": "ignored", "message": "Webhook not relevant to configured sync component."}), 200
            except ValueError:
                print(f"ERROR: MASTER_STUDENT_LIST_BOARD_ID '{MASTER_STUDENT_LIST_BOARD_ID}' or boardId '{master_board_id}' is not a valid integer. Ignoring webhook for safety.")
                return jsonify({"status": "error", "message": "Invalid board ID configuration."}), 500

            process_master_student_person_sync_webhook.delay(event)
            print("Master Student Person Sync webhook successfully queued for background processing.")
            return jsonify({"status": "success", "message": "Webhook received and queued for processing."}), 202

        else:
            print(f"INFO: Received webhook of type '{webhook_type}'. This component only handles 'update_column_value'. Ignoring payload.")
            return jsonify({"status": "ignored", "message": "Webhook type not handled by this component."}), 200

    print("ERROR: Invalid request method. Only POST is allowed for webhooks.")
    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    return "Monday.com Master Student Person Sync App is running! (Webhooks are queued)", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
