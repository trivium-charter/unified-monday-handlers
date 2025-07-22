import os
import json
from flask import Flask, request, jsonify

from celery_app import celery_app
from monday_tasks import process_sped_students_person_sync_webhook # Task lives in monday_tasks.py

app = Flask(__name__)

# --- Global Configuration (Environment Variables) ---
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR = os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}")
try:
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR)
except json.JSONDecodeError:
    print("ERROR: SPED_STUDENTS_PEOPLE_COLUMN_MAPPING environment variable is not valid JSON. Defaulting to empty map.")
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}

@app.route('/monday-sped-students-person-sync', methods=['POST'])
def monday_sped_students_person_sync():
    if request.method == 'POST':
        data = request.get_json()
        print(f"Received webhook payload for SpEd Students Person Sync: {data}")

        if 'challenge' in data:
            print("Responding to Monday.com webhook challenge.")
            return jsonify({'challenge': data['challenge']})

        event = data.get('event', {})
        webhook_type = event.get('type')

        if webhook_type == "update_column_value":
            if not all([MONDAY_API_KEY, SPED_STUDENTS_BOARD_ID]):
                print("ERROR: Missing essential environment variables for SpEd Students Person Sync pre-check. Cannot queue task.")
                return jsonify({"status": "error", "message": "Server configuration incomplete."}), 500

            source_board_id = event.get('boardId')
            trigger_column_id = event.get('columnId')
            try:
                if (not source_board_id or int(source_board_id) != int(SPED_STUDENTS_BOARD_ID) or
                    trigger_column_id not in SPED_STUDENTS_PEOPLE_COLUMN_MAPPING):
                    print(f"INFO: Webhook received. Board ID ({source_board_id}) or trigger column ({trigger_column_id}) not relevant to SpEd Students Sync. Ignoring.")
                    return jsonify({"status": "ignored", "message": "Webhook not relevant to configured sync component."}), 200
            except ValueError:
                print(f"ERROR: SPED_STUDENTS_BOARD_ID '{SPED_STUDENTS_BOARD_ID}' or boardId '{source_board_id}' is not a valid integer. Ignoring webhook for safety.")
                return jsonify({"status": "error", "message": "Invalid board ID configuration."}), 500

            process_sped_students_person_sync_webhook.delay(event)
            print("SpEd Students Person Sync webhook successfully queued for background processing.")
            return jsonify({"status": "success", "message": "Webhook received and queued for processing."}), 202

        else:
            print(f"INFO: Received webhook of type '{webhook_type}'. This component only handles 'update_column_value'. Ignoring payload.")
            return jsonify({"status": "ignored", "message": "Webhook type not handled by this component."}), 200

    print("ERROR: Invalid request method. Only POST is allowed for webhooks.")
    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    return "Monday.com SpEd Students Person Sync App is running! (Webhooks are queued)", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
