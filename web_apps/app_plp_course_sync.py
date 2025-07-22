# web_apps/app_plp_course_sync.py
import os
import json
from flask import Flask, request, jsonify

from celery_app import celery_app
from monday_tasks import process_plp_course_sync_webhook # Task lives in monday_tasks.py

app = Flask(__name__)

# --- Global Configuration (Environment Variables) ---
# Keep these for initial validation in the Flask app
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY") # Only needed for pre-check
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
# Other PLP-related env vars are only needed in the task for full validation
PLP_CATEGORY_TO_CONNECT_COLUMN_MAP_STR = os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}")
try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(PLP_CATEGORY_TO_CONNECT_COLUMN_MAP_STR)
except json.JSONDecodeError:
    print("ERROR: PLP_CATEGORY_TO_CONNECT_COLUMN_MAP environment variable is not valid JSON. Defaulting to empty map.")
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}


@app.route('/monday-plp-course-sync', methods=['POST'])
def monday_plp_course_sync():
    if request.method == 'POST':
        data = request.get_json()
        print(f"Received webhook payload for PLP Course Sync: {data}")

        if 'challenge' in data:
            print("Responding to Monday.com webhook challenge.")
            return jsonify({'challenge': data['challenge']})

        event = data.get('event', {})
        webhook_type = event.get('type')

        if webhook_type == "update_column_value":
            # Perform quick, essential validation before queuing.
            if not all([MONDAY_API_KEY, HS_ROSTER_BOARD_ID, HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID]):
                print("ERROR: Missing essential environment variables for PLP Course Sync pre-check. Cannot queue task.")
                return jsonify({"status": "error", "message": "Server configuration incomplete."}), 500

            parent_item_board_id_from_webhook = event.get('parentItemBoardId')
            trigger_column_id = event.get('columnId')
            try:
                if (not parent_item_board_id_from_webhook or
                    int(parent_item_board_id_from_webhook) != int(HS_ROSTER_BOARD_ID) or
                    trigger_column_id != HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID):
                    print(f"INFO: Webhook received. Parent item board ID ({parent_item_board_id_from_webhook}) does not match configured HS Roster board ({HS_ROSTER_BOARD_ID}) OR trigger column ({trigger_column_id}) does not match configured connect column ({HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID}). Not queuing task.")
                    return jsonify({"status": "ignored", "message": "Webhook not relevant to configured sync component."}), 200
            except ValueError:
                print(f"ERROR: HS_ROSTER_BOARD_ID '{HS_ROSTER_BOARD_ID}' or parentItemBoardId '{parent_item_board_id_from_webhook}' is not a valid integer. Ignoring webhook for safety.")
                return jsonify({"status": "error", "message": "Invalid board ID configuration."}), 500

            process_plp_course_sync_webhook.delay(event)
            print("PLP Course Sync webhook successfully queued for background processing.")
            return jsonify({"status": "success", "message": "Webhook received and queued for processing."}), 202

        else:
            print(f"INFO: Received webhook of type '{webhook_type}'. This component only handles 'update_column_value'. Ignoring payload.")
            return jsonify({"status": "ignored", "message": "Webhook type not handled by this component."}), 200

    print("ERROR: Invalid request method. Only POST is allowed for webhooks.")
    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    return "Monday.com PLP Course Sync App is running! (Webhooks are queued)", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
