# web_apps/app_subitem_logger.py
import os
import json
from flask import Flask, request, jsonify

from celery_app import celery_app
from monday_tasks import process_general_webhook # This task handles the subitem logger logic

app = Flask(__name__)

# --- Configuration (Environment Variables) ---
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
MONDAY_MAIN_BOARD_ID = os.environ.get("MONDAY_MAIN_BOARD_ID")
MONDAY_CONNECT_BOARD_COLUMN_ID = os.environ.get("MONDAY_CONNECT_BOARD_COLUMN_ID")
LINKED_BOARD_ID = os.environ.get("MONDAY_LINKED_BOARD_ID") # For pre-check
MONDAY_SUBJECT_PREFIX = os.environ.get("MONDAY_SUBJECT_PREFIX", "") # For pre-check
MONDAY_ENTRY_TYPE_COLUMN_ID = os.environ.get("MONDAY_ENTRY_TYPE_COLUMN_ID", "") # For pre-check


@app.route('/monday-subitem-log', methods=['POST'])
def monday_subitem_log():
    if request.method == 'POST':
        data = request.get_json()
        print(f"Received webhook payload for Monday Subitem Logger: {data}")

        if 'challenge' in data:
            print("Responding to Monday.com webhook challenge.")
            return jsonify({'challenge': data['challenge']})

        event = data.get('event', {})
        webhook_board_id = event.get('boardId')
        trigger_column_id_from_webhook = event.get('columnId')
        webhook_type = event.get('type')

        if not all([MONDAY_API_KEY, MONDAY_MAIN_BOARD_ID, MONDAY_CONNECT_BOARD_COLUMN_ID, LINKED_BOARD_ID]):
            print("Error: Missing essential environment variables for Subitem Logger pre-check. Cannot queue task.")
            return jsonify({"status": "error", "message": "Server not configured properly. Check all MONDAY_* environment variables."}), 500

        try:
            if (webhook_board_id is None or int(webhook_board_id) != int(MONDAY_MAIN_BOARD_ID) or
                trigger_column_id_from_webhook != MONDAY_CONNECT_BOARD_COLUMN_ID):
                print(f"Webhook for board ID {webhook_board_id} or column ID {trigger_column_id_from_webhook} received, but configured for {MONDAY_MAIN_BOARD_ID} and {MONDAY_CONNECT_BOARD_COLUMN_ID}. Ignoring.")
                return jsonify({"status": "ignored", "message": "Board ID or column ID mismatch."}), 200
        except ValueError:
            print(f"Error: MONDAY_MAIN_BOARD_ID '{MONDAY_MAIN_BOARD_ID}' is not a valid integer. Ignoring webhook.")
            return jsonify({"status": "error", "message": "Main Board ID configuration error."}), 500

        if webhook_type == "update_column_value":
            # Queue the task. Pass the necessary parameters as part of config_for_task
            config_for_task = {
                "log_type": "ConnectBoardChange", # This tells process_general_webhook which logic to run
                "trigger_board_id": MONDAY_MAIN_BOARD_ID,
                "trigger_column_id": MONDAY_CONNECT_BOARD_COLUMN_ID,
                "params": {
                    "linked_board_id": LINKED_BOARD_ID,
                    "monday_subject_prefix": MONDAY_SUBJECT_PREFIX,
                    "monday_entry_type_column_id": MONDAY_ENTRY_TYPE_COLUMN_ID
                }
            }
            process_general_webhook.delay(event, config_for_task)
            print("Monday Subitem Logger webhook successfully queued for background processing.")
            return jsonify({"status": "success", "message": "Webhook received and queued for processing."}), 202

        else:
            print(f"INFO: Received webhook of type '{webhook_type}'. This component only handles 'update_column_value'. Ignoring payload.")
            return jsonify({"status": "ignored", "message": "Webhook type not handled by this component."}), 200

    print("ERROR: Invalid request method. Only POST is allowed for webhooks.")
    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    return "Monday.com Subitem Logger App is running! (Webhooks are queued)", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
