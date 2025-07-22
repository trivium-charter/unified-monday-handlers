# web_apps/app_general_logger.py
import os
import json
from flask import Flask, request, jsonify

from celery_app import celery_app
from monday_tasks import process_general_webhook # Task lives in monday_tasks.py

app = Flask(__name__)

# --- Global Configuration (Environment Variables) ---
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
LOG_CONFIGS = json.loads(os.environ.get("MONDAY_LOGGING_CONFIGS", "[]"))

@app.route('/monday-log-events', methods=['POST'])
def monday_log_events():
    if request.method == 'POST':
        data = request.get_json()
        print(f"Received webhook payload for general logger: {data}")
        if 'challenge' in data:
            print("Responding to Monday.com webhook challenge.")
            return jsonify({'challenge': data['challenge']})
        event = data.get('event', {})
        webhook_board_id = event.get('boardId')
        webhook_type = event.get('type')
        trigger_column_id_from_webhook = event.get('columnId')

        if not MONDAY_API_KEY or not LOG_CONFIGS:
            print("Error: Server not configured properly (API Key or Logging configs missing). Cannot queue task.")
            return jsonify({"status": "error", "message": "Server configuration incomplete."}), 500

        queued_tasks_count = 0
        for config_rule in LOG_CONFIGS:
            log_type = config_rule.get("log_type")
            configured_trigger_board_id = config_rule.get("trigger_board_id")
            configured_trigger_col_id = config_rule.get("trigger_column_id")

            if configured_trigger_board_id:
                try:
                    configured_trigger_board_id = int(configured_trigger_board_id)
                except (ValueError, TypeError):
                    print(f"WARNING: Invalid 'trigger_board_id' in config rule: {config_rule}. Skipping rule.")
                    continue
            else:
                print(f"WARNING: 'trigger_board_id' missing in config rule: {config_rule}. Skipping rule for board matching.")
                continue

            if webhook_board_id is None or int(webhook_board_id) != configured_trigger_board_id:
                continue

            if log_type == "CopyToItemName" and webhook_type in ["create_item", "create_pulse"]:
                print(f"INFO: Matching rule found for log_type: '{log_type}', trigger_type: '{webhook_type}'. Queuing task for board {webhook_board_id}.")
                process_general_webhook.delay(event, config_rule)
                queued_tasks_count += 1
                break
            elif webhook_type == "update_column_value":
                if configured_trigger_col_id and trigger_column_id_from_webhook != configured_trigger_col_id:
                    continue
                print(f"INFO: Matching rule found for log_type: '{log_type}', trigger_column: '{trigger_column_id_from_webhook}'. Queuing task for board {webhook_board_id}.")
                process_general_webhook.delay(event, config_rule)
                queued_tasks_count += 1
                break

        if queued_tasks_count > 0:
            print(f"Successfully queued {queued_tasks_count} tasks for background processing.")
            return jsonify({"status": "success", "message": f"Webhook received and {queued_tasks_count} tasks queued for processing."}), 202
        else:
            print("INFO: No matching config rules found for this webhook. Ignoring payload.")
            return jsonify({"status": "ignored", "message": "No matching configuration rule found."}), 200
    print("ERROR: Invalid request method. Only POST is allowed for webhooks.")
    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    return "Monday.com General Purpose Logger App is running! (Webhooks are queued)", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
