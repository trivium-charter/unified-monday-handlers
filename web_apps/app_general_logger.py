import os
import json
from flask import Flask, request, jsonify

from celery_app import celery_app
from monday_tasks import process_general_webhook

# --- GLOBAL FLASK APP INSTANCE ---
# This line is moved here so Gunicorn can find the 'app' instance.
app = Flask(__name__)

# --- Global Configuration (Environment Variables) ---
# MONDAY_API_KEY: Your Monday.com API v2 key.
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY")
# MONDAY_LOGGING_CONFIGS: A JSON string representing a list of logging rules.
LOG_CONFIGS = json.loads(os.environ.get("MONDAY_LOGGING_CONFIGS", "[]"))

# --- Main Webhook Endpoint ---
@app.route('/monday-log-events', methods=['POST'])
def monday_log_events():
    """
    Central webhook handler for various Monday.com column changes.
    Receives webhooks and dispatches to Celery tasks based on configured rules.
    """
    if request.method == 'POST':
        data = request.get_json()
        print(f"Received webhook payload for general logger: {data}")

        # Monday.com sends a challenge for webhook verification.
        if 'challenge' in data:
            print("Responding to Monday.com webhook challenge.")
            return jsonify({'challenge': data['challenge']})

        event = data.get('event', {})
        webhook_board_id = event.get('boardId')
        webhook_type = event.get('type')
        trigger_column_id_from_webhook = event.get('columnId')

        # Basic configuration check
        if not MONDAY_API_KEY or not LOG_CONFIGS:
            print("Error: Server not configured properly (API Key or Logging configs missing). Cannot queue task.")
            # Return 500 here as it's a server-side configuration issue
            return jsonify({"status": "error", "message": "Server configuration incomplete."}), 500

        queued_tasks_count = 0

        # Iterate through configs to find matching rules and queue tasks
        for config_rule in LOG_CONFIGS:
            log_type = config_rule.get("log_type")
            configured_trigger_board_id = config_rule.get("trigger_board_id")
            configured_trigger_col_id = config_rule.get("trigger_column_id")

            # Validate and convert board ID from config rule
            if configured_trigger_board_id:
                try:
                    configured_trigger_board_id = int(configured_trigger_board_id)
                except (ValueError, TypeError):
                    print(f"WARNING: Invalid 'trigger_board_id' in config rule: {config_rule}. Skipping rule.")
                    continue
            else:
                print(f"WARNING: 'trigger_board_id' missing in config rule: {config_rule}. Skipping rule for board matching.")
                continue

            # Check if the board ID from the webhook matches the rule's configured board ID
            if webhook_board_id is None or int(webhook_board_id) != configured_trigger_board_id:
                continue # This rule doesn't apply to this board

            # --- ROUTING LOGIC TO QUEUE TASKS ---

            # Case 1: Handle item creation events for "CopyToItemName"
            if log_type == "CopyToItemName" and webhook_type in ["create_item", "create_pulse"]:
                print(f"INFO: Matching rule found for log_type: '{log_type}', trigger_type: '{webhook_type}'. Queuing task for board {webhook_board_id}.")
                process_general_webhook.delay(event, config_rule)
                queued_tasks_count += 1
                break # Assuming only one CopyToItemName rule applies per creation event

            # Case 2: Handle column update events (most common for this app)
            elif webhook_type == "update_column_value":
                # Ensure the trigger column ID matches if the rule specifies one
                if configured_trigger_col_id and trigger_column_id_from_webhook != configured_trigger_col_id:
                    continue # This rule doesn't apply to this specific column update

                # If we reach here, the board and column (if specified) match. Queue the task.
                print(f"INFO: Matching rule found for log_type: '{log_type}', trigger_column: '{trigger_column_id_from_webhook}'. Queuing task for board {webhook_board_id}.")
                process_general_webhook.delay(event, config_rule)
                queued_tasks_count += 1
                break # Assuming only one rule applies per column update

        if queued_tasks_count > 0:
            print(f"Successfully queued {queued_tasks_count} tasks for background processing.")
            return jsonify({"status": "success", "message": f"Webhook received and {queued_tasks_count} tasks queued for processing."}), 202 # 202 Accepted
        else:
            print("INFO: No matching config rules found for this webhook. Ignoring payload.")
            return jsonify({"status": "ignored", "message": "No matching configuration rule found."}), 200

    print("ERROR: Invalid request method. Only POST is allowed for webhooks.")
    return jsonify({"status": "error", "message": "Invalid request method."}), 405

@app.route('/')
def home():
    """Simple home route for health check."""
    return "Monday.com General Purpose Logger App is running! (Webhooks are queued)", 200

if __name__ == '__main__':
    # This block now only runs the Flask development server directly,
    # which is useful for local testing, but Gunicorn ignores it in production.
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
