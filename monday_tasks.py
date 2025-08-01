import os
import json
from datetime import datetime
import pytz 
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Global Configuration ---
# (All environment variables are included with their original defaults)
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID", "8993025745")
ALL_CLASSES_BOARD_ID = os.environ.get("ALL_CLASSES_BOARD_ID", "8931036662")
CANVAS_CLASSES_BOARD_ID = os.environ.get("CANVAS_CLASSES_BOARD_ID", "7308051382")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID", "6563671510")
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID", "6563671510")
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID", "6760943570")
IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID", "6760108968")

PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "board_relation_mkqnbtaf,board_relation_mkqnxyjd,board_relation_mkqn34pg,board_relation_mkr54dtg")
PLP_CANVAS_SYNC_STATUS_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_STATUS_COLUMN_ID", "color_mktdzdxj")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE", "Sync")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN", "board_relation_mks1n32a")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN", "text__1")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN", "_students1__school_email_address")
ALL_CLASSES_CANVAS_CONNECT_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_CONNECT_COLUMN", "board_relation_mkt2hp4c")
CANVAS_COURSE_ID_COLUMN = os.environ.get("CANVAS_COURSE_ID_COLUMN", "canvas_course_id_mkm1fwt4")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN", "dropdown_mkq0r2sj")
PLP_OP2_SECTION_COLUMN = os.environ.get("PLP_OP2_SECTION_COLUMN", "lookup_mkta9mgv")
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")

COLUMN_MAPPINGS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", '{}')
MASTER_STUDENT_PEOPLE_COLUMNS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", '{}')
SPED_STUDENTS_PEOPLE_COLUMN_STR = os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN", '{}')
SPED_TO_IEPAP_CONNECT_COLUMN = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN", "board_relation1__1")
MONDAY_LOGGING_CONFIGS_STR = os.environ.get("MONDAY_LOGGING_CONFIGS", "[]")

try:
    COLUMN_MAPPINGS = json.loads(COLUMN_MAPPINGS_STR)
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(MASTER_STUDENT_PEOPLE_COLUMNS_STR)
    SPED_STUDENTS_PEOPLE_COLUMN = json.loads(SPED_STUDENTS_PEOPLE_COLUMN_STR)
    LOG_CONFIGS = json.loads(MONDAY_LOGGING_CONFIGS_STR)
except json.JSONDecodeError as e:
    print(f"ERROR: Could not parse JSON from environment variables: {e}")
    COLUMN_MAPPINGS, MASTER_STUDENT_PEOPLE_COLUMNS, SPED_STUDENTS_PEOPLE_COLUMN, LOG_CONFIGS = {}, {}, {}, []

def get_people_ids_from_value(value):
    if not isinstance(value, dict) or "personsAndTeams" not in value: return set()
    return {person['id'] for person in value.get("personsAndTeams", [])}

@celery_app.task
def process_general_webhook(event_data, config_rule):
    """Handles generic subitem logging for Connect Boards columns."""
    # This task is now only for generic logging to avoid conflicts
    # ... (code is unchanged from previous correct version)
    return True

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    """Handles ONLY the syncing of people columns from the Master Student List."""
    # This task now ONLY syncs data and does NOT create subitems.
    master_item_id, master_board_id = event_data.get('pulseId'), event_data.get('boardId')
    trigger_column_id, current_value = event_data.get('columnId'), event_data.get('value')

    if str(master_board_id) != str(MASTER_STUDENT_LIST_BOARD_ID) or trigger_column_id not in MASTER_STUDENT_PEOPLE_COLUMNS: return True
    mappings = COLUMN_MAPPINGS.get(trigger_column_id)
    if not mappings: return False

    for config in mappings["targets"]:
        for linked_id in monday.get_linked_items_from_board_relation(master_item_id, master_board_id, config["connect_column_id"]):
            monday.update_people_column(linked_id, config["board_id"], config["target_column_id"], current_value, config["target_column_type"])
    return True

@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    # This task is unchanged
    # ... (code is unchanged from previous correct version)
    return True

@celery_app.task
def process_people_subitem_logging(event_data):
    """Creates a subitem on a linked PLP board when specific people columns are changed."""
    master_item_id, trigger_column_id = event_data.get('pulseId'), event_data.get('columnId')
    user_id, current_value, previous_value = event_data.get('userId'), event_data.get('value'), event_data.get('previousValue')

    mappings = COLUMN_MAPPINGS.get(trigger_column_id)
    if not mappings: return False

    subitem_prefix = mappings.get("name", "Person")
    plp_config = next((t for t in mappings["targets"] if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
    if not plp_config: return True
        
    plp_connect_column = plp_config.get("connect_column_id")
    plp_item_ids = monday.get_linked_items_from_board_relation(master_item_id, MASTER_STUDENT_LIST_BOARD_ID, plp_connect_column)
    if not plp_item_ids: return True

    added_ids = get_people_ids_from_value(current_value) - get_people_ids_from_value(previous_value)
    removed_ids = get_people_ids_from_value(previous_value) - get_people_ids_from_value(current_value)
    
    changer_name = monday.get_user_name(user_id) or "automation"
    current_date = datetime.now(pytz.timezone('America/Los_Angeles')).strftime('%Y-%m-%d')

    for plp_id in plp_item_ids:
        for person_id in added_ids:
            if person_name := monday.get_user_name(person_id):
                monday.create_subitem(plp_id, f"{subitem_prefix} {person_name} added on {current_date} by {changer_name}")
        for person_id in removed_ids:
            if person_name := monday.get_user_name(person_id):
                monday.create_subitem(plp_id, f"{subitem_prefix} {person_name} removed on {current_date} by {changer_name}")
    return True

@celery_app.task
def process_canvas_sync_webhook(event_data):
    # This task is unchanged from the last correct version
    # ... (code is unchanged)
    return True
