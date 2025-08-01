import os
import json
from datetime import datetime
import pytz 
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Global Configuration (Environment Variables for Tasks) ---

# --- Environment variables for Canvas Sync Process ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID", "8993025745")
ALL_CLASSES_BOARD_ID = os.environ.get("ALL_CLASSES_BOARD_ID", "8931036662")
CANVAS_CLASSES_BOARD_ID = os.environ.get("CANVAS_CLASSES_BOARD_ID", "7308051382")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID", "6563671510")
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
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN", "labels_mktXXXX") 
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")

# --- Environment variables for General Webhooks ---
MONDAY_LOGGING_CONFIGS_STR = os.environ.get("MONDAY_LOGGING_CONFIGS", "[]")
try:
    MONDAY_LOGGING_CONFIGS = json.loads(MONDAY_LOGGING_CONFIGS_STR)
except json.JSONDecodeError:
    MONDAY_LOGGING_CONFIGS = []

# --- Environment variables for Master Student Person Sync ---
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID", "")
MASTER_STUDENT_PEOPLE_COLUMNS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", "{}")
try:
    MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(MASTER_STUDENT_PEOPLE_COLUMNS_STR)
except json.JSONDecodeError:
    MASTER_STUDENT_PEOPLE_COLUMNS = {}
COLUMN_MAPPINGS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}")
try:
    COLUMN_MAPPINGS = json.loads(COLUMN_MAPPINGS_STR)
except json.JSONDecodeError:
    COLUMN_MAPPINGS = {}
PLP_BOARD_ID_FOR_LOGGING = os.environ.get("PLP_BOARD_ID_FOR_LOGGING", "8993025745")


# --- Celery Tasks ---

@celery_app.task
def process_general_webhook(event_data, config_rule):
    """Handles generic subitem logging and other simple automations."""
    item_id_from_webhook = event_data.get('pulseId')
    trigger_column_id_from_webhook = event_data.get('columnId')
    event_user_id = event_data.get('userId')
    current_column_value = event_data.get('value')
    previous_column_value = event_data.get('previousValue')
    webhook_type = event_data.get('type')

    log_type = config_rule.get("log_type")
    params = config_rule.get("params", {})
    configured_trigger_col_id = config_rule.get("trigger_column_id")

    if webhook_type == "update_column_value" and trigger_column_id_from_webhook == configured_trigger_col_id:
        if log_type == "ConnectBoardChange":
            main_item_id = item_id_from_webhook
            connected_board_id = params.get('linked_board_id')
            subitem_name_prefix = params.get('subitem_name_prefix')
            subitem_entry_type = params.get('subitem_entry_type')
            entry_type_column_id_from_params = params.get('entry_type_column_id')

            current_linked_ids = monday.get_linked_ids_from_connect_column_value(current_column_value)
            previous_linked_ids = monday.get_linked_ids_from_connect_column_value(previous_column_value)
            added_links = current_linked_ids - previous_linked_ids
            removed_links = previous_linked_ids - current_linked_ids

            pacific_tz = pytz.timezone('America/Los_Angeles')
            current_date = datetime.now(pacific_tz).strftime('%Y-%m-%d')
            changer_user_name = monday.get_user_name(event_user_id)
            user_log_text = f" by {changer_user_name}" if changer_user_name else (" by automation" if event_user_id == -4 else "")
            
            additional_subitem_columns = {}
            if entry_type_column_id_from_params:
                additional_subitem_columns[entry_type_column_id_from_params] = {"labels": [str(subitem_entry_type)]}

            for item_id_linked in added_links:
                linked_item_name = monday.get_item_name(item_id_linked, connected_board_id)
                if linked_item_name:
                    subitem_name = f"Added {subitem_name_prefix} '{linked_item_name}' on {current_date}{user_log_text}"
                    monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns)

            for item_id_linked in removed_links:
                linked_item_name = monday.get_item_name(item_id_linked, connected_board_id)
                if linked_item_name:
                    subitem_name = f"Removed {subitem_name_prefix} '{linked_item_name}' on {current_date}{user_log_text}"
                    monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns)
    return True


@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    """Handles syncing people columns from the Master Student List."""
    master_item_id = event_data.get('pulseId')
    master_board_id = event_data.get('boardId')
    trigger_column_id = event_data.get('columnId')
    current_column_value_raw = event_data.get('value')

    if str(master_board_id) != str(MASTER_STUDENT_LIST_BOARD_ID) or trigger_column_id not in MASTER_STUDENT_PEOPLE_COLUMNS:
        return True

    mappings_for_this_column = COLUMN_MAPPINGS.get(trigger_column_id)
    if not mappings_for_this_column:
        return False

    for target_config in mappings_for_this_column["targets"]:
        target_board_id = target_config["board_id"]
        master_connect_column_id = target_config["connect_column_id"]
        target_people_column_id = target_config["target_column_id"]
        target_column_type = target_config["target_column_type"]

        linked_item_ids = monday.get_linked_items_from_board_relation(master_item_id, master_board_id, master_connect_column_id)
        for linked_id in linked_item_ids:
            monday.update_people_column(linked_id, target_board_id, target_people_column_id, current_column_value_raw, target_column_type)

    return True


@celery_app.task
def process_canvas_sync_webhook(event_data):
    """
    Handles the full sync of a student's enrollments and sections in Canvas,
    creating the student if they do not exist.
    """
    print("INFO: CANVAS_SYNC - Task started.")
    plp_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    
    # --- 1. Get All Student Information from Master Student Board ---
    student_name = monday.get_item_name(plp_item_id, PLP_BOARD_ID)
    
    master_student_ids = monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_student_ids:
        print(f"ERROR: CANVAS_SYNC - PLP item {plp_item_id} is not linked to a Master Student. Aborting.")
        return False
    
    master_student_id = list(master_student_ids)[0]
    
    student_email_val = monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_EMAIL_COLUMN)
    student_email = student_email_val.get('text') if student_email_val else None
    
    student_ssid_val = monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_SSID_COLUMN)
    student_ssid = student_ssid_val.get('text') if student_ssid_val else None

    if not all([student_email, student_name]):
        print(f"ERROR: CANVAS_SYNC - Student email or name not found for PLP item {plp_item_id}. Aborting.")
        return False
        
    if not student_ssid or not student_ssid.strip():
        student_ssid = f"monday_{plp_item_id}"
        print(f"WARNING: CANVAS_SYNC - SSID is blank. Using fallback SIS ID: {student_ssid}")

    student_details = {"name": student_name, "email": student_email, "ssid": student_ssid}
    print(f"INFO: CANVAS_SYNC - Syncing for student: {student_details}")

    # --- 2. Determine which classes to sync ---
    plp_connect_cols = [col.strip() for col in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',')]
    linked_class_ids, unlinked_class_ids = set(), set()

    if trigger_column_id in plp_connect_cols:
        current_val = event_data.get('value')
        previous_val = event_data.get('previousValue')
        linked_class_ids = monday.get_linked_ids_from_connect_column_value(current_val)
        unlinked_class_ids = monday.get_linked_ids_from_connect_column_value(previous_val) - linked_class_ids
    elif trigger_column_id == PLP_CANVAS_SYNC_STATUS_COLUMN_ID:
        for col_id in plp_connect_cols:
            linked_class_ids.update(monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, col_id))
    
    print(f"INFO: CANVAS_SYNC - Classes to sync/enroll: {linked_class_ids}")
    print(f"INFO: CANVAS_SYNC - Classes to unenroll: {unlinked_class_ids}")

    # --- 3. Process Classes to Sync/Enroll ---
    for class_item_id in linked_class_ids:
        print(f"--- Processing Class ID: {class_item_id} ---")
        canvas_class_link_ids = monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)
        if not canvas_class_link_ids:
            continue
        
        canvas_class_item_id = list(canvas_class_link_ids)[0]
        canvas_course_id_val = monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN)
        canvas_course_id = canvas_course_id_val.get('text') if canvas_course_id_val else None

        if not canvas_course_id:
            class_item_name = monday.get_item_name(class_item_id, ALL_CLASSES_BOARD_ID) or "Untitled"
            if not CANVAS_TERM_ID: continue
            new_course = canvas.create_canvas_course(class_item_name, CANVAS_TERM_ID)
            if new_course:
                canvas_course_id = new_course.id
                monday.change_column_value_generic(CANVAS_CLASSES_BOARD_ID, canvas_class_item_id, CANVAS_COURSE_ID_COLUMN, str(canvas_course_id))
            else:
                continue
        
        sections_to_enroll = set()
        ag_grad_val = monday.get_column_value(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_AG_GRAD_COLUMN)
        ag_grad_text = ag_grad_val.get('text', '')
        if "AG" in ag_grad_text: sections_to_enroll.add("A-G")
        if "Grad" in ag_grad_text: sections_to_enroll.add("Grad")

        op2_val = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_OP2_SECTION_COLUMN)
        if op2_val and op2_val.get('text') == "Op2 Section": sections_to_enroll.add("Op2")
        
        labels_val = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_M_SERIES_LABELS_COLUMN)
        if labels_val and labels_val.get('text'):
            for m_label in ["M2", "M3", "M4", "M5"]:
                if m_label in labels_val.get('text'):
                    sections_to_enroll.add(m_label)

        for section_name in sections_to_enroll:
            section = canvas.create_section_if_not_exists(canvas_course_id, section_name)
            if section:
                canvas.enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)

    # --- 4. Process Classes to Unenroll ---
    for class_item_id in unlinked_class_ids:
        canvas_class_link_ids = monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)
        if not canvas_class_link_ids: continue
        
        canvas_class_item_id = list(canvas_class_link_ids)[0]
        canvas_course_id_val = monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN)
        canvas_course_id = canvas_course_id_val.get('text') if canvas_course_id_val else None

        if canvas_course_id:
            canvas.unenroll_student_from_course(canvas_course_id, student_email)

    print("INFO: CANVAS_SYNC - Task finished.")
    return True
