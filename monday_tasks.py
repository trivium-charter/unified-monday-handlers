import os
import json
from datetime import datetime
import pytz 
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Global Configuration (Restored Defaults from Original Files) ---
MONDAY_MAIN_BOARD_ID = os.environ.get("MONDAY_MAIN_BOARD_ID", "8993025745")
LINKED_BOARD_ID = os.environ.get("LINKED_BOARD_ID", "8931036662")
MONDAY_CONNECT_BOARD_COLUMN_ID = os.environ.get("MONDAY_CONNECT_BOARD_COLUMN_ID", "board_relation_mkqnbtaf")
MONDAY_ENTRY_TYPE_COLUMN_ID = os.environ.get("MONDAY_ENTRY_TYPE_COLUMN_ID", "entry_type__1")

MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID", "6563671510")
PLP_BOARD_ID_FOR_LOGGING = os.environ.get("PLP_BOARD_ID_FOR_LOGGING", "8993025745")
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID", "6760943570")
IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID", "6760108968")

MASTER_STUDENT_PEOPLE_COLUMNS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", '{"multiple_person_mks1ccav": "Parent/Guardian 1", "people": "Case Manager"}')
SPED_STUDENTS_PEOPLE_COLUMN_STR = os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN", '{"multiple_person_mks841jb": "Case Manager"}')
COLUMN_MAPPINGS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", '{}')
SPED_TO_IEPAP_CONNECT_COLUMN = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN", "board_relation1__1")

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
    """Helper to extract user IDs from a People column value."""
    if not isinstance(value, dict) or "personsAndTeams" not in value:
        return set()
    return {person['id'] for person in value.get("personsAndTeams", [])}

# --- TASKS ---

@celery_app.task
def process_general_webhook(event_data, config_rule):
    """Handles generic subitem logging for non-people columns (e.g., Connect Boards)."""
    item_id, user_id = event_data.get('pulseId'), event_data.get('userId')
    current_value, previous_value = event_data.get('value'), event_data.get('previousValue')
    log_type, params = config_rule.get("log_type"), config_rule.get("params", {})
    
    pacific_tz = pytz.timezone('America/Los_Angeles')
    current_date = datetime.now(pacific_tz).strftime('%Y-%m-%d')
    changer_name = monday.get_user_name(user_id) or ("automation" if user_id == -4 else "")
    user_log_text = f" by {changer_name}" if changer_name else ""

    if log_type == "ConnectBoardChange":
        board_id = params.get('linked_board_id')
        prefix_add, prefix_remove = params.get('subitem_name_prefix_add', 'Added'), params.get('subitem_name_prefix_remove', 'Removed')
        current_ids, previous_ids = monday.get_linked_ids_from_connect_column_value(current_value), monday.get_linked_ids_from_connect_column_value(previous_value)
        
        for item_id_linked in (current_ids - previous_ids):
            linked_item_name = monday.get_item_name(item_id_linked, board_id)
            if linked_item_name: monday.create_subitem(item_id, f"{prefix_add} '{linked_item_name}' on {current_date}{user_log_text}")
        for item_id_linked in (previous_ids - current_ids):
            linked_item_name = monday.get_item_name(item_id_linked, board_id)
            if linked_item_name: monday.create_subitem(item_id, f"{prefix_remove} '{linked_item_name}' on {current_date}{user_log_text}")
    return True

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    """
    Handles syncing people columns from the Master Student List AND
    creates a subitem log on the linked PLP board.
    """
    master_item_id, master_board_id = event_data.get('pulseId'), event_data.get('boardId')
    trigger_column_id, user_id = event_data.get('columnId'), event_data.get('userId')
    current_value, previous_value = event_data.get('value'), event_data.get('previousValue')

    if str(master_board_id) != str(MASTER_STUDENT_LIST_BOARD_ID) or trigger_column_id not in MASTER_STUDENT_PEOPLE_COLUMNS:
        return True

    mappings = COLUMN_MAPPINGS.get(trigger_column_id)
    if not mappings:
        return False

    # --- Action 1: Perform the People Column Sync ---
    for target_config in mappings["targets"]:
        linked_item_ids = monday.get_linked_items_from_board_relation(master_item_id, master_board_id, target_config["connect_column_id"])
        for linked_id in linked_item_ids:
            monday.update_people_column(linked_id, target_config["board_id"], target_config["target_column_id"], current_value, target_config["target_column_type"])

    # --- Action 2: Create the Subitem Log on the PLP Board ---
    subitem_prefix = mappings.get("name", "Person")
    
    plp_target_config = next((t for t in mappings["targets"] if str(t.get("board_id")) == str(PLP_BOARD_ID)), None)
    if not plp_target_config:
        print(f"INFO: No PLP board target found in mapping for column {trigger_column_id}. Skipping subitem creation.")
        return True
        
    plp_connect_column = plp_target_config.get("connect_column_id")
    plp_item_ids = monday.get_linked_items_from_board_relation(master_item_id, master_board_id, plp_connect_column)
    
    if not plp_item_ids:
        print(f"INFO: Master student {master_item_id} is not linked to any PLP items. No subitem to create.")
        return True

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
def process_sped_students_person_sync_webhook(event_data):
    """Syncs people columns from the SPED Students board."""
    sped_item_id, sped_board_id = event_data.get('pulseId'), event_data.get('boardId')
    trigger_column_id, current_value = event_data.get('columnId'), event_data.get('value')

    if str(sped_board_id) != str(SPED_STUDENTS_BOARD_ID) or trigger_column_id not in SPED_STUDENTS_PEOPLE_COLUMN:
        return
    for iep_ap_id in monday.get_linked_items_from_board_relation(sped_item_id, sped_board_id, SPED_TO_IEPAP_CONNECT_COLUMN):
        monday.update_people_column(iep_ap_id, IEP_AP_BOARD_ID, trigger_column_id, current_value, "multiple-person")
    return True

@celery_app.task
def process_canvas_sync_webhook(event_data):
    """Handles syncing a student's enrollments in Canvas."""
    print("INFO: CANVAS_SYNC - Task started.")
    plp_item_id, trigger_column_id = event_data.get('pulseId'), event_data.get('columnId')
    
    master_student_ids = monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_student_ids: return print(f"ERROR: PLP item {plp_item_id} not linked to a Master Student.")
    master_student_id = list(master_student_ids)[0]
    
    student_name = monday.get_item_name(plp_item_id, PLP_BOARD_ID)
    student_email = (monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_EMAIL_COLUMN) or {}).get('text')
    student_ssid = (monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_SSID_COLUMN) or {}).get('text') or f"monday_{plp_item_id}"

    if not all([student_email, student_name]): return print(f"ERROR: Student email or name not found for PLP item {plp_item_id}.")
    student_details = {"name": student_name, "email": student_email, "ssid": student_ssid}
    print(f"INFO: Syncing for student: {student_details}")

    plp_connect_cols = [col.strip() for col in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',')]
    linked_class_ids, unlinked_class_ids = set(), set()

    if trigger_column_id in plp_connect_cols:
        linked_class_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
        unlinked_class_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue')) - linked_class_ids
    elif trigger_column_id == PLP_CANVAS_SYNC_STATUS_COLUMN_ID:
        for col_id in plp_connect_cols: linked_class_ids.update(monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, col_id))
    
    print(f"INFO: Classes to enroll: {linked_class_ids}", f"Classes to unenroll: {unlinked_class_ids}")

    for class_item_id in linked_class_ids:
        print(f"--- Processing Enrollment for Class ID: {class_item_id} ---")
        if not (canvas_class_link_ids := monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)): continue
        canvas_class_item_id = list(canvas_class_link_ids)[0]
        
        canvas_course_id = (monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN) or {}).get('text')

        if not canvas_course_id or not canvas_course_id.strip():
            print(f"INFO: Canvas Course ID is blank. Attempting to create course.")
            class_item_name = monday.get_item_name(class_item_id, ALL_CLASSES_BOARD_ID) or "Untitled"
            if not CANVAS_TERM_ID: print("ERROR: CANVAS_TERM_ID not set."); continue
            
            if new_course := canvas.create_canvas_course(class_item_name, CANVAS_TERM_ID):
                canvas_course_id = new_course.id
                print(f"INFO: New course created with ID: {canvas_course_id}. Updating Monday.com.")
                monday.change_column_value_generic(CANVAS_CLASSES_BOARD_ID, canvas_class_item_id, CANVAS_COURSE_ID_COLUMN, str(canvas_course_id))
            else:
                print(f"ERROR: Course creation for '{class_item_name}' failed or was aborted. Skipping.")
                continue
        else:
            print(f"INFO: Found existing Canvas Course ID: {canvas_course_id}.")
        
        sections = set()
        ag_grad_text = (monday.get_column_value(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_AG_GRAD_COLUMN) or {}).get('text', '')
        if "AG" in ag_grad_text: sections.add("A-G")
        if "Grad" in ag_grad_text: sections.add("Grad")
        if (monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_OP2_SECTION_COLUMN) or {}).get('text') == "Op2 Section": sections.add("Op2")
        
        for section_name in sections:
            if section := canvas.create_section_if_not_exists(canvas_course_id, section_name):
                canvas.enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)

    for class_item_id in unlinked_class_ids:
        print(f"--- Processing Unenrollment for Class ID: {class_item_id} ---")
        if not (canvas_class_link_ids := monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)): continue
        canvas_class_item_id = list(canvas_class_link_ids)[0]
        if canvas_course_id := (monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN) or {}).get('text'):
            if canvas_course_id.strip(): canvas.unenroll_student_from_course(canvas_course_id, student_email)

    print("INFO: CANVAS_SYNC - Task finished.")
    return True
