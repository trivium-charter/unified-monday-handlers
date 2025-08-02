import os
import json
from datetime import datetime
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Environment Variable Loading for Tasks ---
# PLP Course Sync Vars
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID")
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_CATEGORY_TO_CONNECT_COLUMN_MAP_STR = os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}")
try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(PLP_CATEGORY_TO_CONNECT_COLUMN_MAP_STR)
except json.JSONDecodeError:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}

# Master Student Person Sync Vars
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID")
COLUMN_MAPPINGS_STR = os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}")
try:
    COLUMN_MAPPINGS = json.loads(COLUMN_MAPPINGS_STR)
except json.JSONDecodeError:
    COLUMN_MAPPINGS = {}

# SpEd Students Person Sync Vars
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID")
SPED_TO_IEPAP_CONNECT_COLUMN_ID = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN_ID")
SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR = os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}")
try:
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(SPED_STUDENTS_PEOPLE_COLUMN_MAPPING_STR)
except json.JSONDecodeError:
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}

# Canvas Sync Vars
PLP_CANVAS_SYNC_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_COLUMN_ID")
PLP_STUDENT_NAME_COLUMN_ID = os.environ.get("PLP_STUDENT_NAME_COLUMN_ID")
PLP_SSID_COLUMN_ID = os.environ.get("PLP_SSID_COLUMN_ID")
PLP_STUDENT_EMAIL_COLUMN_ID = os.environ.get("PLP_STUDENT_EMAIL_COLUMN_ID")
ALL_CLASSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
ALL_CLASSES_CANVAS_ID_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_ID_COLUMN")
ALL_CLASSES_TERM_ID_COLUMN = os.environ.get("ALL_CLASSES_TERM_ID_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")
ALL_CLASSES_OP1_OP2_COLUMN = os.environ.get("ALL_CLASSES_OP1_OP2_COLUMN")
PLP_CONNECT_COLUMNS_FOR_CANVAS_STR = os.environ.get("PLP_CONNECT_COLUMNS_FOR_CANVAS", "[]")
try:
    PLP_CONNECT_COLUMNS_FOR_CANVAS = json.loads(PLP_CONNECT_COLUMNS_FOR_CANVAS_STR)
except json.JSONDecodeError:
    PLP_CONNECT_COLUMNS_FOR_CANVAS = []


# --- Helper ---
def format_name_last_first(name_str):
    if not name_str or not isinstance(name_str, str): return name_str
    parts = name_str.strip().split()
    return f"{parts[-1]}, {' '.join(parts[:-1])}" if len(parts) >= 2 else name_str

# --- Tasks ---

@celery_app.task
def process_general_webhook(event_data, config_rule):
    """Handles various logging and data manipulation tasks based on rules."""
    item_id = event_data.get('pulseId') or event_data.get('itemId')
    board_id = event_data.get('boardId')
    user_id = event_data.get('userId')
    log_type = config_rule.get("log_type")
    params = config_rule.get("params", {})

    if log_type == "ConnectBoardChange":
        main_item_id = item_id
        connected_board_id = params.get('linked_board_id')
        prefix = params.get('subitem_name_prefix', '')
        entry_type = params.get('subitem_entry_type')
        entry_col_id = params.get('entry_type_column_id')

        current_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
        previous_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
        added = current_ids - previous_ids
        removed = previous_ids - current_ids

        changer_name = monday.get_user_name(user_id) or "automation"
        date_str = datetime.now().strftime('%Y-%m-%d')
        cols = {entry_col_id: {"labels": [str(entry_type)]}} if entry_col_id and entry_type else {}

        for linked_id in added:
            name = monday.get_item_name(linked_id, connected_board_id)
            if name: monday.create_subitem(main_item_id, f"Added {prefix} '{name}' on {date_str} by {changer_name}", cols)
        for linked_id in removed:
            name = monday.get_item_name(linked_id, connected_board_id)
            if name: monday.create_subitem(main_item_id, f"Removed {prefix} '{name}' on {date_str} by {changer_name}", cols)
    return True


@celery_app.task
def process_plp_course_sync_webhook(event_data):
    """Syncs course selections from an HS Roster subitem to the main PLP board."""
    subitem_id = event_data.get('pulseId')
    subitem_board_id = event_data.get('boardId')
    parent_item_id = event_data.get('parentItemId')

    current_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
    previous_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    added_ids = current_ids - previous_ids
    removed_ids = previous_ids - previous_ids

    if not added_ids and not removed_ids: return True

    dropdown_val = monday.get_column_value(subitem_id, subitem_board_id, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID)
    category = dropdown_val.get('text') if dropdown_val else None
    target_plp_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(category)
    if not target_plp_col_id: return True

    plp_link_data = monday.get_column_value(parent_item_id, HS_ROSTER_BOARD_ID, HS_ROSTER_MAIN_ITEM_TO_PLP_CONNECT_COLUMN_ID)
    plp_item_id = list(monday.get_linked_ids_from_connect_column_value(plp_link_data.get('value')))[0] if plp_link_data else None
    if not plp_item_id: return True

    for course_id in added_ids:
        monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_col_id, course_id, action="add")
    for course_id in removed_ids:
        monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_col_id, course_id, action="remove")
    return True


@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    """Syncs a 'Person' column change from the Master Student List to other linked boards."""
    master_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    new_value = event_data.get('value')
    mappings = COLUMN_MAPPINGS.get(trigger_column_id, {}).get("targets", [])

    for target in mappings:
        linked_ids = monday.get_linked_items_from_board_relation(master_item_id, MASTER_STUDENT_LIST_BOARD_ID, target["connect_column_id"])
        for linked_id in linked_ids:
            monday.update_people_column(linked_id, target["board_id"], target["target_column_id"], new_value, target["target_column_type"])
    return True


@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    """Syncs a 'Person' column change from the SpEd Students board to the linked IEP/AP board."""
    source_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    new_value = event_data.get('value')
    mapping = SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.get(trigger_column_id)
    if not mapping: return True

    linked_ids = monday.get_linked_items_from_board_relation(source_item_id, SPED_STUDENTS_BOARD_ID, SPED_TO_IEPAP_CONNECT_COLUMN_ID)
    for linked_id in linked_ids:
        monday.update_people_column(linked_id, IEP_AP_BOARD_ID, mapping["target_column_id"], new_value, mapping["target_column_type"])
    return True


@celery_app.task
def process_canvas_sync_webhook(event_data):
    """Main task to handle Canvas enrollment and unenrollment based on a trigger."""
    plp_item_id = event_data.get('pulseId')
    status_val = event_data.get('value')
    status_label = status_val.get('label', {}).get('text', '') if status_val else ''

    # Fetch student details from the PLP board
    student_name = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_STUDENT_NAME_COLUMN_ID).get('text')
    ssid = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_SSID_COLUMN_ID).get('text')
    email = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_STUDENT_EMAIL_COLUMN_ID).get('text')
    
    if not all([student_name, ssid, email]):
        print(f"CRITICAL: Missing student details for PLP item {plp_item_id}.")
        return False
    
    student_details = {'name': student_name, 'ssid': ssid, 'email': email}
    
    # Get all linked class IDs from all relevant connect columns
    all_class_ids = set()
    for col_id in PLP_CONNECT_COLUMNS_FOR_CANVAS:
        class_link_data = monday.get_column_value(plp_item_id, PLP_BOARD_ID, col_id)
        if class_link_data and class_link_data.get('value'):
            all_class_ids.update(monday.get_linked_ids_from_connect_column_value(class_link_data['value']))

    if not all_class_ids:
        print(f"INFO: No classes linked for PLP item {plp_item_id}. No action taken.")
        return True

    # Process each linked class
    for class_item_id in all_class_ids:
        class_name = monday.get_item_name(class_item_id, ALL_CLASSES_BOARD_ID)
        canvas_course_id = monday.get_column_value(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_ID_COLUMN).get('text')
        term_id = monday.get_column_value(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_TERM_ID_COLUMN).get('text')
        
        # If Canvas Course ID does not exist, create it
        if not canvas_course_id:
            new_course = canvas.create_canvas_course(class_name, term_id)
            if new_course:
                canvas_course_id = new_course.id
                # Update the Monday board with the new Canvas Course ID
                monday.change_column_value_generic(ALL_CLASSES_BOARD_ID, class_item_id, ALL_CLASSES_CANVAS_ID_COLUMN, canvas_course_id)
            else:
                print(f"ERROR: Failed to create Canvas course for '{class_name}'. Skipping enrollment.")
                continue

        if status_label == "Sync to Canvas":
            # Determine sections
            sections = set()
            ag_grad_val = monday.get_column_value(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_AG_GRAD_COLUMN)
            ag_grad_text = ag_grad_val.get('text') if ag_grad_val else ""
            if "AG" in ag_grad_text: sections.add("A-G")
            if "Grad" in ag_grad_text: sections.add("Grad")
            
            op_val = monday.get_column_value(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_OP1_OP2_COLUMN)
            op_text = op_val.get('text') if op_val else ""
            if "Op1" in op_text: sections.add("Op1")
            if "Op2" in op_text: sections.add("Op2")

            # Enroll in each section
            for section_name in sections:
                section = canvas.create_section_if_not_exists(canvas_course_id, section_name)
                if section:
                    enrollment_result = canvas.enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)
                    # Log result as subitem
                    log_text = f"Enrolled in {class_name} ({section_name}): {enrollment_result}"
                    monday.create_subitem(plp_item_id, log_text)

        elif status_label == "Remove from Canvas":
            unenroll_result = canvas.unenroll_student_from_course(canvas_course_id, student_details)
            log_text = f"Unenrolled from {class_name}: {'Success' if unenroll_result else 'Failed'}"
            monday.create_subitem(plp_item_id, log_text)

    return True
