import os
import json
from datetime import datetime
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Load All Environment Variables for Tasks, Using Your Exact Names ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_CANVAS_SYNC_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_COLUMN_ID")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE", "Done")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
PLP_OP2_SECTION_COLUMN = os.environ.get("PLP_OP2_SECTION_COLUMN")
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN")

MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")

ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
ALL_CLASSES_CANVAS_CONNECT_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_CONNECT_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")

HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID")

IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID")
SPED_TO_IEPAP_CONNECT_COLUMN_ID = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN_ID")
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}"))
except json.JSONDecodeError:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}

# --- Helper Function ---
def format_name_last_first(name_str):
    if not name_str or not isinstance(name_str, str): return name_str
    parts = name_str.strip().split()
    return f"{parts[-1]}, {' '.join(parts[:-1])}" if len(parts) >= 2 else name_str

# --- Celery Tasks ---

@celery_app.task
def process_canvas_sync_webhook(event_data):
    plp_item_id = event_data.get('pulseId')

    status_column_data = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_CANVAS_SYNC_COLUMN_ID)
    status_label = status_column_data.get('text', '')
    
    if status_label != PLP_CANVAS_SYNC_STATUS_VALUE:
        print(f"INFO: Canvas sync not triggered. Status is '{status_label}', not '{PLP_CANVAS_SYNC_STATUS_VALUE}'.")
        return True

    print(f"INFO: Canvas sync initiated for PLP item {plp_item_id} because status is '{PLP_CANVAS_SYNC_STATUS_VALUE}'.")

    master_student_ids = monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_student_ids:
        print(f"CRITICAL: No Master Student linked to PLP item {plp_item_id}. Cannot get student details.")
        return False
    master_student_item_id = list(master_student_ids)[0]
    
    student_name = monday.get_item_name(master_student_item_id, MASTER_STUDENT_BOARD_ID)
    ssid_val = monday.get_column_value(master_student_item_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_SSID_COLUMN)
    email_val = monday.get_column_value(master_student_item_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_EMAIL_COLUMN)
    
    ssid = ssid_val.get('text', '') if ssid_val else ''
    email = email_val.get('text', '') if email_val else ''

    if not all([student_name, ssid, email]):
        print(f"CRITICAL: Missing details for Master Student {master_student_item_id} (Name, SSID, or Email).")
        return False
    
    student_details = {'name': student_name, 'ssid': ssid, 'email': email}
    
    course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip() and c.strip() != PLP_CANVAS_SYNC_COLUMN_ID]
    all_class_ids = set()
    for col_id in course_column_ids:
        class_link_data = monday.get_column_value(plp_item_id, PLP_BOARD_ID, col_id)
        if class_link_data and class_link_data.get('value'):
            all_class_ids.update(monday.get_linked_ids_from_connect_column_value(class_link_data['value']))
            
    if not all_class_ids:
        print(f"INFO: Status is '{PLP_CANVAS_SYNC_STATUS_VALUE}' but no courses are linked for PLP item {plp_item_id}.")
        return True

    op2_val = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_OP2_SECTION_COLUMN)
    m_series_val = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_M_SERIES_LABELS_COLUMN)
    op2_text = op2_val.get('text', '') if op2_val else ''
    m_series_text = m_series_val.get('text', '') if m_series_val else ''

    for class_item_id in all_class_ids:
        class_name = monday.get_item_name(class_item_id, ALL_COURSES_BOARD_ID)
        canvas_course_id_val = monday.get_column_value(class_item_id, ALL_COURSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)
        canvas_course_id = canvas_course_id_val.get('text', '') if canvas_course_id_val else ''

        if not canvas_course_id:
            new_course = canvas.create_canvas_course(class_name, CANVAS_TERM_ID)
            if new_course:
                canvas_course_id = new_course.id
                monday.change_column_value_generic(ALL_COURSES_BOARD_ID, class_item_id, ALL_CLASSES_CANVAS_CONNECT_COLUMN, str(canvas_course_id))
            else:
                print(f"ERROR: Failed to create Canvas course for '{class_name}'.")
                continue
        
        sections = set()
        ag_grad_val = monday.get_column_value(class_item_id, ALL_COURSES_BOARD_ID, ALL_CLASSES_AG_GRAD_COLUMN)
        ag_grad_text = ag_grad_val.get('text', '') if ag_grad_val else ''
        if "A-G" in ag_grad_text: sections.add("A-G")
        if "Grad" in op2_text: sections.add("Grad")
        if "M-Series" in m_series_text: sections.add("M-Series")

        if not sections:
            print(f"WARNING: No sections determined for class '{class_name}'.")
            continue

        for section_name in sections:
            section = canvas.create_section_if_not_exists(canvas_course_id, section_name)
            if section:
                canvas.enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)
    return True

@celery_app.task
def process_plp_course_sync_webhook(event_data):
    subitem_id = event_data.get('pulseId')
    subitem_board_id = event_data.get('boardId')
    parent_item_id = event_data.get('parentItemId')
    current_value = event_data.get('value')
    previous_value = event_data.get('previousValue')

    current_all_courses_ids = monday.get_linked_ids_from_connect_column_value(current_value)
    previous_all_courses_ids = monday.get_linked_ids_from_connect_column_value(previous_value)
    added_all_courses_ids = current_all_courses_ids - previous_all_courses_ids
    removed_all_courses_ids = previous_all_courses_ids - current_all_courses_ids

    if not added_all_courses_ids and not removed_all_courses_ids:
        return True

    subitem_dropdown_data = monday.get_column_value(subitem_id, subitem_board_id, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID)
    subitem_dropdown_label = subitem_dropdown_data.get('text') if subitem_dropdown_data else None
    if not subitem_dropdown_label:
        return True

    target_plp_connect_column_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(subitem_dropdown_label)
    if not target_plp_connect_column_id:
        return True

    plp_link_data = monday.get_column_value(parent_item_id, HS_ROSTER_BOARD_ID, HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
    plp_linked_ids = monday.get_linked_ids_from_connect_column_value(plp_link_data.get('value')) if plp_link_data else set()
    if not plp_linked_ids:
        return True
    
    plp_item_id = list(plp_linked_ids)[0]
    operation_successful = True
    for course_id in added_all_courses_ids:
        if not monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id, course_id, "add"):
            operation_successful = False
    for course_id in removed_all_courses_ids:
        if not monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id, course_id, "remove"):
            operation_successful = False
    return operation_successful

@celery_app.task
def process_general_webhook(event_data, config_rule):
    webhook_board_id = event_data.get('boardId')
    item_id_from_webhook = event_data.get('pulseId')
    trigger_column_id_from_webhook = event_data.get('columnId')
    event_user_id = event_data.get('userId')
    current_column_value = event_data.get('value')
    previous_column_value = event_data.get('previousValue')
    webhook_type = event_data.get('type')

    log_type = config_rule.get("log_type")
    params = config_rule.get("params", {})
    configured_trigger_board_id = config_rule.get("trigger_board_id")
    configured_trigger_col_id = config_rule.get("trigger_column_id")

    if configured_trigger_board_id and str(webhook_board_id) != str(configured_trigger_board_id):
        return False

    success = False
    if log_type == "ConnectBoardChange" and webhook_type == "update_column_value" and trigger_column_id_from_webhook == configured_trigger_col_id:
        main_item_id = item_id_from_webhook
        connected_board_id = params.get('linked_board_id')
        subitem_name_prefix = params.get('subitem_name_prefix', '')
        subitem_entry_type = params.get('subitem_entry_type')
        entry_type_column_id = params.get('entry_type_column_id')

        current_linked_ids = monday.get_linked_ids_from_connect_column_value(current_column_value)
        previous_linked_ids = monday.get_linked_ids_from_connect_column_value(previous_column_value)
        added_links = current_linked_ids - previous_linked_ids
        removed_links = previous_linked_ids - current_linked_ids

        if not added_links and not removed_links:
            return True

        overall_op_successful = True
        current_date = datetime.now().strftime('%Y-%m-%d')
        changer_user_name = monday.get_user_name(event_user_id) or "automation"
        user_log_text = f" by {changer_user_name}"
        subject_prefix_text = f"{subitem_name_prefix} " if subitem_name_prefix else ""
        additional_subitem_columns = {entry_type_column_id: {"labels": [str(subitem_entry_type)]}} if entry_type_column_id else {}

        for item_id in added_links:
            linked_item_name = monday.get_item_name(item_id, connected_board_id)
            if linked_item_name:
                subitem_name = f"Added {subject_prefix_text}'{linked_item_name}' on {current_date}{user_log_text}"
                if not monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns):
                    overall_op_successful = False
            else:
                overall_op_successful = False
        
        for item_id in removed_links:
            linked_item_name = monday.get_item_name(item_id, connected_board_id)
            if linked_item_name:
                subitem_name = f"Removed {subject_prefix_text}'{linked_item_name}' on {current_date}{user_log_text}"
                if not monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns):
                    overall_op_successful = False
            else:
                overall_op_successful = False
        success = overall_op_successful
    
    return success

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    master_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    current_column_value_raw = event_data.get('value')
    operation_successful = True

    mappings_for_this_column = MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.get(trigger_column_id)
    if not mappings_for_this_column:
        return False

    for target_config in mappings_for_this_column["targets"]:
        target_board_id = target_config["board_id"]
        master_connect_column_id = target_config["connect_column_id"]
        target_people_column_id = target_config["target_column_id"]
        target_column_type = target_config["target_column_type"]

        linked_item_ids_on_target_board = monday.get_linked_items_from_board_relation(
            item_id=master_item_id,
            board_id=MASTER_STUDENT_BOARD_ID,
            connect_column_id=master_connect_column_id
        )

        for linked_target_item_id in linked_item_ids_on_target_board:
            success = monday.update_people_column(
                item_id=linked_target_item_id,
                board_id=target_board_id,
                people_column_id=target_people_column_id,
                new_people_value=current_column_value_raw,
                target_column_type=target_column_type
            )
            if not success:
                operation_successful = False
    return operation_successful

@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    source_item_id = event_data.get('pulseId')
    source_board_id = event_data.get('boardId')
    trigger_column_id = event_data.get('columnId')
    current_column_value_raw = event_data.get('value')
    operation_successful = True

    column_sync_config = SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.get(trigger_column_id)
    if not column_sync_config:
        return False

    target_people_column_id = column_sync_config["target_column_id"]
    target_column_type = column_sync_config["target_column_type"]

    linked_iep_ap_item_ids = monday.get_linked_items_from_board_relation(
        item_id=source_item_id,
        board_id=source_board_id,
        connect_column_id=SPED_TO_IEPAP_CONNECT_COLUMN_ID
    )

    for linked_iep_ap_item_id in linked_iep_ap_item_ids:
        success = monday.update_people_column(
            item_id=linked_iep_ap_item_id,
            board_id=IEP_AP_BOARD_ID,
            people_column_id=target_people_column_id,
            new_people_value=current_column_value_raw,
            target_column_type=target_column_type
        )
        if not success:
            operation_successful = False
    return operation_successful
