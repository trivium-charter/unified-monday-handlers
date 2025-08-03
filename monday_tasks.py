import os
import json
from datetime import datetime
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Environment Variable Loading ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
PLP_CANVAS_SYNC_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_COLUMN_ID")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE", "Done")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
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
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
SPED_TO_IEPAP_CONNECT_COLUMN_ID = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN_ID")
CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
CANVAS_COURSE_ID_COLUMN = os.environ.get("CANVAS_COURSE_ID_COLUMN")
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
CANVAS_COURSES_TEACHER_COLUMN_ID = os.environ.get("CANVAS_COURSES_TEACHER_COLUMN_ID")
PLP_TO_HS_ROSTER_CONNECT_COLUMN = os.environ.get("PLP_TO_HS_ROSTER_CONNECT_COLUMN")

try:
    CANVAS_BOARD_COURSE_NAME_COLUMN_ID = os.environ.get("CANVAS_BOARD_COURSE_NAME_COLUMN_ID")
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}"))
    LOG_CONFIGS = json.loads(os.environ.get("MONDAY_LOGGING_CONFIGS", "[]"))
except (json.JSONDecodeError, TypeError):
    CANVAS_BOARD_COURSE_NAME_COLUMN_ID = None
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}
    LOG_CONFIGS = []

def get_student_details_from_plp(plp_item_id):
    # This function is unchanged
    master_student_ids = monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_student_ids: return None
    master_student_item_id = list(master_student_ids)[0]
    student_name = monday.get_item_name(master_student_item_id, MASTER_STUDENT_BOARD_ID)
    ssid_val = monday.get_column_value(master_student_item_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_SSID_COLUMN)
    email_val = monday.get_column_value(master_student_item_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_EMAIL_COLUMN)
    ssid = ssid_val.get('text', '') if ssid_val else ''
    email = email_val.get('text', '') if email_val else ''
    if not all([student_name, ssid, email]): return None
    return {'name': student_name, 'ssid': ssid, 'email': email}

def manage_class_enrollment(action, plp_item_id, all_courses_item_id, student_details, user_id):
    # This function is unchanged
    current_date = datetime.now().strftime('%Y-%m-%d')
    changer_user_name = monday.get_user_name(user_id) or "automation"
    user_log_text = f" on {current_date} by {changer_user_name}"

    linked_canvas_item_ids = monday.get_linked_items_from_board_relation(all_courses_item_id, ALL_COURSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)
    canvas_source_item_id = list(linked_canvas_item_ids)[0] if linked_canvas_item_ids else None
    if not canvas_source_item_id:
        monday.create_update(plp_item_id, f"Could not enroll in course (item {all_courses_item_id}): It is not properly linked to the Canvas source board.")
        return
        
    course_name_val = monday.get_column_value(canvas_source_item_id, CANVAS_BOARD_ID, CANVAS_BOARD_COURSE_NAME_COLUMN_ID)
    course_name = course_name_val.get('text') if course_name_val and course_name_val.get('text') else monday.get_item_name(all_courses_item_id, ALL_COURSES_BOARD_ID) or ""
    
    if not course_name:
        monday.create_update(plp_item_id, f"Could not process course (item {all_courses_item_id}): Its title is missing.")
        return
        
    canvas_id_val = monday.get_column_value(canvas_source_item_id, CANVAS_BOARD_ID, CANVAS_COURSE_ID_COLUMN)
    canvas_course_id = canvas_id_val.get('text') if canvas_id_val and canvas_id_val.get('text') else None
    
    if action == "enroll":
        if not canvas_course_id:
            new_course = canvas.create_canvas_course(course_name, CANVAS_TERM_ID)
            if not new_course or not hasattr(new_course, 'id'):
                monday.create_update(plp_item_id, f"Failed to enroll in '{course_name}': The Canvas API did not create the course.")
                return 
            canvas_course_id = str(new_course.id)
            monday.change_column_value_generic(board_id=CANVAS_BOARD_ID, item_id=canvas_source_item_id, column_id=CANVAS_COURSE_ID_COLUMN, value=canvas_course_id)
        
        m_series_val = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_M_SERIES_LABELS_COLUMN)
        m_series_text = (m_series_val.get('text') or '') if m_series_val else ''
        ag_grad_val = monday.get_column_value(all_courses_item_id, ALL_COURSES_BOARD_ID, ALL_CLASSES_AG_GRAD_COLUMN)
        ag_grad_text = (ag_grad_val.get('text') or '') if ag_grad_val else ''
        sections = set()
        if "AG" in ag_grad_text: sections.add("A-G")
        if "Grad" in ag_grad_text: sections.add("Grad")
        if "M-Series" in m_series_text: sections.add("M-Series")
        if not sections: sections.add("All")
        
        for section_name in sections:
            section = canvas.create_section_if_not_exists(canvas_course_id, section_name)
            if section:
                result = canvas.enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)
                log_text = f"Enrolled in {course_name} ({section_name}): {result}{user_log_text}"
                monday.create_subitem(plp_item_id, log_text)
                
    elif action == "unenroll":
        if canvas_course_id:
            result = canvas.unenroll_student_from_course(canvas_course_id, student_details)
            log_text = f"Unenrolled from {course_name}: {'Success' if result else 'Failed'}{user_log_text}"
            monday.create_subitem(plp_item_id, log_text)

@celery_app.task
def process_canvas_full_sync_from_status(event_data):
    # This function is unchanged
    plp_item_id = event_data.get('pulseId')
    user_id = event_data.get('userId')
    status_label = event_data.get('value', {}).get('label', {}).get('text', '')
    if status_label != PLP_CANVAS_SYNC_STATUS_VALUE: return True
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return False
    course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip() and c.strip() != PLP_CANVAS_SYNC_COLUMN_ID]
    all_class_ids = set()
    for col_id in course_column_ids:
        class_link_data = monday.get_column_value(plp_item_id, PLP_BOARD_ID, col_id)
        if class_link_data and class_link_data.get('value'):
            all_class_ids.update(monday.get_linked_ids_from_connect_column_value(class_link_data.get('value')))
    for class_item_id in all_class_ids:
        manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, user_id)
    return True

@celery_app.task

def process_canvas_delta_sync_from_course_change(event_data, user_id):
    # === Part 1: Canvas Sync Logic (Unchanged and correct) ===
    plp_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details: return False
        
    current_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
    previous_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    
    added_ids = current_ids - previous_ids
    removed_ids = previous_ids - current_ids
    
    if not added_ids and not removed_ids:
        return True # No changes, nothing more to do.

    for class_item_id in added_ids:
        manage_class_enrollment("enroll", plp_item_id, class_item_id, student_details, user_id)
    for class_item_id in removed_ids:
        manage_class_enrollment("unenroll", plp_item_id, class_item_id, student_details, user_id)

    # === Part 2: Create Alert Subitem on HS Roster (Simplified) ===
    print("INFO: Canvas sync complete. Checking if alert subitem needs to be created on HS Roster.")

    if not PLP_TO_HS_ROSTER_CONNECT_COLUMN:
        return True # Alerting feature not configured, so we are done.

    hs_roster_linked_ids = monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, PLP_TO_HS_ROSTER_CONNECT_COLUMN)
    if not hs_roster_linked_ids:
        return True # Not an HS student, so we are done.

    hs_roster_parent_item_id = int(list(hs_roster_linked_ids)[0])
    
    try:
        CONNECT_COLUMN_TO_CATEGORY_MAP = {v: k for k, v in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items()}
        category_name = CONNECT_COLUMN_TO_CATEGORY_MAP.get(trigger_column_id)
        if not category_name:
            print(f"ERROR: No category found for column {trigger_column_id}.")
            return False
    except Exception as e:
        print(f"ERROR: Could not reverse category map: {e}")
        return False
        
    # Process any added courses
    for course_id in added_ids:
        course_name = monday.get_item_name(course_id) or "Unknown Course"
        subitem_name = f"⚠️ Added from PLP: {course_name}"
        
        # This dictionary is now simpler, with no status column.
        column_values = {
            HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID: category_name,
            HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID: {"item_ids": [int(course_id)]}
        }
        
        print(f"INFO: Creating 'Added from PLP' subitem for course '{course_name}' under parent {hs_roster_parent_item_id}")
        monday.create_subitem_with_columns(hs_roster_parent_item_id, subitem_name, column_values)

    # Process any removed courses
    for course_id in removed_ids:
        course_name = monday.get_item_name(course_id) or "Unknown Course"
        subitem_name = f"⚠️ Removed from PLP: {course_name}"
        
        # This dictionary is also simpler.
        column_values = {
            HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID: category_name
        }
        
        print(f"INFO: Creating 'Removed from PLP' subitem for course '{course_name}' under parent {hs_roster_parent_item_id}")
        monday.create_subitem_with_columns(hs_roster_parent_item_id, subitem_name, column_values)

    return True
    
@celery_app.task
def process_plp_course_sync_webhook(event_data):
    # This function is unchanged
    subitem_id = event_data.get('pulseId'); subitem_board_id = event_data.get('boardId'); parent_item_id = event_data.get('parentItemId'); current_value = event_data.get('value'); previous_value = event_data.get('previousValue'); user_id = event_data.get('userId')
    current_all_courses_ids = monday.get_linked_ids_from_connect_column_value(current_value); previous_all_courses_ids = monday.get_linked_ids_from_connect_column_value(previous_value)
    added_all_courses_ids = current_all_courses_ids - previous_all_courses_ids; removed_all_courses_ids = previous_all_courses_ids - current_all_courses_ids
    if not added_all_courses_ids and not removed_all_courses_ids: return True
    subitem_dropdown_data = monday.get_column_value(subitem_id, subitem_board_id, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID); subitem_dropdown_label = subitem_dropdown_data.get('text') if subitem_dropdown_data else None
    if not subitem_dropdown_label: return True
    target_plp_connect_column_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(subitem_dropdown_label)
    if not target_plp_connect_column_id: return True
    plp_link_data = monday.get_column_value(parent_item_id, HS_ROSTER_BOARD_ID, HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
    plp_linked_ids = monday.get_linked_ids_from_connect_column_value(plp_link_data.get('value')) if plp_link_data else set()
    if not plp_linked_ids: return True    
    plp_item_id = list(plp_linked_ids)[0]
    original_plp_column_data = monday.get_column_value(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id); original_plp_value = original_plp_column_data.get('value') if original_plp_column_data else {}
    operation_successful = True
    for course_id in added_all_courses_ids:
        if not monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id, course_id, "add"): operation_successful = False
    for course_id in removed_all_courses_ids:
        if not monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id, course_id, "remove"): operation_successful = False
    if not operation_successful: return False
    updated_plp_column_data = monday.get_column_value(plp_item_id, PLP_BOARD_ID, target_plp_connect_column_id); updated_plp_value = updated_plp_column_data.get('value') if updated_plp_column_data else {}    
    downstream_event = {'boardId': int(PLP_BOARD_ID), 'pulseId': plp_item_id, 'columnId': target_plp_connect_column_id, 'value': updated_plp_value, 'previousValue': original_plp_value, 'type': 'update_column_value', 'userId': user_id}
    process_canvas_delta_sync_from_course_change.delay(downstream_event, user_id)
    return True
    
@celery_app.task
def process_general_webhook(event_data, config_rule):
    # This function is unchanged
    webhook_board_id = event_data.get('boardId'); item_id_from_webhook = event_data.get('pulseId'); trigger_column_id_from_webhook = event_data.get('columnId'); event_user_id = event_data.get('userId'); current_column_value = event_data.get('value'); previous_column_value = event_data.get('previousValue'); webhook_type = event_data.get('type')
    log_type = config_rule.get("log_type"); params = config_rule.get("params", {}); configured_trigger_board_id = config_rule.get("trigger_board_id"); configured_trigger_col_id = config_rule.get("trigger_column_id")
    if configured_trigger_board_id and str(webhook_board_id) != str(configured_trigger_board_id): return False
    if log_type == "ConnectBoardChange" and webhook_type == "update_column_value" and trigger_column_id_from_webhook == configured_trigger_col_id:
        main_item_id = item_id_from_webhook; connected_board_id = params.get('linked_board_id'); subitem_name_prefix = params.get('subitem_name_prefix', ''); subitem_entry_type = params.get('subitem_entry_type'); entry_type_column_id = params.get('entry_type_column_id')
        current_linked_ids = monday.get_linked_ids_from_connect_column_value(current_column_value); previous_linked_ids = monday.get_linked_ids_from_connect_column_value(previous_column_value)
        added_links = current_linked_ids - previous_linked_ids; removed_links = previous_linked_ids - current_linked_ids
        if not added_links and not removed_links: return True
        overall_op_successful = True; current_date = datetime.now().strftime('%Y-%m-%d'); changer_user_name = monday.get_user_name(event_user_id) or "automation"; user_log_text = f" on {current_date} by {changer_user_name}"; subject_prefix_text = f"{subitem_name_prefix} " if subitem_name_prefix else ""; additional_subitem_columns = {entry_type_column_id: {"labels": [str(subitem_entry_type)]}} if entry_type_column_id else {}
        for item_id in added_links:
            linked_item_name = monday.get_item_name(item_id, connected_board_id)
            if linked_item_name:
                subitem_name = f"Added {subject_prefix_text}'{linked_item_name}'{user_log_text}"
                if not monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns): overall_op_successful = False
            else: overall_op_successful = False
        for item_id in removed_links:
            linked_item_name = monday.get_item_name(item_id, connected_board_id)
            if linked_item_name:
                subitem_name = f"Removed {subject_prefix_text}'{linked_item_name}'{user_log_text}"
                if not monday.create_subitem(main_item_id, subitem_name, additional_subitem_columns): overall_op_successful = False
            else: overall_op_successful = False
        return overall_op_successful
    return True

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    # This function is unchanged
    master_item_id = event_data.get('pulseId'); trigger_column_id = event_data.get('columnId'); event_user_id = event_data.get('userId'); current_value_raw = event_data.get('value'); previous_value_raw = event_data.get('previousValue') or {}
    operation_successful = True
    column_config = MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.get(trigger_column_id)
    if not column_config: return False
    column_friendly_name = column_config.get("name", "Staff")
    current_persons = current_value_raw.get('personsAndTeams', []) if current_value_raw else []; previous_persons = previous_value_raw.get('personsAndTeams', [])
    current_ids = {p['id'] for p in current_persons}; previous_ids = {p['id'] for p in previous_persons}
    added_ids = current_ids - previous_ids; removed_ids = previous_ids - current_ids
    changer_user_name = monday.get_user_name(event_user_id) or "automation"; current_date = datetime.now().strftime('%Y-%m-%d')
    for target_config in column_config.get("targets", []):
        target_board_id = target_config["board_id"]
        master_connect_column_id = target_config["connect_column_id"]
        target_people_column_id = target_config["target_column_id"]
        linked_target_item_ids = monday.get_linked_items_from_board_relation(item_id=master_item_id, board_id=MASTER_STUDENT_BOARD_ID, connect_column_id=master_connect_column_id)
        for linked_item_id in linked_target_item_ids:
            if str(target_board_id) == str(PLP_BOARD_ID):
                for person_id in added_ids:
                    person_name = monday.get_user_name(person_id) or "a new user"
                    subitem_name = f"{column_friendly_name} changed to {person_name} on {current_date} by {changer_user_name}"
                    monday.create_subitem(linked_item_id, subitem_name)
                for person_id in removed_ids:
                    person_name = monday.get_user_name(person_id) or "a previous user"
                    subitem_name = f"{column_friendly_name} assignment of {person_name} removed on {current_date} by {changer_user_name}"
                    monday.create_subitem(linked_item_id, subitem_name)
            success = monday.update_people_column(item_id=linked_item_id, board_id=target_board_id, people_column_id=target_people_column_id, new_people_value=current_value_raw, target_column_type="people")
            if not success: operation_successful = False
    return operation_successful

@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    # This function is unchanged
    source_item_id = event_data.get('pulseId'); trigger_column_id = event_data.get('columnId'); current_column_value_raw = event_data.get('value'); operation_successful = True
    column_sync_config = SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.get(trigger_column_id)
    if not column_sync_config: return False
    target_people_column_id = column_sync_config["target_column_id"]; target_column_type = column_sync_config["target_column_type"]
    linked_iep_ap_item_ids = monday.get_linked_items_from_board_relation(item_id=source_item_id, board_id=SPED_STUDENTS_BOARD_ID, connect_column_id=SPED_TO_IEPAP_CONNECT_COLUMN_ID)
    for linked_iep_ap_item_id in linked_iep_ap_item_ids:
        success = monday.update_people_column(item_id=linked_iep_ap_item_id, board_id=IEP_AP_BOARD_ID, people_column_id=target_people_column_id, new_people_value=current_column_value_raw, target_column_type=target_column_type)
        if not success: operation_successful = False
    return operation_successful

@celery_app.task
def process_teacher_enrollment_webhook(event_data):
    # This function is unchanged
    item_id = event_data.get('pulseId')
    current_value = event_data.get('value')
    previous_value = event_data.get('previousValue')
    current_ids = {p['id'] for p in current_value.get('personsAndTeams', [])} if current_value else set()
    previous_ids = {p['id'] for p in previous_value.get('personsAndTeams', [])} if previous_value else set()
    added_teacher_ids = current_ids - previous_ids
    removed_teacher_ids = previous_ids - current_ids
    if not added_teacher_ids and not removed_teacher_ids:
        print(f"INFO: No change in teachers for item {item_id}. Exiting.")
        return True
    canvas_course_id_val = monday.get_column_value(item_id, CANVAS_BOARD_ID, CANVAS_COURSE_ID_COLUMN)
    canvas_course_id = canvas_course_id_val.get('text') if canvas_course_id_val and canvas_course_id_val.get('text') else None
    if not canvas_course_id and added_teacher_ids:
        course_name_val = monday.get_column_value(item_id, CANVAS_BOARD_ID, CANVAS_BOARD_COURSE_NAME_COLUMN_ID)
        course_name = (course_name_val.get('text') if course_name_val and course_name_val.get('text')
                       else monday.get_item_name(item_id, CANVAS_BOARD_ID))
        if not course_name:
            monday.create_update(item_id, "ERROR: Cannot create Canvas course because the item name/title column is missing.")
            return False
        print(f"INFO: Canvas Course ID is missing. Creating new course '{course_name}' in Canvas.")
        new_course = canvas.create_canvas_course(course_name, CANVAS_TERM_ID)
        if new_course and hasattr(new_course, 'id'):
            canvas_course_id = str(new_course.id)
            monday.change_column_value_generic(board_id=CANVAS_BOARD_ID, item_id=item_id, column_id=CANVAS_COURSE_ID_COLUMN, value=canvas_course_id)
            monday.create_update(item_id, f"Successfully created new Canvas course '{course_name}' (ID: {canvas_course_id}).")
        else:
            monday.create_update(item_id, f"CRITICAL FAILURE: Could not create Canvas course for '{course_name}'.")
            return False
    if not canvas_course_id:
        monday.create_update(item_id, "ERROR: Cannot enroll/unenroll teacher because Canvas Course ID is still missing.")
        return False
    for teacher_id in added_teacher_ids:
        teacher_details = monday.get_user_details(teacher_id)
        if teacher_details:
            result = canvas.enroll_or_create_and_enroll_teacher(canvas_course_id, teacher_details)
            status = 'Success' if result else 'Failed'
            monday.create_update(item_id, f"Enroll Teacher '{teacher_details.get('name')}': {status}")
    for teacher_id in removed_teacher_ids:
        teacher_details = monday.get_user_details(teacher_id)
        if teacher_details:
            result = canvas.unenroll_teacher_from_course(canvas_course_id, teacher_details)
            status = 'Success' if result else 'Failed'
            monday.create_update(item_id, f"Unenroll Teacher '{teacher_details.get('name')}': {status}")
    return True
