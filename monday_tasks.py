import os
import json
from datetime import datetime
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Environment Variable Loading (Standardized Names) ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
ALL_COURSES_BOARD_ID = os.environ.get("ALL_COURSES_BOARD_ID")
CANVAS_BOARD_ID = os.environ.get("CANVAS_BOARD_ID")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
HS_ROSTER_BOARD_ID = os.environ.get("HS_ROSTER_BOARD_ID")
IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID")
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID")
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")

PLP_CANVAS_SYNC_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_COLUMN_ID")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
PLP_TO_HS_ROSTER_CONNECT_COLUMN = os.environ.get("PLP_TO_HS_ROSTER_CONNECT_COLUMN")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID = os.environ.get("ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID")
CANVAS_COURSE_ID_COLUMN_ID = os.environ.get("CANVAS_COURSE_ID_COLUMN_ID")
CANVAS_BOARD_COURSE_NAME_COLUMN_ID = os.environ.get("CANVAS_BOARD_COURSE_NAME_COLUMN_ID")
CANVAS_COURSES_TEACHER_COLUMN_ID = os.environ.get("CANVAS_COURSES_TEACHER_COLUMN_ID")
HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID = os.environ.get("HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID")
HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID")
HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID = os.environ.get("HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID")
HS_ROSTER_SUBITEM_INTEGRITY_STATUS_COLUMN_ID = os.environ.get("HS_ROSTER_SUBITEM_INTEGRITY_STATUS_COLUMN_ID")
SPED_TO_IEPAP_CONNECT_COLUMN_ID = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN_ID")

HS_ROSTER_SUBITEM_MISMATCH_STATUS_VALUE = os.environ.get("HS_ROSTER_SUBITEM_MISMATCH_STATUS_VALUE", "⚠️ PLP Mismatch")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE", "Done")

try:
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = json.loads(os.environ.get("PLP_CATEGORY_TO_CONNECT_COLUMN_MAP", "{}"))
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", "{}"))
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = json.loads(os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN_MAPPING", "{}"))
except (json.JSONDecodeError, TypeError):
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP = {}
    MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS = {}
    SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}

# --- Helper Functions ---

def get_canvas_api_id_from_all_courses_item(all_courses_item_id):
    """Performs a 'double-hop' lookup to get the numeric Canvas API ID."""
    if not all([ALL_COURSES_BOARD_ID, CANVAS_BOARD_ID, ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID, CANVAS_COURSE_ID_COLUMN_ID]):
        print("ERROR: Missing config variables for get_canvas_api_id.")
        return None
    linked_canvas_ids = monday.get_linked_items_from_board_relation(
        all_courses_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID
    )
    if not linked_canvas_ids: return None
    canvas_item_id = int(list(linked_canvas_ids)[0])
    api_id_value = monday.get_column_value(canvas_item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    return api_id_value.get('text') if api_id_value and api_id_value.get('text') else None

def get_student_details_from_plp(plp_item_id):
    """Gets base student details from Monday.com and finds/creates the Canvas user."""
    master_student_ids = monday.get_linked_items_from_board_relation(int(plp_item_id), int(PLP_BOARD_ID), PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_student_ids: return None
    master_student_id = list(master_student_ids)[0]
    student_name = monday.get_item_name(master_student_id, int(MASTER_STUDENT_BOARD_ID))
    ssid_val = monday.get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_SSID_COLUMN)
    email_val = monday.get_column_value(master_student_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_EMAIL_COLUMN)
    ssid = ssid_val.get('text') if ssid_val else None
    email = email_val.get('text') if email_val else None
    if not all([student_name, email]):
        print(f"ERROR: Missing Name or Email for master student item {master_student_id}")
        return None
    base_details = {'name': student_name, 'ssid': ssid, 'email': email}
    canvas_user = canvas.get_or_create_canvas_user(base_details)
    if canvas_user and hasattr(canvas_user, 'id'):
        base_details['canvas_user_id'] = canvas_user.id
        return base_details
    print(f"FATAL: Could not get or create Canvas user for: {base_details}.")
    return None

def manage_class_enrollment(action, plp_item_id, all_courses_item_id, student_details, user_id):
    """Manages a student's enrollment, creating the Canvas course if it doesn't exist."""
    canvas_api_id = get_canvas_api_id_from_all_courses_item(all_courses_item_id)
    if not canvas_api_id:
        print(f"INFO: No Canvas ID for All Courses item {all_courses_item_id}. Creating...")
        course_name = monday.get_item_name(int(all_courses_item_id), int(ALL_COURSES_BOARD_ID))
        if not course_name: return False
        new_canvas_course = canvas.create_course(course_name)
        if not new_canvas_course or 'id' not in new_canvas_course:
            print(f"ERROR: Failed to create course '{course_name}' in Canvas.")
            return False
        canvas_api_id = new_canvas_course['id']
        linked_canvas_ids = monday.get_linked_items_from_board_relation(all_courses_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
        if linked_canvas_ids:
            canvas_item_id = int(list(linked_canvas_ids)[0])
            monday.change_column_value_generic(int(CANVAS_BOARD_ID), canvas_item_id, CANVAS_COURSE_ID_COLUMN_ID, str(canvas_api_id))
    if not canvas_api_id:
        print(f"FATAL: Could not get or create a Canvas API ID for item {all_courses_item_id}.")
        return False
    if action == "enroll":
        canvas.enroll_user_in_course(canvas_api_id, student_details['canvas_user_id'], "StudentEnrollment")
    elif action == "unenroll":
        canvas.unenroll_user_from_course(canvas_api_id, student_details['canvas_user_id'])
    course_name_for_log = monday.get_item_name(int(all_courses_item_id), int(ALL_COURSES_BOARD_ID)) or f"ID {all_courses_item_id}"
    changer_name = monday.get_user_name(user_id) or "Automation"
    log_message = f"User {changer_name} triggered '{action}' for course '{course_name_for_log}' on {datetime.now().strftime('%Y-%m-%d')}"
    monday.create_subitem(int(plp_item_id), log_message)
    return True

# --- Celery Tasks ---

@celery_app.task
def process_canvas_delta_sync_from_course_change(event_data, user_id):
    """Handles adding/removing a course from a student's PLP, triggering Canvas and HS Roster updates."""
    plp_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details or not student_details.get('canvas_user_id'): return False
    current_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
    previous_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    added_ids, removed_ids = current_ids - previous_ids, previous_ids - current_ids
    if not added_ids and not removed_ids: return True
    for course_id in added_ids: manage_class_enrollment("enroll", plp_item_id, int(course_id), student_details, user_id)
    for course_id in removed_ids: manage_class_enrollment("unenroll", plp_item_id, int(course_id), student_details, user_id)
    hs_roster_linked_ids = monday.get_linked_items_from_board_relation(int(plp_item_id), int(PLP_BOARD_ID), PLP_TO_HS_ROSTER_CONNECT_COLUMN)
    if not hs_roster_linked_ids: return True
    hs_roster_parent_item_id = int(list(hs_roster_linked_ids)[0])
    CONNECT_COLUMN_TO_CATEGORY_MAP = {v: k for k, v in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items()}
    category_name = CONNECT_COLUMN_TO_CATEGORY_MAP.get(trigger_column_id)
    if not category_name: return False
    changer_user_name = monday.get_user_name(user_id) or "an Automation"
    for course_id in added_ids:
        course_name = monday.get_item_name(int(course_id), int(ALL_COURSES_BOARD_ID)) or f"Course ID {course_id}"
        subitem_name = f"⚠️ Added from PLP: {course_name}"
        column_values = {HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID: category_name}
        monday.create_subitem(hs_roster_parent_item_id, subitem_name, column_values)
    if HS_ROSTER_SUBITEM_INTEGRITY_STATUS_COLUMN_ID:
        for course_id in removed_ids:
            target_subitem_id = monday.find_subitem_by_category_and_linked_course(
                hs_roster_parent_item_id, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID, category_name, HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID, int(course_id)
            )
            if target_subitem_id:
                course_name = monday.get_item_name(int(course_id), int(ALL_COURSES_BOARD_ID)) or f"Course ID {course_id}"
                update_text = f"**PROCESS ALERT:**\nThis roster item may be out of sync. \"{course_name}\" was **removed** from the PLP by {changer_user_name}."
                # Board ID for subitems is the parent's board ID for this mutation. Correctly pass integers.
                monday.change_column_value_generic(int(HS_ROSTER_BOARD_ID), target_subitem_id, HS_ROSTER_SUBITEM_INTEGRITY_STATUS_COLUMN_ID, HS_ROSTER_SUBITEM_MISMATCH_STATUS_VALUE)
                monday.create_update(target_subitem_id, update_text)
    return True

@celery_app.task
def process_canvas_full_sync_from_status(event_data):
    """Handles the 'Full Sync' status change on the PLP."""
    plp_item_id = event_data.get('pulseId')
    user_id = event_data.get('userId')
    status_label = event_data.get('value', {}).get('label', {}).get('text', '')
    if status_label != PLP_CANVAS_SYNC_STATUS_VALUE: return True
    student_details = get_student_details_from_plp(plp_item_id)
    if not student_details or not student_details.get('canvas_user_id'): return False
    course_column_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
    all_class_ids = set()
    for col_id in course_column_ids:
        class_link_data = monday.get_column_value(plp_item_id, int(PLP_BOARD_ID), col_id)
        if class_link_data and class_link_data.get('value'):
            all_class_ids.update(monday.get_linked_ids_from_connect_column_value(class_link_data.get('value')))
    for class_item_id in all_class_ids:
        manage_class_enrollment("enroll", plp_item_id, int(class_item_id), student_details, user_id)
    return True

@celery_app.task
def process_plp_course_sync_webhook(event_data):
    """Handles course changes on HS Roster subitems and syncs them to the student's PLP."""
    subitem_id, sb_id, p_id = event_data.get('pulseId'), event_data.get('boardId'), event_data.get('parentItemId')
    curr_v, prev_v, u_id = event_data.get('value'), event_data.get('previousValue'), event_data.get('userId')
    curr_ids = monday.get_linked_ids_from_connect_column_value(curr_v)
    prev_ids = monday.get_linked_ids_from_connect_column_value(prev_v)
    added_ids, removed_ids = curr_ids - prev_ids, prev_ids - prev_ids
    if not added_ids and not removed_ids: return True
    dd_data = monday.get_column_value(subitem_id, sb_id, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID)
    dd_label = dd_data.get('text') if dd_data else None
    if not dd_label: return True
    target_plp_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(dd_label)
    if not target_plp_col_id: return True
    plp_link_data = monday.get_column_value(p_id, int(HS_ROSTER_BOARD_ID), HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
    plp_ids = monday.get_linked_ids_from_connect_column_value(plp_link_data.get('value')) if plp_link_data else set()
    if not plp_ids: return True    
    plp_item_id = list(plp_ids)[0]
    orig_val = monday.get_column_value(plp_item_id, int(PLP_BOARD_ID), target_plp_col_id)
    for c_id in added_ids: monday.update_connect_board_column(plp_item_id, int(PLP_BOARD_ID), target_plp_col_id, c_id, "add")
    for c_id in removed_ids: monday.update_connect_board_column(plp_item_id, int(PLP_BOARD_ID), target_plp_col_id, c_id, "remove")
    updated_val = monday.get_column_value(plp_item_id, int(PLP_BOARD_ID), target_plp_col_id)
    downstream_event = {'boardId': int(PLP_BOARD_ID), 'pulseId': plp_item_id, 'columnId': target_plp_col_id, 'value': updated_val, 'previousValue': orig_val, 'userId': u_id}
    process_canvas_delta_sync_from_course_change.delay(downstream_event, u_id)
    return True

@celery_app.task
def process_general_webhook(event_data, config_rule):
    """A generic task to create subitem logs based on a connect_boards column change."""
    # This code is restored from your complete file and appears correct.
    webhook_board_id, item_id_from_webhook = event_data.get('boardId'), event_data.get('pulseId')
    trigger_column_id_from_webhook, event_user_id = event_data.get('columnId'), event_data.get('userId')
    current_column_value, previous_column_value = event_data.get('value'), event_data.get('previousValue')
    webhook_type = event_data.get('type')
    log_type, params = config_rule.get("log_type"), config_rule.get("params", {})
    cfg_board_id, cfg_col_id = config_rule.get("trigger_board_id"), config_rule.get("trigger_column_id")
    if cfg_board_id and str(webhook_board_id) != str(cfg_board_id): return False
    if log_type == "ConnectBoardChange" and webhook_type == "update_column_value" and trigger_column_id_from_webhook == cfg_col_id:
        main_id, conn_id = item_id_from_webhook, params.get('linked_board_id')
        name_prefix, entry_type = params.get('subitem_name_prefix', ''), params.get('subitem_entry_type')
        entry_type_col_id = params.get('entry_type_column_id')
        curr_links = monday.get_linked_ids_from_connect_column_value(current_column_value)
        prev_links = monday.get_linked_ids_from_connect_column_value(previous_column_value)
        added, removed = curr_links - prev_links, prev_links - curr_links
        if not added and not removed: return True
        current_date = datetime.now().strftime('%Y-%m-%d')
        changer = monday.get_user_name(event_user_id) or "automation"
        log_text = f" on {current_date} by {changer}"
        subject = f"{name_prefix} " if name_prefix else ""
        subitem_cols = {entry_type_col_id: {"labels": [str(entry_type)]}} if entry_type_col_id else {}
        for item_id in added:
            name = monday.get_item_name(item_id, conn_id)
            if name: monday.create_subitem(main_id, f"Added {subject}'{name}'{log_text}", subitem_cols)
        for item_id in removed:
            name = monday.get_item_name(item_id, conn_id)
            if name: monday.create_subitem(main_id, f"Removed {subject}'{name}'{log_text}", subitem_cols)
    return True

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    """Syncs people column changes from Master Student to other boards."""
    # This code is restored from your complete file and appears correct.
    master_item_id, trigger_column_id = event_data.get('pulseId'), event_data.get('columnId')
    event_user_id, current_value_raw = event_data.get('userId'), event_data.get('value')
    column_config = MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.get(trigger_column_id)
    if not column_config: return False
    for target_config in column_config.get("targets", []):
        linked_ids = monday.get_linked_items_from_board_relation(
            item_id=master_item_id, board_id=int(MASTER_STUDENT_BOARD_ID), connect_column_id=target_config["connect_column_id"]
        )
        for linked_item_id in linked_ids:
            monday.update_people_column(
                item_id=linked_item_id, board_id=target_config["board_id"], 
                people_column_id=target_config["target_column_id"], new_people_value=current_value_raw, 
                target_column_type="people" # Assuming single person columns for now
            )
    return True

@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    """Syncs people column changes from SPED Students to IEP/AP."""
    # This code is restored from your complete file and appears correct.
    source_item_id, trigger_column_id = event_data.get('pulseId'), event_data.get('columnId')
    current_value_raw = event_data.get('value')
    column_sync_config = SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.get(trigger_column_id)
    if not column_sync_config: return False
    target_people_col_id, target_col_type = column_sync_config["target_column_id"], column_sync_config["target_column_type"]
    linked_ids = monday.get_linked_items_from_board_relation(
        item_id=source_item_id, board_id=int(SPED_STUDENTS_BOARD_ID), connect_column_id=SPED_TO_IEPAP_CONNECT_COLUMN_ID
    )
    for linked_id in linked_ids:
        monday.update_people_column(
            item_id=linked_id, board_id=int(IEP_AP_BOARD_ID), people_column_id=target_people_col_id,
            new_people_value=current_value_raw, target_column_type=target_col_type
        )
    return True

@celery_app.task
def process_teacher_enrollment_webhook(event_data):
    """Handles adding/removing a teacher from the Canvas Courses board."""
    item_id = event_data.get('pulseId')
    current_value, previous_value = event_data.get('value'), event_data.get('previousValue')
    current_ids = {p['id'] for p in current_value.get('personsAndTeams', [])} if current_value else set()
    previous_ids = {p['id'] for p in previous_value.get('personsAndTeams', [])} if previous_value else set()
    added_teacher_ids, removed_teacher_ids = current_ids - previous_ids, previous_ids - current_ids
    if not added_teacher_ids and not removed_teacher_ids: return True
    canvas_course_id_val = monday.get_column_value(item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID)
    canvas_course_id = canvas_course_id_val.get('text') if canvas_course_id_val else None
    if not canvas_course_id and added_teacher_ids:
        course_name = monday.get_item_name(item_id, int(CANVAS_BOARD_ID))
        if not course_name:
            monday.create_update(item_id, "ERROR: Cannot create Canvas course. Item name is missing.")
            return False
        new_course = canvas.create_templated_course(course_name, CANVAS_TERM_ID)
        if new_course and hasattr(new_course, 'id'):
            canvas_course_id = str(new_course.id)
            monday.change_column_value_generic(int(CANVAS_BOARD_ID), item_id, CANVAS_COURSE_ID_COLUMN_ID, canvas_course_id)
            monday.create_update(item_id, f"Created new Canvas course '{course_name}' (ID: {canvas_course_id}).")
        else:
            monday.create_update(item_id, f"CRITICAL FAILURE: Could not create course for '{course_name}'.")
            return False
    if not canvas_course_id:
        monday.create_update(item_id, "ERROR: Teacher actions failed because Canvas Course ID is missing.")
        return False
    for teacher_id in added_teacher_ids:
        teacher_details = monday.get_user_details(teacher_id)
        if teacher_details:
            result = canvas.enroll_teacher(canvas_course_id, teacher_details)
            status = 'Success' if result else 'Failed'
            monday.create_update(item_id, f"Enroll Teacher '{teacher_details.get('name')}': {status}")
    for teacher_id in removed_teacher_ids:
        teacher_details = monday.get_user_details(teacher_id)
        if teacher_details:
            result = canvas.unenroll_teacher(canvas_course_id, teacher_details)
            status = 'Success' if result else 'Failed'
            monday.create_update(item_id, f"Unenroll Teacher '{teacher_details.get('name')}': {status}")
    return True
