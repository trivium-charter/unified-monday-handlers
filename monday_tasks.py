#
# This is the complete and correct code for monday_tasks.py
#
import os
import json
from datetime import datetime
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Environment Variable Loading ---
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
    PLP_CATEGORY_TO_CONNECT_COLUMN_MAP, MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS, SPED_STUDENTS_PEOPLE_COLUMN_MAPPING = {}, {}, {}

def get_student_details_from_plp(plp_item_id):
    master_ids = monday.get_linked_items_from_board_relation(plp_item_id, int(PLP_BOARD_ID), PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_ids: return None
    master_id = list(master_ids)[0]
    details = {'name': monday.get_item_name(master_id, int(MASTER_STUDENT_BOARD_ID))}
    ssid_val = monday.get_column_value(master_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_SSID_COLUMN)
    email_val = monday.get_column_value(master_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_EMAIL_COLUMN)
    details['ssid'] = ssid_val.get('text') if ssid_val else None
    details['email'] = email_val.get('text') if email_val else None
    if not all(details.values()): return None
    canvas_user = canvas.get_or_create_canvas_user(details)
    if canvas_user:
        details['canvas_user_id'] = canvas_user.id
        return details
    return None

def manage_class_enrollment(action, plp_item_id, all_courses_item_id, student_details, user_id):
    canvas_api_id = monday.get_canvas_api_id_from_all_courses_item(all_courses_item_id)
    if not canvas_api_id:
        if action == "unenroll": return True
        course_name = monday.get_item_name(all_courses_item_id, int(ALL_COURSES_BOARD_ID))
        if not course_name: return False
        new_course = canvas.create_course(course_name)
        if not new_course: return False
        canvas_api_id = new_course['id']
        linked_canvas_ids = monday.get_linked_items_from_board_relation(all_courses_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
        if linked_canvas_ids:
            canvas_item_id = list(linked_canvas_ids)[0]
            monday.change_column_value_generic(int(CANVAS_BOARD_ID), canvas_item_id, CANVAS_COURSE_ID_COLUMN_ID, str(canvas_api_id))
    
    if action == "enroll":
        canvas.enroll_user(canvas_api_id, student_details['canvas_user_id'], "StudentEnrollment")
    elif action == "unenroll":
        canvas.unenroll_user(canvas_api_id, student_details['canvas_user_id'])
    
    course_name_log = monday.get_item_name(all_courses_item_id, int(ALL_COURSES_BOARD_ID)) or f"ID {all_courses_item_id}"
    changer_log = monday.get_user_name(user_id) or "Automation"
    log_message = f"Canvas: '{action.capitalize()}' for course '{course_name_log}' triggered by {changer_log}."
    monday.create_subitem(plp_item_id, log_message)
    return True

@celery_app.task
def process_canvas_delta_sync_from_course_change(event_data, user_id):
    plp_item_id, col_id = event_data.get('pulseId'), event_data.get('columnId')
    details = get_student_details_from_plp(plp_item_id)
    if not details: return False
    current_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
    previous_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    added, removed = current_ids - previous_ids, previous_ids - current_ids
    for course_id in added: manage_class_enrollment("enroll", plp_item_id, course_id, details, user_id)
    for course_id in removed: manage_class_enrollment("unenroll", plp_item_id, course_id, details, user_id)
    return True

# ... Other tasks remain unchanged ...
@celery_app.task
def process_canvas_full_sync_from_status(event_data):
    plp_item_id, user_id = event_data.get('pulseId'), event_data.get('userId')
    if event_data.get('value', {}).get('label', {}).get('text') != PLP_CANVAS_SYNC_STATUS_VALUE: return True
    details = get_student_details_from_plp(plp_item_id)
    if not details: return False
    col_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
    all_class_ids = set().union(*(monday.get_linked_ids_from_connect_column_value(monday.get_column_value(plp_item_id, int(PLP_BOARD_ID), c_id).get('value')) for c_id in col_ids))
    for class_id in all_class_ids: manage_class_enrollment("enroll", plp_item_id, class_id, details, user_id)
    return True

@celery_app.task
def process_plp_course_sync_webhook(event_data):
    sub_id, sb_id, p_id, u_id = event_data.get('pulseId'), event_data.get('boardId'), event_data.get('parentItemId'), event_data.get('userId')
    added, removed = monday.get_linked_ids_from_connect_column_value(event_data.get('value')) - monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue')), monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue')) - monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
    if not added and not removed: return True
    dd_label = monday.get_column_value(sub_id, sb_id, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID).get('text')
    if not dd_label: return True
    target_col_id = PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.get(dd_label)
    if not target_col_id: return True
    plp_ids = monday.get_linked_items_from_board_relation(p_id, int(HS_ROSTER_BOARD_ID), HS_ROSTER_MAIN_ITEM_to_PLP_CONNECT_COLUMN_ID)
    if not plp_ids: return True    
    plp_id = list(plp_ids)[0]
    orig_val = monday.get_column_value(plp_id, int(PLP_BOARD_ID), target_col_id).get('value')
    for c_id in added: monday.update_connect_board_column(plp_id, int(PLP_BOARD_ID), target_col_id, c_id, "add")
    for c_id in removed: monday.update_connect_board_column(plp_id, int(PLP_BOARD_ID), target_col_id, c_id, "remove")
    updated_val = monday.get_column_value(plp_id, int(PLP_BOARD_ID), target_col_id).get('value')
    process_canvas_delta_sync_from_course_change.delay({'pulseId': plp_id, 'columnId': target_col_id, 'value': updated_val, 'previousValue': orig_val}, u_id)
    return True

@celery_app.task
def process_general_webhook(event_data, config_rule):
    log_type, params = config_rule.get("log_type"), config_rule.get("params", {})
    if str(event_data.get('boardId')) != str(config_rule.get("trigger_board_id")): return False
    if log_type == "ConnectBoardChange" and event_data.get('type') == "update_column_value" and event_data.get('columnId') == config_rule.get("trigger_column_id"):
        added, removed = monday.get_linked_ids_from_connect_column_value(event_data.get('value')) - monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue')), monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue')) - monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
        if not added and not removed: return True
        changer = monday.get_user_name(event_data.get('userId')) or "automation"; log_text=f" on {datetime.now().strftime('%Y-%m-%d')} by {changer}"; subject=f"{params.get('subitem_name_prefix', '')} "
        subitem_cols = {params.get('entry_type_column_id'): {"labels": [str(params.get('subitem_entry_type'))]}} if params.get('entry_type_column_id') else {}
        for item_id in added: monday.create_subitem(event_data.get('pulseId'), f"Added {subject}'{monday.get_item_name(item_id, params.get('linked_board_id'))}'{log_text}", subitem_cols)
        for item_id in removed: monday.create_subitem(event_data.get('pulseId'), f"Removed {subject}'{monday.get_item_name(item_id, params.get('linked_board_id'))}'{log_text}", subitem_cols)
    return True

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    col_cfg = MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS.get(event_data.get('columnId'))
    if not col_cfg: return False
    for t_cfg in col_cfg.get("targets", []):
        for linked_id in monday.get_linked_items_from_board_relation(event_data.get('pulseId'), int(MASTER_STUDENT_BOARD_ID), t_cfg["connect_column_id"]):
            monday.update_people_column(linked_id, t_cfg["board_id"], t_cfg["target_column_id"], event_data.get('value'), t_cfg["target_column_type"])
    return True

@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    col_cfg = SPED_STUDENTS_PEOPLE_COLUMN_MAPPING.get(event_data.get('columnId'))
    if not col_cfg: return False
    for linked_id in monday.get_linked_items_from_board_relation(event_data.get('pulseId'), int(SPED_STUDENTS_BOARD_ID), SPED_TO_IEPAP_CONNECT_COLUMN_ID):
        monday.update_people_column(linked_id, int(IEP_AP_BOARD_ID), col_cfg["target_column_id"], event_data.get('value'), col_cfg["target_column_type"])
    return True

@celery_app.task
def process_teacher_enrollment_webhook(event_data):
    item_id, curr_v, prev_v = event_data.get('pulseId'), event_data.get('value'), event_data.get('previousValue')
    added_ids, removed_ids = {p['id'] for p in curr_v.get('personsAndTeams', [])} - {p['id'] for p in (prev_v or {}).get('personsAndTeams', [])}, {p['id'] for p in (prev_v or {}).get('personsAndTeams', [])} - {p['id'] for p in curr_v.get('personsAndTeams', [])}
    if not added_ids and not removed_ids: return True
    canvas_course_id = monday.get_column_value(item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID).get('text')
    if not canvas_course_id and added_ids:
        course_name = monday.get_item_name(item_id, int(CANVAS_BOARD_ID))
        if not course_name: return False
        new_course = canvas.create_templated_course(course_name, CANVAS_TERM_ID)
        if new_course: canvas_course_id = str(new_course.id); monday.change_column_value_generic(int(CANVAS_BOARD_ID), item_id, CANVAS_COURSE_ID_COLUMN_ID, canvas_course_id)
        else: return False
    if not canvas_course_id: return False
    for t_id in added_ids: canvas.enroll_teacher(canvas_course_id, monday.get_user_details(t_id))
    for t_id in removed_ids: canvas.unenroll_teacher(canvas_course_id, monday.get_user_details(t_id))
    return True
