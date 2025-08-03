#
# This is the Final, Correct code for monday_tasks.py
# It restores the subitem logic that was incorrectly removed.
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
    master_ids = monday.get_linked_items_from_board_relation(int(plp_item_id), int(PLP_BOARD_ID), PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_ids: return None
    master_id = list(master_ids)[0]
    details = {'name': monday.get_item_name(master_id, int(MASTER_STUDENT_BOARD_ID))}
    ssid_val = monday.get_column_value(master_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_SSID_COLUMN)
    email_val = monday.get_column_value(master_id, int(MASTER_STUDENT_BOARD_ID), MASTER_STUDENT_EMAIL_COLUMN)
    details['ssid'] = ssid_val.get('text') if ssid_val else None
    details['email'] = email_val.get('text') if email_val else None
    if not details.get('name') or not details.get('email'): return None
    canvas_user = canvas.get_or_create_canvas_user(details)
    if canvas_user:
        details['canvas_user_id'] = canvas_user.id
        return details
    return None

def manage_class_enrollment(action, plp_item_id, all_courses_item_id, student_details, user_id):
    canvas_api_id = monday.get_canvas_api_id_from_all_courses_item(all_courses_item_id)
    if not canvas_api_id and action == "enroll":
        course_name = monday.get_item_name(all_courses_item_id, int(ALL_COURSES_BOARD_ID))
        if not course_name: return False
        new_course = canvas.create_course(course_name)
        if not new_course: return False
        canvas_api_id = new_course['id']
        linked_canvas_ids = monday.get_linked_items_from_board_relation(all_courses_item_id, int(ALL_COURSES_BOARD_ID), ALL_COURSES_TO_CANVAS_CONNECT_COLUMN_ID)
        if linked_canvas_ids:
            canvas_item_id = list(linked_canvas_ids)[0]
            monday.change_column_value_generic(int(CANVAS_BOARD_ID), canvas_item_id, CANVAS_COURSE_ID_COLUMN_ID, str(canvas_api_id))
    
    if canvas_api_id:
        if action == "enroll":
            canvas.enroll_user(canvas_api_id, student_details['canvas_user_id'], "StudentEnrollment")
        elif action == "unenroll":
            canvas.unenroll_user(canvas_api_id, student_details['canvas_user_id'])
    
    course_name_log = monday.get_item_name(all_courses_item_id, int(ALL_COURSES_BOARD_ID)) or f"ID {all_courses_item_id}"
    changer_log = monday.get_user_name(user_id) or "Automation"
    log_message = f"Canvas Log: {action.capitalize()} for '{course_name_log}' by {changer_log}."
    monday.create_subitem(int(plp_item_id), log_message)
    return True

@celery_app.task
def process_canvas_delta_sync_from_course_change(event_data, user_id):
    plp_item_id, col_id = event_data.get('pulseId'), event_data.get('columnId')
    details = get_student_details_from_plp(plp_item_id)
    if not details: return False
    current_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
    previous_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    added, removed = current_ids - previous_ids, previous_ids - current_ids
    if not added and not removed: return True

    for course_id in added: manage_class_enrollment("enroll", plp_item_id, course_id, details, user_id)
    for course_id in removed: manage_class_enrollment("unenroll", plp_item_id, course_id, details, user_id)

    # THIS IS THE HS ROSTER SUBITEM LOGIC THAT WAS MISSING
    hs_roster_linked_ids = monday.get_linked_items_from_board_relation(int(plp_item_id), int(PLP_BOARD_ID), PLP_TO_HS_ROSTER_CONNECT_COLUMN)
    if not hs_roster_linked_ids: return True
    hs_roster_parent_item_id = int(list(hs_roster_linked_ids)[0])
    CONNECT_COLUMN_TO_CATEGORY_MAP = {v: k for k, v in PLP_CATEGORY_TO_CONNECT_COLUMN_MAP.items()}
    category_name = CONNECT_COLUMN_TO_CATEGORY_MAP.get(col_id)
    if not category_name: return False
    changer_user_name = monday.get_user_name(user_id) or "an Automation"
    
    for course_id in added:
        course_name = monday.get_item_name(int(course_id), int(ALL_COURSES_BOARD_ID)) or f"Course ID {course_id}"
        subitem_name = f"⚠️ Added from PLP: {course_name}"
        column_values = {HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID: category_name}
        monday.create_subitem(hs_roster_parent_item_id, subitem_name, column_values)

    if HS_ROSTER_SUBITEM_INTEGRITY_STATUS_COLUMN_ID:
        for course_id in removed:
            target_subitem_id = monday.find_subitem_by_category_and_linked_course(hs_roster_parent_item_id, HS_ROSTER_SUBITEM_DROPDOWN_COLUMN_ID, category_name, HS_ROSTER_CONNECT_ALL_COURSES_COLUMN_ID, int(course_id))
            if target_subitem_id:
                course_name = monday.get_item_name(int(course_id), int(ALL_COURSES_BOARD_ID)) or f"Course ID {course_id}"
                update_text = f"**PROCESS ALERT:**\nThis roster item may be out of sync. \"{course_name}\" was **removed** from the PLP by {changer_user_name}."
                monday.change_column_value_generic(int(HS_ROSTER_BOARD_ID), target_subitem_id, HS_ROSTER_SUBITEM_INTEGRITY_STATUS_COLUMN_ID, HS_ROSTER_SUBITEM_MISMATCH_STATUS_VALUE)
                monday.create_update(target_subitem_id, update_text)
    return True

@celery_app.task
def process_canvas_full_sync_from_status(event_data):
    plp_item_id, user_id = event_data.get('pulseId'), event_data.get('userId')
    if event_data.get('value', {}).get('label', {}).get('text') != PLP_CANVAS_SYNC_STATUS_VALUE: return True
    details = get_student_details_from_plp(plp_item_id)
    if not details: return False
    col_ids = [c.strip() for c in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',') if c.strip()]
    all_class_ids = set()
    for col_id in col_ids:
        col_val = monday.get_column_value(plp_item_id, int(PLP_BOARD_ID), col_id)
        all_class_ids.update(monday.get_linked_ids_from_connect_column_value(col_val.get('value')))
    for class_id in all_class_ids:
        manage_class_enrollment("enroll", plp_item_id, class_id, details, user_id)
    return True

# ... OTHER TASKS ARE UNCHANGED FROM THE LAST STABLE VERSION ...
@celery_app.task
def process_plp_course_sync_webhook(event_data):
    sub_id, sb_id, p_id, u_id = event_data.get('pulseId'), event_data.get('boardId'), event_data.get('parentItemId'), event_data.get('userId')
    curr_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
    prev_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue'))
    added, removed = curr_ids - prev_ids, prev_ids - curr_ids
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
def process_teacher_enrollment_webhook(event_data):
    item_id = event_data.get('pulseId')
    current_ids = {p['id'] for p in event_data.get('value', {}).get('personsAndTeams', [])}
    previous_ids = {p['id'] for p in (event_data.get('previousValue') or {}).get('personsAndTeams', [])}
    added_ids, removed_ids = current_ids - previous_ids, previous_ids - current_ids
    if not added_ids and not removed_ids: return True
    
    canvas_course_id = (monday.get_column_value(item_id, int(CANVAS_BOARD_ID), CANVAS_COURSE_ID_COLUMN_ID) or {}).get('text')
    if not canvas_course_id and added_ids:
        course_name = monday.get_item_name(item_id, int(CANVAS_BOARD_ID))
        if not course_name: return False
        new_course = canvas.create_templated_course(course_name, CANVAS_TERM_ID)
        if new_course and hasattr(new_course, 'id'):
            canvas_course_id = str(new_course.id)
            monday.change_column_value_generic(int(CANVAS_BOARD_ID), item_id, CANVAS_COURSE_ID_COLUMN_ID, canvas_course_id)
        else: return False
    
    if not canvas_course_id: return False
    for t_id in added_ids:
        details = monday.get_user_details(t_id)
        if details: canvas.enroll_teacher(canvas_course_id, details)
    for t_id in removed_ids:
        details = monday.get_user_details(t_id)
        if details: canvas.unenroll_teacher(canvas_course_id, details)
    return True
